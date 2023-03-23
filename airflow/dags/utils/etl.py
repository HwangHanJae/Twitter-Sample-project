EXTRACT = """
#!/bin/bash

START="{{ds}}"
END="{{tomorrow_ds}}"

cat > config.yml << EOF
in :
  type: mongodb
  uri: mongodb://192.168.56.101:27017/twitter
  collection: sample
  query: '{_timestamp: {\$gte: "${START}", \$lt: "${END}"}}'
  projection: '{_timestamp : true, "data.lang" : true, "data.text" : true}'
out :
  type: file
  path_prefix: /home/bigdata/project/data/twitter_sample/twitter_sample_${START}/
  file_ext: json.gz
  formatter:
    type: jsonl
  encoders:
  - type: gzip
EOF

rm -rf /home/bigdata/project/data/twitter_sample/twitter_sample_${START}
mkdir /home/bigdata/project/data/twitter_sample/twitter_sample_${START}

embulk run config.yml

hdfs dfs -put -f /home/bigdata/project/data/twitter_sample/twitter_sample_${START}  /user/bigdata/json_data/ 
"""

LOAD = """
#!/bin/bash

START="{{ds}}"

cat > load.hql << EOF

ADD JAR /usr/local/hive/hcatalog/share/hcatalog/hive-hcatalog-core-3.1.3.jar;

! echo "> ADD JAR /usr/local/hive/hcatalog/share/hcatalog/hive-hcatalog-core-3.1.3.jar";

CREATE TEMPORARY EXTERNAL TABLE twitter_sample(
  record struct<\`_timestamp\` : string, \`data\` : struct<\`lang\` : string, \`text\` : string> >
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE LOCATION '/user/bigdata/json_data/twitter_sample_${START}/';

! echo "> CREATE TEMPORARY EXTERNAL TABLE twitter_sample";

SELECT * FROM twitter_sample LIMIT 5;

CREATE TABLE IF NOT EXISTS twitter_sample_words(
  \`time\` timestamp, \`word\` string
)
PARTITIONED BY(pt string) STORED AS ORC;

! echo "> CREATE TABLE twitter_sample_words";

INSERT OVERWRITE TABLE twitter_sample_words PARTITION (pt='${START}')
SELECT from_unixtime(unix_timestamp(record.\`_timestamp\`, "yyyy-MM-dd'T'hh:mm:ss")) as \`time\`, \`word\`
FROM twitter_sample
LATERAL VIEW explode(split(record.\`data\`.\`text\`, '\\s+')) words as \`word\`
WHERE record.\`data\`.\`lang\` = 'en'
ORDER BY \`time\`;

SELECT * FROM twitter_sample_words LIMIT 5;

EOF

hive -f load.hql -d START=${START}
"""

AGGREGATE = """
#!/bin/bash

START="{{macros.ds_add(ds, -3)}}"
END="{{tomorrow_ds}}"

cat > query.sql << EOF

WITH word_category as
(SELECT word,
if(count > 1000, word, concat('COUNT=', cast(count AS varchar))) category
FROM(
SELECT word, count(*) count
FROM twitter_sample_words
WHERE time BETWEEN DATE('$START') AND DATE('$END') GROUP BY word )
 ),
range_twitter_sample_words as (
select date_trunc('hour', time) time, word
from twitter_sample_words
where time BETWEEN DATE('$START') AND DATE('$END') 
)

select a.time time, b.category category, count(*) count
from range_twitter_sample_words a 
left join word_category b
on a.word = b.word
group by time, category;

EOF

$PRESTO_HOME/presto --catalog hive --schema default -f query.sql --output-format CSV_HEADER > $HOME/project/data/word_summary/word_summary_${START}_to_{{ds}}.csv
"""