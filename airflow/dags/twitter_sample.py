import datetime
from utils import etl
from airflow import DAG

from airflow.operators.bash import BashOperator

with DAG(
    'twitter_sample',
    schedule='@daily',
    start_date=datetime.datetime(2023,3,14),
    tags=['twitter']
) as dag:

  extract = BashOperator(task_id='extract', bash_command=etl.EXTRACT)
  load = BashOperator(task_id='load', bash_command=etl.LOAD)
  aggregate=BashOperator(task_id='aggregate', bash_command=etl.AGGREGATE)

  extract >> load >> aggregate
