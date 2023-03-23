import pymongo
import requests
import json
import sys
from Key import get_key
from time import time, sleep
from pprint import pprint
from datetime import timezone, timedelta, datetime
from log import get_logger

key = get_key()
consumer_key = key['consumer_key']
consumer_secret = key['consumer_secret']
bearer_token =key['bearer_token']

def bearer_oauth(r):
      r.headers['Authorization'] = f"Bearer {bearer_token}"
      return r

def record_log(error, count):
  KST = timedelta(hours=9).seconds
  #복구시간
  recover_time = int(response.headers['x-rate-limit-reset']) + KST
  #현재시간
  now_time = time()+KST
  #남은시간
  remain = recover_time-now_time
  remain_time = timedelta(seconds=remain)

  logger.warning(f"### {error} Error ###")
  logger.info(f"현재 시간 : {datetime.utcfromtimestamp(now_time).strftime('%Y/%m/%d %H:%M:%S')}")
  logger.info(f"복구 시간 : {datetime.utcfromtimestamp(recover_time).strftime('%Y/%m/%d %H:%M:%S')}")
  logger.info(f'남은 시간 : {remain_time}')
  logger.info(f'count : {count}')
  logger.warning(f"### {error} Error ###")

url = 'https://api.twitter.com/2/tweets/sample/stream?tweet.fields=lang,created_at'

KST = timezone(timedelta(hours=9))

while True:
  mongo = pymongo.MongoClient()
  response = requests.request("GET", url, auth=bearer_oauth,stream=True)
  logger = get_logger()
  count = 0
  try:
    for line in response.iter_lines():
            if line:
              tweet = json.loads(line)
              tweet['_timestamp'] = datetime.now(tz=KST).isoformat()
              mongo.twitter.sample.insert_one(tweet)
              count += 1
  except Exception as err:
    error = str(sys.exc_info()[0]) + str(err)
    record_log(error, count)
    print('15분 쉽니다.')
    sleep_time = 60 * 15
    sleep(sleep_time)
