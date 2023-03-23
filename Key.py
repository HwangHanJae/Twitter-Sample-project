import os
import json


def get_key():
    key_path = os.path.join(os.getcwd(), 'twitter_key.json')
    with open(key_path,'r') as f:
        key = json.load(f)
        f.close()
    return key

