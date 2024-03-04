from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests

default_args = {
    'owner' : 'kirangunturu',
    'start_date' : datetime(2024, 2, 29, 11, 00)
}

def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    #res = json.dumps(res, indent=3)
    #print(res)
    return res

def format_data(res):
    data = {}
    data['first_name'] =  res['name']['first']
    data['last_name'] =  res['name']['last']
    data['gender'] =  res['gender']
    data['address'] = str(res['location']['street']['number']) + " " + res['location']['street']['name'] + " " + res['location']['city'] + " " + res['location']['state'] + " " + res['location']['country']
    data['postcode'] = res['location']['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data


def stream_data():
    res = get_data()
    res = format_data(res)
    res = json.dumps(res, indent=3)
    print(res)


 

#with DAG('streaming-t0-cassandra',
#        default_args=default_args,
#       schedule_interval='@daily',
#        catchup=False
#        ) as DAG

streaming_task = PythonOperator(
    task_id = 'stream_data_from_random_api',
    python_callable = stream_data
)

stream_data()