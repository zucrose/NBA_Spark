from airflow import DAG;
from  datetime import  datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args={

    'start_date':datetime(2025,4,8,1,00)
}

def fetch_data():
    import json
    import requests

    res=requests.get('https://randomuser.me/api/')
    print(res.json())

with DAG(dag_id='test_dag',
         schedule_interval='@daily',
         start_date=datetime(2025,4,8,1,00),
         catchup=False) as dag:
    test_task=PythonOperator(
        task_id='test_task',
        python_callable=fetch_data
    )
    op = BashOperator(task_id='hello_world', bash_command="Hello World!")
    test_task >> op



