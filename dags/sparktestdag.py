import airflow
from airflow import DAG;
from datetime import  datetime;
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

def fetch_data():
    import json
    import requests

    res=requests.get('https://randomuser.me/api/')
    print(res.json())


with DAG(dag_id='sparktestdag',schedule_interval='@daily',
         start_date=datetime(2025,4,8,1,00),
         catchup=False ) as dag:
        test_task = PythonOperator(
        task_id='fetch_task',
        python_callable=fetch_data
        )

        spark_submit_operator = SparkSubmitOperator(
        task_id='spark_submit_job',
        application='pspark/testspark.py',
        conn_id='spark'
        )
        test_task>>spark_submit_operator

