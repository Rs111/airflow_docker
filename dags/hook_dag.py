from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depend_on_past': False,
    'start_date': datetime(2018, 11, 5, 10, 00, 00),
    'retries': 1,
    'retry_delay': timedelta(minutes=5.0)
}

def get_activated_sources():
    request = "SELECT * from course.source"
    pg_hook = PostgresHook(postgres_conn_id='postgre_sql', schema='airflow_mdb') # this connection must be set up via airflow UI
    connection = pg_hook.get_conn()
    cursor = connection.cursor() # gets cursor to postgres db
    cursor.execute(request)
    sources = cursor.fetchall() #fetches all data from executed request
    for source in sources:
        print('source: {0} - activated: {1}'.format(source[0], source[1]))

with DAG(
    'hook_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
) as dag:
    start_task = DummyOperator(task_id='start')
    hook_task = PythonOperator(task_id='hook_task', python_callable=get_activated_sources)
    start_task >> hook_task
