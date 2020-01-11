from datetime import datetime, timedelta
# from airflow.operators.my_first_plugin.testdummy_operator import TestdummyOperator
# from airflow.operators.my_first_plugin import TestdummyOperator

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depend_on_past': False,
    'start_date': datetime(2018, 11, 5, 10, 00, 00),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

'''
Postgres con:

con_id postgres_sql
Conn Type Postgres
Host localhost
schema airflow_mdb
login airflow
port 5432
'''
def get_activated_sources(**kwargs):
    ti = kwargs['ti']
    request = "SELECT * from course.source"
    pg_hook = PostgresHook(postgres_conn_id='postgre_sql', schema='airflow_mdb') # this connection must be set up via airflow UI
    connection = pg_hook.get_conn()
    cursor = connection.cursor() # gets cursor to postgres db
    cursor.execute(request)
    sources = cursor.fetchall() #fetches all data from executed request
    for source in sources:
        if source[1]:
            ti.xcom_push(key='activated_source', value=source[0])
            return None # note: we could also just return source[0] here; it would return with a default key
    return None

def source_to_use(**kwargs):
    ti = kwargs['ti']
    source = ti.xcom_pull(task_ids='hook_task') # pull what we wrote
    print('source fetch from XCOM: {0}'.format(source))

with DAG(
    'xcom_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
) as dag:

    start_task = DummyOperator(task_id='start_task')
    hook_task= PythonOperator(task_id='hook_task', python_callable=get_activated_sources, provide_context=True)
    xcom_task = PythonOperator(task_id='xcom_task', python_callable=source_to_use, provide_context=True)
    start_task >> hook_task >> xcom_task
