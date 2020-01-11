from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from subdag_factory import subdag_factory


PARENT_DAG_NAME='main_dag'
SUBDAG_DAG_NAME='subdag'

with DAG(
    dag_id=PARENT_DAG_NAME,
    schedule_interval='*/10 * * * *' ,
    start_date=datetime(2018, 11, 5, 10, 0, 0),
    catchup=False
) as dag:
    start_task = DummyOperator(task_id='start')
    subdag_task = SubDagOperator(
        subdag=subdag_factory(PARENT_DAG_NAME, SUBDAG_DAG_NAME, dag.start_date, dag.schedule_interval), # important to have same schedule
        task_id=SUBDAG_DAG_NAME
        # also have an arg to change executor here; if subdag runs slow, it may be on sequential executor
    )
    end_task = DummyOperator(task_id='end')
    start_task >> subdag_task >> end_task
