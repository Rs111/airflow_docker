from datetime import datetime as dt
import airflow
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'start_date': airflow.utils.dates.days_ago(2)
}

# Instantiate DAG
with DAG(
        dag_id='dynamic_dag',
        schedule_interval='@daily',
        default_args=default_args,
        catchup=False) as dag:

    task_dt = dt.now().strftime("%Y-%m-%d %H:%M:%S")

    opr_insert = PostgresOperator(
        task_id='task_id_ins',
        sql="INSERT INTO local_executor.task (id, timestamp) VALUES ('123', {task_dt})".format(task_dt=task_dt)
    )

    opr_end = BashOperator(task_id="opr_end", bash_command="echo done")

    opr_insert >> opr_end
