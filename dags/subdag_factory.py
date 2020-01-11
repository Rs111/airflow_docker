from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator


def subdag_factory(parent_dag_name, child_dag_name, start_date, schedule_interval):

    subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=False
    )

    with subdag:
        dag_list = [DummyOperator(task_id='subdag_task_{0}'.format(i), dag=subdag) for i in range(5)]
        for i, dag in enumerate(dag_list):
            if i > 0:
                dag_list[i - 1] >> dag

    return subdag
