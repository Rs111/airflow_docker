from datetime import date, timedelta, datetime

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# Import modules
import fetching_tweet
import cleaning_tweet

# Default Args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

# Instantiate DAG
with DAG(
        dag_id='twitter_dag_v1',
        start_date=datetime(2018, 10, 1),
        schedule_interval='@daily',
        default_args=default_args,
        catchup=False) as dag:

    # Define Tasks
    start_task = DummyOperator(
        task_id="start_task",
    )

    waiting_file_task = FileSensor(
        task_id='waiting_file_task',
        fs_conn_id='default',
        filepath='home/airflow/airflow_files/data.csv',
        poke_interval=5,
    )

    fetch_tweet_task = PythonOperator(
        task_id='fetch_tweet_task',
        python_callable=fetching_tweet.main
    )

    clean_tweet_task = PythonOperator(
        task_id='clean_tweet_task',
        python_callable=cleaning_tweet.main
    )

    load_into_hdfs_task = BashOperator(
        task_id='load_into_hdfs_task',
        bash_command='hadoop fs -put -f /tmp/data_cleaned.csv /tmp/'
    )

    transfer_into_hive_task = HiveOperator(
        task_id='transfer_into_hive_task',
        hql='LOAD DATA INPATH "/tmp/data_cleanred.csv" INTO TABLE tweets PARTITION(dt="2018-10-01")'
    )

    # Set dependencies
    start_task >> waiting_file_task >> fetch_tweet_task >> clean_tweet_task >> load_into_hdfs_task >> transfer_into_hive_task

