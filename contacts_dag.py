from datetime import datetime, timedelta
from operator import index
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

hdfs_url = Variable.get("PGDI_HADOOP_URL")
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")

local_tz = pendulum.timezone("America/Mexico_City")
default_args = {
    'owner': 'crukike',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 3, tzinfo=local_tz),
    'email': ['crukike@gmail.com'],
    'email_on failure': False,
    'email_on_retry': False,
    'dagrun_timeout':timedelta(minutes=20)
}

with DAG( \
    dag_id='contacts_etl', \
    default_args=default_args, \
    catchup=False, 
    schedule_interval="@once",
    tags=["etl"]) as dag:

    DummyOperator(
        task_id='start'
    ) >> SparkSubmitOperator(
        task_id='extraction',
        conn_id='spark_connection',
        application=f'{pyspark_app_home}/contacts_etl/extraction.py',
        jars="${SPARK_HOME}/jars/mysql-connector-java-8.0.21.jar",
        total_executor_cores=2,
        executor_cores=2,
        executor_memory='2g',
        driver_memory='2g',
        name='extraction',
        application_args=[
            hdfs_url
        ]
    ) >> SparkSubmitOperator(
        task_id='tranformation_load',
        conn_id='spark_connection',
        application=f'{pyspark_app_home}/contacts_etl/tranformation_load.py',
        packages="io.delta:delta-core_2.12:1.1.0",
        total_executor_cores=2,
        executor_cores=2,
        executor_memory='2g',
        driver_memory='2g',
        name='tranformation_load',
        application_args=[
            hdfs_url
        ]
    ) >> DummyOperator(
        task_id='end'
    )