from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
import os

os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/usr/local/lib/python"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 1, 1),
}

dag_spark = DAG(
    dag_id="project7",
    default_args=default_args,
    start_date=datetime(2022, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)


users_mart = SparkSubmitOperator(
    task_id="users_mart",
    dag=dag_spark,
    application="/lessons/users_mart.py",
    conn_id="yarn_spark",
    application_args=[
        "2022-05-31",
        "30",
        "/user/sumerian17/data/geo/events/",
        "/user/sumerian17/geo_percent.csv",
        "/user/sumerian17/data/analytics/",
    ]
)

zones_mart = SparkSubmitOperator(
    task_id="zones_mart",
    dag=dag_spark,
    application="/lessons/zones_mart.py",
    conn_id="yarn_spark",
    application_args=[
        "2022-05-31",
        "30",
        "/user/sumerian17/data/geo/events/",
        "/user/sumerian17/geo_percent.csv",
        "/user/sumerian17/data/analytics/",
    ]
)


recommendations_mart = SparkSubmitOperator(
    task_id="recommendations_mart",
    dag=dag_spark,
    application="/lessons/recommendations_mart.py",
    conn_id="yarn_spark",
    application_args=[
        "2022-05-31",
        "30",
        "/user/sumerian17/data/geo/events/",
        "/user/sumerian17/geo_percent.csv",
        "/user/sumerian17/data/analytics/",
    ]
)



users_mart >> zones_mart >> recommendations_mart 