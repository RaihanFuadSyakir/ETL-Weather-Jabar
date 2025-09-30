from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F
import time
# JUST EXPERIMENT TO USE SPARK
# use spark if data > 500k and require many calculation - CHATGPT
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# HARD CODED date_start
date_start = {
    "year": 2025,
    "month": 8,
    "day": 1,
    "hour": 0,
    "minute": 0,
    "second": 0
}
with DAG(
    "transform_raw_dag",
    default_args=default_args,
    description="Process raw data from raw_weather",
    schedule=None,
    #schedule="@daily", #for debug purpose
    start_date= datetime(date_start["year"], 
                         date_start["month"],
                         date_start["day"],
                         date_start["hour"],
                         date_start["minute"],
                         date_start["second"]
    ),
    catchup=False
) as dag:
    spark_job = SparkSubmitOperator(
            task_id="run_spark_job",
            application="/opt/airflow/dags/spark/transform_raw_weather.py",
            conn_id="spark_master",
            verbose=True,
            conf={
                "spark.jars.ivy":"/tmp/.ivy"
#                "spark.executor.memory": "2g",
#                "spark.executor.cores": "1"
            },
        )
