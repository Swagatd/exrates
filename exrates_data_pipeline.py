from airflow import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta


import requests
import json

default_args = {
    "owner": "airflow",
    "start_date": datetime(2018, 1, 1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "swagatuce@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


# Download exchange rates to json

def download_rates():

            indata = requests.get('https://api.exchangeratesapi.io/history?start_at=2018-01-01&end_at+ dates ).json()
            outdata = {'base': base, 'rates': {}}
            for dates in end_at:
                outdata['rates'] = indata['rates']
            with open('/usr/local/airflow/dags/files/ex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')


with DAG(dag_id="exrates_data_pipeline", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    is_ex_rates_available = HttpSensor(
        task_id="is_ex_rates_available",
        method="GET",
        http_conn_id="ex_api",
        endpoint="2020-10-06",
        response_check=lambda response: "rates" in response.text,
        poke_interval=5,
        timeout=20
    )


    downloading_rates = PythonOperator(
        task_id="downloading_rates",
        python_callable=download_rates
    )

    saving_rates = BashOperator(
        task_id="saving_rates",
        bash_command="""
            hdfs dfs -mkdir -p /ex && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/ex_rates.json /ex
            """
    )

    creating_ex_rates_table = HiveOperator(
        task_id="creating_ex_rates_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS ex_rates(
                base STRING,
                eur DOUBLE,
                usd DOUBLE,
                jpy DOUBLE,
                cad DOUBLE,
                gbp DOUBLE,
                nzd DOUBLE,
                ind DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )

    ex_processing = SparkSubmitOperator(
        task_id="ex_processing",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/exrates_processing.py",
        verbose=False
    )

