from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import json

def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def extract(**cxt):
    link = cxt["params"]["url"]
    lat = cxt["params"]["lat"]
    lon = cxt["params"]["lon"]
    api_key = Variable.get("open_weather_api_key")

    task_instance = cxt['task_instance']
    execution_date = cxt['execution_date']

    logging.info("execution_date: {0}".format(execution_date))
    f = requests.get(link.format(lat=lat, lon=lon, api_key=api_key))
    return (f.text)

def transform(**cxt):
    ret = []
    text = cxt["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    for d in json.loads(text)["daily"]:
        day = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
        temp = d["temp"]["day"]
        min_temp = d["temp"]["min"]
        max_temp = d["temp"]["max"]
        ret.append("{0},{1},{2},{3}".format(day,temp,min_temp,max_temp))
    return ret

def load(**cxt):
    schema = cxt["params"]["schema"]
    table = cxt["params"]["table"]

    cur = get_Redshift_connection()
    data = cxt["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    sql = "DELETE FROM {schema}.{table};".format(schema=schema, table=table)
    for d in data:
        (day,temp,min_temp,max_temp) = d.split(",")
        sql += f"""INSERT INTO {schema}.{table} VALUES ('{day}','{temp}','{min_temp}','{max_temp}');"""
    logging.info(sql)
    cur.execute(sql)
    cur.execute("COMMIT;")


dag = DAG(
    dag_id = 'weather_forecast',
    catchup = False,
    start_date = datetime(2023,4,23),
    schedule = '0 1 * * *',
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
)

extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {
        'url' : Variable.get("one_call_url"),
        'lat' : 37.5665,
        'lon' : 121.8853
    },
    dag = dag
)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    params = {
    },
    dag = dag
)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'schema': 'jhongy1994',   ## 자신의 스키마로 변경
        'table': 'weather_forecast'
    },
    dag = dag)

extract >> transform >> load
