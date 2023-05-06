from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime

dag = DAG(
    dag_id = 'trigger_test',
    start_date = datetime(2022,5,5),
    catchup=False,
    schedule = '0 2 * * *')

def print_goodbye():
    print("goodbye!")
    return "goodbye!"

print_goodbye = PythonOperator(
    task_id = 'print_goodbye',
    python_callable = print_goodbye,
    dag = dag)

t1 = TriggerDagRunOperator(
    task_id = 'trigger',
    trigger_dag_id='name_gender_v4',
    execution_date='{{ execution_date }}',
    wait_for_completion=True,
    poke_interval=30,
    reset_dag_run=True,
    dag=dag
)

t1 >> print_goodbye

