from airflow import DAG
from airflow.operators.empty import EmptyOperator   # ✅ correct import
from datetime import datetime

with DAG(
    dag_id='empty_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    start = EmptyOperator(task_id='start')
    end   = EmptyOperator(task_id='end')

    start >> end