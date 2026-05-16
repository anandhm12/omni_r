from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='empty_task_example',
    start_date=datetime(2024, 1, 1),
    schedule='* * * * *',  # This runs the DAG every single minute
    catchup=False,
    tags=['example']
) as dag:

    # 1. Defining the empty tasks
    start_node = EmptyOperator(task_id='begin_workflow')
    
    end_node = EmptyOperator(task_id='end_workflow')

    # 2. Setting the dependencies
    start_node >> end_node