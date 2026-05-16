from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule 
# ---------------------------------------------------------
# Default Arguments & Configuration
# ---------------------------------------------------------
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

airflow_scripts_path = "/airflow/script/glue"
# ---------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------
with DAG(
    dag_id='omniroute_daily_batch_pipeline',
    default_args=default_args,
    description='Daily batch processing for OmniRoute Asset History and Reporting',
    schedule='15 21 * * *',                # FIXED: Updated from schedule_interval to schedule
    start_date=datetime(2026, 4, 1), 
    catchup=False,
    tags=['omniroute', 'batch', 'gold_layer'],
) as dag:

    # ---------------------------------------------------------
    # Task Definitions
    # ---------------------------------------------------------
    
    start_pipeline = EmptyOperator(task_id='start_pipeline')
    
    # The bottleneck task
    # process_asset_history = landing_to_staging = BashOperator(task_id='landing_to_staging',
    #                               bash_command="python3 " + airflow_scripts_path +
    #                                            "/glue_wrapper.py " + airflow_scripts_path + "/config.json",retries=1, retry_delay=timedelta(seconds=60), dag=dag)
    
    injection = BashOperator(
        task_id='landing_to_staging',
        bash_command=(
            "python3 " + airflow_scripts_path +
            "/glue_wrapper_landing_to_staging.py " +
            airflow_scripts_path + "/config.json"
        ),
        retries=1,
        retry_delay=timedelta(seconds=60),
    )

    
    # Downstream reporting tasks meant to run concurrently


    end_pipeline = EmptyOperator(task_id='end_pipeline')

    # ---------------------------------------------------------
    # Dependencies (Execution Order)
    # ---------------------------------------------------------
    
    # Asset History acts as the bottleneck. Once complete, reporting jobs run in parallel.
    start_pipeline >> injection >> end_pipeline