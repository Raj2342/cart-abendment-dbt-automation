#   We write a DAG (Directed Acyclic Graph) using Python. Think of a DAG as a robotic assembly line. You are programming the robot to wake up,
#    execute your dbt command, check for errors,
#   and go back to sleep.

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. The "Rules of Engagement" (Senior-level error handling)
default_args = {
    'owner': 'analytics_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'email_on_failure': True, # Alerts you if the pipeline breaks
    'retries': 1,             # If it fails, wait and try one more time
    'retry_delay': timedelta(minutes=5),
}

# 2. Defining the Assembly Line (The DAG)
with DAG(
    'ecommerce_cold_path_pipeline',
    default_args=default_args,
    description='Nightly dbt run for Cart Abandonment ML features',
    schedule_interval='0 2 * * *', # Cron syntax: Runs exactly at 2:00 AM every night
    catchup=False,
) as dag:

    # 3. The Robotic Arm (The Task)
    # We use a BashOperator because dbt runs in the terminal
    run_dbt_transformations = BashOperator(
        task_id='run_dbt_unified_and_downstream',
        bash_command='cd /path/to/your/dbt_project && dbt run --select stg_ecommerce_unified+',
    )

    # Note: If we had a Python script to retrain the ML model, 
    # we would add it here and chain them together like this:
    # run_dbt_transformations >> retrain_xgboost_model