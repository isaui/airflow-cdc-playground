"""
CDC DAG - Schedule and run CDC operations on configured tables

This DAG uses the CDC Python operator to process tables based on global configuration
"""

import os
import sys
import json
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Import CDC modules - assuming the project is mounted at /opt/airflow
sys.path.append('/opt/airflow')
from scripts.run_cdc import run_cdc, load_config

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Try to get config path from environment variable or use default
config_path = os.getenv('CDC_CONFIG_PATH', '/opt/airflow/config.json')

# Load the configuration
try:
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Get scheduling parameters from config
    scheduling = config.get('global_settings', {}).get('scheduling', {})
    enabled = scheduling.get('enabled', True)
    interval_seconds = scheduling.get('interval_seconds', 3600)  # Default 1 hour
    max_retries = scheduling.get('max_retries', 3)
    retry_delay_seconds = scheduling.get('retry_delay_seconds', 300)  # 5 minutes
except Exception as e:
    print(f"Error loading config from {config_path}: {e}")
    # Default values if config can't be loaded
    enabled = True
    interval_seconds = 3600
    max_retries = 3
    retry_delay_seconds = 300

# Update default args with config values
default_args.update({
    'retries': max_retries,
    'retry_delay': timedelta(seconds=retry_delay_seconds),
})

# Create the DAG
dag = DAG(
    'cdc_snapshot_diff',
    default_args=default_args,
    description='Run CDC operations on configured tables',
    schedule_interval=timedelta(seconds=interval_seconds),
    start_date=days_ago(1),
    catchup=False,
    tags=['cdc'],
    is_paused_upon_creation=not enabled,
)

# Task to check if config exists
check_config = BashOperator(
    task_id='check_config',
    bash_command=f'test -f {config_path} || (echo "Config file not found at {config_path}" && exit 1)',
    dag=dag,
)

# Function to run CDC process for all tables
def process_all_tables(**kwargs):
    """Process all tables defined in the configuration."""
    config = load_config(config_path)
    run_cdc(config_path, config)
    return "CDC processing completed for all tables"

# Create a task to process all tables
process_all = PythonOperator(
    task_id='process_all_tables',
    python_callable=process_all_tables,
    dag=dag,
)

# Function to run CDC process for a specific table
def process_table(table_name, **kwargs):
    """Process a specific table."""
    config = load_config(config_path)
    run_cdc(config_path, config, [table_name])
    return f"CDC processing completed for table {table_name}"

# Create individual tasks for each table
try:
    for table_name in config.get('tables', {}):
        table_task = PythonOperator(
            task_id=f'process_table_{table_name}',
            python_callable=process_table,
            op_kwargs={'table_name': table_name},
            dag=dag,
        )
        # Set dependency: check_config -> individual table task
        check_config >> table_task
except Exception as e:
    print(f"Error creating table tasks: {e}")

# Set dependencies for the process_all task
check_config >> process_all
