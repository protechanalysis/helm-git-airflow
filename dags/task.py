from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from notification.email_alert import task_fail_alert
from includes.extraction import extract_world_bank_data, validate_data
from includes.database import create_metric_table, load_dataframe_to_postgres


default_args = {
    'owner': 'adewunmi',
    'depends_on_past': False,
    'on_failure_callback': task_fail_alert,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="world_bank_agric_etl",
    start_date=datetime(2026, 3, 29),
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:
    
    extract_data = PythonOperator(
        task_id="extract_agric_data",
        python_callable=extract_world_bank_data
    )

    tranform_validate = PythonOperator(
        task_id="tranform_and_validate_data",
        python_callable=validate_data
    )

    table_creation = PythonOperator(
        task_id="create_table",
        python_callable=create_metric_table
    )

    load_data = PythonOperator(
        task_id="loading_to_postgres",
        python_callable=load_dataframe_to_postgres
    )


    extract_data >> tranform_validate >> table_creation >> load_data