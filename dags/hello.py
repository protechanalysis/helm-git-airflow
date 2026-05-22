from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
# from notification.email_alert import task_fail_alert
def hello_world():
    print("Hello, World!")
    print("Welcome to Airflow DAGs!")
    print("This is a simple DAG to demonstrate Airflow's capabilities.")
    print("Airflow allows you to schedule and orchestrate complex workflows with ease.") 
    print("You can use Airflow to automate data pipelines, machine learning workflows, and much more!")
    print("Happy Airflow-ing!")

default_args = {
    'owner': 'adewunmi',
    'depends_on_past': False,
    # 'on_failure_callback': task_fail_alert,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="hello",
    start_date=datetime(2026, 3, 29),
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:
    
    extract_data = PythonOperator(
        task_id="hello_test",
        python_callable=hello_world
    )

extract_data