from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from Weekly_Email import weekly_email_function
from airflow.utils.dates import days_ago

my_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['yrbhootda@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
my_dag = DAG(
    'spotify_email_dag',
    default_args = my_args,
    description= 'Spotify Weekly Email',
    schedule_interval= '58 15 * * 2'
)


run_email = PythonOperator(
    task_id='spotify_email_weekly',
    python_callable= weekly_email_function,
    dag=my_dag
)
run_email
