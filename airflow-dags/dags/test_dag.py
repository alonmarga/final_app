from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

def copy_file_function(**kwargs):
    exec(open("/home/alonm/final_app/testing_ground/files/copy.py").read())


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    'copy_file_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 8, 1),
    catchup=False
)

start = EmptyOperator(
    task_id='start',
    dag=dag,
)

copy_file_task = PythonOperator(
    task_id='copy_file',
    python_callable=copy_file_function,
    provide_context=True,
    dag=dag,
)

end = EmptyOperator(
    task_id='end',
    dag=dag,
)

start >> copy_file_task >> end