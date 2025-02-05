import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

#derive yesterday date
yesterday = datetime.combine(datetime.today()-timedelta(1), datetime.min.time())

default_args = {
    'start_date':yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def print_hello():
    print("Hey I am Python operator")

#DAG definitions
with DAG(
    dag_id = "bash_python_operator_demo",
    catchup= False,
    schedule_interval = timedelta(days=1),
    default_args = default_args
) as dag:
    start = EmptyOperator(
        task_id="start",
        dag = dag
    )

    bash_task = BashOperator(
        task_id= "bash_task",
        bash_command= "date; echo 'Hey I am bash operator'"
    )

    python_task = PythonOperator(
        task_id= "python_task",
        python_callable= print_hello,
        dag= dag
    )

    end = EmptyOperator(
        task_id="end",
        dag=dag
    )

start>> bash_task >> python_task >> end
