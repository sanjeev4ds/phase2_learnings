# about project - set variables using shell script
# 1. save .sh file using python operator
# 2. read shell script file content using python operator again
# 3. saving in xcom shell script command
# 4 using xcom we will execute BashOperator command
from multiprocessing.sharedctypes import synchronized

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timezone
from airflow.operators.bash import BashOperator
from airflow.models import Variable, XCom

from airflow.utils.db import provide_session

# Not using GCS because need to use local Airflow environment
# from google.oauth2 import service_account
# from google.cloud import storage
# import os


default_ags = {
    "retries": 1
}

DAG_ID = "project_2_set_env_variable"

def WriteShellScriptToGCS(command_in_params):
    file_to_store = "set_env_variable.sh"
    print(command_in_params)
    with open(file_to_store, mode="w") as file:
        file.write(command_in_params)
    print(".sh file successfully created...")

def SaveCommandToXcom(ti):
    file_to_read = "set_env_variable.sh"
    with open(file_to_read, mode="r") as file:
        shell_command = file.read()
        print(shell_command)
        ti.xcom_push(key="shell_command", value=shell_command)

def UseAirflowVariable():
    print(Variable.get("FILE_SYSTEM_CONN"))

@provide_session
def ClearXComs(dag_id, task_id, until_date, session=None):
    """Clears XComs for a specific task and Dag until a given date."""
    try:
        deleted_count= session.query(XCom).filter(
            XCom.dag_id == dag_id,
            XCom.task_id == task_id,
            XCom.execution_date<= until_date
        ).delete(synchronize_session='fetch')
        session.commit()
        print(f'Deleted {deleted_count} XCom entries.')
    except Exception as e:
        print(str(e))
        session.rollback()
        raise AirflowException(f"Error clearing Xcom")

# def DeleteXcom(**kwargs):
#     ti = kwargs["ti"] # Get the task instance
#     task_id = "save_shell_command_to_xcom"
#     XCom.clear(
#         dag_id=DAG_ID,
#         task_id= task_id,
#         execution_date=datetime.now(timezone.utc)
#     )
#     print("XCom deleted for save_shell_command_to_xcom")

with DAG(
    dag_id= DAG_ID,
    default_args=default_ags,
    schedule_interval="@daily",
    catchup=False,
    concurrency=4,
    start_date= datetime(2025,2,21)
) as Dag:
    start = EmptyOperator(
        task_id="start",
        dag= Dag
    )

    save_shell_command_to_gcs = PythonOperator(
        task_id="shell_script_to_gcs",
        python_callable=WriteShellScriptToGCS,
        #op_args=["airflow variables set FILE_SYSTEM_CONN=file_system_conn && airflow scheduler -D && airflow worker -D"]
        op_args=['airflow variables set FILE_SYSTEM_CONN "file_system_conn"']
    )

    save_shell_command_to_xcom = PythonOperator(
        task_id= "save_shell_command_to_xcom",
        python_callable=SaveCommandToXcom,
        dag=Dag
    )

    run_bash_script = BashOperator(
        task_id="execute_variable_command",
        bash_command="{{ti.xcom_pull(task_ids='save_shell_command_to_xcom',key='shell_command')}}",
        dag=Dag
    )

    use_variable_airflow = PythonOperator(
        task_id="use_variable_airflow",
        python_callable=UseAirflowVariable,
        dag=Dag
    )

    delete_xcom_task = PythonOperator(
        task_id="delete_xcom_task",
        # python_callable=DeleteXcom,
        python_callable=ClearXComs,
        op_kwargs={
            'dag_id': DAG_ID,
            'task_id': 'save_shell_command_to_xcom',
            'until_date': datetime.now(timezone.utc)
        },
        provide_context=True,  # this ensures kwargs are passed to callabel
        dag=Dag
    )

    end = EmptyOperator(
        task_id="end",
        dag=Dag
    )

start >> save_shell_command_to_gcs >> save_shell_command_to_xcom >> run_bash_script >> use_variable_airflow >> delete_xcom_task >> end