from datetime import datetime
from airflow import DAG
from docker.types import Mount
# don't need these libraries when using Airbyte
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.docker.operators.docker import DockerOperator
import subprocess

CONN_ID = '9d9b5475-6e04-4cf1-a3de-35365a6d53d9'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# function for running the ELT script through Airflow
# def run_elt_script():
#     script_path = "/opt/airflow/elt/elt_script.py"
#     result = subprocess.run(["python", script_path],
#                             capture_output=True, text=True)
#     if result.returncode != 0:
#         raise Exception(f"Script failed with error: {result.stderr}")
#     else:
#         print(result.stdout)


dag = DAG(
    'elt_and_dbt',
    default_args=default_args,
    description='An ELT worflow with dbt',
    start_date=datetime(2024, 2, 11),
    catchup=False,
)

t1 = AirbyteTriggerSyncOperator(
    task_id="airbyte_postgres_postgres",
    airbyte_conn_id='airbyte',
    connection_id=CONN_ID,
    asynchronous=False,
    timeout=3600,
    wait_seconds=3,
    dag=dag
)

# task run by script through Airflow before adding Airbyte
# t1 = PythonOperator(
#     task_id="run_elt_script",
#     python_callable=run_elt_script,
#     dag=dag
# )

t2 = DockerOperator(
    task_id="dbt_run",
    image="ghcr.io/dbt-labs/dbt-postgres:1.4.7",
    command=[
        "run",
        "--profiles-dir",
        "/root",
        "--project-dir",
        "/dbt"
    ],
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    mounts=[
        Mount(source='/home/daniel/Documents/Data Engineer learning/elt/custom_postgres',
              target='/dbt', type='bind'),
        Mount(source='/home/daniel/.dbt',
              target='/root', type='bind')
    ],
    dag=dag
)

t1 >> t2
