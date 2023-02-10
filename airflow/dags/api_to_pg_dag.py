from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
import os
from datetime import datetime
from datetime import timedelta

MAIN_PG_HOSTNAME = os.environ.get('MAIN_PG_HOSTNAME')
MAIN_PG_USER = os.environ.get('MAIN_PG_USER')
MAIN_PG_PASSWORD = os.environ.get('MAIN_PG_PASSWORD')
MAIN_PG_DB = os.environ.get('MAIN_PG_DB')

with DAG(
        dag_id='api_to_pg_dag',
        start_date=datetime(2023, 2, 10, 1, 1, 0, 0),
        catchup=False,
        schedule_interval=timedelta(hours=12)
) as dag:
    api_to_pg_dag = DockerOperator(
        task_id='extract_data',
        image='nhlapp:v1',
        container_name='nhlapp',
        environment={'MAIN_PG_HOSTNAME': MAIN_PG_HOSTNAME, 'MAIN_PG_USER': MAIN_PG_USER,
                     'MAIN_PG_PASSWORD': MAIN_PG_PASSWORD, 'MAIN_PG_DB': MAIN_PG_DB},
        api_version='auto',
        auto_remove=True,
        command="python extraction_app.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="test_pipeline_postgres_main"
    )

api_to_pg_dag