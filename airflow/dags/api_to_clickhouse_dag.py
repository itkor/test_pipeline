from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
import os
from datetime import datetime
from datetime import timedelta

# CH_HOSTNAME = os.environ.get('CLH_HOSTNAME')
# CH_USER = os.environ.get('CLH_USER')
# CH_PASSWORD = os.environ.get('CLH_PASS')
# CH_DB = os.environ.get('CLH_DB_NAME')
#
# with DAG(
#         dag_id='api_to_clickhouse_dag',
#         start_date=datetime(2023, 2, 9, 1, 1, 0, 0),
#         catchup=False,
#         schedule_interval=timedelta(hours=12)
# ) as dag:
#     scrapy_parser_task = DockerOperator(
#         task_id='extract_data',
#         image='scraper:v1',
#         container_name='rates_crawler',
#         environment={'MAIN_PG_HOSTNAME': MAIN_PG_HOSTNAME, 'MAIN_PG_USER': MAIN_PG_USER,
#                      'MAIN_PG_PASSWORD': MAIN_PG_PASSWORD, 'MAIN_PG_DB': MAIN_PG_DB,
#                      'MAIN_PG_LOGS_DB': MAIN_PG_LOGS_DB},
#         api_version='auto',
#         auto_remove=True,
#         command="scrapy crawl ratescrawler",
#         docker_url="unix://var/run/docker.sock",
#         network_mode="rates_scraper_postgres_main"
#     )
#
# api_to_clickhouse_dag