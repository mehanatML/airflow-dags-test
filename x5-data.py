from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
import pandas as pd
import os
import pendulum


data_path = '/opt/x5'
archive_name = 'retailhero-uplift.zip'

dag = DAG(
    dag_id="x5-data-preparation",
    schedule_interval="10 * * * * *",
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
)


def download_zip_archive():
    if not archive_name in os.listdir():
        os.system('wget https://storage.yandexcloud.net/datasouls-ods/materials/9c6913e5/retailhero-uplift.zip')

download_zip = PythonOperator(
    task_id='download_zip',
    python_callable = download_zip_archive,
    op_kwargs = {},
    dag=dag,
)