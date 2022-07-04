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
        os.system('wget -P ./ https://storage.yandexcloud.net/datasouls-ods/materials/9c6913e5/retailhero-uplift.zip')

def unzip_data_archive():
    os.system(f'unzip ./{archive_name}')

download_zip = PythonOperator(
    task_id='download_zip',
    python_callable = download_zip_archive,
    op_kwargs = {},
    dag=dag,
)

unzip_archive = PythonOperator(
    task_id='unzip_archive',
    python_callable = unzip_data_archive,
    op_kwargs = {},
    dag=dag,
)

download_zip >> unzip_archive

