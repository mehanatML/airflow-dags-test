from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
import pandas as pd
import os
import pendulum

from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.types.base import (
    DataContextConfig,
    CheckpointConfig
)

ge_root_dir='/opt/x5/great_expectations'
data_path = '/data'
archive_name = 'retailhero-uplift.zip'

dag = DAG(
    dag_id="x5-data-preparation",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
)


def download_zip_archive():
    if not archive_name in os.listdir():
        os.system('wget -P ./ https://storage.yandexcloud.net/datasouls-ods/materials/9c6913e5/retailhero-uplift.zip')

def unzip_data_archive():
    os.system(f'unzip ./{archive_name}')

def join_data():
    df_clients = pd.read_csv(f'{data_path}/clients.csv', index_col='client_id')
    df_train = pd.read_csv(f'{data_path}/uplift_train.csv', index_col='client_id')
    df_test = pd.read_csv(f'{data_path}/uplift_test.csv', index_col='client_id')

    df_clients['first_issue_unixtime'] = pd.to_datetime(df_clients['first_issue_date']).astype(int)/10**9
    df_clients['first_redeem_unixtime'] = pd.to_datetime(df_clients['first_redeem_date']).astype(int)/10**9
    df_features = pd.DataFrame({
        'gender_M': (df_clients['gender'] == 'M').astype(int),
        'gender_F': (df_clients['gender'] == 'F').astype(int),
        'gender_U': (df_clients['gender'] == 'U').astype(int),
        'age': df_clients['age'],
        'first_issue_time': df_clients['first_issue_unixtime'],
        'first_redeem_time': df_clients['first_redeem_unixtime'],
        'issue_redeem_delay': df_clients['first_redeem_unixtime'] - df_clients['first_issue_unixtime'],
    }).fillna(0)
    df_features.to_csv(f'{data_path}/df_features.csv')

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

data_peprocessing = PythonOperator(
    task_id='data_peprocessing',
    python_callable = join_data,
    op_kwargs = {},
    dag=dag,
)

ge_client_data = GreatExpectationsOperator(
    task_id="ge_client_data",
    data_context_root_dir=ge_root_dir,
    checkpoint_name="getting_started",
)



download_zip >> unzip_archive >> ge_client_data  >> data_peprocessing

