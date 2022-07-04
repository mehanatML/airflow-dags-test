from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
import pandas as pd
import os
import pendulum


data_path = '/opt/x5'

dag = DAG(
    dag_id="x5-data-preparation",
    schedule_interval="1 * * * * *",
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
)

clients_data = PythonOperator(
    task_id='clients_data',
    python_callable= pd.read_csv,
    op_kwargs = {"filepath_or_buffer" : os.path.join(data_path, 'clients.csv')},
    dag=dag,
)

products_data = PythonOperator(
    task_id='products_data',
    python_callable= pd.read_csv,
    op_kwargs = {"filepath_or_buffer" : os.path.join(data_path, 'products.csv')},
    dag=dag,
)

purchases_data = PythonOperator(
    task_id='purchases_data',
    python_callable= pd.read_csv,
    op_kwargs = {"filepath_or_buffer" : os.path.join(data_path, 'purchases.csv')},
    dag=dag,
)