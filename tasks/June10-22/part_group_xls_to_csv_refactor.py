import pandas as pd
import os
from airflow import DAG
from datetime import date, datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.hooks.wasb_hook import WasbHook

AZURE_CONTAINER = "175-warranty-analytics"
AZURE_BLOB = f'poc-data/MANUAL-DATA/pm.xls'
SOURCE_FILE = "pm.xls"
DESTINATION_FILE= "part_group.csv"


def trans_xlx_csv():

    # UCE & HIMALAYAN

    az_hook = WasbHook(wasb_conn_id='wasb_default')
    az_hook.get_file(SOURCE_FILE, container_name=AZURE_CONTAINER, blob_name=AZURE_BLOB)

    df = pd.read_excel(SOURCE_FILE, sheet_name=0)
    df = df[['Part No', 'Part Name', 'Part Group', 'Part category', 'Category']]
    df.columns = ['Part No', 'Part Name', 'Part Group', 'Part Variant', 'Part Category']
    for i in ['Part Name', 'Part Group', 'Part Variant', 'Part Category']:
        df[i] = df[i].str.upper()
    df.dropna(subset=['Part No'], inplace=True)
    df.drop_duplicates(subset=['Part No'], keep='first', inplace=True)
    df['ModelType'] = 'UCE'
    uce_df = df.copy()
    him_df = df.copy()
    him_df['ModelType'] = 'HIMALAYAN'

    # METEOR & NEWCLASSIC

    df = pd.read_excel(SOURCE_FILE, sheet_name=1)
    df = df[['Part No', 'Part Name', 'Part Group', 'Part category', 'Category']]
    df.columns = ['Part No', 'Part Name', 'Part Group', 'Part Variant', 'Part Category']
    for i in ['Part Name', 'Part Group', 'Part Variant', 'Part Category']:
        df[i] = df[i].str.upper()
    df.dropna(subset=['Part No'], inplace=True)
    df.drop_duplicates(subset=['Part No'], keep='first', inplace=True)
    df['ModelType'] = 'METEOR'
    meteor_df = df.copy()
    nc_df = df.copy()
    nc_df['ModelType'] = 'NEW CLASSIC'

    # TWINS

    df = pd.read_excel(SOURCE_FILE, sheet_name=2)
    os.remove(SOURCE_FILE)
    df = df[['Part No', 'Part Name', 'Part Group', 'Part category', 'Category']]
    df.columns = ['Part No', 'Part Name', 'Part Group', 'Part Variant', 'Part Category']
    for i in ['Part Name', 'Part Group', 'Part Variant', 'Part Category']:
        df[i] = df[i].str.upper()
    df.dropna(subset=['Part No'], inplace=True)
    df.drop_duplicates(subset=['Part No'], keep='first', inplace=True)
    df['ModelType'] = 'TWINS'
    twins_df = df.copy()
    all_df = pd.concat([nc_df, twins_df, meteor_df, uce_df, him_df], ignore_index=True)
    all_df.to_csv(DESTINATION_FILE, header=True, index=False)
    # CHECK WITH RAKESH FOR BLOB
    az_hook.load_file(DESTINATION_FILE, container_name=AZURE_CONTAINER, blob_name=AZURE_BLOB)
    print(all_df.shape)


def delete_local_files():
    os.remove(DESTINATION_FILE)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 13),
    'email': ['rakeshaswath@saturam.com','naveen@saturam.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'PART_MASTER_XLS_TO_CSV',
    default_args=default_args,
    description='Updating xls to csv with updated columns to our requirement',
    # schedule_interval=timedelta(days=1),
    schedule_interval="0 0 * * *"
)

conversion_task = PythonOperator(
    task_id='xls_to_csv',
    python_callable=trans_xlx_csv,
    dag=dag
)

delete = PythonOperator(
    task_id='delete_local_file',
    python_callable=delete_local_files,
    dag=dag
)


success_update_email = EmailOperator(
        task_id='success_mail',
        to=["rakeshaswath@saturam.com", "naveen@saturam.com"],
        subject='Part file update',
        html_content="""<h3>Part group file updated successfully</h3> """,
        dag=dag
)

conversion_task >> delete >> success_update_email