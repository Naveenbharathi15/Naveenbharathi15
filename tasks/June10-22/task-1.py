import pandas as pd
import os
from airflow import DAG
from datetime import date, datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.hooks.wasb_hook import WasbHook

AZURE_CONTAINER = "175-warranty-analytics"
AZURE_BLOB = f'poc-data/MANUAL-DATA/pm.xls'
SOURCE_FILE = "pm.xls"
DESTINATION_FILE= "part_group_test.csv"

GCS_FILE_PATH = f'poc-data/MANUAL-DATA/{DESTINATION_FILE}'
GCS_BUCKET = 're-prod-data-env'

GCP_CONN_ID = "google_cloud_default"
GCP_PROJECT = "re-warranty-analytics-prod"
BQ_DATASET = "re_wa_prod"
BQ_TABLE = "re_part_group_table"


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
    print(all_df.shape)

    # az_hook.load_file(DESTINATION_FILE, AZURE_CONTAINER, AZURE_DESTINATION_BLOB)
    # os.remove(DESTINATION_FILE)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 10),
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
    schedule_interval="0 2 * * *",
)

conversion_task = PythonOperator(
    task_id='xls_to_csv',
    python_callable=trans_xlx_csv,
    dag=dag
)

gcs_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket=GCS_BUCKET,
    source_objects=[GCS_FILE_PATH],
    schema_fields=[
        {"name": "PartNumber", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartGroup", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "PartVariant", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartCategory", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ModelType", "type": "STRING", "mode": "NULLABLE"},
    ],
    destination_project_dataset_table=f'{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}',
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    allow_quoted_newlines=True,
    google_cloud_storage_conn_id=GCP_CONN_ID,
    bigquery_conn_id=GCP_CONN_ID,
    dag=dag
)

success_update_email = EmailOperator(
        task_id='part_file_update_mail',
        to=["rakeshaswath@saturam.com", "naveen@saturam.com"],
        subject='Part file update required',
        html_content="""<h3>Part group file updated successfully</h3> """,
        dag=dag
)

conversion_task >> success_update_email
