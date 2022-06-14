from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.hooks.wasb_hook import WasbHook
import pandas as pd
import os

PART_IGNORE_FILE="part_ignore_list.csv"
AZURE_BLOB_PART_IGNORE_FILE = f'poc-data/MANUAL-DATA/{PART_IGNORE_FILE}'
AZURE_CONTAINER = "175-warranty-analytics"
DESTINATION_FILE = 'part_exclude.csv'
DESTINATION_TABLE = 'part_exclude'
GCS_FILE_PATH = f'poc-data/MANUAL-DATA/{DESTINATION_FILE}'
GCS_BUCKET = 're-prod-data-env'
GCP_CONN_ID = "google_cloud_default"
GCP_PROJECT = "re-warranty-analytics-prod"
BQ_DATASET = "re_wa_prod"


def filter_partignore():
    az_hook = WasbHook(wasb_conn_id='wasb_default')
    az_hook.get_file(PART_IGNORE_FILE, container_name=AZURE_CONTAINER, blob_name=AZURE_BLOB_PART_IGNORE_FILE)
    part_no_df = pd.read_csv(PART_IGNORE_FILE, usecols=['Part No', 'Repeat/Repair Service'])
    os.remove(PART_IGNORE_FILE)
    part_no_df['Part No'] = part_no_df['PartNo']
    part_no_df['Repeat/Repair Service'] = part_no_df['Repeat_Repair_Service']
    part_no_df.drop(['Part Description'], inplace=True, axis=1)
    part_no_df = part_no_df[part_no_df['Repeat_Repair_Service'] == 'Yes']['Part No']
    part_no_df.to_csv(DESTINATION_FILE, header="True", index=False)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 13),
    'email': ['rakeshaswath@saturam.com', 'naveen@saturam.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'PART_IGNORE_LIST_DAILY_UPDATION',
    default_args=default_args,
    description='Creating part ignore list in BQ',
    # schedule_interval=timedelta(days=1),
    schedule_interval="45 1 * * *",
)

filter_part_ignore = PythonOperator(
    task_id='filter_partignore',
    python_callable=filter_partignore,
    dag=dag
)

local_gcs = LocalFilesystemToGCSOperator(
    task_id="local_to_gcs",
    src=DESTINATION_FILE,
    dst=GCS_FILE_PATH,
    bucket=GCS_BUCKET,
    # mime_type='text/csv',
    google_cloud_storage_conn_id=GCP_CONN_ID,
    dag=dag
)

gcs_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket=GCS_BUCKET,
    source_objects=[GCS_FILE_PATH],
    schema_fields=[
        {"name": "PartNo", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Repeat_Repair_Service", "type": "STRING", "mode": "NULLABLE"},
    ],
    destination_project_dataset_table=f'{GCP_PROJECT}.{BQ_DATASET}.{DESTINATION_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    allow_quoted_newlines=True,
    google_cloud_storage_conn_id=GCP_CONN_ID,
    bigquery_conn_id=GCP_CONN_ID,
    dag=dag
)

success_email = EmailOperator(
        task_id='part_ignore_mail',
        to=["rakeshaswath@saturam.com", "naveen@saturam.com"],
        subject='Part ignore list updated',
        html_content="""<h3>Part ignore list have been updated successfully in BigQuery</h3> """,
        dag=dag
)

filter_part_ignore >> local_gcs >> gcs_bq >> success_email
