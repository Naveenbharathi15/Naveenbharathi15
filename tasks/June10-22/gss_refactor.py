from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.hooks.wasb_hook import WasbHook
import pandas as pd
import os

GENERAL_SERVICE_FILE="general_scheduled_service.csv"
AZURE_BLOB_GENERAL_SERVICE_FILE = f'poc-data/MANUAL-DATA/{GENERAL_SERVICE_FILE}'
AZURE_CONTAINER = "175-warranty-analytics"
GCS_BUCKET = 're-prod-data-env'
AZURE_CONTAINER = "175-warranty-analytics"
DESTINATION_FILE = 'gen-service.csv'
DESTINATION_TABLE = 're-gen-schedule-service'
GCS_FILE_PATH = f'poc-data/MANUAL-DATA/{DESTINATION_FILE}'
GCS_BUCKET = 're-prod-data-env'
GCP_CONN_ID = "google_cloud_default"
GCP_PROJECT = "re-warranty-analytics-prod"
BQ_DATASET = "re_wa_prod"


def transfer():
    az_hook = WasbHook(wasb_conn_id='wasb_default')
    az_hook.get_file(GENERAL_SERVICE_FILE, container_name=AZURE_CONTAINER, blob_name=AZURE_BLOB_GENERAL_SERVICE_FILE)
    gen_df = pd.read_csv(GENERAL_SERVICE_FILE, usecols=['Part No', 'Repeat/Repair Service'])
    os.remove(GENERAL_SERVICE_FILE)
    
    # ADD COLUMN NAMES
    gen_df.columns = ['LaborCode', 'LaborDescription']
    gen_df.to_csv(DESTINATION_FILE, header="True", index=False)


def delete_local_files():
    os.remove(DESTINATION_FILE)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 14),
    'email': ['rakeshaswath@saturam.com', 'naveen@saturam.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'GEN_SCHEDULE_SERVICE_DAILY_UPDATION',
    default_args=default_args,
    description='Updation of General Scheduled Service table',
    # schedule_interval=timedelta(days=1),
    schedule_interval="45 1 * * *",
)

transfer = PythonOperator(
    task_id='transfer',
    python_callable=transfer,
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

delete = PythonOperator(
    task_id='delete_local_files',
    python_callable=delete_local_files,
    dag=dag
)

gcs_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket=GCS_BUCKET,
    source_objects=[GCS_FILE_PATH],
    schema_fields=[
        # CHANGE COLUMN NAME
        {"name": "LaborCode", "type": "STRING", "mode": "NULLABLE"},
        {"name": "LaborDescription", "type": "STRING", "mode": "NULLABLE"},
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
        task_id='success_mail',
        to=["rakeshaswath@saturam.com", "naveen@saturam.com"],
        subject='General Schedule Service Updation',
        html_content="""<h3>General schedule service have been updated successfully in BigQuery</h3> """,
        dag=dag
)

transfer >> local_gcs >> delete >> gcs_bq >> success_email
