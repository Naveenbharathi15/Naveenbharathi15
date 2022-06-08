from datetime import timedelta,date,datetime
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.python_operator import BranchPythonOperator

from google.cloud import storage
import os
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 8),
    'email': ['rakeshaswath@saturam.com','naveen@saturam.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}
dag = DAG(
    'RELATIONSHIP_TREE_USECASE_data_ingestion',
    default_args=default_args,
    description='Relationship Tree Failure Metric',
    # schedule_interval=timedelta(days=1),
    schedule_interval="45 1 * * *",
)

GCP_CONN_ID = "google_cloud_default"
GCP_PROJECT = "re-warranty-analytics-prod"
GCS_BUCKET = 'naveentest'


def check_data():
    sales_csv = f'dms-sales-2022-06-01.csv'
    sap_prod_csv = f'sap-production-2022-06-01.csv'
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    print(storage.Blob(bucket=bucket, name=sales_csv).exists(storage_client))
    print(storage.Blob(bucket=bucket, name=sap_prod_csv).exists(storage_client))
    blob = bucket.blob(sales_csv)
    blob.download_to_filename("sales.csv")
    df = pd.read_csv('sales.csv')
    print(df.shape[0])
    print(df['CHASSISNUMBER'].nunique())
    count = df.shape[0]
    distinct_count = df['CHASSISNUMBER'].nunique()
    # For all columns
    # df[df['CHASSISNUMBER'].duplicated()].to_csv('dup_chassis_num.csv', index=False)
    df.groupby(df.columns.tolist()).size().reset_index(). \
        rename(columns={0: 'records'})
    # For only Chassis Number
    df[df['CHASSISNUMBER'].duplicated()]['CHASSISNUMBER'].to_csv('dup_chassis_num.csv', index=False)

    if count == distinct_count:
        return "send_success_mail_sales"
    return "send_poordata_mail_sales"


testing_data = BranchPythonOperator(
    task_id='data_check',
    python_callable=check_data,
    dag=dag,
)

send_success = EmailOperator(
        task_id='send_success_email',
        to=["rakeshaswath@saturam.com", "naveen@saturam.com"],
        subject='Sales Data quality',
        html_content="""<h3>Sales data has no redundancy, evaluated successfully</h3>""",
        dag=dag
)

send_poordata_mail = EmailOperator(
        task_id='send_poordata_mail',
        to=["rakeshaswath@saturam.com", "naveen@saturam.com"],
        subject='Redundant Chassis Numbers',
        html_content="""<h3>Please find the attached file to see the redundant Vehicle Chassis Numbers</h3>""",
        files=['dup_chassis_num.csv']
)
testing_data