from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from airflow.contrib.hooks import gcs_hook


def download_gcs():
    hook_gcs = gcs_hook.GoogleCloudStorageHook(google_cloud_storage_conn_id=GCP_CONN_ID)
    hook_gcs.download(bucket=GCS_BUCKET, object=SOURCE_FILE, filename="dms_jc.csv")


def check_data():
    df1 = pd.read_csv('/usr/local/airflow/dags/dms_jc.csv')
    df1 = df1.dropna(subset=['PartNumber'])
    df1 = df1[df1["PartGroup"].isnull()]
    checker = df1["PartGroup"].isnull().count() 
    if checker == 0:
        return ["no_missing_parts_mail"]
    else:
        df1 = df1.drop_duplicates(subset=['PartNumber', 'ModelType'], keep='first')
        cols = ['PartNumber', 'ModelType']
        df1.to_csv('missing_part_nos.csv', columns=cols)
        return ["missing_parts_csv_mail"]


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 13),
    'email': ['rakeshaswath@saturam.com', 'naveen@saturam.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'CHECK_MISSING_PARTS_JC_PART_LIST',
    default_args=default_args,
    description='Check missing part numbers',
    # schedule_interval=timedelta(days=1),
    schedule_interval="45 1 * * *",
)

today = datetime.today()
GCP_CONN_ID = "google_cloud_default"
GCP_PROJECT = "re-warranty-analytics-prod"
GCS_BUCKET = 're-prod-data-env'
SOURCE_FILE = f'poc-data/DMS/Delta-Data/dms-{today}.csv'

gcs_to_local = PythonOperator(
    task_id='gcs_to_local',
    python_callable=download_gcs,
    dag=dag
)

testing_data = BranchPythonOperator(
    task_id='data_check',
    python_callable=check_data,
    dag=dag,
)

no_missing_parts_mail = EmailOperator(
        task_id='no_missing_parts_mail',
        to=["rakeshaswath@saturam.com", "naveen@saturam.com"],
        subject='No missing partnumbers',
        html_content="""<h3>Data has no missing part numbers and its evaluated successfully</h3>""",
        dag=dag
)

missing_parts_csv_mail = EmailOperator(
        task_id='missing_parts_csv_mail',
        to=["rakeshaswath@saturam.com", "naveen@saturam.com"],
        subject='Missing PartNumber csv',
        html_content="""<h3>Please find the attached file to containing missing part 
        numbers with respective modeltype</h3>""",
        files=['missing_part_nos.csv'],
        dag=dag
)

gcs_to_local >> testing_data >> no_missing_parts_mail
gcs_to_local >> testing_data >> missing_parts_csv_mail
