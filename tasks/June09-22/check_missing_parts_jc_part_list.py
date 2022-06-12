from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
import pandas as pd
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 8),
    'email': ['rakeshaswath@saturam.com', 'naveen@saturam.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}
dag = DAG(
    'CHECK_MISSING_PARTS_JC_PART_LIST',
    default_args=default_args,
    description='Check missing part numbers',
    # schedule_interval=timedelta(days=1),
    schedule_interval="45 1 * * *",
)

# GCP_CONN_ID = "google_cloud_default"
# GCP_PROJECT = "re-warranty-analytics-prod"
# GCS_BUCKET = ''


def check_data():
    df1 = pd.read_csv('/usr/local/airflow/dags/JC_transformed.csv')
    # blob2 = bucket.blob(sales_csv)
    # blob2.download_to_filename("part_list.csv")
    df2 = pd.read_csv('/usr/local/airflow/dags/pg_all_models_latest.csv')
    df1 = df1.dropna(subset=['PartNumber'])
    df3 = df1.merge(df2, how='left', left_on=['PartNumber', 'ModelType'], right_on=['Part No', 'ModelType'])
    df3 = df3[df3["Part No"].isnull()]
    checker = df3["Part No"].isnull().count()
    if checker == 0:
        return ["no_missing_parts_mail"]
    else:
        df3 = df3.drop_duplicates(subset=['PartNumber', 'ModelType'], keep='first')
        cols = ['PartNumber', 'ModelType']
        df3.to_csv('missing_part_nos.csv', columns=cols)
        return ["missing_parts_csv_mail"]


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
testing_data >> no_missing_parts_mail
testing_data >> missing_parts_csv_mail