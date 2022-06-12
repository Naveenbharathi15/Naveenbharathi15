import pandas as pd
import xlrd
from airflow import DAG
from datetime import date, datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
# NEW TRY CHECK WITH RAKESH
from azure.storage.blob import BlobClient
# END
from airflow.operators.email_operator import EmailOperator


def trans_xlx_csv():

    # UCE & HIMALAYAN

    df = pd.read_excel('pm.xls', sheet_name=0)
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

    df = pd.read_excel('pm.xls', sheet_name=1)
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

    df = pd.read_excel('pm.xls', sheet_name=2)
    df = df[['Part No', 'Part Name', 'Part Group', 'Part category', 'Category']]
    df.columns = ['Part No', 'Part Name', 'Part Group', 'Part Variant', 'Part Category']
    for i in ['Part Name', 'Part Group', 'Part Variant', 'Part Category']:
        df[i] = df[i].str.upper()
    df.dropna(subset=['Part No'], inplace=True)
    df.drop_duplicates(subset=['Part No'], keep='first', inplace=True)
    df['ModelType'] = 'TWINS'
    twins_df = df.copy()
    all_df = pd.concat([nc_df, twins_df, meteor_df, uce_df, him_df], ignore_index=True)
    all_df.to_csv('pg_all_models_latest.csv', header=True, index=False)
    print(all_df.shape)


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
    schedule_interval="0 3 * * *",
)


def check_updation():

    # HAVE TO CHECK WITH RAKESH START
    blob = BlobClient(account_url="https://<account_name>.blob.core.windows.net",
                      container_name="<container_name>", blob_name = "<blob_name>",
                      credential = "<account_key>")
    last_modified = blob.properties.last_modified
    today = date.today()
    yesterday = today - timedelta(days=1)
    if last_modified == yesterday or last_modified == today:
        # END
        trans_xlx_csv()
        return ["part_file_update_mail"]
    else:
        return ["no_change_mail"]


bo = BranchPythonOperator(
    task_id='check_updation',
    python_callable=check_updation,
    do_xcom_push=False,
    # trigger_rule='none_failed_or_skipped',
    dag=dag
)

success_update_email = EmailOperator(
        task_id='part_file_update_mail',
        to=["rakeshaswath@saturam.com", "naveen@saturam.com"],
        subject='Part file update required',
        html_content="""<h3>Part group file updated successfully</h3> """,
        dag=dag
)

no_update_email = EmailOperator(
        task_id='no_change_mail',
        to=["rakeshaswath@saturam.com", "naveen@saturam.com"],
        subject='No update in partmaster.xlx',
        html_content=""" <h3>No changes in partmaster Azure file, DAG ran successfully</h3> """,
        dag=dag
)

bo >> success_update_email
bo >> no_update_email