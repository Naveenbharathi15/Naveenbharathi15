from datetime import datetime,time,timedelta,date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.utils.dates import days_ago
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.contrib.operators import gcs_to_bq
import pandas as pd
from google.cloud import bigquery,storage
import pandas_gbq
import traceback
import numpy as np
import os
import ast
import os.path

today = date.today()
GCS_BUCKET = 're-prod-data-env'
GCP_CONN_ID = "google_cloud_default"
GCP_PROJECT = "re-warranty-analytics-prod"
BQ_DATASET = "re_wa_prod"
BQ_SOURCE_TABLE = "re_dms_delta_table"
BQ_SOURCE_TABLE2 = "re_ppap_part_present_in_dms_table"
BQ_DESTINATION_TABLE = "re_trigger_cutoff_ppap_usecase_table"
GCS_FILE_PATH = f'poc-data/PPAP/Delta-Data/usecase/ppap-usecase-data-{today}.csv'
LOCAL_FILE = f'ppap-usecase-data-{today}.csv'
GCP_SOURCE_OBJECTS = [f'poc-data/PPAP/Delta-Data/usecase/ppap-usecase-data-{today}.csv']
JC_FILE = f'poc-data/DMS/Delta-Data/dms-{today}.csv'

con_df = pd.DataFrame()
part_df = pd.DataFrame()


def jc_today():
    global jc_df
    bq_hook = BigQueryHook(bigquery_conn_id=GCP_CONN_ID, delegate_to=None, use_legacy_sql=False)
    bq_client = bigquery.Client(project=bq_hook._get_field(GCP_PROJECT), credentials=bq_hook._get_credentials())

    sql = f"""
                SELECT VehicleChassisNumber, JobCard, Zone, Region, State, DealerName, DealerCode, City, CreatedOn,
                PartNumber, PartDescription, PartGroup, PartDefectCode, BillType, ModelID, ModelType, Kilometers,
                KmsGroup, MfgMonth, MfgYear
                FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_SOURCE_TABLE}` ;
                """

    jc_df = bq_client.query(sql).to_dataframe()


def concession_report(part_details,model,mfg_date,papp_no,papp_title,ppap_type,ppap_status,ppap_plant,process,purpose,plant):
    jc_today()
    global part_df
    parts = ast.literal_eval(part_details)
    mfg_date = str(mfg_date)
    model = ast.literal_eval(model)

    for indx, j in enumerate(parts):

        temp_part_df=part_df[part_df['MfgDate'] > datetime.strptime(mfg_date, '%Y-%m-%d').date()]
        temp_part_list=temp_part_df['PartDetailsNew'].tolist()
        temp_mfgdate_list = temp_part_df['MfgDate'].tolist()
        tpi_bool=0
        tpi_bool_value=""

        for tpi, tpiv in enumerate(temp_part_list):

            if j in ast.literal_eval(tpiv):
                tpi_bool=1
                tpi_bool_value=temp_mfgdate_list[tpi]
                break

        if tpi_bool:
            jc_df = jc_df[jc_df['PartNumber'] == j]
            jc_df = jc_df[jc_df['ModelID'].isin(model)]
            jc_df['MfgDate'] = pd.to_datetime(jc_df['MfgDate']).dt.date
            x = datetime.datetime.strptime(mfg_date, '%Y-%m-%d').date()
            y = datetime.datetime.strptime(tpi_bool_value, '%Y-%m-%d').date()
            jc_df = jc_df[jc_df['MfgDate'] >= x]
            jc_df = jc_df[jc_df['MfgDate'] < y]
        else:
            jc_df = jc_df[jc_df['PartNumber'] == j]
            jc_df = jc_df[jc_df['ModelID'].isin(model)]
            jc_df['MfgDate'] = pd.to_datetime(jc_df['MfgDate']).dt.date
            x = datetime.datetime.strptime(mfg_date, '%Y-%m-%d').date()
            jc_df = jc_df[jc_df['MfgDate'] >= x]

        jc_df.drop_duplicates(subset="JobCard", inplace=True)
        global con_df

        if jc_df.shape[0]:
            jc_df['PPAPNumber'] = papp_no
            jc_df['PPAPTitle'] = papp_title
            jc_df['PPAPType'] = ppap_type
            jc_df['PPAPStatus'] = ppap_status
            jc_df['PPAPPlant'] = ppap_plant
            jc_df['ProcessArea'] = process
            jc_df['Purpose'] = purpose
            jc_df['MfgDate'] = mfg_date
            con_df = con_df.append(jc_df, ignore_index=True)


def transformation():

    today = date.today()
    JOBCARD_FILE_STATUS_BLOB = f'poc-data/DMS/Delta-Data/dms-{today}.csv'
    # PPAP_FILE_STATUS_BLOB = f'poc-data/PPAP/Delta-Data/plm-ppap-{today}.csv'

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    JOBCARD_FILE_STATUS = storage.Blob(bucket=bucket, name=JOBCARD_FILE_STATUS_BLOB).exists(storage_client)
    # PPAP_FILE_STATUS = storage.Blob(bucket=bucket, name=PPAP_FILE_STATUS_BLOB).exists(storage_client)

    # if JOBCARD_FILE_STATUS and PPAP_FILE_STATUS:
    if JOBCARD_FILE_STATUS:
        global part_df
        global con_df
        
        sql = f"""
                SELECT *
                FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_SOURCE_TABLE2}`                
                order by MfgDate asc;
                """

        bq_hook = BigQueryHook(bigquery_conn_id=GCP_CONN_ID, delegate_to=None, use_legacy_sql=False)
        bq_client = bigquery.Client(project=bq_hook._get_field(GCP_PROJECT), credentials=bq_hook._get_credentials())
        part_df = bq_client.query(sql).to_dataframe()

        part_df["MfgDate"] = pd.to_datetime(part_df["MfgDate"]).dt.date
        part_df = part_df.sort_values(by=['MfgDate'], ascending=True)

        df = part_df.copy()
        for index, row in df.iterrows():
            concession_report(row["PartDetailsNew"],row["ModelDetails"],row["MfgDate"],row['PPAPNumber'],row['PPAPTitle'],row['PPAPType'],row['PPAPStatus'],row['PPAPPlant'],row['ProcessArea'],row['Purpose'],row['PPAPPlant'])

        if con_df.shape[0]:
            con_df['PiperrInDate'] = pd.Timestamp.today().date()
            con_df.to_csv(LOCAL_FILE, header=True, index=False)
            print("wrote data to localfile")
            print(f"File Exits flag = {os.path.exists(LOCAL_FILE)}")

        else:
            print("No Records in con_df df")

    else:
        print(f"Skipping as JOBCARD_FILE_STATUS is {JOBCARD_FILE_STATUS}")
        # print(f"Skipping as MANUAL_FILE_STATUS is {PPAP_FILE_STATUS}")
        print(f"File Exits flag = {os.path.exists(LOCAL_FILE)}")


def delete_local_files():

    print(f'delete_local_files start')
    os.remove(LOCAL_FILE)
    print(f'delete_local_files end')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 20),
    'email': ['rakeshaswath@saturam.com','naveen@saturam.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

dag = DAG(
    'TRIGGER_CUTOFF_PPAP_USECASE_DATA_INGESTION',
    default_args=default_args,
    description='Trigger Cutoff PPAP Metric',
    schedule_interval="15 3 * * *",
)

trigger_cutoff = PythonOperator(
    task_id='trigger_cutoff_ppap_metric',
    python_callable=transformation,
    dag=dag,
)

gcs_upload = LocalFilesystemToGCSOperator(
    task_id="PPAP_usecase_data_upload_file",
    src=LOCAL_FILE,
    dst=GCS_FILE_PATH,
    bucket=GCS_BUCKET,
    mime_type='text/csv',
    google_cloud_storage_conn_id='google_cloud_default',
    dag=dag
)

gcs_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='PPAP_Usecase_Data_GCS_to_Bigquery',
    bucket=GCS_BUCKET,
    source_objects = GCP_SOURCE_OBJECTS,
    schema_fields=[
    {"name": "VehicleChassisNumber", "type": "STRING"},
    {"name": "JobCard", "type": "STRING"},
    {"name": "Zone", "type": "STRING"},
    {"name": "Region", "type": "STRING"},
    {"name": "State", "type": "STRING"},
    {"name": "DealerName", "type": "STRING"},
    {"name": "DealerCode", "type": "STRING"},
    {"name": "City", "type": "STRING"},
    {"name": "CreatedOn", "type": "DATE"},
    {"name": "PartNumber", "type": "STRING"},
    {"name": "PartDescription", "type": "STRING"},
    {"name": "PartGroup", "type": "STRING"},
    {"name": "PartDefectCode", "type": "STRING"},
    {"name": "BillType", "type": "STRING"},
    {"name": "ModelID", "type": "STRING"},
    {"name": "ModelType", "type": "STRING"},
    {"name": "Kilometers", "type": "INTEGER"},
    {"name": "KmsGroup", "type": "STRING"},
    {"name": "MfgMonth", "type": "INTEGER"},
    {"name": "MfgYear", "type": "INTEGER"},
    {"name": "PPAPNumber", "type": "STRING"},
    {"name": "PPAPTitle", "type": "STRING"},
    {"name": "PPAPType", "type": "STRING"},
    {"name": "PPAPStatus", "type": "STRING"},
    {"name": "PPAPPlant", "type": "STRING"},
    {"name": "ProcessArea", "type": "STRING"},
    {"name": "Purpose", "type": "STRING"},
    {"name": "MfgDate", "type": "DATE"},
    {"name": "PiperrInDate", "type": "DATE"}
 ],
    destination_project_dataset_table=f'{GCP_PROJECT}.{BQ_DATASET}.{BQ_DESTINATION_TABLE}',
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    allow_quoted_newlines=True,
    google_cloud_storage_conn_id=GCP_CONN_ID,
    bigquery_conn_id=GCP_CONN_ID,
    dag = dag
)

delete = PythonOperator(
        task_id='PPAP_delete_local_file',
        python_callable=delete_local_files,
        dag=dag
    )

email_success = EmailOperator(
        task_id='send_email',
        to=["rakeshaswath@saturam.com","naveen@saturam.com"],
        subject='Trigger Cutoff PPAP Use case Success',
        html_content=""" <h3>Trigger Cutoff PPAP Use case metric performed and loaded into BQ Table</h3> """,
        dag=dag
)

trigger_cutoff >> gcs_upload >> gcs_bq >> delete >> email_success
