from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.utils.dates import days_ago
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
import pandas as pd
from google.cloud import bigquery,storage
import pandas_gbq
import traceback
import os

GCP_CONN_ID = "google_cloud_default"
GCP_PROJECT = "re-warranty-analytics-prod"
BQ_DATASET = "re_wa_prod"
BQ_SOURCE_TABLE = "re_dms_table_v1"
BQ_DESTINATION_TABLE = "re_unplanned_visits_usecase_table_v1"
BQ_DESTINATION_TABLE2 = "re_unplanned_visits_percentage_usecase_table_v1"
PART_IGNORE_FILE="part_ignore_list.csv"
AZURE_BLOB_PART_IGNORE_FILE = f'poc-data/MANUAL-DATA/{PART_IGNORE_FILE}'
GENERAL_SERVICE_FILE="general_scheduled_service.csv"
AZURE_BLOB_GENERAL_SERVICE_FILE = f'poc-data/MANUAL-DATA/{GENERAL_SERVICE_FILE}'
AZURE_CONTAINER = "175-warranty-analytics"
GCS_BUCKET = 're-prod-data-env'


def check_gcs():
    today = date.today()
    JOBCARD_FILE_STATUS_BLOB = f'poc-data/DMS/Delta-Data/dms-{today}.csv'
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    return storage.Blob(bucket=bucket, name=JOBCARD_FILE_STATUS_BLOB).exists(storage_client)


def service(kms, diff, ModelName):
    if ModelName is not None:
        if ('INTERCEPTOR 650' in ModelName.upper()) or ('CONTINENTAL 650' in ModelName.upper()):

            if (kms >= 0 and kms <= 800) or (diff >= 0 and diff <= 75):
                return 'Before Service-1'
            elif (kms > 800 and kms <= 10500) or (diff > 75 and diff <= 405):
                return 'Between Service-1 & Service-2'
            elif (kms > 10500 and kms <= 20600) or (diff > 405 and diff <= 780):
                return 'Between Service-2 & Service-3'
            elif (kms > 20600 and kms <= 30700) or (diff > 780 and diff <= 1145):
                return 'Between Service-3 & Service-4'
            elif (kms > 30700) or (diff > 1145):
                return 'After Service-4'
            else:
                return 'NA'

        else:

            if (kms >= 0 and kms <= 800) or (diff >= 0 and diff <= 75):
                return 'Before Service-1'
            elif (kms > 800 and kms <= 5500) or (diff > 75 and diff <= 220):
                return 'Between Service-1 & Service-2'
            elif (kms > 5500 and kms <= 10600) or (diff > 220 and diff <= 410):
                return 'Between Service-2 & Service-3'
            elif (kms > 10600 and kms <= 15700) or (diff > 410 and diff <= 590):
                return 'Between Service-3 & Service-4'
            elif (kms > 15700) or (diff > 590):
                return 'After Service-4'
            else:
                return 'NA'
    else:
        return None


def age(diff):

    if (diff >= 0 and diff <= 90):
        return '(A) 0 - 3 Months'
    elif (diff > 90 and diff <= 365):
        return '(B) 3 - 12 Months'
    elif (diff > 365 and diff <= 730):
        return '(C) 12 - 24 Months'
    elif (diff > 730 and diff <= 1095):
        return '(D) 24 - 36 Months'
    elif (diff > 1095):
        return '(E) 36 Months & Above'
    else:
        return 'NA'


def get_df():

    if check_gcs():

        bq_hook = BigQueryHook(bigquery_conn_id=GCP_CONN_ID, delegate_to=None, use_legacy_sql=False)
        bq_client = bigquery.Client(project=bq_hook._get_field(GCP_PROJECT), credentials=bq_hook._get_credentials())

        az_hook = WasbHook(wasb_conn_id='wasb_default')
        az_hook.get_file(PART_IGNORE_FILE, container_name=AZURE_CONTAINER, blob_name=AZURE_BLOB_PART_IGNORE_FILE)
        part_no_df = pd.read_csv(PART_IGNORE_FILE, usecols=['Part No', 'Repeat/Repair Service'])
        os.remove(PART_IGNORE_FILE)
        PART_EXCLUDE = part_no_df[part_no_df['Repeat/Repair Service'] == 'Yes']['Part No'].tolist()

        az_hook.get_file(GENERAL_SERVICE_FILE, container_name=AZURE_CONTAINER,
                         blob_name=AZURE_BLOB_GENERAL_SERVICE_FILE)
        general_service_df = pd.read_csv(GENERAL_SERVICE_FILE, usecols=['Labor code'])
        os.remove(GENERAL_SERVICE_FILE)
        FREE_SERVICE_LABOR_CODES = general_service_df['Labor code'].tolist()

        sql = f"""
                SELECT VehicleChassisNumber,JobCard,PartNumber,PartDescription, ItemType,PartAction,PartGroup,PartVariant, PartCategory, PartDefectCode, JobSubType, BillType, CustomerComplaint,
                Zone,Region,State,DealerCode,DealerName,City,Kilometers, KmsGroup, 
                CreatedOn AS ServiceDate, Model, ModelType, ModelName, CAST(DateofSale AS DATE) AS SalesDate, DATETIME_DIFF(CreatedOn, DateofSale,DAY) as DaysDifference,
                Plant, MfgMonth, MfgYear,CreatedOnYear, CreatedOnMonth,  CURRENT_DATE() as PiperrInDate,
                "ss" as ServiceSchedule, "va" as VehicleAge, FORMAT_DATE("%b-%Y", CreatedOn) as JCMonthNameYear, 
                FORMAT_DATE("%Y-%m", CreatedOn) as JCMonthYear, "unplanned_services" as BreakPoint
                FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_SOURCE_TABLE}` 
                where PiperrInDate = CURRENT_DATE() and length(VehicleChassisNumber)=17 and length(JobCard) = 16 and ModelType not like 'NA' and 
                Model not in ('U','NA','MACHISMO', '', 'TAURUS','ROYAL','DSW') and Plant not in ('NA') and PartAction is null and 
                ItemType like '%Part%' and PrimaryConsequential like '%Primary%' and PartNumber is not null and PartDescription is not null and Country like 'Domestic' and MfgYear !=1900 and JobCard not in 
                (select distinct JobCard FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_SOURCE_TABLE}` 
                where PiperrInDate = CURRENT_DATE() and PartNumber in {tuple(FREE_SERVICE_LABOR_CODES)}
                ); 
        """
        df = bq_client.query(sql).to_dataframe()
        print("Read SQL and converted to df !!")

        table_schema = [
            {'name': 'VehicleChassisNumber', 'type': 'STRING'},
            {'name': 'JobCard', 'type': 'STRING'},
            {'name': 'PartNumber', 'type': 'STRING'},
            {'name': 'PartDescription', 'type': 'STRING'},
            {'name': 'ItemType', 'type': 'STRING'},
            {'name': 'PartAction', 'type': 'STRING'},
            {'name': 'PartGroup', 'type': 'STRING'},
            {'name': 'PartVariant', 'type': 'STRING'},
            {'name': 'PartCategory', 'type': 'STRING'},
            {'name': 'PartDefectCode', 'type': 'STRING'},
            {'name': 'JobSubType', 'type': 'STRING'},
            {'name': 'BillType', 'type': 'STRING'},
            {'name': 'CustomerComplaint', 'type': 'STRING'},
            {'name': 'Zone', 'type': 'STRING'},
            {'name': 'Region', 'type': 'STRING'},
            {'name': 'State', 'type': 'STRING'},
            {'name': 'DealerCode', 'type': 'STRING'},
            {'name': 'DealerName', 'type': 'STRING'},
            {'name': 'City', 'type': 'STRING'},
            {'name': 'Kilometers', 'type': 'INTEGER'},
            {'name': 'KmsGroup', 'type': 'STRING'},
            {'name': 'ServiceDate', 'type': 'DATE'},
            {'name': 'Model', 'type': 'STRING'},
            {'name': 'ModelType', 'type': 'STRING'},
            {'name': 'ModelName', 'type': 'STRING'},
            {'name': 'SalesDate', 'type': 'DATE'},
            {'name': 'DaysDifference', 'type': 'INTEGER'},
            {'name': 'Plant', 'type': 'STRING'},
            {'name': 'MfgMonth', 'type': 'INTEGER'},
            {'name': 'MfgYear', 'type': 'INTEGER'},
            {'name': 'MfgMonthName', 'type': 'STRING'},
            {'name': 'MfgDateQuaters', 'type': 'STRING'},
            {'name': 'MfgDateQuatersInNumbers', 'type': 'INTEGER'},
            {'name': 'CreatedOnYear', 'type': 'INTEGER'},
            {'name': 'CreatedOnMonth', 'type': 'INTEGER'},
            {'name': 'CreatedOnMonthName', 'type': 'STRING'},
            {'name': 'CreatedOnQuaters', 'type': 'STRING'},
            {'name': 'CreatedOnQuatersInNumbers', 'type': 'INTEGER'},
            {'name': 'PiperrInDate', 'type': 'DATE'},
            {'name': 'ServiceSchedule', 'type': 'STRING'},
            {'name': 'VehicleAge', 'type': 'STRING'},
            {'name': 'JCMonthNameYear', 'type': 'STRING'},
            {'name': 'JCMonthYear', 'type': 'STRING'},
            {'name': 'BreakPoint', 'type': 'STRING'}
        ]

        if df.shape[0] > 0:

            df = df[~(df['PartNumber'].isin(PART_EXCLUDE))]
            df['ServiceSchedule'] = df.apply(lambda x: service(x['Kilometers'], x['DaysDifference'], x['ModelName']),
                                             axis=1)
            print("ServiceSchedule Complete !!")
            df['VehicleAge'] = df.apply(lambda x: age(x['DaysDifference']), axis=1)
            print("VehicleAge Complete !!")
            df['DaysDifference'] = df['DaysDifference'].fillna(-1)
            df['DaysDifference'] = df['DaysDifference'].astype(int)
            df['MfgMonthYear'] = pd.to_datetime(dict(year=df.MfgYear, month=df.MfgMonth, day=1)).dt.strftime("%Y-%m")
            df['MfgMonthNameYear'] = pd.to_datetime(dict(year=df.MfgYear, month=df.MfgMonth, day=1)).dt.strftime(
                "%b-%Y")

            pandas_gbq.to_gbq(df, f'{BQ_DATASET}.{BQ_DESTINATION_TABLE}', project_id=f'{GCP_PROJECT}',
                              if_exists='append', credentials=bq_hook._get_credentials(),table_schema=table_schema)
            print(f"Loaded {df.shape[0]} rows into BQ Table 1 !!")

            df['DMart'] = "unplanned table"
            pandas_gbq.to_gbq(df, f'{BQ_DATASET}.{BQ_DESTINATION_TABLE2}', project_id=f'{GCP_PROJECT}',
                              if_exists='append', credentials=bq_hook._get_credentials(), table_schema=table_schema)
            print(f"Loaded {df.shape[0]} rows into BQ Table 2!!")

        dms_sql = f"""
                SELECT VehicleChassisNumber, JobCard, PartNumber, PartDescription, ItemType, PartAction,PartGroup, PartVariant, PartCategory, PartDefectCode, 
                JobSubType, BillType, CustomerComplaint, Zone, Region, State, DealerCode, DealerName, City, Kilometers, KmsGroup, 
                CreatedOn as ServiceDate, Model, ModelType, ModelName, CAST(DateofSale AS DATE) AS SalesDate, 
                DATETIME_DIFF(CreatedOn, DateofSale,DAY) as DaysDifference, Plant, MfgMonth, MfgYear, CreatedOnYear, CreatedOnMonth,  CURRENT_DATE() as PiperrInDate,
                "ss" as ServiceSchedule, "va" as VehicleAge, FORMAT_DATE("%b-%Y", CreatedOn) as JCMonthNameYear, 
                FORMAT_DATE("%Y-%m", CreatedOn) as JCMonthYear, "unplanned_services" as BreakPoint
                FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_SOURCE_TABLE}`
                where PiperrInDate = CURRENT_DATE() and JobSubType not in ('ACCIDENTAL','PDI'); 
                        """
        dms = bq_client.query(dms_sql).to_dataframe()
        # print(dms.shape)
        # print(dms.columns)

        if dms.shape[0] > 0:
            dms['ServiceSchedule'] = dms.apply(
                lambda x: service(x['Kilometers'], x['DaysDifference'], x['ModelName']), axis=1)
            print("ServiceSchedule Complete !!")
            dms['VehicleAge'] = dms.apply(lambda x: age(x['DaysDifference']), axis=1)
            print("VehicleAge Complete !!")
            dms['DaysDifference'] = dms['DaysDifference'].fillna(-1)
            dms['DaysDifference'] = dms['DaysDifference'].astype(int)
            dms['MfgMonthYear'] = pd.to_datetime(dict(year=dms.MfgYear, month=dms.MfgMonth, day=1)).dt.strftime(
                "%Y-%m")
            dms['MfgMonthNameYear'] = pd.to_datetime(
                dict(year=dms.MfgYear, month=dms.MfgMonth, day=1)).dt.strftime("%b-%Y")
            dms['DMart'] = "dms raw table"

            pandas_gbq.to_gbq(dms, f'{BQ_DATASET}.{BQ_DESTINATION_TABLE2}', project_id=f'{GCP_PROJECT}',
                              if_exists='append', credentials=bq_hook._get_credentials(), table_schema=table_schema)
            print(f"Loaded {dms.shape[0]} rows into BQ Table 2!!")

    else:
        print(f"Skipping !!")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 2),
    'email': ['rakeshaswath@saturam.com','naveen@saturam.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

dag = DAG(
    'UNPLANNED_VISITS_USECASE_data_ingestion',
    default_args=default_args,
    description='Unplanned Visits Metric',
    # schedule_interval=timedelta(days=1),
    schedule_interval="0 2 * * *",
)

unplanned = PythonOperator(
    task_id='unplanned_service',
    python_callable=get_df,
    dag=dag,
)

email_success = EmailOperator(
        task_id='send_email',
        to=["rakeshaswath@saturam.com","naveen@saturam.com"],
        subject='Unplanned Visits Use case Success',
        html_content=""" <h3>Unplanned Visits Use case metric performed and loaded into BQ Table</h3> """,
        dag=dag
)

unplanned >> email_success


