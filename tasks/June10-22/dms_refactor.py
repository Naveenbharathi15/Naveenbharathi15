import time
import os
import pandas as pd
import traceback
from datetime import date, datetime, timedelta
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python_operator import BranchPythonOperator

from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators import gcs_to_bq
from airflow.utils.dates import days_ago

import smtplib
import mimetypes
import email
import email.mime.application
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

today = date.today()
PART_GROUP_FILE = 'part_group.csv'
PART_GROUP_BLOB = f'poc-data/MANUAL-DATA/{PART_GROUP_FILE}'
DEALER_MASTER_FILE = 'dealer_master.csv'
DEALER_MASTER_BLOB = f'poc-data/MANUAL-DATA/{DEALER_MASTER_FILE}'
LOCAL_FILE = f'dms-{today}.csv'
AZURE_BLOB = f'poc-data/DMS/Delta-Data/dms-{today}.csv'
GCS_FILE_PATH = f'poc-data/DMS/Delta-Data/dms-{today}.csv'
AZURE_CONTAINER = "175-warranty-analytics"
GCS_BUCKET = 're-prod-data-env'

GCP_CONN_ID = "google_cloud_default"
GCP_PROJECT = "re-warranty-analytics-prod"
BQ_DATASET = "re_wa_prod"
BQ_TABLE = "re_dms_table_v1"
BQ_TABLE2 = "re_dms_delta_table"


def initialize():
    global today
    today = date.today()

    global LOCAL_FILE
    LOCAL_FILE = f'dms-{today}.csv'

    global AZURE_BLOB
    AZURE_BLOB = f'poc-data/DMS/Delta-Data/dms-{today}.csv'

    global GCS_FILE_PATH
    GCS_FILE_PATH = f'poc-data/DMS/Delta-Data/dms-{today}.csv'


def check_blob():

    az_hook = WasbHook(wasb_conn_id='wasb_default')
    return az_hook.check_for_blob(container_name=AZURE_CONTAINER, blob_name=AZURE_BLOB)


def send_mail_file_status():
    user = 'royalenfield'
    password = os.environ["SMTP_PWD"]
    to =[ "zzhemant@royalenfield.com"]
    cc =[ "zzrakeshaswath@royalenfield.com,rakeshaswath@saturam.com,ram@saturam.com,naveen@saturam.com,subanand@royalenfield.com,jayaprakash@royalenfield.com,sureshe@royalenfield.com,jgokul@royalenfield.com"]
    sender = 'noreply@royalenfield.com'

    msg = MIMEMultipart()
    msg['Subject'] = f'DMS JobCard Data - Delta file ({today})'
    msg['From'] = sender
    msg['To'] = ','.join(to)
    msg['Cc'] = ','.join(cc)

    body = MIMEText(
        f"""Hi Hemant,\n\nDMS JobCard Data - Delta file ({today}) is not uploaded into AZURE BLOB.\nPlease fix the issue and re-upload the file\n\n\nThank You. """)
    msg.attach(body)

    s = smtplib.SMTP("smtpbp.falconide.com", 587)
    s.starttls()

    s.login(user, password)
    s.sendmail(sender, (to+cc), msg.as_string())
    s.quit()


def fetch_data_from_blob():

        az_hook = WasbHook(wasb_conn_id='wasb_default')
        az_hook.get_file(LOCAL_FILE, container_name=AZURE_CONTAINER, blob_name=AZURE_BLOB)
        az_hook.get_file(PART_GROUP_FILE, container_name=AZURE_CONTAINER, blob_name=PART_GROUP_BLOB)
        az_hook.get_file(DEALER_MASTER_FILE, container_name=AZURE_CONTAINER, blob_name=DEALER_MASTER_BLOB)


def modelType(test, model):
    if test == 'U':
        return 'UCE'
    elif test == 'D':
        return 'HIMALAYAN'
    elif test == 'P':
        return 'TWINS'
    elif test == 'J' and 'CLASSIC' in model:
        return 'NEW CLASSIC'
    elif test == 'J':
        return 'METEOR'
    else:
        return 'NA'


def modelClassic(test, product, model):
    if test == 'J' and 'CLASSIC' in product:
        return 'NEW CLASSIC'
    else:
        return model


def modelName(x, y):
    if y == '3':
        return x + ' ' + '350'
    elif y == '4':
        return x + ' ' + '410'
    elif y == '5':
        return x + ' ' + '500'
    elif y == '6':
        return x + ' ' + '535'
    elif y == '7':
        return x + ' ' + '650'
    else:
        return 'NA'


def plant(test, test1, test2):
    if test == 'U' or test == 'D':
        if test1 == '0':
            return 'TVT'
        elif test1 == '1':
            return 'ORG'
        elif test1 == '2':
            return 'VLM'
        else:
            return 'NA'
    elif test == 'P' or test == 'J':
        if test2 == '0':
            return 'TVT'
        elif test2 == '1':
            return 'ORG'
        elif test2 == '2':
            return 'VLM'
        else:
            return 'NA'
    else:
        return 'NA'


def mfgMonth(test, test1, test2):
    if test == 'U' or test == 'D':

        temp1 = mfg_mon.get(test2)
        if temp1:
            return temp1
        else:
            return 1
    elif test == 'P' or test == 'J':
        temp2 = mfg_mon.get(test1)
        if temp2:
            return temp2
        else:
            return 1
    else:
        return 1


def kmsGroup(kms):
    if (kms >= 0 and kms <= 500):
        return '(A) 0 - 500'

    elif (kms > 500 and kms <= 3000):
        return '(B) 500 - 3000'

    elif (kms > 3000 and kms <= 5000):
        return '(C) 3000 - 5000'

    elif (kms > 5000 and kms <= 10000):
        return '(D) 5000 - 10000'

    elif (kms > 10000 and kms <= 15000):
        return '(E) 10000 - 15000'

    elif (kms > 15000 and kms <= 20000):
        return '(F) 15000 - 20000'

    elif (kms > 20000 and kms <= 25000):
        return '(G) 20000 - 25000'

    elif (kms > 25000 and kms <= 30000):
        return '(H) 25000 - 30000'

    elif kms > 30000:
        return '(I) 30000 & Above'

    else:
        return 'NA'


def transformation():

        global t1
        t1 = time.time()
        df = pd.read_csv(LOCAL_FILE, low_memory=False,
                         dtype={'Job_Card': str, 'Part/Labour_Number': str, 'Part/Labour_Description': str,
                                'ItemType': str, 'Observation': str,
                                'Requested_Quantity': float, 'Issued_Quantity_New': float, 'Is_Repeated': str,
                                'DefectID': str,
                                'Defect_code': str, 'primary/Consequential': str, 'Primary_Part': str,
                                'PrimaryPartNumber': str, 'Part_Action': str, 'Status_Reason': str,
                                'Customer_Complaint': str, 'Customer_Voice': str,
                                'Region': str, 'Registration_Number_Customer_Asset': str,
                                'Vehicle_Chassis_Number': str,
                                'Job_Type': str, 'JobSubType': str,
                                'State': str, 'Zone': str, 'Dealer city': str, 'Name_store_account': str,
                                'Final_Customer': str,
                                'Bill_Type': str, 'Product': str, 'RepairCategory': str, 'part_batch_code': str,
                                'Model': str, 'Dealercode': str, 'PARTPRICE': float, 'LineAmount': float,
                                'Technical Observation': str,
                                'ComplaintObservedbyDealer': str, 'CorrectiveActiontaken': str,
                                'Status': str}
                         )
        os.remove(LOCAL_FILE)
        df.columns = ['JobCard', 'PartNumber', 'LineAmount', 'PartDescription', 'ItemType', 'Observation',
                      'RequestedQuantity',
                      'IssuedQuantityNew',
                      'IsRepeated', 'PartDefectID', 'PartDefectCode', 'PrimaryConsequential',
                      'PrimaryPart',
                      'PrimaryPartNumber', 'PartAction', 'StatusReason',
                      'CreatedOnDateTime', 'CustomerComplaint', 'CustomerVoice',
                      'CashierInvoicingDateTime', 'Region',
                      'RegistrationNumber',
                      'VehicleChassisNumber', 'JobType', 'JobSubType', 'Kilometers', 'State', 'Zone', 'City',
                      'DealerName',
                      'FinalCustomer', 'BillType', 'DateofSale', 'Product', 'RepairCategory',
                      'PartBatchCode',
                      'ModelID', 'DealerCode', 'FieldObservationReport',
                      'TechnicalObservation',
                      'ComplaintObservedByDealer', 'CorrectiveActiontaken', 'JobCardStatus']

        df.drop(['City'], inplace=True, axis=1)

        df['CreatedOnDateTime'] = pd.to_datetime(df['CreatedOnDateTime'])
        df['CashierInvoicingDateTime'] = pd.to_datetime(df['CashierInvoicingDateTime'])
        df['DateofSale'] = pd.to_datetime(df['DateofSale'])

        df['CreatedOn'] = df['CreatedOnDateTime'].dt.date
        df['CreatedOnYear'] = df['CreatedOnDateTime'].dt.year
        df['CreatedOnMonth'] = df['CreatedOnDateTime'].dt.month

        df['PiperrInDate'] = date.today()

        df['Product'] = df['Product'].str.upper()
        df['Product'] = df['Product'].fillna("")
        df['Model'] = df['Product'].str.split().str[0]
        df['Model'] = df['Model'].str.upper()
        df['Model'] = df['Model'].str.split(',').str[0]
        df['Model'] = df['Model'].str.split('\d+').str[0]
        df['Model'] = df['Model'].str.split('-').str[0]
        df['Model'] = df['Model'].map(lambda x: x if isinstance(x, str) else '')
        df.loc[df['Model'].str.contains("CONTINENTAL"), 'Model'] = 'CONTINENTAL'
        df.loc[df['Model'] == 'CL', 'Model'] = 'CLASSIC'
        # df.loc[df['Model']=='SIGNALS','Model'] = 'METEOR'
        df.loc[df['Model'] == 'SIGNALS', 'Model'] = 'CLASSIC'

        df['VehicleChassisNumber'] = df['VehicleChassisNumber'].str.upper()
        df['VehicleChassisNumber'] = df['VehicleChassisNumber'].fillna("")
        df['U/D'] = df['VehicleChassisNumber'].str[3:4]
        df['CC'] = df['VehicleChassisNumber'].str[4:5]
        df['PlantOrMonth'] = df['VehicleChassisNumber'].str[8:9]
        df['ManufacturingYear'] = df['VehicleChassisNumber'].str[9:10]
        df['MonthOrPlant'] = df['VehicleChassisNumber'].str[10:11]

        df['ModelType'] = df.apply(
            lambda x: modelType(x['U/D'], x['Product']) if len(x['VehicleChassisNumber']) == 17 and x[
                'Product'] else 'NA', axis=1)

        df['Model'] = df.apply(
            lambda x: modelClassic(x['U/D'], x['Product'], x['Model']) if len(x['VehicleChassisNumber']) == 17 and x[
                'Product'] else x['Model'], axis=1)

        # df['ModelType'] = df.apply(lambda x: modelType(x['U/D'], x['Product']), axis=1)
        #
        # df['Model'] = df.apply(lambda x: modelClassic(x['U/D'], x['Product']), axis=1)

        df['ModelName'] = df.apply(lambda x: modelName(x['Model'], x['CC']), axis=1)

        df['Plant'] = df.apply(lambda x: plant(x['U/D'], x['PlantOrMonth'], x['MonthOrPlant']), axis=1)

        global mfg_mon
        mfg_mon = {'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5, 'F': 6, 'G': 7, 'H': 8, 'K': 9, 'L': 10, 'M': 11, 'N': 12}
        df['MfgMonth'] = df.apply(lambda x: mfgMonth(x['U/D'], x['PlantOrMonth'], x['MonthOrPlant']), axis=1)

        mfg_year = {'A': 2010, 'B': 2011, 'C': 2012, 'D': 2013, 'E': 2014, 'F': 2015, 'G': 2016, 'H': 2017, 'J': 2018,
                    'K': 2019, 'L': 2020, 'M': 2021, 'N': 2022, 'O': 2023}
        df['MfgYear'] = df.apply(
            lambda x: mfg_year.get(x['ManufacturingYear']) if mfg_year.get(x['ManufacturingYear']) else 1900, axis=1)

        df['MfgDate'] = pd.to_datetime(dict(year=df.MfgYear, month=df.MfgMonth, day=1))
        df['Country'] = df.apply(lambda x: 'Domestic' if x['CC'] in ['3', '4', '5', '6', '7'] else 'International',
                                 axis=1)
        df["Kilometers"] = df["Kilometers"].fillna("-1").astype(int)
        df['KmsGroup'] = df.apply(lambda x: kmsGroup(x['Kilometers']), axis=1)
        df.drop(['U/D', 'CC', 'PlantOrMonth', 'ManufacturingYear', 'MonthOrPlant'], inplace=True, axis=1)

        global data
        data1 = pd.read_csv(PART_GROUP_FILE)
        data2 = pd.read_csv(PART_GROUP_FILE)
        os.remove(PART_GROUP_FILE)

        data1.columns = ['PartNumber', 'PartName', 'PartGroup', 'PartVariant', 'PartCategory','ModelType']
        data1.drop(['PartName'], inplace=True, axis=1)
        data1 = data1[~(data1['PartNumber'].isna())]
        data1.drop_duplicates(subset=['PartNumber'], inplace=True)
        df = pd.merge(df, data1, on=['ModelType','PartNumber'], how='left')

        df['PrimaryPart'] = df['PrimaryPart'].fillna("")
        df['PrimaryPart'] = df['PrimaryPart'].astype(str)
        df['PrimaryPartNew'] = df['PrimaryPart'].str.split('RJC').str[0]
        df['PrimaryPartNew'] = df['PrimaryPartNew'].fillna("")
        df['PrimaryPartNew'] = df['PrimaryPartNew'].astype(str)
        df['PrimaryPartFilter']=df['PrimaryPartNew'].str.encode('ascii', 'ignore').str.decode('ascii')
        df['PartFilter']=df['PartDescription'].str.encode('ascii', 'ignore').str.decode('ascii')
        data2.columns = ['PrimaryPartNumber', 'PrimaryPartNew', 'PrimaryPartGroup', 'PrimaryPartVariant',
                         'PrimaryPartCategory','ModelType']
        data2.drop(['PrimaryPartNew', 'PrimaryPartVariant', 'PrimaryPartCategory'], inplace=True, axis=1)
        data2 = data2[~(data2['PrimaryPartNumber'].isna())]
        data2.drop_duplicates(subset=['PrimaryPartNumber'], inplace=True)
        df = pd.merge(df, data2, on=['ModelType','PrimaryPartNumber'], how='left')


        data3 = pd.read_csv(DEALER_MASTER_FILE)
        os.remove(DEALER_MASTER_FILE)
        data3 = data3[~(data3['Code'].isna())]
        data3 = data3[['Code', 'CityCode']]
        data3.columns = ['DealerCode', 'City']
        data3.drop_duplicates(subset=['DealerCode'], inplace=True)
        df = pd.merge(df, data3, on='DealerCode', how='left')
        df['City'] = df['City'].str.upper()

        df.to_csv(LOCAL_FILE, header=True, index=False)

        global t2
        t2 = time.time()


def delete_local_files():
        os.remove(LOCAL_FILE)


def choose_tasks():
    initialize()
    if check_blob():
        fetch_data_from_blob()
        transformation()
        return ['local_to_gcs','delete_local_file','gcs_to_bigquery','send_email_on_success']
    else:
        send_mail_file_status()
        return ['send_email_on_failure']


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 25),
    'email': ['rakeshaswath@saturam.com','naveen@saturam.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'DMS_JOBCARD_DATA_INGESTION_PIPELINE',
    default_args=default_args,
    description='DMS JobCard Data Ingestion Pipeline metric',
    # schedule_interval=timedelta(days=1),
    schedule_interval="30 0 * * *",
)

bo = BranchPythonOperator(
    task_id='choose_tasks',
    python_callable=choose_tasks,
    do_xcom_push=False,
    dag=dag
)

gcs = LocalFilesystemToGCSOperator(
    task_id="local_to_gcs",
    src=LOCAL_FILE,
    dst=GCS_FILE_PATH,
    bucket=GCS_BUCKET,
    mime_type='text/csv',
    google_cloud_storage_conn_id=GCP_CONN_ID,
    dag=dag
)

delete = PythonOperator(
    task_id='delete_local_file',
    python_callable=delete_local_files,
    dag=dag
)

gcs_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket=GCS_BUCKET,
    source_objects=[GCS_FILE_PATH],
    schema_fields=[
        {"name": "JobCard", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartNumber", "type": "STRING", "mode": "NULLABLE"},
        {"name": "LineAmount", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "PartDescription", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ItemType", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Observation", "type": "STRING", "mode": "NULLABLE"},
        {"name": "RequestedQuantity", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "IssuedQuantityNew", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "IsRepeated", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartDefectID", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartDefectCode", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PrimaryConsequential", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PrimaryPart", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PrimaryPartNumber", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartAction", "type": "STRING", "mode": "NULLABLE"},
        {"name": "StatusReason", "type": "STRING", "mode": "NULLABLE"},
        {"name": "CreatedOnDateTime", "type": "DATETIME", "mode": "NULLABLE"},
        {"name": "CustomerComplaint", "type": "STRING", "mode": "NULLABLE"},
        {"name": "CustomerVoice", "type": "STRING", "mode": "NULLABLE"},
        {"name": "CashierInvoicingDateTime", "type": "DATETIME", "mode": "NULLABLE"},
        {"name": "Region", "type": "STRING", "mode": "NULLABLE"},
        {"name": "RegistrationNumber", "type": "STRING", "mode": "NULLABLE"},
        {"name": "VehicleChassisNumber", "type": "STRING", "mode": "NULLABLE"},
        {"name": "JobType", "type": "STRING", "mode": "NULLABLE"},
        {"name": "JobSubType", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Kilometers", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "State", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Zone", "type": "STRING", "mode": "NULLABLE"},
        {"name": "DealerName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "FinalCustomer", "type": "STRING", "mode": "NULLABLE"},
        {"name": "BillType", "type": "STRING", "mode": "NULLABLE"},
        {"name": "DateofSale", "type": "DATETIME", "mode": "NULLABLE"},
        {"name": "Product", "type": "STRING", "mode": "NULLABLE"},
        {"name": "RepairCategory", "type": "STRING", "mode": "NULLABLE"},
        {"name": "part_batch_code", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ModelID", "type": "STRING", "mode": "NULLABLE"},
        {"name": "DealerCode", "type": "STRING", "mode": "NULLABLE"},
        {"name": "FieldObservationReport", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "TechnicalObservation", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ComplaintObservedByDealer", "type": "STRING", "mode": "NULLABLE"},
        {"name": "CorrectiveActiontaken", "type": "STRING", "mode": "NULLABLE"},
        {"name": "JobCardStatus", "type": "STRING", "mode": "NULLABLE"},
        {"name": "CreatedOn", "type": "DATE", "mode": "NULLABLE"},
        {"name": "CreatedOnYear", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "CreatedOnMonth", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "PiperrInDate", "type": "DATE", "mode": "NULLABLE"},
        {"name": "Model", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ModelType", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ModelName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Plant", "type": "STRING", "mode": "NULLABLE"},
        {"name": "MfgMonth", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "MfgYear", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "MfgDate", "type": "DATE", "mode": "NULLABLE"},
        {"name": "Country", "type": "STRING", "mode": "NULLABLE"},
        {"name": "KmsGroup", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartGroup", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartVariant", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartCategory", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PrimaryPartNew", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PrimaryPartFilter", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartFilter", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PrimaryPartGroup", "type": "STRING", "mode": "NULLABLE"},
        {"name": "City", "type": "STRING", "mode": "NULLABLE"},
    ],
    destination_project_dataset_table=f'{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}',
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    allow_quoted_newlines=True,
    google_cloud_storage_conn_id=GCP_CONN_ID,
    bigquery_conn_id=GCP_CONN_ID,
    dag=dag
)

gcs_bq_delta = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bigquery_delta',
    bucket=GCS_BUCKET,
    source_objects=[GCS_FILE_PATH],
    schema_fields=[
        {"name": "JobCard", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartNumber", "type": "STRING", "mode": "NULLABLE"},
        {"name": "LineAmount", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "PartDescription", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ItemType", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Observation", "type": "STRING", "mode": "NULLABLE"},
        {"name": "RequestedQuantity", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "IssuedQuantityNew", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "IsRepeated", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartDefectID", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartDefectCode", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PrimaryConsequential", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PrimaryPart", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PrimaryPartNumber", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartAction", "type": "STRING", "mode": "NULLABLE"},
        {"name": "StatusReason", "type": "STRING", "mode": "NULLABLE"},
        {"name": "CreatedOnDateTime", "type": "DATETIME", "mode": "NULLABLE"},
        {"name": "CustomerComplaint", "type": "STRING", "mode": "NULLABLE"},
        {"name": "CustomerVoice", "type": "STRING", "mode": "NULLABLE"},
        {"name": "CashierInvoicingDateTime", "type": "DATETIME", "mode": "NULLABLE"},
        {"name": "Region", "type": "STRING", "mode": "NULLABLE"},
        {"name": "RegistrationNumber", "type": "STRING", "mode": "NULLABLE"},
        {"name": "VehicleChassisNumber", "type": "STRING", "mode": "NULLABLE"},
        {"name": "JobType", "type": "STRING", "mode": "NULLABLE"},
        {"name": "JobSubType", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Kilometers", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "State", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Zone", "type": "STRING", "mode": "NULLABLE"},
        {"name": "DealerName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "FinalCustomer", "type": "STRING", "mode": "NULLABLE"},
        {"name": "BillType", "type": "STRING", "mode": "NULLABLE"},
        {"name": "DateofSale", "type": "DATETIME", "mode": "NULLABLE"},
        {"name": "Product", "type": "STRING", "mode": "NULLABLE"},
        {"name": "RepairCategory", "type": "STRING", "mode": "NULLABLE"},
        {"name": "part_batch_code", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ModelID", "type": "STRING", "mode": "NULLABLE"},
        {"name": "DealerCode", "type": "STRING", "mode": "NULLABLE"},
        {"name": "FieldObservationReport", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "TechnicalObservation", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ComplaintObservedByDealer", "type": "STRING", "mode": "NULLABLE"},
        {"name": "CorrectiveActiontaken", "type": "STRING", "mode": "NULLABLE"},
        {"name": "JobCardStatus", "type": "STRING", "mode": "NULLABLE"},
        {"name": "CreatedOn", "type": "DATE", "mode": "NULLABLE"},
        {"name": "CreatedOnYear", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "CreatedOnMonth", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "PiperrInDate", "type": "DATE", "mode": "NULLABLE"},
        {"name": "Model", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ModelType", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ModelName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Plant", "type": "STRING", "mode": "NULLABLE"},
        {"name": "MfgMonth", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "MfgYear", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "MfgDate", "type": "DATE", "mode": "NULLABLE"},
        {"name": "Country", "type": "STRING", "mode": "NULLABLE"},
        {"name": "KmsGroup", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartGroup", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartVariant", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartCategory", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PrimaryPartNew", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PrimaryPartFilter", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PartFilter", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PrimaryPartGroup", "type": "STRING", "mode": "NULLABLE"},
        {"name": "City", "type": "STRING", "mode": "NULLABLE"},
    ],
    destination_project_dataset_table=f'{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE2}',
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    allow_quoted_newlines=True,
    google_cloud_storage_conn_id=GCP_CONN_ID,
    bigquery_conn_id=GCP_CONN_ID,
    dag=dag
)

email_success = EmailOperator(
        task_id='send_email_on_success',
        to=["rakeshaswath@saturam.com","naveen@saturam.com"],
        subject='DMS JobCard data ingestion pipeline Success',
        html_content=""" <h3>DMS JobCard data ingestion pipeline ingested data successfully</h3> """,
        dag=dag
)

email_failure = EmailOperator(
        task_id='send_email_on_failure',
        to=["rakeshaswath@saturam.com", "naveen@saturam.com"],
        subject='DMS JobCard data not loaded to bigquery',
        html_content=""" <h3>DMS JobCard data file not uploaded in azure blob</h3> """,
        dag=dag
)

bo >> gcs >> delete >> gcs_bq >> gcs_bq_delta >> email_success
bo >> email_failure



