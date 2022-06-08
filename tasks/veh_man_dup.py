import time
import os
from datetime import date,datetime,timedelta
import pandas as pd

from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.contrib.operators import gcs_to_bq
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import BranchPythonOperator


import smtplib
import mimetypes
import email
import email.mime.application
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


class varclass:
    def __init__(self):
        self.today = date.today()
        self.LOCAL_FILE = f'dms-sales-{self.today}.csv'
        self.AZURE_BLOB = f'poc-data/DMS-SALES/Delta-Data/dms-sales-{self.today}.csv'
        self.GCS_FILE_PATH = f'poc-data/DMS-SALES/Delta-Data/dms-sales-{self.today}.csv'
        self.AZURE_CONTAINER = "175-warranty-analytics"
        self.GCS_BUCKET = 're-prod-data-env'
        self.GCP_CONN_ID = "google_cloud_default"
        self.GCP_PROJECT = "re-warranty-analytics-prod"
        self.BQ_DATASET = "re_wa_prod"
        self.BQ_TABLE = "re_dms_sales_table_v1"
        self.count = 0
        self.dist_count = 0

obj = varclass()


def check_blob():
    az_hook = WasbHook(wasb_conn_id='wasb_default')
    return az_hook.check_for_blob(container_name=obj.AZURE_CONTAINER, blob_name=obj.AZURE_BLOB)


def send_mail_file_status():
    user = 'royalenfield'
    password = os.environ["SMTP_PWD"]
    to =[ "zzhemant@royalenfield.com"]
    cc =[ "zzrakeshaswath@royalenfield.com, rakeshaswath@saturam.com,ram@saturam.com,naveen@saturam.com,subanand@royalenfield.com,jayaprakash@royalenfield.com,sureshe@royalenfield.com,jgokul@royalenfield.com"]
    sender = 'noreply@royalenfield.com'

    msg = MIMEMultipart()
    msg['Subject'] = f'DMS Sales Data - Delta file ({obj.today})'
    msg['From'] = sender
    msg['To'] = ','.join(to)
    msg['Cc'] = ','.join(cc)

    body = MIMEText(
        f"""Hi Hemant,\n\nDMS Sales Data - Delta file ({obj.today}) is not uploaded into AZURE BLOB.\nPlease fix the issue and re-upload the file\n\n\nThank You. """)
    msg.attach(body)

    s = smtplib.SMTP("smtpbp.falconide.com", 587)
    s.starttls()

    s.login(user, password)
    s.sendmail(sender, (to+cc), msg.as_string())
    s.quit()


def fetch_data_from_blob():
    az_hook = WasbHook(wasb_conn_id='wasb_default')
    az_hook.get_file(obj.LOCAL_FILE, container_name=obj.AZURE_CONTAINER, blob_name=obj.AZURE_BLOB)


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


def transformation():

    if check_blob():

        print(f'Transformation Start - {time.time()}')
        global t1
        t1 = time.time()

        df=pd.read_csv(
                    obj.LOCAL_FILE,
                    skiprows=1,
                    names=['FirstName','LastName','DealerName',
                    'SalesOrderDate','DealerCode', 'ModelID', 'ModelFamily', 'Product','VehicleChassisNumber','Zone', 'Region','State','City'],
                    low_memory=False,
                    dtype={'FirstName':str, 'LastName':str, 'DealerName':str, 'DealerCode':str, 'ModelID':str,
                    'ModelFamily':str, 'Product':str, 'VehicleChassisNumber':str,
                    'Zone':str, 'Region':str, 'State':str, 'City':str}
            )
        os.remove(obj.LOCAL_FILE)

        # newly added naveen
        obj.count = df.shape[0]
        obj.dist_count = df['VehicleChassisNumber'].nunique()
        if obj.count != obj.dist_count:
            df[df['VehicleChassisNumber'].duplicated()]['VehicleChassisNumber'].to_csv('dup_chassis_num.csv', index=False)
        # ended naveen

        df['SalesOrderDate']=pd.to_datetime(df['SalesOrderDate'])
        print(f'Date Complete - {time.time()}')

        df['SalesOrderDateYear'] = df['SalesOrderDate'].dt.year
        df['SalesOrderDateMonth'] = df['SalesOrderDate'].dt.month
        df['SalesOrderDateMonthName'] = df['SalesOrderDate'].dt.month_name()
        df['SalesOrderDate'] = df['SalesOrderDate'].dt.date

        df['PiperrInDate']=date.today()
        print(f'PiperrInDate Complete - {time.time()}')

        df['Product'] = df['Product'].str.upper()
        df['Product'] = df['Product'].fillna("")
        df['Model'] = df['Product'].str.split().str[0]
        # df['Model'] = df['Model'].str.upper()
        df['Model'] = df['Model'].str.split(',').str[0]
        df['Model'] = df['Model'].str.split('\d+').str[0]
        df['Model'] = df['Model'].str.split('-').str[0]
        df['Model'] = df['Model'].map(lambda x: x if isinstance(x,str) else '')
        df.loc[df['Model'].str.contains("CONTINENTAL"),'Model'] = 'CONTINENTAL'
        df.loc[df['Model']=='CL','Model'] = 'CLASSIC'
        df.loc[df['Model'] == 'SIGNALS', 'Model'] = 'CLASSIC'
        print(f'Model Complete - {time.time()}')

        df['VehicleChassisNumber'] = df['VehicleChassisNumber'].str.upper()
        df['VehicleChassisNumber'] = df['VehicleChassisNumber'].fillna("")
        df['U/D']=df['VehicleChassisNumber'].str[3:4]
        df['CC'] = df['VehicleChassisNumber'].str[4:5]
        df['PlantOrMonth'] = df['VehicleChassisNumber'].str[8:9]
        df['ManufacturingYear'] = df['VehicleChassisNumber'].str[9:10]
        df['MonthOrPlant'] = df['VehicleChassisNumber'].str[10:11]

        df['ModelType'] = df.apply(
            lambda x: modelType(x['U/D'], x['Product']) if len(x['VehicleChassisNumber']) == 17 and x[
                'Product'] else 'NA', axis=1)
        print(f'ModelType Complete - {time.time()}')

        df['Model'] = df.apply(
            lambda x: modelClassic(x['U/D'], x['Product'], x['Model']) if len(x['VehicleChassisNumber']) == 17 and x[
                'Product'] else x['Model'], axis=1)
        print(f'Model Complete - {time.time()}')

        # df['ModelType'] = df.apply(lambda x: modelType(x['U/D'],x['Model']), axis=1)
        # print(f'ModelType Complete - {time.time()}')
        #
        # df['Model'] = df.apply(lambda x: modelClassic(x['U/D'], x['Model']), axis=1)
        # print(f'Model Complete - {time.time()}')

        df['ModelName'] = df.apply(lambda x:modelName(x['Model'],x['CC']),axis = 1)
        print(f'ModelName Complete - {time.time()}')

        df['Plant'] = df.apply(lambda x: plant(x['U/D'],x['PlantOrMonth'],x['MonthOrPlant']), axis=1)
        print(f'Plant Complete - {time.time()}')

        global mfg_mon
        mfg_mon = {'A':1, 'B':2,'C':3,'D':4,'E':5,'F':6,'G':7,'H':8,'K':9,'L':10,'M':11,'N':12}
        df['MfgMonth'] = df.apply(lambda x: mfgMonth(x['U/D'],x['PlantOrMonth'],x['MonthOrPlant']), axis=1)
        print(f'MfgMonth Complete - {time.time()}')

        mfg_year = {'A':2010, 'B':2011,'C':2012,'D':2013,'E':2014,'F':2015,'G':2016,'H':2017,'J':2018,'K':2019,'L':2020,'M':2021,'N':2022,'O':2023}
        df['MfgYear'] = df.apply(lambda x: mfg_year.get(x['ManufacturingYear']) if mfg_year.get(x['ManufacturingYear']) else 1900, axis=1)
        print(f'MfgYear Complete - {time.time()}')
        df['MfgDate'] = pd.to_datetime(dict(year=df.MfgYear, month=df.MfgMonth, day=1))
        df['Country']= df.apply(lambda x: 'Domestic' if x['CC'] in ['3','4','5','6','7'] else 'International', axis=1)
        df.drop(['U/D', 'CC', 'PlantOrMonth', 'ManufacturingYear', 'MonthOrPlant'],inplace=True,axis=1)
        df.to_csv(obj.LOCAL_FILE, header=True, index=False)

        global t2
        t2 = time.time()
        print(f"It takes {(t2 - t1)} seconds to load and transform data")
        print('Transformation End')


def delete_local_files():
    os.remove(obj.LOCAL_FILE)


def check_data():
    if obj.count != obj.dist_count:
        return ['send_poordata_mail']
    else:
        return ['send_email']


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
    'DMS_SALES_DATA_INGESTION_PIPELINE',
    default_args=default_args,
    description='DMS SALES Data Ingestion Pipeline Metric',
    # schedule_interval=timedelta(days=1),
    schedule_interval="45 0 * * *",
)


def choose_tasks():
    if check_blob():
        fetch_data_from_blob()
        transformation()
        return ['dms_sales_data_upload_file', 'dms_sales_data_delete', 'DMS_Sales_Data_GCS_to_Bigquery']
    else:
        send_mail_file_status()
        return ['send_email_failure']


def check_data_redundancy():
    if obj.count != obj.dist_count:
        return ['send_poordata_mail']
    else:
        return ['send_email_success']


bo = BranchPythonOperator(
    task_id='choose_tasks',
    python_callable=choose_tasks,
    do_xcom_push=False,
    dag=dag
)

gcs = LocalFilesystemToGCSOperator(
    task_id="dms_sales_data_upload_file",
    src=obj.LOCAL_FILE,
    dst=obj.GCS_FILE_PATH,
    bucket=obj.GCS_BUCKET,
    mime_type='text/csv',
    google_cloud_storage_conn_id=obj.GCP_CONN_ID,
    dag=dag
)


delete = PythonOperator(
        task_id='dms_sales_data_delete',
        python_callable=delete_local_files,
        dag=dag
    )

gcs_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='DMS_Sales_Data_GCS_to_Bigquery',
    bucket=obj.GCS_BUCKET,
    source_objects = [obj.GCS_FILE_PATH],
    schema_fields=[
        {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "DealerName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "SalesOrderDate", "type": "DATE", "mode": "NULLABLE"},
        {"name": "DealerCode", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ModelID", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ModelFamily", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Product", "type": "STRING", "mode": "NULLABLE"},
        {"name": "VehicleChassisNumber", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Zone", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Region", "type": "STRING", "mode": "NULLABLE"},
        {"name": "State", "type": "STRING", "mode": "NULLABLE"},
        {"name": "City", "type": "STRING", "mode": "NULLABLE"},
        {"name": "SalesOrderDateYear", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "SalesOrderDateMonth", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "SalesOrderDateMonthName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PiperrInDate", "type": "DATE", "mode": "NULLABLE"},
        {"name": "Model", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ModelType", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ModelName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Plant", "type": "STRING", "mode": "NULLABLE"},
        {"name": "MfgMonth", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "MfgYear", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "MfgDate", "type": "DATE", "mode": "NULLABLE"},
        {"name": "Country", "type": "STRING", "mode": "NULLABLE"}
    ],
    destination_project_dataset_table=f'{obj.GCP_PROJECT}.{obj.BQ_DATASET}.{obj.BQ_TABLE}',
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    allow_quoted_newlines=True,
    google_cloud_storage_conn_id=obj.GCP_CONN_ID,
    bigquery_conn_id=obj.GCP_CONN_ID,
    dag = dag
)

branch_mail_task = BranchPythonOperator(
    task_id='data_check',
    python_callable=check_data_redundancy,
    dag=dag,
)

email_success = EmailOperator(
        task_id='send_email_success',
        to=["rakeshaswath@saturam.com","naveen@saturam.com"],
        subject='DMS Sales data ingestion pipeline Success',
        html_content=""" <h3>DMS Sales data pipeline ingested data successfully</h3> """,
        dag=dag
)

email_failure = EmailOperator(
        task_id='send_email_failure',
        to=["rakeshaswath@saturam.com","naveen@saturam.com"],
        subject='DMS Sales data file Missing',
        html_content=""" <h3>DMS Sales data file not uploaded in azure blob</h3> """,
        trigger_rule='none_failed_or_skipped',
        dag=dag
)

send_poordata_mail = EmailOperator(
        task_id='send_poordata_mail',
        to=["rakeshaswath@saturam.com", "naveen@saturam.com"],
        subject='Redundant Chassis Numbers',
        html_content="""<h3>Redundant chassis number in data but DAG ran successfully</h3>""",
        files=['dup_chassis_num.csv']
)

bo >> gcs >> delete >> gcs_bq >> branch_mail_task >> email_success
bo >> gcs >> delete >> gcs_bq >> branch_mail_task >> send_poordata_mail
bo >> email_failure
