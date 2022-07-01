from datetime import datetime, time,timedelta,date
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.utils.dates import days_ago
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.wasb_hook import WasbHook

import pandas as pd
from google.cloud import bigquery,storage
import pandas_gbq
import traceback
import numpy as np
import os
import ast
import smtplib
import mimetypes
import email
import email.mime.application
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# user = 'royalenfield'
# password = os.environ["SMTP_PWD"]
# to = "zzrakeshaswath@royalenfield.com,subanand@royalenfield.com,jayaprakash@royalenfield.com,sureshe@royalenfield.com,jgokul@royalenfield.com"
# # to = "zzrakeshaswath@royalenfield.com"
# sender = 'noreply@royalenfield.com'

GCS_BUCKET = 're-prod-data-env'
GCP_CONN_ID = "google_cloud_default"
GCP_PROJECT = "re-warranty-analytics-prod"
BQ_DATASET = "re_wa_prod"
BQ_SOURCE_TABLE = "re_dms_delta_table"
BQ_DESTINATION_TABLE = "trigger_cutoff_plm_usecase_table_v1"

LOCAL_FILE = f'plm_sb_data_production.csv'
ATTACHMENT_FILE='trigger_cutoff_summary.xlsx'
AZURE_BLOB = f"poc-data/MANUAL-DATA/{LOCAL_FILE}"
AZURE_CONTAINER = "175-warranty-analytics"

con_df = pd.DataFrame()
sum_df = pd.DataFrame()
part_df = pd.DataFrame()


def check_blob_gcs():
    today = date.today()
    JOBCARD_FILE_STATUS_BLOB = f'poc-data/DMS/Delta-Data/dms-{today}.csv'

    az_hook = WasbHook(wasb_conn_id='wasb_default')
    MANUAL_FILE_STATUS = az_hook.check_for_blob(container_name=AZURE_CONTAINER, blob_name=AZURE_BLOB)

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    JOBCARD_FILE_STATUS = storage.Blob(bucket=bucket, name=JOBCARD_FILE_STATUS_BLOB).exists(storage_client)
    return MANUAL_FILE_STATUS, JOBCARD_FILE_STATUS


def part_date_ref1(part_details):

    part_details=part_details.replace('\n', ' ').replace('\r', '')
    part_details = ast.literal_eval(part_details)
    parts=[i["Part No"] for i in part_details]
    return ",".join(parts)


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


def concession_report(part_details,part_defect,model,mfg_date,plant,mod_detail,eff_date,reason_for_change):
    global part_df
    jc_today()
    part_details = part_details.replace('\n', ' ').replace('\r', '')
    part_details = ast.literal_eval(part_details)
    eff_date = str(eff_date)
    model = ast.literal_eval(model)
    temp_model = ','.join(model)

    parts = [i["Part No"] for i in part_details]
    parts_desc = [k["Part Description"] for k in part_details]

    for indx, j in enumerate(parts):

        temp_part_df = part_df[part_df['EffectiveDate'] > datetime.strptime(eff_date,
                                                                            '%Y-%m-%d').date()]  # datetime.strptime(eff_date, '%Y-%m-%d').date()
        temp_part_list = temp_part_df['PartDetails'].tolist()
        temp_mfgdate_list = temp_part_df['MfgDate'].tolist()
        tpi_bool = 0
        tpi_bool_value = ""

        for tpi, tpiv in enumerate(temp_part_list):

            if j in tpiv.split():
                tpi_bool = 1
                tpi_bool_value = temp_mfgdate_list[tpi]
                break

        # if tpi_bool:
        #     sql = f"""
        #         SELECT VehicleChassisNumber, JobCard, Region, DealerName
        #         FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_SOURCE_TABLE}`
        #         where PartNumber like '{j}' and {model_sql} and MfgDate >= '{mfg_date}'
        #         and MfgDate < '{tpi_bool_value}' and Plant like '{plant}' and PiperrInDate = CURRENT_DATE()
        #         """
        # else:
        #     sql = f"""
        #         SELECT VehicleChassisNumber, JobCard, Region, DealerName
        #         FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_SOURCE_TABLE}`
        #         where PartNumber like '{j}' and {model_sql} and
        #         MfgDate >= '{mfg_date}' and Plant like '{plant}' and PiperrInDate = CURRENT_DATE()
        #         """
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

        # print(sql)
        # bq_hook = BigQueryHook(bigquery_conn_id=GCP_CONN_ID, delegate_to=None, use_legacy_sql=False)
        # bq_client = bigquery.Client(project=bq_hook._get_field(GCP_PROJECT), credentials=bq_hook._get_credentials())
        # cr = bq_client.query(sql).to_dataframe()
        # cr.drop_duplicates(subset="JobCard", inplace=True)
        # global con_df
        # print(cr.shape)
        jc_df.drop_duplicates(subset="JobCard", inplace=True)
        global con_df

        if jc_df.shape[0] > 0:
            jc_df['PartNumber'] = j
            jc_df['PartDescription'] = parts_desc[indx]
            jc_df['PartDefect'] = part_defect
            jc_df['ApplicableModels'] = temp_model
            jc_df['Plant'] = plant
            jc_df['ReasonForChange'] = reason_for_change
            jc_df['ModificationDetail'] = mod_detail
            jc_df['EffectiveDate'] = eff_date
            con_df = con_df.append(jc_df, ignore_index=True)


def transformation():

    MANUAL_FILE_STATUS, JOBCARD_FILE_STATUS = check_blob_gcs()

    if JOBCARD_FILE_STATUS and MANUAL_FILE_STATUS:

        global part_df
        global con_df

        az_hook = WasbHook(wasb_conn_id='wasb_default')

        az_hook.get_file(LOCAL_FILE, container_name=AZURE_CONTAINER, blob_name=AZURE_BLOB)
        part_df = pd.read_csv(LOCAL_FILE, parse_dates=['EffectiveDate'])
        part_df = part_df.sort_values(by=['EffectiveDate'], ascending=True)
        part_df['PartDetails'] = part_df['PartDetails'].apply(lambda x: part_date_ref1(x))
        part_df["EffectiveDate"] = pd.to_datetime(part_df["EffectiveDate"]).dt.date

        df = pd.read_csv(LOCAL_FILE,parse_dates=['EffectiveDate'])
        os.remove(LOCAL_FILE)
        df["EffectiveDate"] = pd.to_datetime(df["EffectiveDate"]).dt.date
        df = df.sort_values(by=['EffectiveDate'], ascending=True)
        for index, row in df.iterrows():
            concession_report(row["PartDetails"],row["PartDefectCode"],row["ApplicableModels"],row["MfgDate"],row['Plant'],row['ModificationDetail'],row['EffectiveDate'],row['ReasonForChange'])

        table_schema = [
        {"name": "VehicleChassisNumber", "type": "STRING"},
        {"name": "PartNumber", "type": "STRING"},
        {"name": "PartDescription", "type": "STRING"},
        {"name": "PartDefect", "type": "STRING"},
        {"name": "ApplicableModels", "type": "STRING"},
        {"name": "Plant", "type": "STRING"},
        {"name": "ReasonForChange", "type": "STRING"},
        {"name": "ModificationDetail", "type": "STRING"},
        {"name": "EffectiveDate", "type": "DATE"},
        {"name": "PiperrInDate", "type": "DATE"},
        {"name": "EffectiveYear", "type": "STRING"},
        {"name": "EffectiveMonth", "type": "STRING"},
        {"name": "EffectiveMonthName", "type": "STRING"}
        ]
        con_df["EffectiveDate"] = pd.to_datetime(con_df["EffectiveDate"]).dt.date
        con_df['PiperrInDate'] = pd.Timestamp.today().date()
        con_df['EffectiveYear'] = pd.to_datetime(con_df["EffectiveDate"]).dt.strftime("%Y")
        con_df['EffectiveMonth'] = pd.to_datetime(con_df["EffectiveDate"]).dt.strftime("%m")
        con_df['EffectiveMonthName'] = pd.to_datetime(con_df["EffectiveDate"]).dt.strftime("%b")
        bq_hook = BigQueryHook(bigquery_conn_id=GCP_CONN_ID, delegate_to=None, use_legacy_sql=False)
        pandas_gbq.to_gbq(con_df, f'{BQ_DATASET}.{BQ_DESTINATION_TABLE}', project_id=f'{GCP_PROJECT}', if_exists='append', table_schema=table_schema,credentials=bq_hook._get_credentials())
        summary()
        send_mail_task()
        # con_df.to_csv(ATTACHMENT_FILE, index=False, header=True)

    else:
        print(f"Skipping")


def summary():

    global sum_df
    sql = f"""
       SELECT PartNumber, EffectiveDate,max(PartDescription), max(PartDefect), max(ReasonForChange), max(ModificationDetail), count(distinct JobCard) as ReportedIssues
       FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_DESTINATION_TABLE}`
       group by PartNumber,EffectiveDate
       order by PartNumber,EffectiveDate;
       """

    bq_hook = BigQueryHook(bigquery_conn_id=GCP_CONN_ID, delegate_to=None, use_legacy_sql=False)
    bq_client = bigquery.Client(project=bq_hook._get_field(GCP_PROJECT), credentials=bq_hook._get_credentials())
    sum_df = bq_client.query(sql).to_dataframe()
    sum_df.columns = ['PartNumber', 'EffectiveDate', 'PartDescription', 'PartDefect', 'ReasonForChange', 'ModificationDetail', 'ReportedIssues']
    sum_df = sum_df.sort_values(['EffectiveDate']).groupby(['PartNumber']).tail(1)


def send_mail_task():

    with pd.ExcelWriter(ATTACHMENT_FILE) as writer:
        sum_df.to_excel(writer, sheet_name='summary_sheet', index=False)
        con_df.to_excel(writer, sheet_name='trigger_cutoff_JobCard_details_for_(n-1)th_day', index=False)
    msg = MIMEMultipart()
    msg['Subject'] = 'RE PROD- WARRANTY ANALYTICS - Trigger Cutoff - Chassis Numbers'
    msg['From'] = sender
    msg['To'] = to

    body = MIMEText("""Hi,\n\nPlease find attachment of Chassis Numbers that was reported after taking corrective action.\n\n\nThank You. """)
    msg.attach(body)

    fp=open(ATTACHMENT_FILE,'rb')
    att = MIMEApplication(fp.read(),_subtype="xlsx")
    fp.close()
    att.add_header('Content-Disposition','attachment',filename=ATTACHMENT_FILE)
    msg.attach(att)

    s = smtplib.SMTP("smtpbp.falconide.com",587)
    s.starttls()

    s.login(user,password)
    s.sendmail(sender,to.split(","), msg.as_string())
    s.quit()


def remove_file():

    MANUAL_FILE_STATUS, JOBCARD_FILE_STATUS = check_blob_gcs()

    if JOBCARD_FILE_STATUS and MANUAL_FILE_STATUS:
        print("Removing file")
        os.remove(ATTACHMENT_FILE)
    else:
        print(f"Skipping")


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
    'TRIGGER_CUTOFF_USECASE_data_ingestion',
    default_args=default_args,
    description='Trigger Cutoff Metric',
    # schedule_interval=timedelta(days=1),
    schedule_interval="0 3 * * *",
)

trigger_cutoff = PythonOperator(
    task_id='trigger_cutoff_operator',
    python_callable=transformation,
    dag=dag,
)

trigger_rf = PythonOperator(
    task_id='trigger_cutoff_remove_file',
    python_callable=remove_file,
    dag=dag,
)

email_success = EmailOperator(
        task_id='send_email',
        to=["rakeshaswath@saturam.com","naveen@saturam.com"],
        subject='Trigger Cutoff Use case Success',
        html_content=""" <h3>Trigger Cutoff Use case metric performed and loaded into BQ Table</h3> """,
        dag=dag
)

trigger_cutoff >> trigger_rf >> email_success
# >> trigger_cutoff_summary >> trigger_cutoff_mail



