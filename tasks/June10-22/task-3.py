from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
import pandas as pd


GCP_CONN_ID = "google_cloud_default"
GCP_PROJECT = "re-warranty-analytics-prod"
BQ_DATASET = "re_wa_prod"
BQ_SOURCE_TABLE_PROD = "sap_vehicle_production_table_v2"
BQ_SOURCE_TABLE_SALES = "re_dms_sales_table_v1"
BQ_DESTINATION_TABLE_PROD = "mfg_data_duplicate_chassis_table"
BQ_DESTINATION_TABLE_SALES = "sales_data_duplicate_chassis_table"

SALES_DUPLICATE_CHASSIS_ATTACHMENT = 'SALES_DATA_DUPLICATE_CHASSIS_DETAILS.csv'
MFG_DUPLICATE_CHASSIS_ATTACHMENT = 'MFG_DATA_DUPLICATE_CHASSIS_DETAILS.csv'


def check_table_sales():

    bq_hook = BigQueryHook(bigquery_conn_id=GCP_CONN_ID, delegate_to=None, use_legacy_sql=False)
    bq_client = bigquery.Client(project=bq_hook._get_field(GCP_PROJECT), credentials=bq_hook._get_credentials())

    sql_sales = f"""
            SELECT VehicleChassisNumber, Count(*) as Count
            FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_SOURCE_TABLE_SALES}`
            GROUP BY VehicleChassisNumber
            HAVING Count(*) > 1
    """

    df = bq_client.query(sql_sales).to_dataframe()

    if df.shape[0] != 0:
        df.to_csv(SALES_DUPLICATE_CHASSIS_ATTACHMENT, header="True", index=False)
        return ['dupvcn_sales_mail']
    else:
        return ['no_dupvcn_sales_mail']


def check_table_prod():
    bq_hook = BigQueryHook(bigquery_conn_id=GCP_CONN_ID, delegate_to=None, use_legacy_sql=False)
    bq_client = bigquery.Client(project=bq_hook._get_field(GCP_PROJECT), credentials=bq_hook._get_credentials())

    sql_prod = f"""
            SELECT VehicleChassisNumber, Count(*) as Count
            FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_SOURCE_TABLE_PROD}`
            GROUP BY VehicleChassisNumber
            HAVING Count(*) > 1
    """

    df = bq_client.query(sql_prod).to_dataframe()    

    if df.shape[0] != 0:
        df.to_csv(MFG_DUPLICATE_CHASSIS_ATTACHMENT, header="True", index=False)
        return ['dupvcn_prod_mail']
    else:
        return ['no_dupvcn_prod_mail']


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 13),
    'email': ['rakeshaswath@saturam.com', 'naveen@saturam.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}
dag = DAG(
    'SALES_VEHPROD_VCN_ANALYSIS',
    default_args=default_args,
    description='Check missing part numbers',
    # schedule_interval=timedelta(days=1),
    schedule_interval="45 1 * * *",
)


bo_prod = BranchPythonOperator(
    task_id='table_check_prod',
    python_callable=check_table_prod,
    dag=dag,
)

bo_sales = BranchPythonOperator(
    task_id='table_check_sales',
    python_callable=check_table_sales,
    dag=dag,
)

no_dupvcn_prod_mail = EmailOperator(
        task_id='no_dupvcn_prod_mail',
        to=["rakeshaswath@saturam.com", "naveen@saturam.com"],
        subject='No duplicates in Vehicle Chassis Number in production data',
        html_content="""<h3>Data has no duplicate Vehicle Chassis Number in production data 
        and its evaluated successfully</h3>""",
        dag=dag
)

dupvcn_prod_mail = EmailOperator(
        task_id='dupvcn_prod_mail',
        to=["rakeshaswath@saturam.com", "naveen@saturam.com"],
        subject='Duplicates in Vehicle Chassis Number in production data',
        html_content="""<h3>Duplicate Vehicle Chassis Numbers are found in production data please 
        refer BigQuery for more details</h3>""",
        files=[MFG_DUPLICATE_CHASSIS_ATTACHMENT],
        dag=dag
)

no_dupvcn_sales_mail = EmailOperator(
        task_id='no_dupvcn_sales_mail',
        to=["rakeshaswath@saturam.com", "naveen@saturam.com"],
        subject='No duplicates in Vehicle Chassis Number in sales data',
        html_content="""<h3>Data has no duplicate Vehicle Chassis Number in sales data 
        and its evaluated successfully</h3>""",
        dag=dag
)

dupvcn_sales_mail = EmailOperator(
        task_id='dupvcn_sales_mail',
        to=["rakeshaswath@saturam.com", "naveen@saturam.com"],
        subject='Duplicates in Vehicle Chassis Number in sales data',
        html_content="""<h3>Duplicate Vehicle Chassis Numbers are found in sales data please 
        refer BigQuery for more details</h3>""",
        files=[SALES_DUPLICATE_CHASSIS_ATTACHMENT],
        dag=dag
)

bo_prod >> dupvcn_prod_mail
bo_prod >> no_dupvcn_prod_mail
bo_sales >> dupvcn_sales_mail
bo_sales >> no_dupvcn_sales_mail

