from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import pandas as pd

# NEW TRY
from google.cloud import bigquery
#

GCP_CONN_ID = "google_cloud_default"
GCP_PROJECT = "re-warranty-analytics-prod"
BQ_DATASET = "re_wa_prod"
BQ_SOURCE_TABLE_PROD = ""
BQ_SOURCE_TABLE_SALES = ""
BQ_DESTINATION_TABLE_PROD = ""
BQ_DESTINATION_TABLE_SALES = ""


def check_table_sales():
    client = bigquery.Client()
    query = f"""SELECT Count(*) FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_DESTINATION_TABLE_SALES}` """
    query_job = client.query(
        query,
        location="US"
    )
    results = query_job.result()  # Wait for query to complete.
    rows = results.total_rows
    if rows != 0:
        return ['dupvcn_sales_mail']
    else:
        return ['no_dupvcn_sales_mail']


def check_table_prod():
    client = bigquery.Client()
    query = f"""SELECT Count(*) FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_DESTINATION_TABLE_PROD}` """
    query_job = client.query(
        query,
        location="US"
    )
    results = query_job.result()  # Wait for query to complete.
    rows = results.total_rows
    if rows != 0:
        return ['dupvcn_prod_mail']
    else:
        return ['no_dupvcn_prod_mail']


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 9),
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

# CHECK THE COLUMN (VEHICLECHASSISNUMBER) FROM PROD TABLE
sql_prod = f"""
        SELECT VEHICLECHASSISUMBER, Count(*)
        FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_SOURCE_TABLE_PROD}`
        GROUP BY VEHICLECHASSISUMBER
        HAVING Count(*) > 1
"""

# CHECK THE COLUMN (VEHICLECHASSISNUMBER) FROM SALES TABLE
sql_sales = f"""
        SELECT VEHICLECHASSISUMBER, Count(*) 
        FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_SOURCE_TABLE_SALES}`
        GROUP BY VEHICLECHASSISUMBER
        HAVING Count(*) > 1
"""

prod_vcn = BigQueryOperator(
    task_id='prod_vcn_table ',
    destination_dataset_table=f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_DESTINATION_TABLE_PROD}",
    write_disposition='WRITE_TRUNCATE',
    sql=sql_prod,
    google_cloud_storage_conn_id=GCP_CONN_ID,
    bigquery_conn_id=GCP_CONN_ID,
    dag=dag
)

sales_vcn = BigQueryOperator(
    task_id='sales_vcn_table ',
    destination_dataset_table=f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_DESTINATION_TABLE_SALES}",
    write_disposition='WRITE_TRUNCATE',
    sql=sql_sales,
    google_cloud_storage_conn_id=GCP_CONN_ID,
    bigquery_conn_id=GCP_CONN_ID,
    dag=dag
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
        files=['missing_part_nos.csv'],
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
        files=['missing_part_nos.csv'],
        dag=dag
)

prod_vcn >> sales_vcn >> bo_prod >> dupvcn_prod_mail
prod_vcn >> sales_vcn >> bo_prod >> no_dupvcn_prod_mail
prod_vcn >> sales_vcn >> bo_sales >> dupvcn_sales_mail
prod_vcn >> sales_vcn >> bo_sales >> no_dupvcn_sales_mail

