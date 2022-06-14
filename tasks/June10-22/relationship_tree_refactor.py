from datetime import timedelta,date,datetime
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
import os

GCP_CONN_ID = "google_cloud_default"
GCP_PROJECT = "re-warranty-analytics-prod"
BQ_DATASET = "re_wa_prod"
BQ_SOURCE_TABLE = "re_dms_delta_table"
BQ_SOURCE_TABLE2 = 'part_exclude.csv'
BQ_DESTINATION_TABLE = "re_relationshiptree_usecase_table_v1"

sql = f"""
            SELECT t1.VehicleChassisNumber,t1.JobCard,t1.CreatedOn,t1.PartNumber,t1.PartDescription,t1.JobSubType,t1.BillType,t1.PrimaryPartNumber,t1.
            PartGroup,t1.PrimaryPartNew,t1.PrimaryPartGroup,t1.Zone,t1.Region,t1.State,t1.DealerCode,t1.DealerName,t1.City,ModelType,t1.Model,t1.Kilometers,t1.KmsGroup,t1.Plant,t1.MfgMonth,t1.
            MfgYear,t1.CreatedOnYear,t1.CreatedOnMonth, t1.PiperrInDate
            FROM(SELECT dms.VehicleChassisNumber,dms.JobCard,dms.CreatedOn,dms.PartNumber,dms.PartDescription,dms.JobSubType,dms.BillType,dms.PrimaryPartNumber,dms.
            PartGroup,dms.PrimaryPartNew,dms.PrimaryPartGroup,dms.Zone,dms.Region,dms.State,dms.DealerCode,dms.DealerName,dms.City,ModelType,dms.Model,dms.Kilometers,dms.KmsGroup,dms.Plant,dms.MfgMonth,dms.
            MfgYear,dms.CreatedOnYear,dms.CreatedOnMonth, dms.PiperrInDate FROM(SELECT VehicleChassisNumber, JobCard, CreatedOn, PartNumber, PartDescription, JobSubType, BillType, PrimaryPartNumber, 
            PartGroup, PrimaryPartNew, PrimaryPartGroup, Zone, Region, State, DealerCode, DealerName, City,ModelType, Model, Kilometers, KmsGroup, Plant, MfgMonth, 
            MfgYear, CreatedOnYear, CreatedOnMonth,CURRENT_DATE() as PiperrInDate
            FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_SOURCE_TABLE}`
            WHERE ItemType like '%Part%' and PrimaryConsequential like '%Consequential%' and
            ModelType not in ('NA') and Model not in ('U','NA','MACHISMO', '', 'TAURUS','ROYAL','DSW') and Plant not in ('NA')
            and PrimaryPart is not null and length(JobCard) = 16 and
            length(VehicleChassisNumber)=17 and PartNumber is not null and PartDescription is not null and Country like 'Domestic' and MfgYear !=1900
            and PartAction is null) dms
            LEFT JOIN `{GCP_PROJECT}.{BQ_DATASET}.{BQ_SOURCE_TABLE2}` pe
            ON dms.PartNumber = pe.PartNo
            WHERE pe.PartNo IS NULL) t1
            LEFT JOIN `{GCP_PROJECT}.{BQ_DATASET}.{BQ_SOURCE_TABLE2}` t2
            ON t1.PrimaryPartNumber = t2.PartNo
            ;
            """


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
    'RELATIONSHIP_TREE_USECASE_data_ingestion',
    default_args=default_args,
    description='Relationship Tree Failure Metric',
    # schedule_interval=timedelta(days=1),
    schedule_interval="45 1 * * *",
)


relation_task = BigQueryOperator(
    task_id='relation_tree',
    use_legacy_sql=False,
    sql=sql,
    destination_dataset_table=f'{GCP_PROJECT}:{BQ_DATASET}.{BQ_DESTINATION_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id=GCP_CONN_ID,
    dag=dag,
)

email_success = EmailOperator(
        task_id='send_email',
        to=["rakeshaswath@saturam.com", "naveen@saturam.com"],
        subject='Relationship Tree Use case Success',
        html_content=""" <h3>Relationship Tree Use case metric performed and loaded into BQ Table</h3> """,
        dag=dag
)

relation_task >> email_success


