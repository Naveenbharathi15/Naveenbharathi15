from airflow import DAG
from datetime import date, datetime, timedelta
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.email_operator import EmailOperator


class varclass:
    def __init__(self):
        self.GCP_CONN_ID = "google_cloud_default"
        self.GCP_PROJECT = "re-warranty-analytics-prod"
        self.BQ_DATASET = "re_wa_prod"
        self.BQ_TABLE = "Daily_data_analysis"


def query_function():
    today = date.today()
    # PIPELINE TABLES
    dms_table = 're_dms_table_v1'
    sales_table = 're_dms_sales_table_v1'
    call_centre_table1 = 're_dms_call_center_usecase_table'
    call_centre_table2 = 're_dms_call_center_usecase_unique_prediction_table'
    ppap_table = 're_ppap_table'
    rsa_table = 're_rsa_table'
    sap_con_table = 'concession_rework_table_v1'
    sap_veh_table = 'sap_vehicle_production_table_v2'
    soc_media_table1 = 're_social_media_table_v1'
    soc_media_table2 = 're_social_media_unique_prediction_table_v1'

    # USECASE TABLES

    # RELATIONSHIP_TREE_USECASE_data_ingestion
    relation_table = "re_relationshiptree_usecase_table_v1"

    # UNPLANNED_VISITS_USECASE_data_ingestion
    unplanned_table1 = "re_unplanned_visits_usecase_table_v1"
    unplanned_table2 = "re_unplanned_visits_percentage_usecase_table_v1"

    # REPEAT_ISSUES_USECASE_data_ingestion
    repeat_table1 = "re_service_quality_usecase_table_v1"
    repeat_table2 = "re_service_quality_percentage_usecase_table_v1"

    # REPEAT_ISSUES_VEHICLE_COUNT_USECASE_data_ingestion
    repeat_vehcount_table = "re_service_quality_vehicle_count_usecase_table"

    # TRIGGER_CUTOFF_USECASE_data_ingestion
    trigger_table = "trigger_cutoff_plm_usecase_table_v1"

    # TRIGGER_CUTOFF_PPAP_USECASE_DATA_INGESTION
    trigger_ppap = "re_trigger_cutoff_ppap_usecase_table"

    # concession_usecase_data_ingestion
    concession_table = "re_concession_rework_usecase_table_v1"

    # CUSTOMER_VOICE_USECASE_CALL_CENTER_JC_PART_LABOUR_data_ingestion
    cus_voice_table = "dms_call_center_usecase_chassis_part_labor_table_v1"

    # customer_voice_summary_dag
    cus_voice_sum_table = "re_customer_voice_summary_table"

    # EO_PRODUCTION_MONTH_USECASE_data_ingestion
    eo_prod_month_table = "re_engine_opening_production_month_wise_table_v2"

    # EO_RECEIPT_MONTH_USECASE_data_ingestion
    eo_receipt_table = "re_engine_opening_receipt_month_wise_v2"

    # FF_PART_PRODUCTION_MONTH_USECASE_data_ingestion
    ff_part_prod_table = "re_ff_production_month_wise_part_trend_table_v2"

    # FF_PART_RECEIPT_MONTH_USECASE_data_ingestion
    ff_receipt_table = "re_ff_receipt_month_wise_part_trend_table_v1"

    # FF_PRODUCTION_MONTH_USECASE_data_ingestion
    ff_prod_month_table = "re_ff_production_month_wise_table_v2"

    # WC_PROD_MONTH_USECASE_METRIC
    wc_prod_month_table = "re_wc_production_month_wise_table_v2"

    # WC_RECEIPT_MONTH_USECASE_METRIC
    wc_receipt_month_table = "re_wc_receipt_month_wise_v2"

    # total 28 tables
    tables = [dms_table, sales_table, call_centre_table1, call_centre_table2, ppap_table, rsa_table, sap_con_table,
              sap_veh_table, soc_media_table1, soc_media_table2, relation_table, unplanned_table1, unplanned_table2,
              repeat_table1, repeat_table2, repeat_vehcount_table, trigger_table, trigger_ppap, concession_table, cus_voice_table,
              cus_voice_sum_table, eo_prod_month_table, eo_receipt_table, ff_part_prod_table, ff_receipt_table, ff_prod_month_table,
              wc_prod_month_table, wc_receipt_month_table]
    sql = ""
    len_tables = len(tables)
    for i in range(len_tables):
        if i == len_tables - 1:
            sql = sql + f"""SELECT '{tables[i]}' as 'TableName', count(*) as DeltaRecords FROM `{varclass.GCP_PROJECT}.{varclass.BQ_DATASET}.{tables[i]}` where PiperrInDate = '{today}' """
        else:
            sql = sql + f"""SELECT '{tables[i]}' as TableName, count(*) as DeltaRecords FROM `{varclass.GCP_PROJECT}.{varclass.BQ_DATASET}.{tables[i]}` where PiperrInDate = '{today}' UNION ALL"""+"\n"
    print(sql)
    return sql  


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 8),
    'email': ['rakeshaswath@saturam.com','naveen@saturam.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'END_POINT_CHECK_ALL_DELTA_RECORDS',
    default_args=default_args,
    description='Check all tables for count of Delta Records',
    # schedule_interval=timedelta(days=1),
    schedule_interval="0 3 * * *",
)

create_delta_count_table = BigQueryOperator(task_id='creating_delta_table', 
        sql=query_function(),
        dag=dag,
        write_disposition="WRITE_TRUNCATE",
        bigquery_conn_id=varclass.GCP_CONN_ID,
        skip_leading_rows=1,
        destination_project_dataset_table=f'{varclass.GCP_PROJECT}.{varclass.BQ_DATASET}.{varclass.BQ_TABLE}'
)

email_success = EmailOperator(
        task_id='send_email',
        to=["rakeshaswath@saturam.com", "naveen@saturam.com"],
        subject='Delta Record analysis table creation',
        html_content=""" <h3>End Table for analysi of Delta records has been created successfully</h3> """,
        dag=dag
)

create_delta_count_table >> email_success
