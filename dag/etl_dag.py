import os
import sys
from datetime import datetime, timedelta

# Add current directory to python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.smtp.operators.smtp import EmailOperator

from extract import extract_complaints_data
from load import dump_raw_data_to_mysql, dump_to_google_sheet
from transform import transform_complaints_data

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 8, 31),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

CSV_FILE_PATH = "/opt/airflow/DATA/consumer_complaints_transformed.csv"
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1z6FkbZw1uD3IqEtxfeM2-h7Nqae3Nl-N5lsMjjdGoIk"

def task_extract(**context):
    df_raw = extract_complaints_data()
    return len(df_raw)

def task_load_mysql(**context):
    dump_raw_data_to_mysql()

def task_transform(**context):
    output_path = transform_complaints_data()
    return output_path

def task_load_sheets(**context):
    sheet_url = dump_to_google_sheet()
    return sheet_url

with DAG(
    dag_id="consumer_financial_etl",
    default_args=default_args,
    description="CFPB Consumer Complaints ETL Pipeline",
    schedule="0 0 * * *",
    catchup=False,
    tags=["etl", "cde_assignment"],
) as dag:

    t1_extract = PythonOperator(
        task_id="extract_data_from_source",
        python_callable=task_extract,
    )

    t2_load_mysql = PythonOperator(
        task_id="dump_data_to_mysql",
        python_callable=task_load_mysql,
    )

    t3_transform = PythonOperator(
        task_id="transform_data",
        python_callable=task_transform,
    )

    t4_file_sensor = FileSensor(
        task_id="check_consumer_financial_csv",
        filepath=CSV_FILE_PATH,
        poke_interval=10,
        timeout=300,
        mode="poke",
    )

    t5_dump_sheets = PythonOperator(
        task_id="dump_googlesheet",
        python_callable=task_load_sheets,
    )

    t6_email_alert = EmailOperator(
        task_id="send_googlesheet_url_via_email",
        to="alishbanaeem58@gmail.com",
        subject="[SUCCESS] Consumer Financial Complaints ETL Completed",
        html_content=f"""
        <h3>Consumer Financial Complaints ETL Pipeline Completed!</h3>
        <p>Google Sheet Link: <a href="{GOOGLE_SHEET_URL}">{GOOGLE_SHEET_URL}</a></p>
        """,
    )

    t1_extract >> t2_load_mysql >> t3_transform >> t4_file_sensor >> t5_dump_sheets >> t6_email_alert
