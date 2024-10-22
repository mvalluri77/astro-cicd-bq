import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)

# Custom Python logic for derriving data value
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

DATASET = "simple_bigquery_example_dag"
TABLE = "sampledata"


# Default arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definitions
with DAG(dag_id='simple_bigquery',
         catchup=False,
         schedule_interval=timedelta(days=1),
         description="Example DAG showcasing loading and data quality checking with BigQuery.",
         default_args=default_args
         ) as dag:

    begin = DummyOperator(task_id="begin")

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET,
        gcp_conn_id='google_cloud_conn_id'
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET,
        table_id=TABLE,
        schema_fields=[
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "STRING"},
            {"name": "year", "type": "INTEGER"},
            {"name": "city", "type": "STRING"},
            
        ],
        gcp_conn_id='google_cloud_conn_id'
    )

    check_table_exists = BigQueryTableExistenceSensor(
        task_id="check_for_table",
        project_id="high-plating-431207-s0",
        dataset_id=DATASET,
        table_id=TABLE,
        gcp_conn_id='google_cloud_conn_id'
    )

    load_data = BigQueryInsertJobOperator(
        task_id="insert_query",
        configuration={
            "load": {
                "sourceUris": [f"gs://sample_bucket21/*.csv"],
                "destinationTable": {
                    "projectId": "high-plating-431207-s0",
                    "datasetId": DATASET,
                    "tableId": TABLE,
                },
                "sourceFormat": "CSV",
                "autodetect": True,
            }
        },
        gcp_conn_id='google_cloud_conn_id'
    )

    check_bq_row_count = BigQueryValueCheckOperator(
        task_id="check_row_count",
        sql=f"SELECT COUNT(*) FROM {DATASET}.{TABLE}",
        pass_value=300, 
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_conn_id'
    )


    end = DummyOperator(task_id="end")

begin >> create_dataset >> create_table >> check_table_exists >> load_data >> check_bq_row_count >> end
