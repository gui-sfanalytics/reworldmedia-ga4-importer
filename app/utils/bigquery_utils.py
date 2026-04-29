# app/utils/bigquery_utils.py
import pandas as pd
from google.cloud import bigquery
from typing import Any
import os

# Constants
PROJECT_ID = os.environ['PROJECT_ID']
DATASET_ID = os.environ['DATASET_ID']

def get_bq_type(column_name, dtype):
    """Determine the BigQuery data type from a Pandas dtype."""

    # Explicitly set the 'date' column to DATE type
    if column_name.lower() == 'date':
        return "DATE"
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        # If the dtype is datetime64, check if all times are at midnight, indicating a date
        if pd.api.types.is_datetime64_ns_dtype(dtype):
            try:
                all_times_midnight = all(dtype.dt.time == pd.Timestamp("00:00:00").time())
                if all_times_midnight:
                   return "DATE"
            except AttributeError:
                pass # Handle cases where dtype doesn't support .dt (e.g., NaT)
        return "TIMESTAMP"
    else:
        return "STRING"

def load_to_bigquery(
    df: pd.DataFrame, bigquery_client: bigquery.Client, file_name: str
) -> None:
    """Load DataFrame to BigQuery with schema matching CSV headers."""

    table_name = os.path.splitext(file_name)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    # Infer schema from DataFrame columns
    schema = [
        bigquery.SchemaField(column, get_bq_type(column, df[column].dtype))
        for column in df.columns
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
    )

    try:
        load_job = bigquery_client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        load_job.result()  # Wait for the job to complete

        print(f"Loaded {len(df)} rows into {table_id}.")

        # Fetch and print the schema of the loaded table
        table = bigquery_client.get_table(table_id)
        print("Loaded table schema:")
        # for field in table.schema:
        #     print(f"\t{field.name}: {field.field_type}")

    except Exception as e:
        print(f"Error loading data to BigQuery: {str(e)}")
        raise


def process_report(bigquery_client: bigquery.Client, report: str, query_date: str, client_project_id: str, client_dataset_id: str) -> tuple[str, bool]:
    """Process a single report and return the report name and success status."""
    try:
        source_table = f"{client_project_id}.{client_dataset_id}.{report}"
        destination_table = f"{client_project_id}.{client_dataset_id}.{report.replace('_intradays4', '')}"
        query = f"""
            SELECT *
            FROM `{source_table}`
            WHERE date = '{query_date}'
        """
        job_config = bigquery.QueryJobConfig(destination=destination_table)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        query_job = bigquery_client.query(query, job_config=job_config)
        query_job.result()
        print(f"Data from {source_table} inserted into {destination_table} for date {query_date}.")
        return report, True

    except Exception as e:
        print(f"Error processing report {report}: {str(e)}")
        return report, False
