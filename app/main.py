# main.py
from __future__ import annotations
import os
import json
import logging
from logging.handlers import RotatingFileHandler
import tempfile
import asyncio
import time
import concurrent.futures
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple, Optional, Union
import pandas as pd
import functions_framework
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from google.cloud import bigquery, storage, secretmanager
from google.oauth2 import service_account
import pytz
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

import subprocess

# Import your utility modules
from app.utils.config import Config
from app.utils.gcp_clients import get_storage_client, get_bigquery_client, get_ga4_credentials_json
from app.utils.pubsub_utils import publish_to_pubsub
from app.utils.validation import validate_config

# Configure logging
def setup_logging():
    """Set up proper logging with rotation and formatting."""
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, 'ga4_connector.log')
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # File handler with rotation (10MB max, keep 5 backups)
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    # Suppress specific warnings
    logging.getLogger('google.cloud.bigquery.core').setLevel(logging.ERROR)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
    
    # Suppress specific warnings
    import warnings
    warnings.filterwarnings("ignore", message="Cannot create BigQuery Storage client")
    warnings.filterwarnings("ignore", category=UserWarning)
    
    return logger

# Initialize configurations and logging
logger = setup_logging()
config = Config()
#validate_config(config)  # Ensure everything is correct at startup
os.environ["TZ"] = "UTC"

# Constants
REQUIRED_COLUMNS = ['property_id', 'date', '_airbyte_raw_id', '_airbyte_extracted_at', '_airbyte_meta']
os.environ["GOOGLE_CLOUD_PROJECT"] = config.CLIENT_PROJECT_ID

# Utility functions
def get_ga4_credentials():
    """Get GA4 credentials from Secret Manager, local file, or OAuth."""

    # 1. Service Account depuis Secret Manager
    sa_secret = getattr(config, "GA4_SERVICE_ACCOUNT_SECRET", None)

    if sa_secret:
        try:
            creds_json_dict = get_ga4_credentials_json(sa_secret)

            logger.info(f"Using GA4 Service Account credentials from Secret Manager: {sa_secret}")

            return {
                "auth_type": "Service",
                "credentials_json": json.dumps(creds_json_dict)
            }

        except Exception as e:
            logger.error(f"Error reading GA4 service account secret {sa_secret}: {str(e)}")
            raise

    # 2. Service Account depuis fichier local
    sa_path = getattr(config, "GA4_SERVICE_ACCOUNT_PATH", None)

    if sa_path:
        if os.path.exists(sa_path):
            with open(sa_path, "r") as f:
                creds_json = f.read()

            logger.info(f"Using GA4 Service Account credentials from file: {sa_path}")

            return {
                "auth_type": "Service",
                "credentials_json": creds_json
            }

    # 3. Fallback OAuth
    client_id = os.environ.get("GA4_CLIENT_ID")
    client_secret = os.environ.get("GA4_CLIENT_SECRET")
    refresh_token = os.environ.get("GA4_REFRESH_TOKEN")

    if all([client_id, client_secret, refresh_token]):
        logger.info("Using OAuth Client credentials from environment.")

        return {
            "auth_type": "Client",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
        }

    raise ValueError("No valid GA4 credentials found")

def create_sync_state_table_if_not_exists(bigquery_client: bigquery.Client) -> None:
    """Create the sync state tracking table if it doesn't exist."""
    try:
        table_id = f"{config.CLIENT_PROJECT_ID}.{config.CLIENT_DATASET_ID}.airbyte_sync_state"
        
        try:
            bigquery_client.get_table(table_id)
            logger.info(f"Sync state table {table_id} already exists")
            return
        except Exception:
            # Table doesn't exist, create it
            schema = [
                bigquery.SchemaField("connector_id", "STRING", description="Unique identifier for the connector"),
                bigquery.SchemaField("last_sync_time", "TIMESTAMP", description="Last successful sync timestamp"),
                bigquery.SchemaField("status", "STRING", description="Status of the last sync"),
                bigquery.SchemaField("details", "STRING", description="Additional details about the sync"),
                bigquery.SchemaField("created_at", "TIMESTAMP", description="Record creation timestamp")
            ]
            
            table = bigquery.Table(table_id, schema=schema)
            # Set time to live for records (optional - 90 days)
            table.expires = datetime.now() + timedelta(days=90)
            
            bigquery_client.create_table(table)
            logger.info(f"Created sync state table {table_id}")
    except Exception as e:
        logger.error(f"Error creating sync state table: {str(e)}")
        raise

def get_last_sync_time(connector_id: str = "ga4_analytics") -> Optional[datetime]:
    """
    Retrieve the last sync time from the state table in BigQuery.
    
    Args:
        connector_id: Identifier for the connector (default: 'ga4_analytics')
        
    Returns:
        The last sync timestamp or None if not found
    """
    try:
        bigquery_client = get_bigquery_client()
        
        # Ensure the state table exists
        create_sync_state_table_if_not_exists(bigquery_client)
        
        # Query for the last sync time
        query = f"""
            SELECT last_sync_time 
            FROM `{config.CLIENT_PROJECT_ID}.{config.CLIENT_DATASET_ID}.airbyte_sync_state` 
            WHERE connector_id = @connector_id
            ORDER BY last_sync_time DESC 
            LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("connector_id", "STRING", connector_id)
            ]
        )
        
        query_job = bigquery_client.query(query, job_config=job_config)
        results = query_job.result()
        
        # Extract the result
        for row in results:
            sync_time = row.last_sync_time
            logger.info(f"Retrieved last sync time: {sync_time}")
            
            # Convert string to datetime if needed
            if isinstance(sync_time, str):
                try:
                    sync_time = datetime.fromisoformat(sync_time)
                except ValueError:
                    # Try parsing as RFC 3339 format (BigQuery standard)
                    sync_time = datetime.strptime(sync_time, "%Y-%m-%dT%H:%M:%S.%fZ")
            
            return sync_time
            
        logger.info(f"No previous sync time found for connector {connector_id}")
        return None
        
    except Exception as e:
        logger.error(f"Error retrieving last sync time: {str(e)}")
        return None

def save_last_sync_time(sync_time: datetime, 
                       connector_id: str = "ga4_analytics", 
                       status: str = "success", 
                       details: str = None) -> bool:
    """
    Save the last sync time to the state table in BigQuery.
    
    Args:
        sync_time: The timestamp to save
        connector_id: Identifier for the connector (default: 'ga4_analytics')
        status: Status of the sync (default: 'success')
        details: Additional details about the sync
        
    Returns:
        True if successful, False otherwise
    """
    try:
        bigquery_client = get_bigquery_client()
        
        # Ensure the state table exists
        create_sync_state_table_if_not_exists(bigquery_client)
        
        # Table reference
        table_id = f"{config.CLIENT_PROJECT_ID}.{config.CLIENT_DATASET_ID}.airbyte_sync_state"
        
        # Convert datetime objects to strings for JSON serialization
        sync_time_str = sync_time.isoformat() if isinstance(sync_time, datetime) else sync_time
        current_time_str = datetime.now().isoformat()
        
        # Insert the new state
        rows_to_insert = [{
            "connector_id": connector_id,
            "last_sync_time": sync_time_str,  # Use string format
            "status": status,
            "details": details or "",
            "created_at": current_time_str  # Use string format
        }]
        
        logger.info(f"Inserting sync state: {rows_to_insert}")
        
        errors = bigquery_client.insert_rows_json(table_id, rows_to_insert)
        
        if errors:
            logger.error(f"Error inserting sync state: {errors}")
            return False
            
        logger.info(f"Successfully saved sync time {sync_time} for connector {connector_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving last sync time: {str(e)}")
        return False

def read_csv_from_gcs(storage_client: storage.Client, bucket_name: str, file_name: str) -> pd.DataFrame:
    """Read CSV data from GCS and return as DataFrame."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        if not blob.exists():
            raise FileNotFoundError(f"File {file_name} not found in bucket {bucket_name}")
            
        logger.info(f"Reading file {file_name} from bucket {bucket_name}")
        
        # Read in chunks to handle large files
        with blob.open("r") as f:
            chunk_list = []
            for chunk in pd.read_csv(f, chunksize=100000):
                chunk_list.append(chunk)
            
        if not chunk_list:
            raise ValueError(f"File {file_name} is empty")
            
        return pd.concat(chunk_list, ignore_index=True)
    except Exception as e:
        logger.error(f"Error reading CSV from GCS: {str(e)}")
        raise

def validate_data(df: pd.DataFrame) -> None:
    """Validate the DataFrame has the required columns."""
    missing_columns = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing_columns:
        error_msg = f"Missing required columns: {', '.join(missing_columns)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Validate data types
    if 'date' in df.columns:
        try:
            # Ensure date column is properly formatted
            df['date'] = pd.to_datetime(df['date'])
        except Exception as e:
            logger.error(f"Error converting date column: {str(e)}")
            raise ValueError(f"Invalid date format in date column: {str(e)}")
    
    logger.info(f"Data validation passed, found {len(df)} rows")

def get_bq_type(column_name, dtype):
    """Determine BigQuery data type from pandas dtype."""
    column_name = column_name.lower()
    
    if column_name == 'date':
        return "DATE"
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        if pd.api.types.is_datetime64_ns_dtype(dtype):
            try:
                all_times_midnight = all(dtype.dt.time == pd.Timestamp("00:00:00").time())
                if all_times_midnight:
                    return "DATE"
            except (AttributeError, ValueError):
                pass
        return "TIMESTAMP"
    else:
        return "STRING"

@retry(
    retry=retry_if_exception_type((Exception)),
    wait=wait_exponential(multiplier=1, min=4, max=120),
    stop=stop_after_attempt(5)
)
def load_to_bigquery(df: pd.DataFrame, bigquery_client: bigquery.Client, table_name: str, 
                    write_disposition: str = "WRITE_TRUNCATE") -> None:
    """Load DataFrame to BigQuery with schema matching CSV headers and retry logic."""
    table_id = f'{config.CLIENT_PROJECT_ID}.{config.CLIENT_DATASET_ID}.{table_name}'
    logger.info(f"Loading {len(df)} rows to BigQuery table: {table_id}")
    
    # Infer schema from DataFrame
    schema = [bigquery.SchemaField(column, get_bq_type(column, df[column].dtype)) 
              for column in df.columns]
    
    # Add metadata fields if not present
    if '_airbyte_ab_id' not in df.columns:
        df['_airbyte_ab_id'] = [f"ab_{i}" for i in range(len(df))]
    
    if '_airbyte_emitted_at' not in df.columns:
        df['_airbyte_emitted_at'] = datetime.now()
    
    # Set up job config
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=getattr(bigquery.WriteDisposition, write_disposition),
        source_format=bigquery.SourceFormat.CSV
    )
    
    try:
        # Check if table exists
        try:
            bigquery_client.get_table(table_id)
            logger.info(f"Table {table_id} exists, using {write_disposition}")
        except Exception:
            logger.info(f"Table {table_id} does not exist, will be created")
        
        # Load data
        load_job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        
        # Get the job result
        errors = load_job.errors
        if errors:
            logger.error(f"Errors during load: {errors}")
            raise Exception(f"BigQuery load errors: {errors}")
            
        logger.info(f"Successfully loaded {len(df)} rows into {table_id}")
        
    except Exception as e:
        logger.error(f"Error loading data to BigQuery: {str(e)}")
        raise

@functions_framework.cloud_event
def gcs_to_bigquery(cloud_event):
    """Cloud Function to process GCS events and load data to BigQuery."""
    data = cloud_event.data
    event_id = cloud_event["id"]
    event_type = cloud_event["type"]
    bucket_name = data["bucket"]
    file_name = data["name"]
    
    logger.info(f"Processing GCS event: Event ID: {event_id}, Type: {event_type}, File: {file_name}")

    if not file_name.endswith('.csv'):
        logger.info(f"Skipping non-CSV file: {file_name}")
        return "Skipped non-CSV file"

    storage_client = get_storage_client()
    bigquery_client = get_bigquery_client()

    try:
        df = read_csv_from_gcs(storage_client, bucket_name, file_name)
        validate_data(df)
        
        # Extract table name from file name (remove extension)
        table_name = os.path.splitext(os.path.basename(file_name))[0]
        
        # Load to BigQuery
        load_to_bigquery(df, bigquery_client, table_name)
        
        logger.info(f"Successfully processed file {file_name}")
        return "Success"
    except Exception as e:
        error_message = f"Error processing file {file_name}: {str(e)}"
        logger.error(error_message)
        
        # Send error notification
        publish_to_pubsub(config.SERVICE_SLACK_MESSAGE_TOPIC, {
            "status": "error",
            "message": error_message,
            "file": file_name,
            "timestamp": datetime.now().isoformat()
        })
        
        raise

@retry(
    retry=retry_if_exception_type((Exception)),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    stop=stop_after_attempt(3)
)
def process_report(bigquery_client: bigquery.Client, report: str, query_date: str) -> Tuple[str, bool]:
    """Process a single report with retry logic and return the report name and success status."""
    try:
        source_table = f"{config.CLIENT_PROJECT_ID}.{config.CLIENT_DATASET_ID}.{report}" # intraday4 table 
        destination_table = f"{config.CLIENT_PROJECT_ID}.{config.CLIENT_DATASET_ID}.{report.replace('4', '')}" # daily table
        
        logger.info(f"Processing report {report} for date {query_date}")
        
        # Query to filter data for the specified date (using parameterized query to prevent SQL injection)
        query = f"""
            SELECT *
            FROM `{source_table}`
            WHERE date = @query_date
            """

        # Set up job configuration with query parameters
        job_config = bigquery.QueryJobConfig(
            destination=destination_table,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            query_parameters=[
                bigquery.ScalarQueryParameter("query_date", "DATE", query_date)
            ]
        )
        
        # Execute query
        query_job = bigquery_client.query(query, job_config=job_config)
        result = query_job.result()  # Wait for job to complete
        
        # Get statistics
        total_rows = 0
        # Use destination_table to get the number of rows
        destination_table_ref = bigquery_client.get_table(destination_table)
        inserted_rows = destination_table_ref.num_rows
        
        logger.info(f"Data from {source_table} inserted into {destination_table} for date {query_date}. Rows: {inserted_rows}")
        return report, True
        
    except Exception as e:
        logger.error(f"Error processing report {report}: {str(e)}")
        return report, False

def process_all_reports(bigquery_client: bigquery.Client, query_date: str) -> Dict[str, bool]:
    """Process all reports and return a dictionary of results."""
    results = {}
    reports = config.AVAILABLE_REPORTS.copy()
    
    logger.info(f"Processing {len(reports)} reports for date {query_date}")
    
    # Process in batches to avoid overloading
    batch_size = 5
    for i in range(0, len(reports), batch_size):
        batch = reports[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}, reports: {batch}")
        
        # Add delay between batches
        if i > 0:
            time.sleep(5)  # 5 second delay between batches
        
        # Process batch in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
            # Create a dictionary mapping from future to report name
            future_to_report = {
                executor.submit(process_report, bigquery_client, report, query_date): report
                for report in batch
            }
            
            # Process as they complete
            for future in concurrent.futures.as_completed(future_to_report):
                report = future_to_report[future]
                try:
                    report_name, success = future.result()
                    results[report_name] = success
                except Exception as e:
                    logger.error(f"Exception processing report {report}: {str(e)}")
                    results[report] = False
    
    return results

async def process_reports(request: Request):
    """Process all reports for a given date with improved error handling."""
    try:
        # Get BigQuery client
        bigquery_client = get_bigquery_client()
        
        # Get query date from request or use default (4 days ago)
        query_date = request.query_params.get('date') or (
            datetime.now() - timedelta(days=4)).strftime('%Y-%m-%d')
        
        logger.info(f"Processing reports for date: {query_date}")
        
        # Process all reports
        results = process_all_reports(bigquery_client, query_date)
        
        # Check results
        all_successful = all(results.values())
        failed_reports = [report for report, success in results.items() if not success]
        successful_reports = [report for report, success in results.items() if success]
        
        # Create response message
        if all_successful:
            status = "success"
            message = "All reports processed successfully"
        else:
            status = "partial_failure"
            message = f"Some reports failed to process: {', '.join(failed_reports)}"
        
        # Create detailed response
        response_data = {
            "status": status,
            "message": message,
            "date": query_date,
            "successful_reports": successful_reports,
            "failed_reports": failed_reports,
            "total_reports": len(results),
            "success_count": len(successful_reports),
            "failure_count": len(failed_reports),
            "timestamp": datetime.now().isoformat()
        }
        
        # Publish to Pub/Sub
        publish_to_pubsub(config.SERVICE_GA4_TO_REPORT_TOPIC, response_data)
        
        # Always publish completion message
        publish_to_pubsub(config.SERVICE_GA4_TO_REPORT_TOPIC, response_data)
        
        logger.info(f"Processed {len(successful_reports)}/{len(results)} reports successfully")
        
        return (json.dumps(response_data), 200)
        
    except Exception as e:
        error_message = f"Error processing reports: {str(e)}"
        logger.error(error_message)
        
        # Create detailed error message
        error_response = {
            "status": "error",
            "message": error_message,
            "timestamp": datetime.now().isoformat()
        }
        
        # Publish error to Pub/Sub
        publish_to_pubsub(config.SERVICE_SLACK_MESSAGE_TOPIC, error_response)
        
        return (json.dumps(error_response), 500)

@functions_framework.http
def intradays_process(request):
    """Cloud Function entry point for processing intraday reports."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(process_reports(request))
    loop.close()
    logger.info("intradays_process Done!")
    return result

# main.py
def run_airbyte_command(args: list[str]) -> list[dict]:
    process = subprocess.run(
        args,
        capture_output=True,
        text=True,
        check=False
    )

    if process.returncode != 0:
        logger.error(f"Airbyte command failed: {' '.join(args)}")
        logger.error(process.stderr)
        raise Exception(process.stderr or process.stdout)

    messages = []
    for line in process.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            messages.append(json.loads(line))
        except json.JSONDecodeError:
            logger.info(f"Non-JSON Airbyte output: {line}")

    return messages


def write_json_tmp(filename: str, data: dict) -> str:
    path = f"/tmp/{filename}"
    with open(path, "w") as f:
        json.dump(data, f)
    return path


def build_configured_catalog(discover_messages: list[dict], wanted_streams: list[str]) -> dict:
    catalog = None

    for msg in discover_messages:
        if msg.get("type") == "CATALOG":
            catalog = msg.get("catalog")

    if not catalog:
        raise Exception("No Airbyte catalog returned by discover")

    selected_streams = []
    available = []

    for stream_obj in catalog.get("streams", []):
        stream_def = stream_obj.get("stream", stream_obj)
        stream_name = stream_def.get("name")

        if not stream_name:
            logger.warning(f"Skipping malformed catalog stream: {stream_obj}")
            continue

        available.append(stream_name)

        if stream_name in wanted_streams:
            selected_streams.append({
                "stream": stream_def,
                "sync_mode": "full_refresh",
                "destination_sync_mode": "overwrite"
            })

    if not selected_streams:
        raise Exception(f"No matching streams found. Wanted={wanted_streams}, Available={available}")

    return {"streams": selected_streams}

def get_ga4_source_config(start_date: str, end_date: str):
    return {
        "property_ids": [config.PROPERTY_IDS],
        "date_ranges_start_date": start_date,
        "date_ranges_end_date": end_date,
        "window_in_days": 1,
        "credentials": get_ga4_credentials(),
        "custom_reports_array": [
            {
                "name": "overall_report",
                "dimensions": ["date"],
                "metrics": [
                    "sessions",
                    "totalUsers",
                    "bounceRate",
                    "averageSessionDuration",
                    "engagedSessions",
                    "userEngagementDuration",
                    "addToCarts",
                    "checkouts",
                    "ecommercePurchases",
                    "purchaseRevenue"
                ],
                "date_ranges": [
                    {
                        "start_date": start_date,
                        "end_date": end_date
                    }
                ]
            },
            {
                "name": "cart_events",
                 "dimensions": [
                    "date",
                    "eventName"
                ],
                "metrics": [
                    "eventCount"
                ],
                "dimension_filter": {
                    "filter": {
                        "field_name": "eventName",
                        "string_filter": {
                            "match_type": "EXACT",
                            "value": "view_cart"
                        }
                    }
                },
                "date_ranges": [
                    {
                        "start_date": start_date,
                        "end_date": end_date
                    }
                ]
            },
            {
                "name": "cart_page_sessions",
                "dimensions": [
                    "date",
                    "pagePath"
                ],
                "metrics": [
                    "sessions",
                    "screenPageViews"
                ],
                "dimension_filter": {
                    "filter": {
                        "field_name": "pagePath",
                        "string_filter": {
                            "match_type": "EXACT",
                            "value": "/cart"
                        }
                    }
                },
                "date_ranges": [
                    {
                        "start_date": start_date,
                        "end_date": end_date
                    }
                ]
            },
            {
                "name": "purchase_sessions",
                "dimensions": [
                    "date",
                    "eventName"
                ],
                "metrics": [
                    "sessions",
                    "eventCount"
                ],
                "dimension_filter": {
                    "filter": {
                        "field_name": "eventName",
                            "string_filter": {
                                "match_type": "EXACT",
                                "value": "purchase"
                            }
                    }
                },
                "date_ranges": [
                    {
                        "start_date": start_date,
                        "end_date": end_date
                    }
                ]
            }
        ]
    }

def process_ga4_reports_standalone(available_reports: list[str]) -> dict:
    current_time = datetime.now()
    start_date = (current_time.date() - timedelta(days=10)).strftime("%Y-%m-%d")
    end_date = (current_time.date() - timedelta(days=1)).strftime("%Y-%m-%d")

    source_config = get_ga4_source_config(start_date, end_date)

    config_path = write_json_tmp("ga4_config.json", source_config)

    logger.info("Checking GA4 Airbyte connector")
    check_messages = run_airbyte_command([
        "source-google-analytics-data-api",
        "check",
        "--config",
        config_path
    ])

    logger.info(f"Check result: {check_messages}")

    logger.info("Discovering GA4 streams")
    discover_messages = run_airbyte_command([
        "source-google-analytics-data-api",
        "discover",
        "--config",
        config_path
    ])

    wanted_streams = [
        report.replace("_intradays4", "")
        for report in available_reports
    ]

    catalog = build_configured_catalog(discover_messages, wanted_streams)
    catalog_path = write_json_tmp("ga4_catalog.json", catalog)

    logger.info(f"Reading GA4 streams: {wanted_streams}")
    read_messages = run_airbyte_command([
        "source-google-analytics-data-api",
        "read",
        "--config",
        config_path,
        "--catalog",
        catalog_path
    ])

    records_by_stream = {}

    for msg in read_messages:
        if msg.get("type") != "RECORD":
            continue

        record = msg.get("record", {})
        stream = record.get("stream") or record.get("stream_descriptor", {}).get("name")
        data = record.get("data")

        if not stream or data is None:
            logger.warning(f"Skipping malformed Airbyte record: {msg}")
            continue

        records_by_stream.setdefault(stream, []).append(data)

    bigquery_client = get_bigquery_client()
    successful_reports = []
    failed_reports = []

    for report in available_reports:
        stream_name = report.replace("_intradays4", "")
        rows = records_by_stream.get(stream_name, [])

        if not rows:
            logger.warning(f"No data returned for {stream_name}")
            failed_reports.append(report)
            continue

        df = pd.DataFrame(rows)

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

        df["_processed_at"] = datetime.now().isoformat()

        try:
            load_to_bigquery(df, bigquery_client, report)
            successful_reports.append(report)
        except Exception as e:
            logger.error(f"Error loading {report}: {str(e)}")
            failed_reports.append(report)

    status = "success" if not failed_reports else "partial_failure"

    return {
        "status": status,
        "successful_reports": successful_reports,
        "failed_reports": failed_reports,
        "message": "GA4 import completed",
    }


def initialize_ga4_source_with_custom_reports():
    validate_config(config)
    config.validate()
    return process_ga4_reports_standalone(config.AVAILABLE_REPORTS)

app = FastAPI()

@app.get("/")
async def test_function(request: Request):
    """API endpoint to process reports."""
    return await process_reports(request)

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/process")
async def process_endpoint(request: Request):
    return await process_reports(request)

@app.get("/init")
async def setup_default_daily_table(request: Request):
    try:
        result = initialize_ga4_source_with_custom_reports()
        return result
    except Exception as e:
        logger.error(f"Error in setup endpoint: {str(e)}")
        return {"status": "error", "message": str(e)}

# For local execution
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
    
    # Uncomment to run the API locally
    # uvicorn.run(app, host="0.0.0.0", port=8080, reload=True)