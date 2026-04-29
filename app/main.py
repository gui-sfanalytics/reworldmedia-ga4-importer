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
import airbyte as ab
import pandas as pd
import functions_framework
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from google.cloud import bigquery, storage, secretmanager
from google.oauth2 import service_account
import pytz
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from airbyte.caches.bigquery import BigQueryCache
from airbyte.secrets.google_gsm import GoogleGSMSecretManager

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
validate_config(config)  # Ensure everything is correct at startup
os.environ["TZ"] = "UTC"

# Constants
REQUIRED_COLUMNS = ['property_id', 'date', '_airbyte_raw_id', '_airbyte_extracted_at', '_airbyte_meta']
os.environ["GOOGLE_CLOUD_PROJECT"] = config.CLIENT_PROJECT_ID

# Utility functions
def get_ga4_credentials():
    """Get GA4 credentials from configuration (Service Account or OAuth)."""
    # Check for Service Account first
    sa_path = getattr(config, 'GA4_SERVICE_ACCOUNT_PATH', None)
    if sa_path:
        if os.path.exists(sa_path):
            try:
                with open(sa_path, 'r') as f:
                    creds_json = f.read()
                    logger.info(f"Using Service Account credentials from {sa_path}.")
                    return {
                        "auth_type": "Service",
                        "credentials_json": creds_json
                    }
            except Exception as e:
                logger.error(f"Error reading service account file {sa_path}: {str(e)}")

    # Fallback to OAuth
    client_id = os.environ.get('GA4_CLIENT_ID')
    client_secret = os.environ.get('GA4_CLIENT_SECRET')
    refresh_token = os.environ.get('GA4_REFRESH_TOKEN')
    
    if all([client_id, client_secret, refresh_token]):
        logger.info("Using OAuth Client credentials from environment.")
        return {
            "auth_type": "Client",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
        }

    logger.error("No valid GA4 credentials found (Service Account or OAuth)")
    raise ValueError("Missing required GA4 credentials")

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
def initialize_bigquery_cache():
    """Initialize a BigQuery cache for Airbyte."""
    try:
        # Get credentials for BQ
        credentials_json = get_ga4_credentials_json(config.CLIENT_BIGQUERY_CREDS)
        
        # Check if credentials_json is already a dict
        if isinstance(credentials_json, str):
            credentials_dict = json.loads(credentials_json)
        else:
            credentials_dict = credentials_json
        
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Create dataset if it doesn't exist
        raw_dataset_id = f"{config.CLIENT_DATASET_ID}_raw"
        client = bigquery.Client(
            project=config.CLIENT_PROJECT_ID,
            credentials=credentials
        )
        
        dataset_ref = f"{config.CLIENT_PROJECT_ID}.{raw_dataset_id}"
        try:
            client.get_dataset(dataset_ref)
            logger.info(f"Dataset {raw_dataset_id} already exists")
        except Exception:
            # Create the dataset
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Set your preferred location
            client.create_dataset(dataset)
            logger.info(f"Created dataset {raw_dataset_id}")
        
        # Create the BQ cache
        bq_cache = BigQueryCache(
            project_name=config.CLIENT_PROJECT_ID,
            dataset_name=raw_dataset_id,
            credentials_path=config.CLIENT_BIGQUERY_CREDS if config.CLIENT_BIGQUERY_CREDS.endswith('.json') else None
        )
        
        return bq_cache
    except Exception as e:
        logger.error(f"Error initializing BigQuery cache: {str(e)}")
        return ab.get_default_cache()  # Fall back to default cache

@retry(
    retry=retry_if_exception_type((Exception)),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    stop=stop_after_attempt(5)
)
def process_stream_with_retry(source: ab.Source, report_name: str, cache) -> pd.DataFrame:
    """Process a GA4 stream with retry logic."""
    logger.info(f"Processing stream: {report_name}")
    
    # Select the stream
    source.select_streams(report_name)
    
    # Read the data with force_full_refresh to ensure fresh data
    result = source.read(cache=cache, force_full_refresh=True)
    
    # Get data from cache
    if report_name not in cache:
        raise KeyError(f"Stream {report_name} not found in cache after reading")
    
    # Convert to pandas DataFrame
    try:
        dataset = cache[report_name].to_pandas()
    except Exception as e:
        # Fallback for BigQueryCache which may not support to_pandas()
        logger.info(f"Cache to_pandas() failed for {report_name}, attempting direct BigQuery query: {str(e)}")
        try:
            bigquery_client = get_bigquery_client()
            # BigQueryCache uses dataset_name (aliased to schema_name)
            dataset_name = getattr(cache, 'dataset_name', f"{config.CLIENT_DATASET_ID}_raw")
            table_id = f"{config.CLIENT_PROJECT_ID}.{dataset_name}.{report_name}"
            
            logger.info(f"Querying table directly: {table_id}")
            query = f"SELECT * FROM `{table_id}`"
            dataset = bigquery_client.query(query).to_dataframe()
        except Exception as bq_err:
            logger.error(f"Fallback Direct BigQuery query failed: {str(bq_err)}")
            raise e # Raise original error if fallback fails
            
    if dataset.empty:
        logger.warning(f"No data returned for stream {report_name}")
    else:
        logger.info(f"Retrieved {len(dataset)} rows for stream {report_name}")
    
    return dataset

def process_ga4_reports(available_reports: List[str], source: ab.Source) -> Dict[str, Union[List[str], str]]:
    """Process each report in the available_reports list with improved error handling."""
    # Try to use BigQuery cache or fall back to default
    try:
        cache = initialize_bigquery_cache()
        # cache = ab.get_default_cache()
        logger.info("Using BigQuery cache for Airbyte")
    except Exception as e:
        logger.warning(f"Falling back to default cache: {str(e)}")
        cache = ab.get_default_cache()
    
    failed_reports = []
    successful_reports = []
    bigquery_client = get_bigquery_client()
    
    for report_name in available_reports:
        try:
            # Strip suffix for the source stream name
            stream_name = report_name.replace('_intradays4', '')
            logger.info(f"Processing report: {report_name} (Source stream: {stream_name})")
            
            # Process stream with retry logic
            dataset = process_stream_with_retry(source, stream_name, cache)
            
            # Prepare data for BigQuery
            bq_tablename = f"{report_name}"
            # bq_tablename = f"{report_name}"
            # Convert the date column from YYYYMMDD to YYYY-MM-DD
            if 'date' in dataset.columns:
                dataset['date'] = pd.to_datetime(dataset['date'], format='%Y%m%d')
                dataset['date'] = dataset['date'].dt.strftime('%Y-%m-%d')
            
            # Add metadata
            dataset['_processed_at'] = datetime.now().isoformat()
            
            # Save to temporary CSV (useful for debugging)
            csv_filename = f"{report_name}.csv"
            csv_path = f"/tmp/{csv_filename}"
            dataset.to_csv(csv_path, index=False)
            logger.info(f"Saved CSV to {csv_path}")
            
            # Load to BigQuery
            load_to_bigquery(dataset, bigquery_client, bq_tablename)
            
            logger.info(f"Successfully processed {report_name}")
            successful_reports.append(report_name)
            
        except KeyError as e:
            logger.error(f"Error: {report_name} not found in cache. Skipping. Details: {str(e)}")
            failed_reports.append(report_name)
        except Exception as e:
            logger.error(f"Error processing {report_name}: {str(e)}")
            failed_reports.append(report_name)
    
    # Prepare result
    result = {
        "successful_reports": successful_reports,
        "failed_reports": failed_reports
    }
    
    # Prepare and send Pub/Sub message
    if not failed_reports:
        message = "All available_reports processed"
        result["status"] = "success"
    else:
        message = f"The following reports failed: {', '.join(failed_reports)}"
        result["status"] = "partial_failure"
    
    # Include message in result
    result["message"] = message
    
    # Publish to Pub/Sub
    publish_to_pubsub(config.SERVICE_GA4_TO_REPORT_TOPIC, message)
    
    logger.info(f"process_reports Done! {len(successful_reports)}/{len(available_reports)} successful")
    return result

def get_ga4_source_config(start_date: str, end_date: str):
    """Get properly configured GA4 source with custom reports."""
    ga4_credentials = get_ga4_credentials()
    
    source_config = {
        "property_ids": [config.PROPERTY_IDS],
        "date_ranges_start_date": start_date,
        "date_ranges_end_date": end_date,
        "window_in_days": 1,
        "credentials": ga4_credentials,
        "custom_reports_array": [
            {
                "name": "engagement",
                "dimensions": [
                    "date"  
                ],
                "metrics": [
                    "sessions"
                ],
                "date_ranges": [
                    {
                        "start_date": start_date,
                        "end_date": end_date
                    }
                ]
            }
        ]
    }
    
    return source_config

def initialize_ga4_source_with_custom_reports():
    """Initialize GA4 source and verify custom reports are available."""
    try:
        # Calculate date range
        current_time = datetime.now()
        start_date = (current_time - timedelta(days=10)).strftime("%Y-%m-%d")
        end_date = (current_time.date() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        source_config = get_ga4_source_config(start_date, end_date)
        
        # Create source using locally installed connector (no Docker required)
        source = ab.get_source(
            "source-google-analytics-data-api",
            config=source_config,
            local_executable="source-google-analytics-data-api"
        )
        
        # Test connection first
        check_result = source.check()
        
        
        # Get available streams AFTER configuration
        available_streams = source.get_available_streams()
        logger.info(f"Available streams after config: {available_streams}")
        
        # Process the reports
        result = process_ga4_reports(config.AVAILABLE_REPORTS, source)    
    except Exception as e:
        logger.error(f"Error initializing GA4 source: {str(e)}")
        raise
def initial_setup():
    """Set up GA4 connector and process reports with proper date handling and sync time tracking."""
    try:
        logger.info("Starting initial setup")
        
        # Get the last sync time or use default time range
        last_sync_time = get_last_sync_time()
        current_time = datetime.now()
        
        if last_sync_time:
            # Use last sync time as start date with a small overlap
            # Add a 1-day overlap to ensure no data is missed
            start_date = (last_sync_time - timedelta(days=90)).date()
            logger.info(f"Using last sync time as basis: {last_sync_time}")
        else:
            # Default to last 3 days if no previous sync
            start_date = current_time.date() - timedelta(days=3)
            logger.info("No previous sync found, using default date range")
        
        # End date is yesterday
        end_date = current_time.date() - timedelta(days=1)
        
        logger.info(f"Using date range: {start_date} to {end_date}")
        
        # Get configuration with custom reports
        source_config = get_ga4_source_config(
            start_date.strftime("%Y-%m-%d"), 
            end_date.strftime("%Y-%m-%d")
        )

        # Create and configure the source connector using locally installed connector (no Docker required)
        source = ab.get_source(
            "source-google-analytics-data-api",
            config=source_config,
            local_executable="source-google-analytics-data-api"
        )
        
        # Test connection
        logger.info("Testing connection to GA4...")
        try:
            check_result = source.check()
            logger.info(f"Connection check result: {check_result}")
        except Exception as e:
            logger.error(f"Connection check failed with error: {str(e)}")
            import traceback
            traceback.print_exc()
            # Save failed sync attempt
            save_last_sync_time(current_time, status="error", details=f"Connection check failed: {str(e)}")
            return {"status": "error", "message": f"Error connecting to GA4: {str(e)}"}
        
        # Get available streams
        try:
            logger.info("Getting available streams...")
            all_available_reports = source.get_available_streams()
            logger.info(f"Available streams: {all_available_reports}")
        except Exception as e:
            logger.error(f"Failed to get available streams: {str(e)}")
            # Save failed sync attempt
            save_last_sync_time(current_time, status="error", details=f"Failed to get streams: {str(e)}")
            return {"status": "error", "message": f"Error getting streams: {str(e)}"}
        
        # For initial testing, just use daily_active_users
        #available_reports = [
        #    'daily_active_users'
        #]
        logger.info(f"Using reports: {config.AVAILABLE_REPORTS}")
        # Process the reports
        result = process_ga4_reports(config.AVAILABLE_REPORTS, source)
        logger.info("Initial setup completed")
        
        # Save successful sync time
        if result["status"] == "success":
            save_last_sync_time(current_time, status="success")
        else:
            details = f"Partial success. Failed reports: {','.join(result.get('failed_reports', []))}"
            save_last_sync_time(current_time, status="partial", details=details)
        
        return result
        
    except Exception as e:
        logger.error(f"Error in initial_setup: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # Save failed sync attempt
        current_time = datetime.now()
        save_last_sync_time(current_time, status="error", details=f"Exception: {str(e)}")
        
        return {"status": "error", "message": f"Error: {str(e)}"}

# FastAPI app
app = FastAPI()

@app.get("/")
async def test_function(request: Request):
    """API endpoint to process reports."""
    return await process_reports(request)

@app.get("/init")
async def setup_default_daily_table(request: Request):
    """API endpoint to set up initial data."""
    try:
        result = initialize_ga4_source_with_custom_reports()
        return result
    except Exception as e:
        logger.error(f"Error in setup endpoint: {str(e)}")
        return {"status": "error", "message": str(e)}, 500

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

# For local execution
if __name__ == "__main__":
    # For local testing
    try:
        result = initialize_ga4_source_with_custom_reports()
        print(f"Initial setup result: {result}")
    except Exception as e:
        print(f"Error during execution: {str(e)}")
    
    # Uncomment to run the API locally
    # uvicorn.run(app, host="0.0.0.0", port=8080, reload=True)