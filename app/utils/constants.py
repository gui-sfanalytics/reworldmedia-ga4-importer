"""Application constants and configuration values."""

# Logging Configuration
LOG_FILE_MAX_SIZE_MB = 10
LOG_FILE_MAX_SIZE_BYTES = LOG_FILE_MAX_SIZE_MB * 1024 * 1024
LOG_BACKUP_COUNT = 5

# Data Processing
CSV_CHUNK_SIZE = 100_000  # Read CSV in 100k row chunks for memory efficiency
DATAFRAME_CHUNK_SIZE = 100_000

# Report Processing
REPORT_BATCH_SIZE = 5  # Process 5 reports in parallel to avoid quota issues
BATCH_DELAY_SECONDS = 5  # Delay between batches to prevent rate limiting

# Date Ranges
INTRADAY_DELAY_DAYS = 4  # GA4 intraday data has 4-day delay for accuracy
DEFAULT_LOOKBACK_DAYS = 90  # Default historical data range
INCREMENTAL_SYNC_LOOKBACK_DAYS = 30  # Lookback for incremental syncs
SYNC_STATE_RETENTION_DAYS = 90  # Keep sync state for 90 days
DEFAULT_DATE_RANGE_DAYS = 3  # Default range for incremental syncs

# Retry Configuration
MAX_RETRY_ATTEMPTS = 5
RETRY_MIN_WAIT_SECONDS = 4
RETRY_MAX_WAIT_SECONDS = 60
SHORT_RETRY_MAX_WAIT_SECONDS = 30
SHORT_RETRY_ATTEMPTS = 3

# BigQuery
BQ_LOAD_TIMEOUT_SECONDS = 300  # 5 minutes
BQ_QUERY_TIMEOUT_SECONDS = 600  # 10 minutes

# Pub/Sub
PUBSUB_PUBLISH_TIMEOUT_SECONDS = 30

# Table Names
SYNC_STATE_TABLE_NAME = "airbyte_sync_state"
RAW_DATASET_SUFFIX = "_raw"

# Airbyte Column Names
REQUIRED_AIRBYTE_COLUMNS = [
    'property_id',
    'date',
    '_airbyte_raw_id',
    '_airbyte_extracted_at',
    '_airbyte_meta'
]

# Report Naming
INTRADAY_TABLE_SUFFIX = "_intradays4"
DAILY_TABLE_SUFFIX = ""

# Date Formats
DATE_FORMAT_YYYYMMDD = "%Y%m%d"  # GA4 format
DATE_FORMAT_ISO = "%Y-%m-%d"  # ISO format for BigQuery
TIMESTAMP_FORMAT_ISO = "%Y-%m-%dT%H:%M:%S.%fZ"

# File Extensions
ALLOWED_CSV_EXTENSIONS = ['.csv']
ALLOWED_JSON_EXTENSIONS = ['.json']

# GCS Configuration
TEMP_FILE_PATH = "/tmp"

# Airbyte Configuration
AIRBYTE_DOCKER_IMAGE = "source-google-analytics-data-api"
AIRBYTE_DEFAULT_VERSION = "2.9.5"
