# GA4 to BigQuery Data Pipeline

A production-ready Google Analytics 4 (GA4) data pipeline that syncs analytics data to Google BigQuery using Airbyte connectors. The application supports both Cloud Function deployment and local FastAPI server execution.

deploy 
## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [API Endpoints](#api-endpoints)
- [Deployment](#deployment)
- [Development](#development)
- [Monitoring & Logging](#monitoring--logging)
- [Troubleshooting](#troubleshooting)
- [Project Structure](#project-structure)

## Overview

This application automates the process of extracting Google Analytics 4 data and loading it into BigQuery for analysis. It handles:

- **Incremental data sync** with state tracking to avoid duplicates
- **Multiple GA4 reports** including user metrics, traffic sources, events, and conversions
- **Intraday to daily report consolidation** for real-time data processing
- **Error handling and retry logic** with exponential backoff
- **Pub/Sub notifications** for monitoring and alerting
- **Custom report generation** using GA4 Data API

### Key Features

- Automated sync with last sync time tracking
- Support for 30+ standard GA4 reports
- Cloud Function triggers for GCS events
- RESTful API for manual triggering
- Comprehensive logging and error reporting
- Secure credential management via Google Secret Manager

## Architecture

```
┌─────────────────┐
│   GA4 API       │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────┐
│  Airbyte Connector              │
│  (source-google-analytics-      │
│   data-api)                     │
└────────┬────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│  Application                    │
│  - Data extraction              │
│  - Transformation               │
│  - State management             │
└────────┬────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│  BigQuery                       │
│  - Raw data storage             │
│  - Daily tables                 │
│  - Sync state tracking          │
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│  Pub/Sub (Notifications)        │
└─────────────────────────────────┘
```

## Prerequisites

### Required

- Python 3.11.x
- Google Cloud Platform (GCP) account with active project
- GA4 property with API access
- GCP Services enabled:
  - BigQuery API
  - Cloud Storage API
  - Secret Manager API
  - Pub/Sub API
  - Cloud Functions API (for serverless deployment)

### GCP IAM Permissions

Your service account needs:
- `bigquery.dataEditor`
- `bigquery.jobUser`
- `storage.objectViewer`
- `secretmanager.secretAccessor`
- `pubsub.publisher`

## Installation

### 1. Clone the Repository

```bash
cd GA42BIGQUERY-main
```

### 2. Set Up Python Environment

Using `uv` (recommended):

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Sync dependencies
uv sync
```

Using `pip`:

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Install Airbyte GA4 Connector

```bash
pip install airbyte-source-google-analytics-data-api
```

## Configuration

### 1. Environment Variables

Copy the template and fill in your values:

```bash
cp .env.template .env
```

Edit `.env` with your configuration:

```bash
# GCP Project Configuration
CLIENT_PROJECT_ID=your-client-project-id
CLIENT_DATASET_ID=your_dataset_id
SERVICE_PROJECT_ID=your-service-project-id
SECRET_PROJECT_ID=your-secret-project-id

# BigQuery Credentials (Secret Manager secret name)
CLIENT_BIGQUERY_CREDS=BIGQUERY_CREDS_SECRET_NAME

# GA4 Configuration
PROPERTY_IDS=123456789

# Pub/Sub Topics
SERVICE_GA4_TO_REPORT_TOPIC=ga4topic
SERVICE_SLACK_MESSAGE_TOPIC=slack_notifications
```

### 2. Configure Available Reports

Edit `app/utils/config.py` to customize which reports to sync:

```python
AVAILABLE_REPORTS = [
    'daily_active_users_intradays4',
    'weekly_active_users_intradays4',
    'devices_intradays4',
    'locations_intradays4',
    # Add or remove reports as needed
]
```

### 3. Set Up Google Cloud Secrets

Create secrets in Google Secret Manager:

```bash
# GA4 OAuth Credentials
echo -n "your-client-id" | gcloud secrets create GA4_CLIENT_ID --data-file=-
echo -n "your-client-secret" | gcloud secrets create GA4_CLIENT_SECRET --data-file=-
echo -n "your-refresh-token" | gcloud secrets create GA4_REFRESH_TOKEN --data-file=-

# BigQuery Service Account Key
gcloud secrets create BIGQUERY_CREDS_SECRET_NAME --data-file=service-account-key.json
```

### 4. Getting GA4 OAuth Credentials

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Navigate to APIs & Services > Credentials
3. Create OAuth 2.0 Client ID (type: Desktop app)
4. Use OAuth Playground to get refresh token:
   - URL: https://developers.google.com/oauthplayground
   - Select Google Analytics API v4
   - Authorize and exchange authorization code for tokens

## Usage

### Local Development

#### Run FastAPI Server

```bash
# Activate virtual environment
source .venv/bin/activate

# Run the server
python -m uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

#### Run One-Time Sync

```bash
python app/main.py
```

### API Endpoints

Once the server is running (default: http://localhost:8080):

#### Health Check
```bash
curl http://localhost:8080/health
```

#### Initial Setup (Custom Reports)
```bash
curl http://localhost:8080/init
```

#### Process Intraday Reports
```bash
# Process reports for specific date
curl "http://localhost:8080/?date=2025-01-15"

# Process reports for default date (4 days ago)
curl http://localhost:8080/
```

### Response Format

```json
{
  "status": "success",
  "message": "All reports processed successfully",
  "date": "2025-01-15",
  "successful_reports": ["report1", "report2"],
  "failed_reports": [],
  "total_reports": 30,
  "success_count": 30,
  "failure_count": 0,
  "timestamp": "2025-01-17T10:30:00"
}
```

## Deployment

### Deploy as Cloud Function

#### For GCS Trigger (gcs_to_bigquery)

```bash
gcloud functions deploy gcs_to_bigquery \
  --gen2 \
  --runtime python311 \
  --entry-point gcs_to_bigquery \
  --source . \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=your-bucket-name" \
  --set-env-vars PROJECT_ID=your-project-id \
  --memory 2GB \
  --timeout 540s
```

#### For HTTP Trigger (intradays_process)

```bash
gcloud functions deploy intradays_process \
  --gen2 \
  --runtime python311 \
  --entry-point intradays_process \
  --source . \
  --trigger-http \
  --allow-unauthenticated \
  --set-env-vars-file .env.yaml \
  --memory 2GB \
  --timeout 540s
```

### Deploy as Cloud Run

```bash
# Build container
gcloud builds submit --tag gcr.io/PROJECT_ID/ga4-pipeline

# Deploy
gcloud run deploy ga4-pipeline \
  --image gcr.io/PROJECT_ID/ga4-pipeline \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars-file .env.yaml \
  --memory 2Gi \
  --timeout 3600
```

## Development

### Project Structure

```
GA42BIGQUERY-main/
├── app/
│   ├── __init__.py
│   ├── main.py                 # Main application entry point
│   └── utils/
│       ├── bigquery_utils.py   # BigQuery operations
│       ├── config.py            # Configuration management
│       ├── constants.py         # Application constants
│       ├── gcp_clients.py       # GCP client initialization
│       ├── pubsub_utils.py      # Pub/Sub publishing
│       ├── secret_manager.py    # Secret Manager access
│       └── validation.py        # Data validation
├── .env.template               # Environment variables template
├── .gitignore                  # Git ignore rules
├── pyproject.toml              # Project metadata and dependencies
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

### Key Components

#### Main Application (`app/main.py`)

- **`initial_setup()`**: Main entry point for GA4 data sync
- **`process_reports()`**: Process intraday reports to daily tables
- **`gcs_to_bigquery()`**: Cloud Function handler for GCS events
- **`initialize_ga4_source_with_custom_reports()`**: Setup custom GA4 reports

#### Configuration (`app/utils/config.py`)

- Centralized configuration management
- Environment variable loading
- Report definitions

#### GCP Clients (`app/utils/gcp_clients.py`)

- BigQuery client initialization
- Storage client setup
- Credential management

### Adding Custom Reports

To add a custom GA4 report:

1. Edit `app/main.py` in the `initialize_ga4_source_with_custom_reports()` function:

```python
"custom_reports_array": [
    {
        "name": "my_custom_report",
        "dimensions": ["date", "pagePath", "country"],
        "metrics": ["activeUsers", "sessions"],
        "date_ranges": [
            {
                "start_date": start_date,
                "end_date": end_date
            }
        ]
    }
]
```

2. Add the report name to `config.py`:

```python
AVAILABLE_REPORTS = [
    # ... existing reports
    'my_custom_report',
]
```

### Testing

```bash
# Run with test credentials
export GA4_CLIENT_ID=test-client-id
export GA4_CLIENT_SECRET=test-client-secret
export GA4_REFRESH_TOKEN=test-refresh-token

python app/main.py
```

## Monitoring & Logging

### Logs Location

- **Local**: `app/logs/ga4_connector.log`
- **Cloud Functions**: Cloud Logging (filtered by function name)
- **Cloud Run**: Cloud Logging (filtered by service name)

### Log Levels

- **INFO**: Normal operations
- **WARNING**: Recoverable issues
- **ERROR**: Failed operations requiring attention
- **DEBUG**: Detailed diagnostic information (file logs only)

### Viewing Logs

```bash
# Cloud Functions
gcloud functions logs read intradays_process --limit 50

# Cloud Run
gcloud run services logs read ga4-pipeline --limit 50
```

### Pub/Sub Notifications

The application publishes status messages to configured topics:

- **Success**: Published to `SERVICE_GA4_TO_REPORT_TOPIC`
- **Errors**: Published to `SERVICE_SLACK_MESSAGE_TOPIC`

## Troubleshooting

### Common Issues

#### 1. Missing GA4 Credentials

```
ValueError: Missing required GA4 credentials
```

**Solution**: Ensure secrets are created in Secret Manager and environment variables point to correct secret names.

#### 2. BigQuery Permission Denied

```
403 Access Denied: BigQuery BigQuery: Permission denied
```

**Solution**: Verify service account has `bigquery.dataEditor` and `bigquery.jobUser` roles.

#### 3. Airbyte Connection Failed

```
Error initializing GA4 source: Connection check failed
```

**Solution**:
- Verify GA4 property ID is correct
- Check OAuth credentials are valid
- Ensure refresh token hasn't expired

#### 4. No Data Returned

```
WARNING: No data returned for stream report_name
```

**Solution**:
- Check date range (GA4 data may not be available for recent dates)
- Verify the report exists in your GA4 property
- Check if the property has data for the specified date range

#### 5. Schema Mismatch

```
Error loading data to BigQuery: schema mismatch
```

**Solution**: Drop and recreate the affected table, or adjust `write_disposition` to `WRITE_TRUNCATE`.

### Debug Mode

Enable detailed logging:

```python
# In app/main.py, modify setup_logging():
logger.setLevel(logging.DEBUG)
console_handler.setLevel(logging.DEBUG)
```

### State Management

Reset sync state if needed:

```sql
-- View current state
SELECT * FROM `project.dataset.airbyte_sync_state`
ORDER BY last_sync_time DESC;

-- Delete state to force full refresh
DELETE FROM `project.dataset.airbyte_sync_state`
WHERE connector_id = 'ga4_analytics';
```

## Security Best Practices

1. **Never commit `.env` file** - It's in `.gitignore` for a reason
2. **Use Secret Manager** - Store all credentials in Google Secret Manager
3. **Rotate credentials regularly** - Update OAuth tokens and service account keys
4. **Limit service account permissions** - Follow principle of least privilege
5. **Enable audit logging** - Monitor access to sensitive resources
6. **Use VPC Service Controls** - Restrict data exfiltration (production)

## Performance Optimization

### Batch Processing

Reports are processed in batches of 5 with 5-second delays to avoid rate limiting:

```python
batch_size = 5  # Adjust in app/main.py
```

### Date Range

Limit date range for initial sync to reduce API calls:

```python
# In initial_setup()
start_date = current_time.date() - timedelta(days=30)  # Adjust as needed
```

### Retry Configuration

Adjust retry behavior in decorators:

```python
@retry(
    wait=wait_exponential(multiplier=1, min=4, max=60),
    stop=stop_after_attempt(5)  # Reduce for faster failures
)
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make changes and test thoroughly
4. Commit changes: `git commit -m 'Add feature'`
5. Push to branch: `git push origin feature-name`
6. Create a Pull Request

## License

This project is provided as-is for internal use.

## Support

For issues and questions:
- Check [Troubleshooting](#troubleshooting) section
- Review application logs
- Contact the development team

## Additional Resources

- [GA4 Data API Documentation](https://developers.google.com/analytics/devguides/reporting/data/v1)
- [Airbyte Connector Documentation](https://docs.airbyte.com/integrations/sources/google-analytics-data-api)
- [BigQuery Best Practices](https://cloud.google.com/bigquery/docs/best-practices)
- [Google Cloud Secret Manager](https://cloud.google.com/secret-manager/docs)
