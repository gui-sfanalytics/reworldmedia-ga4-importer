# GA4 to BigQuery Data Pipeline

## Overview
A production-ready Google Analytics 4 (GA4) data pipeline that syncs analytics data to Google BigQuery using Airbyte connectors.

## Core Features
- **Incremental Sync**: Uses Airbyte to pull data from GA4 based on date ranges.
- **BigQuery Integration**: Maps GA4 data to BigQuery tables.
- **Intraday Processing**: Specifically handles the transition from intraday reports to daily consolidated tables.
- **Deployment Options**: Designed for Cloud Functions (GCS/HTTP triggers) or Cloud Run (FastAPI).

## Component Structure
- `app/main.py`: Main entry point, FastAPI app, and Cloud Function handlers.
- `app/utils/`:
    - `config.py`: Configuration management (currently contains some hardcoded test values).
    - `gcp_clients.py`: GCP client initializations (BigQuery, Storage, Secret Manager).
    - `bigquery_utils.py`: (Assumed helper for BQ operations).
    - `pubsub_utils.py`: Utilities for error/status notifications.

## Active Status
- [ ] Refactor hardcoded values in `config.py` to use environment variables.
- [ ] Verify connection to GA4 and BigQuery.
- [ ] Implement robust error handling for API rate limits.

## Infrastructure Dependencies
- **GCP Project**: `optimalways` (from hardcoded config)
- **Dataset**: `poc_ga4_export`
- **Secrets**: `BIGQUERY_CREDS_SECRET_NAME`, `GA4_CLIENT_ID`, etc.
