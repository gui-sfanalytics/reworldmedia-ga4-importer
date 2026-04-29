# app/utils/gcp_clients.py
import json
from typing import Any
from google.oauth2 import service_account
from google.cloud import storage, bigquery
import os
from app.utils.secret_manager import get_secret
from app.utils.config import Config

config = Config()

def get_client(secret_name: str, client_class: Any) -> Any:
    """
    Create a GCP client using credentials from Secret Manager.

    Args:
        secret_name (str): The name of the secret containing the service account credentials.
        client_class (Any): The GCP client class to instantiate (e.g., storage.Client, bigquery.Client).

    Returns:
        Any: An instance of the specified GCP client class.

    Raises:
        Exception: If there is an error creating the client.
    """
    try:
        # Check if the "secret_name" is actually a local file path
        if secret_name.endswith('.json') and os.path.exists(secret_name):
            with open(secret_name, 'r') as f:
                creds_json = f.read()
        else:
            creds_json = get_secret(secret_name)
            
        creds = service_account.Credentials.from_service_account_info(json.loads(creds_json))
        return client_class(credentials=creds, project=config.CLIENT_PROJECT_ID)
    except Exception as e:
        print(f"Error creating {client_class.__name__} client: {str(e)}")
        raise

def get_storage_client() -> storage.Client:
    """
    Creates a Google Cloud Storage client.

    Returns:
        storage.Client: A Google Cloud Storage client object.
    """
    return get_client('STORAGE_CREDS_SECRET_NAME', storage.Client)


def get_bigquery_client() -> bigquery.Client:
    """
    Creates a Google BigQuery client.

    Returns:
        bigquery.Client: A Google BigQuery client object.
    """
    return get_client(config.CLIENT_BIGQUERY_CREDS, bigquery.Client)

def get_ga4_credentials_json(secret_name: str):
    try:
        # Check if local file path
        if secret_name.endswith('.json') and os.path.exists(secret_name):
            with open(secret_name, 'r') as f:
                creds_json = f.read()
        else:
            creds_json = get_secret(secret_name)
        creds = json.loads(creds_json)
        return creds
    except Exception as e:
        print(f"Error get GA4 secret: {str(e)}")
        raise   