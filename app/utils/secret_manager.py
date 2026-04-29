# app/utils/secret_manager.py
from google.cloud import secretmanager
from functools import lru_cache
import os

# Load the Secret Project ID from environment variables
SECRET_PROJECT_ID = os.environ.get('SERVICE_SECRET_PROJECT_ID') or os.environ.get('SECRET_PROJECT_ID')

@lru_cache(maxsize=None)
def get_secret(secret_id: str) -> str:
    """
    Retrieve a secret from Google Cloud Secret Manager.
    """
    if not SECRET_PROJECT_ID:
        raise ValueError("Environment variable SERVICE_SECRET_PROJECT_ID is not set.")

    from app.utils.config import Config
    config = Config()
    
    # Try to use Service Account credentials if available
    creds = None
    if config.GA4_SERVICE_ACCOUNT_PATH and os.path.exists(config.GA4_SERVICE_ACCOUNT_PATH):
        try:
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_file(config.GA4_SERVICE_ACCOUNT_PATH)
        except Exception as e:
            print(f"Warning: Could not load Service Account for Secret Manager: {str(e)}")

    client = secretmanager.SecretManagerServiceClient(credentials=creds)
    name = f"projects/{SECRET_PROJECT_ID}/secrets/{secret_id}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
      print(f"Error retrieving secret {secret_id}: {str(e)}")
      raise