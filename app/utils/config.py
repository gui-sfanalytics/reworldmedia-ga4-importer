# app/config.py
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file at the top
project_root = Path(__file__).parent.parent.parent
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path)

class Config:
    """Configuration class to load and validate environment variables."""
    # GCP Project Configuration
    CLIENT_PROJECT_ID = os.getenv('CLIENT_PROJECT_ID', 'optimalways')
    CLIENT_DATASET_ID = os.getenv('CLIENT_DATASET_ID', 'poc_ga4_export')
    CLIENT_BIGQUERY_CREDS = os.getenv('CLIENT_BIGQUERY_CREDS', 'BIGQUERY_CREDS_SECRET_NAME')
    
    # Service Configuration
    SERVICE_PROJECT_ID = os.getenv('SERVICE_PROJECT_ID', 'mythic-benefit-690')
    SERVICE_SECRET_PROJECT_ID = os.getenv('SERVICE_SECRET_PROJECT_ID', 'mythic-benefit-690')
    
    # Pub/Sub Configuration
    SERVICE_GA4_TO_REPORT_TOPIC = os.getenv('SERVICE_GA4_TO_REPORT_TOPIC', 'ga4topic')
    SERVICE_SLACK_MESSAGE_TOPIC = os.getenv('SERVICE_SLACK_MESSAGE_TOPIC', 'ga4topic')
    
    # GA4 Configuration
    GA4_JSON_KEY = os.getenv('GA4_JSON_KEY', 'BIGQUERY_CREDS_SECRET_NAME')
    PROPERTY_IDS = os.getenv('PROPERTY_IDS', '288233728')
    GA4_CLIENT_ID = os.getenv('GA4_CLIENT_ID')
    GA4_CLIENT_SECRET = os.getenv('GA4_CLIENT_SECRET')
    GA4_REFRESH_TOKEN = os.getenv('GA4_REFRESH_TOKEN')
    GA4_SERVICE_ACCOUNT_PATH = os.getenv('GA4_SERVICE_ACCOUNT_PATH')

    # Reports available for processing
    AVAILABLE_REPORTS = [
        'daily_active_users_intradays4',
        'engagement'
    ]

    @classmethod
    def validate(cls):
        """Validates that all required environment variables are set."""
        # Required common vars
        common_required = [
            'CLIENT_PROJECT_ID', 'CLIENT_DATASET_ID', 'CLIENT_BIGQUERY_CREDS',
            'SERVICE_PROJECT_ID', 'SERVICE_SECRET_PROJECT_ID', 
            'SERVICE_GA4_TO_REPORT_TOPIC', 'SERVICE_SLACK_MESSAGE_TOPIC',
            'PROPERTY_IDS'
        ]
        
        missing_common = [var for var in common_required if not getattr(cls, var)]
        
        # Check authentication (either Service Account OR full OAuth triplet)
        has_sa = bool(cls.GA4_SERVICE_ACCOUNT_PATH)
        has_oauth = all([cls.GA4_CLIENT_ID, cls.GA4_CLIENT_SECRET, cls.GA4_REFRESH_TOKEN])
        
        if not (has_sa or has_oauth):
            missing_auth = ["GA4_SERVICE_ACCOUNT_PATH or (GA4_CLIENT_ID, GA4_CLIENT_SECRET, GA4_REFRESH_TOKEN)"]
        else:
            missing_auth = []

        missing = missing_common + missing_auth
        if missing:
            raise ValueError(f"Required configuration variables are missing: {', '.join(missing)}")

# Create a Config instance and validate it
config = Config()
config.validate()