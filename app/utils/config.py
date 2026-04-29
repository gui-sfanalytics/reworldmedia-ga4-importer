# app/utils/config.py
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file (local dev only)
project_root = Path(__file__).parent.parent.parent
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path)

class Config:
    """Configuration class to load and validate environment variables."""
    CLIENT_PROJECT_ID = os.getenv('CLIENT_PROJECT_ID', 'optimalways')
    CLIENT_DATASET_ID = os.getenv('CLIENT_DATASET_ID', 'poc_ga4_export')
    # Sur Cloud Run : plus besoin de clé JSON
    CLIENT_BIGQUERY_CREDS = os.getenv('CLIENT_BIGQUERY_CREDS', None)

    SERVICE_PROJECT_ID = os.getenv('SERVICE_PROJECT_ID', 'mythic-benefit-690')
    SERVICE_SECRET_PROJECT_ID = os.getenv('SERVICE_SECRET_PROJECT_ID', 'mythic-benefit-690')

    SERVICE_GA4_TO_REPORT_TOPIC = os.getenv('SERVICE_GA4_TO_REPORT_TOPIC', 'ga4topic')
    SERVICE_SLACK_MESSAGE_TOPIC = os.getenv('SERVICE_SLACK_MESSAGE_TOPIC', 'ga4topic')

    GA4_JSON_KEY = os.getenv('GA4_JSON_KEY', None)
    PROPERTY_IDS = os.getenv('PROPERTY_IDS', '288233728')
    GA4_CLIENT_ID = os.getenv('GA4_CLIENT_ID')
    GA4_CLIENT_SECRET = os.getenv('GA4_CLIENT_SECRET')
    GA4_REFRESH_TOKEN = os.getenv('GA4_REFRESH_TOKEN')
    GA4_SERVICE_ACCOUNT_PATH = os.getenv('GA4_SERVICE_ACCOUNT_PATH', None)

    AVAILABLE_REPORTS = [
        'daily_active_users_intradays4',
        'engagement'
    ]

    @classmethod
    def validate(cls):
        common_required = [
            'CLIENT_PROJECT_ID', 'CLIENT_DATASET_ID',
            'SERVICE_PROJECT_ID', 'SERVICE_SECRET_PROJECT_ID',
            'SERVICE_GA4_TO_REPORT_TOPIC', 'SERVICE_SLACK_MESSAGE_TOPIC',
            'PROPERTY_IDS'
        ]
        missing_common = [var for var in common_required if not getattr(cls, var)]

        has_sa = bool(cls.GA4_SERVICE_ACCOUNT_PATH)
        has_oauth = all([cls.GA4_CLIENT_ID, cls.GA4_CLIENT_SECRET, cls.GA4_REFRESH_TOKEN])
        # K_SERVICE est injecté automatiquement par Cloud Run → ADC disponible
        has_adc = os.getenv('K_SERVICE') is not None

        if not (has_sa or has_oauth or has_adc):
            missing_auth = ["GA4_SERVICE_ACCOUNT_PATH ou (GA4_CLIENT_ID + GA4_CLIENT_SECRET + GA4_REFRESH_TOKEN)"]
        else:
            missing_auth = []

        missing = missing_common + missing_auth
        if missing:
            raise ValueError(f"Variables de configuration manquantes : {', '.join(missing)}")

config = Config()
config.validate()