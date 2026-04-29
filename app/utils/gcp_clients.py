# app/utils/gcp_clients.py
import os
import json
import logging
from google.auth import default as google_auth_default
from google.cloud import bigquery, storage, secretmanager

logger = logging.getLogger(__name__)


def get_credentials():
    """
    Retourne les credentials GCP.
    - Sur Cloud Run  : google.auth.default() utilise le Service Account attaché (ADC)
    - En local       : utilise GOOGLE_APPLICATION_CREDENTIALS ou gcloud auth
    """
    credentials, project = google_auth_default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return credentials, project


def get_bigquery_client(project_id: str = None) -> bigquery.Client:
    """Crée un client BigQuery en utilisant les credentials ADC."""
    credentials, default_project = get_credentials()
    project = project_id or default_project or os.getenv('CLIENT_PROJECT_ID')
    logger.info(f"Création du client BigQuery pour le projet : {project}")
    return bigquery.Client(project=project, credentials=credentials)


def get_storage_client(project_id: str = None) -> storage.Client:
    """Crée un client Cloud Storage en utilisant les credentials ADC."""
    credentials, default_project = get_credentials()
    project = project_id or default_project or os.getenv('CLIENT_PROJECT_ID')
    logger.info(f"Création du client Storage pour le projet : {project}")
    return storage.Client(project=project, credentials=credentials)


def get_secret_manager_client() -> secretmanager.SecretManagerServiceClient:
    """Crée un client Secret Manager en utilisant les credentials ADC."""
    credentials, _ = get_credentials()
    return secretmanager.SecretManagerServiceClient(credentials=credentials)


def get_ga4_credentials_json(secret_name_or_path: str = None) -> dict:
    """
    Retourne les credentials GA4 sous forme de dict.

    Priorité :
    1. Si secret_name_or_path est un fichier .json local → lecture directe
    2. Si c'est un nom de secret → lecture depuis Secret Manager
    3. Sinon → retourne None (l'appelant utilisera ADC ou OAuth)
    """
    if not secret_name_or_path:
        logger.info("Aucun secret GA4 configuré, utilisation de ADC.")
        return None

    # Cas 1 : fichier local
    if secret_name_or_path.endswith('.json') and os.path.exists(secret_name_or_path):
        logger.info(f"Lecture des credentials GA4 depuis le fichier : {secret_name_or_path}")
        with open(secret_name_or_path, 'r') as f:
            return json.load(f)

    # Cas 2 : secret dans Secret Manager
    try:
        project_id = os.getenv('SERVICE_SECRET_PROJECT_ID') or os.getenv('CLIENT_PROJECT_ID')
        secret_path = f"projects/{project_id}/secrets/{secret_name_or_path}/versions/latest"
        logger.info(f"Lecture du secret depuis Secret Manager : {secret_path}")

        client = get_secret_manager_client()
        response = client.access_secret_version(request={"name": secret_path})
        secret_value = response.payload.data.decode("UTF-8")
        return json.loads(secret_value)

    except Exception as e:
        logger.warning(f"Impossible de lire le secret '{secret_name_or_path}' : {e}")
        return None