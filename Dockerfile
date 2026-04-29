FROM python:3.11-slim

WORKDIR /app

# Copier les dépendances
COPY requirements.txt .

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install airbyte-source-google-analytics-data-api

# Copier le code
COPY app/ ./app/

# Exposer le port
EXPOSE 8080

# Lancer l'app avec uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]