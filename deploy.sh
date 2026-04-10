#!/bin/bash
set -e  # Arrêt immédiat en cas d'erreur

echo "========================================"
echo "   ATUT 2025 — Pipeline ETL Books"
echo "========================================"

# Chargement des variables d'environnement depuis .env
if [ -f .env ]; then
    export $(grep -v '^#' .env | grep -v '^$' | xargs)
    echo "Variables d'environnement chargées depuis .env"
else
    echo "ERREUR : fichier .env introuvable. Copier .env.example en .env et le remplir."
    exit 1
fi

# Vérification des variables critiques
: "${MINIO_ACCESS_KEY:?Variable MINIO_ACCESS_KEY manquante dans .env}"
: "${MINIO_SECRET_KEY:?Variable MINIO_SECRET_KEY manquante dans .env}"
: "${AIRFLOW_ADMIN_PASSWORD:?Variable AIRFLOW_ADMIN_PASSWORD manquante dans .env}"

# Fonction utilitaire : attente santé d'un service HTTP
wait_for_service() {
    local name=$1
    local url=$2
    local max_retries=15
    local count=0
    echo "  Attente du service $name ($url)..."
    until curl -sf "$url" > /dev/null 2>&1; do
        count=$((count + 1))
        if [ $count -ge $max_retries ]; then
            echo "ERREUR : $name non disponible après $max_retries tentatives."
            exit 1
        fi
        echo "    → Tentative $count/$max_retries..."
        sleep 5
    done
    echo "  $name est prêt ✓"
}

#Étape 1 : Lancer les conteneurs
echo ""
echo "- Étape 1 : Démarrage des conteneurs Docker..."
docker-compose up --build -d

# Health checks — attendre que les services soient vraiment prêts
echo ""
echo "- Vérification de santé des services..."
wait_for_service "Airflow" "http://localhost:8080/health"
wait_for_service "MinIO"   "http://localhost:9001"

#Étape 2 : Créer les buckets MinIO
echo ""
echo "- Étape 2 : Création des buckets MinIO..."
docker exec etl_project-airflow-1 python -c "
import boto3, os
from botocore.client import Config
client = boto3.client('s3',
    endpoint_url=os.environ['MINIO_ENDPOINT'],
    aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
    aws_secret_access_key=os.environ['MINIO_SECRET_KEY'],
    config=Config(signature_version='s3v4'))
for bucket in ['bronze', 'silver', 'gold']:
    try:
        client.create_bucket(Bucket=bucket)
        print(f'Bucket créé : {bucket}')
    except Exception:
        print(f'Bucket existant : {bucket}')
"

#Étape 3 : Lancer l'extraction
echo ""
echo "- Étape 3 : Extraction des données (Bronze)..."
docker exec etl_project-airflow-1 python /opt/airflow/scripts/extractor/extract_web.py
docker exec etl_project-airflow-1 python /opt/airflow/scripts/extractor/extract_csv.py
docker exec etl_project-airflow-1 python /opt/airflow/scripts/extractor/extract_api.py

#Étape 4 : Lancer la transformation
echo ""
echo "- Étape 4 : Transformation des données (Silver)..."
docker exec etl_project-airflow-1 python /opt/airflow/scripts/transformer/transform_web.py
docker exec etl_project-airflow-1 python /opt/airflow/scripts/transformer/transform_csv.py
docker exec etl_project-airflow-1 python /opt/airflow/scripts/transformer/transform_api.py

#Étape 5 : Lancer le chargement Gold
echo ""
echo "- Étape 5 : Chargement Gold Layer..."
docker exec etl_project-airflow-1 python /opt/airflow/scripts/loader/load_gold.py

#Résumé
echo ""
echo "========================================"
echo "Pipeline ETL déployé avec succès !"
echo ""
echo "Interfaces disponibles :"
echo "  → Airflow : http://localhost:8080"
echo "  → MinIO   : http://localhost:9001"
echo "  → Spark   : http://localhost:8081"
echo ""
echo "Consulter les identifiants dans votre fichier .env"
echo "========================================"