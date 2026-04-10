#!/bin/bash

echo "========================================"
echo "   ATUT 2025 — Pipeline ETL Books"
echo "========================================"

#Étape 1 : Lancer les conteneurs
echo ""
echo "-Étape 1 : Démarrage des conteneurs Docker..."
docker-compose up --build -d

echo "Attente que les services soient prêts (30s)..."
sleep 30

#Étape 2 : Créer les buckets MinIO
echo ""
echo "-Étape 2 : Création des buckets MinIO..."
docker exec etl_project-airflow-1 python -c "
import boto3
from botocore.client import Config
client = boto3.client('s3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
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
echo "-Étape 3 : Extraction des données (Bronze)..."
docker exec etl_project-airflow-1 python /opt/airflow/scripts/extractor/extract_web.py
docker exec etl_project-airflow-1 python /opt/airflow/scripts/extractor/extract_csv.py
docker exec etl_project-airflow-1 python /opt/airflow/scripts/extractor/extract_api.py

#Étape 4 : Lancer la transformation
echo ""
echo "-Étape 4 : Transformation des données (Silver)..."
docker exec etl_project-airflow-1 python /opt/airflow/scripts/transformer/transform_web.py
docker exec etl_project-airflow-1 python /opt/airflow/scripts/transformer/transform_csv.py
docker exec etl_project-airflow-1 python /opt/airflow/scripts/transformer/transform_api.py

#Étape 5 : Lancer le chargement Gold
echo ""
echo "-Étape 5 : Chargement Gold Layer..."
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
echo "Identifiants Airflow : admin / admin123"
echo "Identifiants MinIO   : minioadmin / minioadmin"
echo "========================================"