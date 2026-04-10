import pandas as pd
import logging
from datetime import datetime
import boto3
from botocore.client import Config
import sys
sys.path.append("/opt/airflow/scripts")
from config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

# LOGGING

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# CONNEXION MINIO

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )

# EXTRACTION CSV

def read_csv(file_path):
    logger.info(f"Lecture du fichier CSV : {file_path}")

    try:
        df = pd.read_csv(
            file_path,
            sep=",",
            encoding="utf-8",
            on_bad_lines="skip"
        )

        logger.info(f"Fichier CSV lu avec succès")
        return df

    except UnicodeDecodeError :
        logger.warning("Erreur de décodage UTF-8, tentative avec 'latin1'")
        df = pd.read_csv(
            file_path,
            sep=",",
            encoding="latin1",
            on_bad_lines="skip"
        )
        logger.info(f"Fichier CSV lu avec succès")
        return df


#depot dans bronze

def run():
    logger.info("Démarrage de l'extraction des données CSV")
    
    df = read_csv("/opt/airflow/data/Books.csv")

    df["source"] = "csv"
    df["extracted_at"] = datetime.now().isoformat()

    csv_bytes = df.to_csv(index=False).encode("utf-8")

    file_name = f"csv/books_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
    client = get_minio_client()
    client.put_object(
        Bucket="bronze",
        Key=file_name,
        Body=df.to_csv(index=False).encode("utf-8"),
        ContentType="text/csv"
    )

    logger.info(f"Fichier {file_name} déposé dans le bucket bronze")
    return file_name

if __name__ == "__main__":
    run()