import requests
import json
import logging
import time
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


def fetch_books_from_api(subjects=["python", "data engineering", "machine learning"], limit=20):
    """
    Récupère les livres depuis l'API Open Library
    """
    all_books = []
    
    for subject in subjects:
        logger.info(f"Récupération des livres pour le sujet : {subject}")
        
        url = f"https://openlibrary.org/subjects/{subject}.json?limit={limit}"
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            for work in data.get("works", []):
                book = {
                    "title": work.get("title", ""),
                    "authors": [
                        a.get("name", "") 
                        for a in work.get("authors", [])
                    ],
                    "first_publish_year": work.get("first_publish_year", None),
                    "subject": subject,
                    "edition_count": work.get("edition_count", 0),
                    "key": work.get("key", ""),
                    "source": "openlibrary_api",
                    "extracted_at": datetime.now().isoformat()
                }
                all_books.append(book)

        except Exception as e:
            logger.error(f"Erreur lors de la récupération des livres pour le sujet {subject} : {e}")
            continue

    logger.info(f"{len(all_books)} livres récupérés au total")
        
    return all_books

#depot dans bronze

def run():
    t_start = time.time()
    logger.info("Démarrage de l'extraction des livres depuis l'API Open Library")

    t0 = time.time()
    books = fetch_books_from_api()
    logger.info(f"[PERF] Appels API terminés en {time.time() - t0:.2f}s — {len(books)} livres")

    data = json.dumps(books, indent=2)

    file_name = f"api/books_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    t0 = time.time()
    client = get_minio_client()
    client.put_object(
        Bucket="bronze",
        Key=file_name,
        Body=data.encode("utf-8"),
        ContentType="application/json"
    )
    logger.info(f"[PERF] Upload MinIO terminé en {time.time() - t0:.2f}s")

    logger.info(f"Fichier {file_name} déposé dans le bucket bronze")
    logger.info(f"[PERF] Extraction API totale : {time.time() - t_start:.2f}s")
    return file_name

if __name__ == "__main__":
    run()

