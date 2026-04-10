import requests
from bs4 import BeautifulSoup
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

# EXTRACTION WEB

def scrape_books(max_pages=10):
    logger.info("Démarrage du scraping des livres")
    
    base_url = "https://books.toscrape.com/catalogue/"
    all_books = []

    rating_map = {
        "One": 1,
        "Two": 2,
        "Three": 3,
        "Four": 4,
        "Five": 5
    }

    for page in range(1, max_pages + 1):
        url = f"{base_url}page-{page}.html"
        logger.info(f"  → Scraping page {page}/{max_pages}")
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            articles = soup.find_all("article", class_="product_pod")
            
            for article in articles:
                title = article.find("h3").find("a")["title"]
                price = article.find("p", class_="price_color").text.strip()
                price = float(price.replace("£", "").replace("Â", ""))
                rating_word = article.find("p", class_="star-rating")["class"][1]
                rating = rating_map.get(rating_word, 0)
                availability = article.find("p", class_="instock").text.strip()
                
                all_books.append({
                    "title": title,
                    "price_gbp": price,
                    "rating": rating,
                    "availability": availability,
                    "source": "books.toscrape.com",
                    "extracted_at": datetime.now().isoformat()
                })

        except Exception as e:
            logger.error(f"Erreur lors du scraping de la page {page} : {e}")
            continue

    logger.info(f"{len(all_books)} livres récupérés au total")
        
    return all_books

#depot dans bronze

def run():
    t_start = time.time()
    logger.info("Démarrage de l'extraction des livres depuis le site web")

    t0 = time.time()
    books = scrape_books(max_pages=10)
    logger.info(f"[PERF] Scraping terminé en {time.time() - t0:.2f}s — {len(books)} livres")

    data = json.dumps(books, indent=2, ensure_ascii=False)

    file_name = f"web/books_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

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
    logger.info(f"[PERF] Extraction web totale : {time.time() - t_start:.2f}s")
    return file_name

if __name__ == "__main__":
    run()