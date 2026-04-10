import logging
from datetime import datetime
import sys
import os

# Assure que le dossier extractor est dans le path Python
#sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import extract_web
import extract_csv
import extract_api

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_all():
    logger.info("Démarrage de l'extraction des livres depuis les différentes sources")
    
    result = {}
    
    # Extraction web
    try:
        result["web"] = extract_web.run()
        logger.info("Extraction web terminée avec succès")
    except Exception as e:
        result["web"] = None
        logger.error(f"Erreur lors de l'extraction web : {e}")
    
    # Extraction CSV
    try:
        result["csv"] = extract_csv.run()
        logger.info("Extraction CSV terminée avec succès")
    except Exception as e:
        result["csv"] = None
        logger.error(f"Erreur lors de l'extraction CSV : {e}")
    
    # Extraction API
    try:
        result["api"] = extract_api.run()
        logger.info("Extraction API terminée avec succès")
    except Exception as e:
        result["api"] = None
        logger.error(f"Erreur lors de l'extraction API : {e}")
    
    logger.info("Resume de l'extraction")
    
    for source, file_name in result.items():
        status = "OK" if file_name else "ERROR"
        logger.info(f"{source}: {file_name} ({status})")
    

if __name__ == "__main__":
    run_all()