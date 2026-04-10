import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import transform_web
import transform_csv
import transform_api

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_all():
    logger.info("Démarrage de la transformation des livres depuis les différentes sources")
    
    result = {}
    
    # Transformation web
    try:
        transform_web.run()
        result["web"] = "OK"
        logger.info("Transformation web terminée avec succès")
    except Exception as e:
        result["web"] = "ERROR"
        logger.error(f"Erreur lors de la transformation web : {e}")
    
    # Transformation CSV
    try:
        transform_csv.run()
        result["csv"] = "OK"
        logger.info("Transformation CSV terminée avec succès")
    except Exception as e:
        result["csv"] = "ERROR"
        logger.error(f"Erreur lors de la transformation CSV : {e}")
    
    # Transformation API
    try:
        transform_api.run()
        result["api"] = "OK"
        logger.info("Transformation API terminée avec succès")
    except Exception as e:
        result["api"] = "ERROR"
        logger.error(f"Erreur lors de la transformation API : {e}")
    
    logger.info("Resume de la transformation")
    
    for source, status in result.items():
        logger.info(f"{source}: {status}")
    
if __name__ == "__main__":
    run_all()