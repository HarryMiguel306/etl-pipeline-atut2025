import logging
import sys
import os
import time

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
    t_total = time.time()
    logger.info("Démarrage de la transformation des livres depuis les différentes sources")

    result = {}

    # Transformation web
    try:
        t0 = time.time()
        transform_web.run()
        result["web"] = f"OK ({time.time() - t0:.1f}s)"
        logger.info("Transformation web terminée avec succès")
    except Exception as e:
        result["web"] = "ERROR"
        logger.error(f"Erreur lors de la transformation web : {e}")

    # Transformation CSV
    try:
        t0 = time.time()
        transform_csv.run()
        result["csv"] = f"OK ({time.time() - t0:.1f}s)"
        logger.info("Transformation CSV terminée avec succès")
    except Exception as e:
        result["csv"] = "ERROR"
        logger.error(f"Erreur lors de la transformation CSV : {e}")

    # Transformation API
    try:
        t0 = time.time()
        transform_api.run()
        result["api"] = f"OK ({time.time() - t0:.1f}s)"
        logger.info("Transformation API terminée avec succès")
    except Exception as e:
        result["api"] = "ERROR"
        logger.error(f"Erreur lors de la transformation API : {e}")

    logger.info("=== Résumé de la transformation ===")
    for source, status in result.items():
        logger.info(f"  {source}: {status}")
    logger.info(f"[PERF] Durée totale transformation : {time.time() - t_total:.2f}s")
    
if __name__ == "__main__":
    run_all()