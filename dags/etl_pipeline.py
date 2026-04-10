from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import logging

logger = logging.getLogger(__name__)

#configuration du dag

default_args = {
    "owner": "atut2025",
    "retries": 2,                           # Réessaie 2 fois si échec
    "retry_delay": timedelta(minutes=5),    # Attend 5 min entre chaque essai
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    dag_id="etl_pipeline_books",
    description="Pipeline ETL complet — Bronze → Silver → Gold",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 0 * * *",  # Tous les jours à minuit
    catchup=False,                  # Ne rattrape pas les exécutions passées
    tags=["atut2025", "etl", "books"],
)

#fonction utilitaire

def run_script(script_path):
    """Exécute un script Python en utilisant subprocess"""
    logger.info(f"Exécution du script {script_path}")

    try:
        result = subprocess.run(
            ["python", script_path],
            check=True,
            capture_output=True,
            text=True,
        )
        logger.info(f"Script {script_path} terminé avec succès")
        logger.info(f"Sortie:")
        logger.info(result.stdout)
        if result.stderr:
            logger.warning(f"Erreurs:")
            logger.warning(result.stderr)
    except subprocess.CalledProcessError as e:
        logger.error(f"Erreur lors de l'exécution de {script_path}")
        logger.error(f"Code de retour: {e.returncode}")
        logger.error(f"Sortie:")
        logger.error(e.stdout)
        logger.error(f"Erreurs:")
        logger.error(e.stderr)
        raise

#définition des tâches

#extract

extract_api = PythonOperator(
    task_id="extract_api",
    python_callable=lambda: run_script("/opt/airflow/scripts/extractor/extract_api.py"),
    dag=dag,
)

extract_csv = PythonOperator(
    task_id="extract_csv",
    python_callable=lambda: run_script("/opt/airflow/scripts/extractor/extract_csv.py"),
    dag=dag,
)

extract_web = PythonOperator(
    task_id="extract_web",
    python_callable=lambda: run_script("/opt/airflow/scripts/extractor/extract_web.py"),
    dag=dag,
)

#transform

transform_api = PythonOperator(
    task_id="transform_api",
    python_callable=lambda: run_script("/opt/airflow/scripts/transformer/transform_api.py"),
    dag=dag,
)

transform_csv = PythonOperator(
    task_id="transform_csv",
    python_callable=lambda: run_script("/opt/airflow/scripts/transformer/transform_csv.py"),
    dag=dag,
)

transform_web = PythonOperator(
    task_id="transform_web",
    python_callable=lambda: run_script("/opt/airflow/scripts/transformer/transform_web.py"),
    dag=dag,
)

#load

load_gold = PythonOperator(
    task_id="load_gold",
    python_callable=lambda: run_script("/opt/airflow/scripts/loader/load_gold.py"),
    dag=dag,
)

#définition des dépendances

extract_api >> transform_api
extract_csv >> transform_csv
extract_web >> transform_web

[transform_web, transform_csv, transform_api] >> load_gold