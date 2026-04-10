# Pipeline ETL Industrialisé — ATUT 2025

## Description
Pipeline ETL automatisé intégrant PySpark, Apache Airflow, MinIO et Docker.
Projet de synthèse du programme ATUT 2025 — Université d'Abomey-Calavi, Bénin.

## Architecture
Sources → Bronze (MinIO) → Silver (PySpark) → Gold (Agrégations) → Airflow (Orchestration)

## Sources de données
- **Web** : books.toscrape.com (scraping)
- **CSV** : Dataset Books (271 000+ livres)
- **API** : Open Library API

## Stack technique
- **PySpark 3.5** — Traitement distribué
- **Apache Airflow 2.8** — Orchestration
- **MinIO** — Stockage objet (Bronze/Silver/Gold)
- **Docker** — Conteneurisation
- **PostgreSQL** — Base de données Airflow

## Lancer le projet

### Prérequis
- Docker Desktop installé
- 10 Go d'espace libre

### Démarrage
```bash
docker-compose up --build -d
```

### Accès aux interfaces
| Service | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin123 |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |
| Spark | http://localhost:8081 | — |

### Lancer le pipeline manuellement
```bash
# Extraction
docker exec -it etl_project-airflow-1 python /opt/airflow/scripts/extractor/extract_main.py

# Transformation
docker exec -it etl_project-airflow-1 python /opt/airflow/scripts/transformer/transform_web.py
docker exec -it etl_project-airflow-1 python /opt/airflow/scripts/transformer/transform_csv.py
docker exec -it etl_project-airflow-1 python /opt/airflow/scripts/transformer/transform_api.py

# Gold
docker exec -it etl_project-airflow-1 python /opt/airflow/scripts/loader/load_gold.py
```

## Structure du projet

etl_project/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── README.md
├── dags/
│   └── etl_pipeline.py
├── scripts/
│   ├── extractor/
│   │   ├── extract_web.py
│   │   ├── extract_csv.py
│   │   ├── extract_api.py
│   │   └── extract_main.py
│   ├── transformer/
│   │   ├── transform_web.py
│   │   ├── transform_csv.py
│   │   ├── transform_api.py
│   │   └── transform_main.py
│   └── loader/
│       └── load_gold.py
└── data/

## Auteur
**Miguel ANAGO** - Programme ATUT 2025