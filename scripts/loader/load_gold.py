from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
import logging

import sys
sys.path.append("/opt/airflow/scripts")
from config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
    SPARK_MASTER, SPARK_APP_NAME, SPARK_JARS
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

#session spark 
def create_spark_session():
    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.jars", SPARK_JARS) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

# GOLD 1 — TOP 20 LIVRES LES MIEUX NOTÉS
# Depuis Silver/web — livres scrappés

def gold_top_rated(spark):
    logger.info("Début du chargement Gold - Top 20")
    
    #lecture fichier parquet
    df = spark.read \
        .parquet("s3a://silver/web/")
    
    logger.info(f"{df.count()} lignes lues depuis Silver/web")
    
    df_gold = df \
        .select("title", "price_gbp", "rating", "availability") \
        .filter(F.col("rating").isNotNull()) \
        .orderBy(F.col("rating").desc(), F.col("price_gbp").asc()) \
        .limit(20)

    df_gold.write \
        .mode("overwrite") \
        .parquet("s3a://gold/top_rated_books/")
    
    
    logger.info("Top 20 des livres les mieux notés chargés dans Gold/top_rated_books")
    
# GOLD 2 — LIVRES PAR ANNÉE DE PUBLICATION
# Depuis Silver/csv — dataset Books.csv

def gold_by_year(spark) :
    logger.info("Livres par année de publication")
    
    #lecture fichier parquet
    df = spark.read \
        .parquet("s3a://silver/csv/")
    
    logger.info(f"{df.count()} lignes lues depuis Silver/csv")
    
    df_gold = df \
        .filter(F.col("year").isNotNull()) \
        .groupBy("year") \
        .agg(
            F.count("*").alias("nombre_livres"),
            F.countDistinct("author").alias("nombre_auteurs"),
            F.countDistinct("publisher").alias("nombre_editeurs")
        ) \
        .orderBy(F.col("year").asc())

    df_gold.write \
        .mode("overwrite") \
        .parquet("s3a://gold/books_by_year/")

    count = df_gold.count()
    logger.info(f"{count} lignes écrites dans Gold/books_by_year")
    
# GOLD 3 — STATISTIQUES DE PRIX PAR NOTES
# Depuis Silver/web

def gold_price_stats(spark) :
    logger.info("Statistiques de prix par notes")
    
    #lecture fichier parquet
    df = spark.read \
        .parquet("s3a://silver/web/")
    
    logger.info(f"{df.count()} lignes lues depuis Silver/web")
    
    df_gold = df \
        .groupBy("rating") \
        .agg(
            F.count("*").alias("nombre_livres"),
            F.round(F.min("price_gbp"), 2).alias("prix_min"),
            F.round(F.max("price_gbp"), 2).alias("prix_max"),
            F.round(F.avg("price_gbp"), 2).alias("prix_moyen")
        ) \
        .orderBy(F.col("rating").desc())

    df_gold.write \
        .mode("overwrite") \
        .parquet("s3a://gold/price_stats_by_rating/")

    logger.info("Statistiques de prix par notes chargées dans Gold/price_stats_by_rating")

# GOLD 4 — TOP SUJETS API
# Depuis Silver/api

def gold_top_subjects(spark) :
    logger.info("Top sujets API")
    
    #lecture fichier parquet
    df = spark.read \
        .parquet("s3a://silver/api/")
    
    logger.info(f"{df.count()} lignes lues depuis Silver/api")
    
    df_gold = df \
        .groupBy("subject") \
        .agg(
            F.count("*").alias("nombre_livres"),
            F.round(F.avg("edition_count"), 2).alias("editions_moyennes"),
            F.min("first_publish_year").alias("plus_ancien"),
            F.max("first_publish_year").alias("plus_recent")
        ) \
        .orderBy(F.col("nombre_livres").desc())

    df_gold.write \
        .mode("overwrite") \
        .parquet("s3a://gold/top_subjects/")

    logger.info("Top sujets API chargés dans Gold/top_subjects")

#fonction principale
def run() :
    logger.info("Début du chargement Gold")
    spark = create_spark_session()
    
    try:
        gold_top_rated(spark)
        gold_by_year(spark)
        gold_price_stats(spark)
        gold_top_subjects(spark)
    except Exception as e:
        logger.error(f"Erreur lors du chargement Gold: {e}")
    finally:
        spark.stop()
        logger.info("Fin du chargement Gold")

if __name__ == "__main__":
    run()