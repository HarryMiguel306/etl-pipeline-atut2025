from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, StringType
import logging
import time

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

def transform(spark):
    logger.info("Début de la transformation API")
    
    #lecture fichier csv
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("s3a://bronze/csv/")

    logger.info(f"{df.count()} lignes lues depuis Bronze/csv")
    
    df_clean = df \
        .dropDuplicates(["ISBN"]) \
        .dropna(subset=["ISBN", "Book-Title", "Book-Author"]) \
        .withColumnRenamed("Book-Title", "title") \
        .withColumnRenamed("Book-Author", "author") \
        .withColumnRenamed("Year-Of-Publication", "year") \
        .withColumnRenamed("Publisher", "publisher") \
        .withColumnRenamed("ISBN", "isbn") \
        .withColumn("year", F.col("year").cast(IntegerType())) \
        .filter(
            (F.col("year") >= 1800) &
            (F.col("year") <= 2024)
        ) \
        .withColumn("title", F.trim(F.col("title"))) \
        .withColumn("author", F.trim(F.col("author"))) \
        .drop("Image-URL-S", "Image-URL-M", "Image-URL-L") \
        .withColumn("layer", F.lit("silver")) \
        .withColumn("transformed_at", F.current_timestamp())
    
    df_clean.write \
        .mode("overwrite") \
        .parquet("s3a://silver/csv")
    
    count = df_clean.count()
    logger.info(f"{count} lignes écrites dans Silver/csv")
    return count

#point d'entrée
def run() :
    t_start = time.time()
    logger.info("Début de la transformation CSV")
    spark = create_spark_session()

    try:
        count = transform(spark)
        logger.info(f"[PERF] Transformation CSV terminée en {time.time() - t_start:.2f}s — {count} lignes")
    except Exception as e:
        logger.error(f"Erreur lors de la transformation CSV: {e}")
    finally:
        spark.stop()
        logger.info("Fin de la transformation CSV")

if __name__ == "__main__":
    run()