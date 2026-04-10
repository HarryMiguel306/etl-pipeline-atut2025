from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
import logging

import sys
sys.path.append("/opt/airflow/scripts")
from config import (MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
                    SPARK_MASTER, SPARK_APP_NAME, SPARK_JARS)

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

#transformation
def transform(spark):
    logger.info("Début de la transformation API")
    
    #lecture fichier json
    df = spark.read \
    .option("multiline", "true") \
    .json("s3a://bronze/api/")
    logger.info(f"{df.count()} lignes lues depuis Bronze/api")
    
    df_clean = df \
        .dropDuplicates(["key"]) \
        .dropna(subset=["title"]) \
        .withColumn("title", F.trim(F.col("title"))) \
        .withColumn("first_publish_year",
                    F.col("first_publish_year").cast(IntegerType())) \
        .withColumn("edition_count",
                    F.col("edition_count").cast(IntegerType())) \
        .withColumn("authors", F.col("authors").cast(StringType())) \
        .filter(F.col("first_publish_year") >= 1800) \
        .withColumn("layer", F.lit("silver")) \
        .withColumn("transformed_at", F.current_timestamp())

    df_clean.write \
        .mode("overwrite") \
        .parquet("s3a://silver/api/")

    count = df_clean.count()
    logger.info(f"{count} lignes écrites dans Silver/api")
    return count

#point d'entrée
def run() :
    logger.info("Début de la transformation API")
    spark = create_spark_session()
    
    try:
        transform(spark)
    except Exception as e:
        logger.error(f"Erreur lors de la transformation API: {e}")
    finally:
        spark.stop()
        logger.info("Fin de la transformation API")

if __name__ == "__main__":
    run()