from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, StringType
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

#création session spark
def create_spark_session():
    spark = SparkSession.builder \
        .appName("ATUT2025_Transform_csv") \
        .master("local[*]") \
        .config("spark.jars", 
            "/home/airflow/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,"
            "/home/airflow/.ivy2/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,"
            "/home/airflow/.ivy2/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Session Spark créée")
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
    logger.info("Début de la transformation CSV")
    spark = create_spark_session()
    
    try:
        transform(spark)
    except Exception as e:
        logger.error(f"Erreur lors de la transformation CSV: {e}")
    finally:
        spark.stop()
        logger.info("Fin de la transformation CSV")

if __name__ == "__main__":
    run()