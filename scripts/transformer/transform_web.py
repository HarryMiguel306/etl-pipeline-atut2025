from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

#session spark
def create_spark_session():
    spark = SparkSession.builder \
        .appName("ATUT2025_Transform_Web") \
        .master("local[*]") \
        .config("spark.jars", 
            "/home/airflow/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,"
            "/home/airflow/.ivy2/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,"
            "/home/airflow/.ivy2/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Session Spark créée")
    return spark

#transformation
def transform(spark): 
    logger.info("Début de la transformation web")

    #lecture fichier csv
    df = spark.read \
    .option("multiline", "true") \
    .json("s3a://bronze/web/")

    logger.info(f"{df.count()} lignes lues depuis Bronze/web")

    df_clean = df \
        .dropDuplicates(["title"]) \
        .filter(F.col("price_gbp").isNotNull()) \
        .filter(F.col("price_gbp") > 0) \
        .withColumn("price_gbp", F.col("price_gbp").cast(FloatType())) \
        .withColumn("rating", F.col("rating").cast(IntegerType())) \
        .withColumn("availability", F.trim(F.col("availability"))) \
        .withColumn("title", F.trim(F.col("title"))) \
        .withColumn("layer", F.lit("silver")) \
        .withColumn("transformed_at", F.current_timestamp())

    df_clean.write \
        .mode("overwrite") \
        .parquet("s3a://silver/web")
    
    count = df_clean.count()
    logger.info(f"{count} lignes écrites dans Silver/web")
    return count
    
#point d'entrée

def run():
    logger.info("Début de la transformation web")
    spark = create_spark_session()
    
    try:
        transform(spark)
    except Exception as e:
        logger.error(f"Erreur lors de la transformation web: {e}")
    finally:
        spark.stop()
        logger.info("Fin de la transformation web")

if __name__ == "__main__":
    run()
    