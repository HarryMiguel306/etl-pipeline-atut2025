import os
from dotenv import load_dotenv

load_dotenv()

# MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
MINIO_BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")
MINIO_BUCKET_GOLD = os.getenv("MINIO_BUCKET_GOLD", "gold")


# Spark
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "ATUT2025")
SPARK_JARS = (
    "/home/airflow/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,"
    "/home/airflow/.ivy2/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,"
    "/home/airflow/.ivy2/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar"
)