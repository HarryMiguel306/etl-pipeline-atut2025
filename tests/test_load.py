import pytest
import sys
sys.path.append("/opt/airflow/scripts")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField,
                                StringType, FloatType, IntegerType)

# Fixture spark partagée
@pytest.fixture(scope="session")
def spark():
    from config import SPARK_JARS
    spark = SparkSession.builder \
        .appName("ATUT2025_Tests_Gold") \
        .master("local[2]") \
        .config("spark.jars", SPARK_JARS) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()

# Test gold top rated
class TestGoldTopRated:

    def test_top_rated_limit(self, spark):
        """Le top rated doit retourner au maximum 20 livres"""
        data = [(f"Book {i}", float(i), i % 5 + 1, "In stock")
                for i in range(50)]
        schema = StructType([
            StructField("title", StringType()),
            StructField("price_gbp", FloatType()),
            StructField("rating", IntegerType()),
            StructField("availability", StringType()),
        ])
        df = spark.createDataFrame(data, schema)
        df_gold = df.orderBy(F.col("rating").desc()).limit(20)
        assert df_gold.count() == 20

    def test_top_rated_ordered_by_rating(self, spark):
        """Les livres doivent être triés par note décroissante"""
        data = [
            ("Book A", 10.0, 5, "In stock"),
            ("Book B", 20.0, 3, "In stock"),
            ("Book C", 15.0, 4, "In stock"),
        ]
        schema = StructType([
            StructField("title", StringType()),
            StructField("price_gbp", FloatType()),
            StructField("rating", IntegerType()),
            StructField("availability", StringType()),
        ])
        df = spark.createDataFrame(data, schema)
        df_gold = df.orderBy(F.col("rating").desc())
        first = df_gold.first()
        assert first["rating"] == 5

# Test gold books by year
class TestGoldBooksByYear:

    def test_books_by_year_count(self, spark):
        """Le groupBy année doit retourner le bon nombre de groupes"""
        data = [
            ("978001", "Book A", "Author A", 2020, "Pub A"),
            ("978002", "Book B", "Author B", 2020, "Pub B"),
            ("978003", "Book C", "Author C", 2021, "Pub C"),
        ]
        schema = StructType([
            StructField("isbn", StringType()),
            StructField("title", StringType()),
            StructField("author", StringType()),
            StructField("year", IntegerType()),
            StructField("publisher", StringType()),
        ])
        df = spark.createDataFrame(data, schema)
        df_gold = df.groupBy("year").agg(F.count("*").alias("nombre_livres"))
        assert df_gold.count() == 2

    def test_books_by_year_correct_count(self, spark):
        """Le nombre de livres par année doit être correct"""
        data = [
            ("978001", "Book A", 2020),
            ("978002", "Book B", 2020),
            ("978003", "Book C", 2021),
        ]
        schema = StructType([
            StructField("isbn", StringType()),
            StructField("title", StringType()),
            StructField("year", IntegerType()),
        ])
        df = spark.createDataFrame(data, schema)
        df_gold = df.groupBy("year") \
            .agg(F.count("*").alias("nombre_livres")) \
            .filter(F.col("year") == 2020)
        assert df_gold.first()["nombre_livres"] == 2

# Test gold price stats
class TestGoldPriceStats:

    def test_price_stats_columns(self, spark):
        """Les stats de prix doivent avoir les bonnes colonnes"""
        data = [
            ("Book A", 10.0, 5),
            ("Book B", 20.0, 5),
            ("Book C", 15.0, 4),
        ]
        schema = StructType([
            StructField("title", StringType()),
            StructField("price_gbp", FloatType()),
            StructField("rating", IntegerType()),
        ])
        df = spark.createDataFrame(data, schema)
        df_gold = df.groupBy("rating").agg(
            F.min("price_gbp").alias("prix_min"),
            F.max("price_gbp").alias("prix_max"),
            F.avg("price_gbp").alias("prix_moyen"),
        )
        assert "prix_min" in df_gold.columns
        assert "prix_max" in df_gold.columns
        assert "prix_moyen" in df_gold.columns

    def test_price_stats_correct_values(self, spark):
        """Les valeurs min/max doivent être correctes"""
        data = [
            ("Book A", 10.0, 5),
            ("Book B", 20.0, 5),
        ]
        schema = StructType([
            StructField("title", StringType()),
            StructField("price_gbp", FloatType()),
            StructField("rating", IntegerType()),
        ])
        df = spark.createDataFrame(data, schema)
        df_gold = df.groupBy("rating").agg(
            F.min("price_gbp").alias("prix_min"),
            F.max("price_gbp").alias("prix_max"),
        )
        row = df_gold.first()
        assert row["prix_min"] == 10.0
        assert row["prix_max"] == 20.0