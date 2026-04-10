import pytest
import sys
sys.path.append("/opt/airflow/scripts")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType


@pytest.fixture(scope="session")
def spark():
    from config import SPARK_JARS
    spark = SparkSession.builder \
        .appName("ATUT2025_Tests") \
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

# Test transform web
class TestTransformWeb:

    def test_drop_duplicates(self, spark):
        """Les doublons sur le titre doivent être supprimés"""
        data = [
            ("Book A", 10.0, 3, "In stock", "web", "2024-01-01"),
            ("Book A", 10.0, 3, "In stock", "web", "2024-01-01"),
            ("Book B", 20.0, 4, "In stock", "web", "2024-01-01"),
        ]
        schema = StructType([
            StructField("title", StringType()),
            StructField("price_gbp", FloatType()),
            StructField("rating", IntegerType()),
            StructField("availability", StringType()),
            StructField("source", StringType()),
            StructField("extracted_at", StringType()),
        ])
        df = spark.createDataFrame(data, schema)
        df_clean = df.dropDuplicates(["title"])
        assert df_clean.count() == 2

    def test_filter_null_price(self, spark):
        """Les livres sans prix doivent être supprimés"""
        data = [
            ("Book A", 10.0, 3, "In stock"),
            ("Book B", None, 4, "In stock"),
        ]
        schema = StructType([
            StructField("title", StringType()),
            StructField("price_gbp", FloatType()),
            StructField("rating", IntegerType()),
            StructField("availability", StringType()),
        ])
        df = spark.createDataFrame(data, schema)
        df_clean = df.filter(F.col("price_gbp").isNotNull())
        assert df_clean.count() == 1

    def test_filter_negative_price(self, spark):
        """Les prix négatifs ou nuls doivent être supprimés"""
        data = [
            ("Book A", 10.0, 3, "In stock"),
            ("Book B", -5.0, 4, "In stock"),
            ("Book C", 0.0, 2, "In stock"),
        ]
        schema = StructType([
            StructField("title", StringType()),
            StructField("price_gbp", FloatType()),
            StructField("rating", IntegerType()),
            StructField("availability", StringType()),
        ])
        df = spark.createDataFrame(data, schema)
        df_clean = df.filter(F.col("price_gbp") > 0)
        assert df_clean.count() == 1

    def test_trim_whitespace(self, spark):
        """Les espaces en début/fin doivent être supprimés"""
        data = [("  Book A  ", 10.0)]
        schema = StructType([
            StructField("title", StringType()),
            StructField("price_gbp", FloatType()),
        ])
        df = spark.createDataFrame(data, schema)
        df_clean = df.withColumn("title", F.trim(F.col("title")))
        assert df_clean.first()["title"] == "Book A"

# Test transform csv
class TestTransformCSV:

    def test_rename_columns(self, spark):
        """Les colonnes doivent être renommées correctement"""
        data = [("978123", "Python", "Author", 2020, "Publisher")]
        schema = StructType([
            StructField("ISBN", StringType()),
            StructField("Book-Title", StringType()),
            StructField("Book-Author", StringType()),
            StructField("Year-Of-Publication", IntegerType()),
            StructField("Publisher", StringType()),
        ])
        df = spark.createDataFrame(data, schema)
        df_clean = df \
            .withColumnRenamed("Book-Title", "title") \
            .withColumnRenamed("Book-Author", "author") \
            .withColumnRenamed("Year-Of-Publication", "year") \
            .withColumnRenamed("ISBN", "isbn")
        assert "title" in df_clean.columns
        assert "author" in df_clean.columns
        assert "year" in df_clean.columns

    def test_filter_invalid_years(self, spark):
        """Les années invalides doivent être filtrées"""
        data = [
            ("978123", "Book A", 2020),
            ("978124", "Book B", 1750),
            ("978125", "Book C", 9999),
        ]
        schema = StructType([
            StructField("isbn", StringType()),
            StructField("title", StringType()),
            StructField("year", IntegerType()),
        ])
        df = spark.createDataFrame(data, schema)
        df_clean = df.filter(
            (F.col("year") >= 1800) &
            (F.col("year") <= 2024)
        )
        assert df_clean.count() == 1

    def test_dropna_required_fields(self, spark):
        """Les lignes sans ISBN ou titre doivent être supprimées"""
        data = [
            ("978123", "Book A", "Author A"),
            (None, "Book B", "Author B"),
            ("978125", None, "Author C"),
        ]
        schema = StructType([
            StructField("isbn", StringType()),
            StructField("title", StringType()),
            StructField("author", StringType()),
        ])
        df = spark.createDataFrame(data, schema)
        df_clean = df.dropna(subset=["isbn", "title"])
        assert df_clean.count() == 1