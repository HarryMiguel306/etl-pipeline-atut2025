import pytest
import json
import sys
from unittest.mock import patch, MagicMock
sys.path.append("/opt/airflow/scripts")


#test extract_web
class TestExtractWeb:
    
    def test_scrape_returns_list(self):
        """Le scraping doit retourner une liste"""
        from extractor.extract_web import scrape_books
        result = scrape_books(max_pages=1)
        assert isinstance(result, list)

    def test_scrape_not_empty(self):
        """Le scraping doit retourner au moins 1 livre"""
        from extractor.extract_web import scrape_books
        result = scrape_books(max_pages=1)
        assert len(result) > 0

    def test_scrape_book_has_required_fields(self):
        """Chaque livre doit avoir les champs obligatoires"""
        from extractor.extract_web import scrape_books
        result = scrape_books(max_pages=1)
        required_fields = ["title", "price_gbp", "rating", "availability", "source"]
        for field in required_fields:
            assert field in result[0], f"Champ manquant : {field}"

    def test_scrape_price_is_positive(self):
        """Le prix doit être positif"""
        from extractor.extract_web import scrape_books
        result = scrape_books(max_pages=1)
        for book in result:
            assert book["price_gbp"] > 0

    def test_scrape_rating_between_1_and_5(self):
        """La note doit être entre 1 et 5"""
        from extractor.extract_web import scrape_books
        result = scrape_books(max_pages=1)
        for book in result:
            assert 1 <= book["rating"] <= 5


#test extract_csv
class TestExtractCSV:

    def test_read_csv_returns_dataframe(self):
        """La lecture CSV doit retourner un DataFrame"""
        import pandas as pd
        from extractor.extract_csv import read_csv
        df = read_csv("/opt/airflow/data/Books.csv")
        assert isinstance(df, pd.DataFrame)

    def test_read_csv_not_empty(self):
        """Le DataFrame ne doit pas être vide"""
        from extractor.extract_csv import read_csv
        df = read_csv("/opt/airflow/data/Books.csv")
        assert len(df) > 0

    def test_read_csv_has_required_columns(self):
        """Le CSV doit avoir les colonnes obligatoires"""
        from extractor.extract_csv import read_csv
        df = read_csv("/opt/airflow/data/Books.csv")
        required_cols = ["ISBN", "Book-Title", "Book-Author",
                        "Year-Of-Publication", "Publisher"]
        for col in required_cols:
            assert col in df.columns, f"Colonne manquante : {col}"

#test extract_api
class TestExtractAPI:

    def test_fetch_returns_list(self):
        """L'API doit retourner une liste"""
        from extractor.extract_api import fetch_books_from_api
        # On simule une réponse API sans connexion internet
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "works": [
                {"title": "Python Book", "authors": [{"name": "Author"}],
                 "first_publish_year": 2020, "edition_count": 5, "key": "/works/123"}
            ]
        }
        with patch("requests.get", return_value=mock_response):
            result = fetch_books_from_api(subjects=["python"], limit=5)
        assert isinstance(result, list)

    def test_fetch_not_empty(self):
        """L'API doit retourner au moins 1 résultat"""
        from extractor.extract_api import fetch_books_from_api
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "works": [
                {"title": "Python Book", "authors": [{"name": "Author"}],
                 "first_publish_year": 2020, "edition_count": 5, "key": "/works/123"}
            ]
        }
        with patch("requests.get", return_value=mock_response):
            result = fetch_books_from_api(subjects=["python"], limit=5)
        assert len(result) > 0

    def test_fetch_book_has_required_fields(self):
        """Chaque livre API doit avoir les champs obligatoires"""
        from extractor.extract_api import fetch_books_from_api
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "works": [
                {"title": "Python Book", "authors": [{"name": "Author"}],
                 "first_publish_year": 2020, "edition_count": 5, "key": "/works/123"}
            ]
        }
        with patch("requests.get", return_value=mock_response):
            result = fetch_books_from_api(subjects=["python"], limit=5)
        required_fields = ["title", "subject", "source"]
        for field in required_fields:
            assert field in result[0], f"Champ manquant : {field}"


class TestExtractWebRun:

    def test_run_returns_filename(self):
        """run() doit retourner un nom de fichier"""
        from unittest.mock import patch, MagicMock
        from extractor.extract_web import run

        mock_client = MagicMock()
        with patch("extractor.extract_web.get_minio_client", return_value=mock_client):
            result = run()
        assert result is not None
        assert "web/" in result
        assert ".json" in result

class TestExtractCSVRun:

    def test_run_returns_filename(self):
        """run() doit retourner un nom de fichier"""
        from unittest.mock import patch, MagicMock
        from extractor.extract_csv import run

        mock_client = MagicMock()
        with patch("extractor.extract_csv.get_minio_client", return_value=mock_client):
            result = run()
        assert result is not None
        assert "csv/" in result
        assert ".csv" in result

class TestExtractAPIRun:

    def test_run_returns_filename(self):
        """run() doit retourner un nom de fichier"""
        from unittest.mock import patch, MagicMock
        from extractor.extract_api import run

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "works": [
                {"title": "Python Book", "authors": [{"name": "Author"}],
                 "first_publish_year": 2020, "edition_count": 5, "key": "/works/123"}
            ]
        }
        mock_client = MagicMock()
        with patch("requests.get", return_value=mock_response), \
             patch("extractor.extract_api.get_minio_client", return_value=mock_client):
            result = run()
        assert result is not None
        assert "api/" in result
        assert ".json" in result