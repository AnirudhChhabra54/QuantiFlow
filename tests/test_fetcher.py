"""
Unit tests for YahooFinanceFetcher in src/fetcher.py.
Tests API fetching, retry behavior, non-imputation filter, and error handling.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock
import pytest
import requests

from src.fetcher import YahooFinanceFetcher, StockFetchError

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def valid_payload():
    with open(FIXTURES_DIR / "valid_response.json") as f:
        return json.load(f)


@pytest.fixture
def missing_data_payload():
    with open(FIXTURES_DIR / "missing_data.json") as f:
        return json.load(f)


@pytest.fixture
def malformed_payload():
    with open(FIXTURES_DIR / "malformed_response.json") as f:
        return json.load(f)


def test_parse_valid_payload(valid_payload):
    """Verify clean parsing of a well-formed Yahoo Finance response."""
    fetcher = YahooFinanceFetcher()
    records, rejected = fetcher.parse_and_validate("AAPL", valid_payload)

    assert len(records) == 3
    assert rejected == 0
    assert records[0].symbol == "AAPL"
    assert records[0].open_price == 220.50
    assert records[0].close_price == 222.66
    assert records[0].volume == 45000000


def test_non_imputation_policy_skips_missing_data(missing_data_payload):
    """
    Verify that records with missing open or close values are discarded,
    strictly enforcing the no-imputation rule.
    """
    fetcher = YahooFinanceFetcher()
    records, rejected = fetcher.parse_and_validate("MSFT", missing_data_payload)

    # In missing_data.json, index 1 has null open, index 2 has null close.
    # Only index 0 is valid.
    assert len(records) == 1
    assert rejected == 2
    assert records[0].open_price == 420.00
    assert records[0].close_price == 424.50


def test_api_error_response_raises_fetch_error(malformed_payload):
    """Verify that an API-level error object raises StockFetchError."""
    fetcher = YahooFinanceFetcher()
    with pytest.raises(StockFetchError) as exc_info:
        fetcher.parse_and_validate("INVALID", malformed_payload)
    assert "No data found" in str(exc_info.value)


def test_market_closure_empty_data_handled_gracefully():
    """Verify that empty chart data (e.g. during weekends) returns 0 records without crashing."""
    fetcher = YahooFinanceFetcher()
    empty_payload = {
        "chart": {
            "result": [
                {
                    "meta": {"symbol": "AAPL"},
                    "timestamp": [],
                    "indicators": {"quote": [{}]}
                }
            ],
            "error": None
        }
    }
    records, rejected = fetcher.parse_and_validate("AAPL", empty_payload)
    assert records == []
    assert rejected == 0


def test_http_200_fetch_success(valid_payload):
    """Verify fetch_raw_chart_data successfully returns JSON on HTTP 200 when crumb is set."""
    mock_session = MagicMock(spec=requests.Session)
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = valid_payload
    mock_session.get.return_value = mock_response

    fetcher = YahooFinanceFetcher(session=mock_session)
    fetcher.crumb = "test_crumb"
    result = fetcher.fetch_raw_chart_data("AAPL")

    assert result == valid_payload
    mock_session.get.assert_called_once()


def test_http_429_rate_limit_raises_fetch_error():
    """Verify HTTP 429 triggers StockFetchError with rate limit messaging."""
    mock_session = MagicMock(spec=requests.Session)
    mock_response = MagicMock()
    mock_response.status_code = 429
    mock_session.get.return_value = mock_response

    fetcher = YahooFinanceFetcher(session=mock_session)
    fetcher.crumb = "test_crumb"
    with pytest.raises(StockFetchError) as exc_info:
        fetcher.fetch_raw_chart_data("AAPL")
    assert "HTTP 429: Rate limited" in str(exc_info.value)


def test_http_500_server_error_raises_fetch_error():
    """Verify HTTP 500 triggers StockFetchError."""
    mock_session = MagicMock(spec=requests.Session)
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    mock_session.get.return_value = mock_response

    fetcher = YahooFinanceFetcher(session=mock_session)
    fetcher.crumb = "test_crumb"
    with pytest.raises(StockFetchError) as exc_info:
        fetcher.fetch_raw_chart_data("AAPL")
    assert "HTTP 500" in str(exc_info.value)


def test_socket_timeout_raises_fetch_error():
    """Verify network socket timeout raises StockFetchError."""
    mock_session = MagicMock(spec=requests.Session)
    mock_session.get.side_effect = requests.exceptions.Timeout("Read timeout")

    fetcher = YahooFinanceFetcher(session=mock_session)
    fetcher.crumb = "test_crumb"
    with pytest.raises(StockFetchError) as exc_info:
        fetcher.fetch_raw_chart_data("AAPL")
    assert "Connection timed out" in str(exc_info.value)
