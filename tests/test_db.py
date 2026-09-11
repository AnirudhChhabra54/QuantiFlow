"""
Unit and integration tests for DatabaseManager in src/db.py.
Tests idempotent batch UPSERT logic and pipeline audit logging.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import pytest

from src.db import DatabaseManager
from src.models import StockPriceRecord


def test_upsert_empty_records_returns_zero():
    """Verify that an empty record list gracefully returns (0, 0)."""
    db = DatabaseManager(dsn="dummy_dsn")
    inserted, updated = db.upsert_stock_quotes([])
    assert inserted == 0
    assert updated == 0


@patch("src.db.psycopg2.connect")
@patch("src.db.execute_values")
def test_upsert_stock_quotes_with_mocked_db(mock_execute_values, mock_connect):
    """
    Verify batch UPSERT query execution and counting of inserted vs updated rows
    via PostgreSQL's (xmax = 0) flag.
    """
    mock_conn = mock_connect.return_value
    mock_cur = mock_conn.cursor.return_value.__enter__.return_value

    # Simulate execute_values returning:
    # 2 rows where xmax=0 (True -> newly inserted)
    # 1 row where xmax!=0 (False -> updated in place)
    mock_execute_values.return_value = [(True,), (True,), (False,)]

    records = [
        StockPriceRecord(
            symbol="AAPL",
            timestamp=datetime(2026, 9, 11, 14, 0, tzinfo=timezone.utc),
            open_price=220.0,
            high_price=225.0,
            low_price=219.0,
            close_price=224.0,
            volume=1000000
        ),
        StockPriceRecord(
            symbol="MSFT",
            timestamp=datetime(2026, 9, 11, 14, 0, tzinfo=timezone.utc),
            open_price=420.0,
            high_price=425.0,
            low_price=419.0,
            close_price=423.0,
            volume=2000000
        ),
        StockPriceRecord(
            symbol="GOOGL",
            timestamp=datetime(2026, 9, 11, 14, 0, tzinfo=timezone.utc),
            open_price=170.0,
            high_price=175.0,
            low_price=169.0,
            close_price=174.0,
            volume=3000000
        )
    ]

    db = DatabaseManager(dsn="dbname=stock_db user=airflow")
    inserted, updated = db.upsert_stock_quotes(records)

    assert inserted == 2
    assert updated == 1
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()


@patch("src.db.psycopg2.connect")
@patch("src.db.execute_values")
def test_upsert_rolls_back_on_error(mock_execute_values, mock_connect):
    """Verify that a database error triggers a transaction rollback."""
    mock_conn = mock_connect.return_value
    mock_execute_values.side_effect = Exception("DB Connection severed")

    records = [
        StockPriceRecord(
            symbol="AAPL",
            timestamp=datetime(2026, 9, 11, 14, 0, tzinfo=timezone.utc),
            open_price=220.0,
            high_price=225.0,
            low_price=219.0,
            close_price=224.0,
            volume=1000000
        )
    ]

    db = DatabaseManager(dsn="dbname=stock_db")
    with pytest.raises(Exception) as exc_info:
        db.upsert_stock_quotes(records)

    assert "DB Connection severed" in str(exc_info.value)
    mock_conn.rollback.assert_called_once()


@patch("src.db.psycopg2.connect")
def test_write_audit_log(mock_connect):
    """Verify that write_audit_log properly binds parameters and commits."""
    mock_conn = mock_connect.return_value
    mock_cur = mock_conn.cursor.return_value.__enter__.return_value
    mock_cur.fetchone.return_value = (42,)  # Returned audit ID

    db = DatabaseManager(dsn="dbname=stock_db")
    audit_id = db.write_audit_log(
        run_id="manual__test_run_1",
        started_at=datetime.utcnow(),
        finished_at=datetime.utcnow(),
        status="SUCCESS",
        tickers_processed=3,
        records_fetched=150,
        records_inserted=150,
        records_updated=0,
        records_rejected=0
    )

    assert audit_id == 42
    mock_cur.execute.assert_called_once()
    mock_conn.commit.assert_called_once()
