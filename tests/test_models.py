"""
Unit tests for Pydantic models in src/models.py.
Validates strict schema enforcement and non-imputation domain rules.
"""

from datetime import datetime, timezone
import pytest
from pydantic import ValidationError

from src.models import StockPriceRecord, AuditLogRecord


def test_valid_stock_price_record():
    """Verify that a well-formed record parses correctly."""
    record = StockPriceRecord(
        symbol="aapl",
        timestamp=datetime(2026, 9, 11, 14, 30, tzinfo=timezone.utc),
        open_price=220.50,
        high_price=224.00,
        low_price=219.80,
        close_price=222.66,
        volume=45000000
    )
    assert record.symbol == "AAPL"  # Normalized to uppercase
    assert record.close_price == 222.66
    assert record.volume == 45000000


def test_naive_timestamp_converted_to_utc():
    """Verify that naive datetimes are automatically tagged with UTC."""
    naive_dt = datetime(2026, 9, 11, 14, 30)
    record = StockPriceRecord(
        symbol="MSFT",
        timestamp=naive_dt,
        open_price=420.0,
        high_price=425.0,
        low_price=418.0,
        close_price=424.0,
        volume=1000000
    )
    assert record.timestamp.tzinfo is not None
    assert record.timestamp.tzinfo == timezone.utc


def test_missing_close_price_fails_validation():
    """Verify that missing close price is rejected (no artificial imputation)."""
    with pytest.raises(ValidationError):
        StockPriceRecord(
            symbol="GOOGL",
            timestamp=datetime.now(timezone.utc),
            open_price=170.0,
            high_price=175.0,
            low_price=169.0,
            close_price=None,  # Missing
            volume=500000
        )


def test_inverted_high_low_bounds_rejected():
    """Verify that high_price < low_price raises a validation error."""
    with pytest.raises(ValidationError) as exc_info:
        StockPriceRecord(
            symbol="NVDA",
            timestamp=datetime.now(timezone.utc),
            open_price=120.0,
            high_price=110.0,  # Lower than low
            low_price=115.0,
            close_price=118.0,
            volume=2000000
        )
    assert "cannot be lower than low price" in str(exc_info.value)


def test_high_lower_than_close_rejected():
    """Verify that high_price < close_price raises a validation error."""
    with pytest.raises(ValidationError) as exc_info:
        StockPriceRecord(
            symbol="AMZN",
            timestamp=datetime.now(timezone.utc),
            open_price=180.0,
            high_price=182.0,
            low_price=178.0,
            close_price=185.0,  # Higher than high
            volume=3000000
        )
    assert "cannot be lower than close price" in str(exc_info.value)


def test_negative_price_or_volume_rejected():
    """Verify that prices <= 0 or volume < 0 are strictly rejected."""
    with pytest.raises(ValidationError):
        StockPriceRecord(
            symbol="META",
            timestamp=datetime.now(timezone.utc),
            open_price=500.0,
            high_price=510.0,
            low_price=-5.0,  # Negative
            close_price=505.0,
            volume=100000
        )

    with pytest.raises(ValidationError):
        StockPriceRecord(
            symbol="META",
            timestamp=datetime.now(timezone.utc),
            open_price=500.0,
            high_price=510.0,
            low_price=490.0,
            close_price=505.0,
            volume=-100  # Negative volume
        )


def test_audit_log_record():
    """Verify audit log schema."""
    audit = AuditLogRecord(
        run_id="manual__test_run",
        started_at=datetime.now(timezone.utc),
        status="SUCCESS",
        tickers_processed=3,
        records_fetched=60,
        records_inserted=58,
        records_updated=0,
        records_rejected=2
    )
    assert audit.status == "SUCCESS"
    assert audit.records_rejected == 2
