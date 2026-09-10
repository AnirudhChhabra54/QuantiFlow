"""
Data models and contract validation schemas using Pydantic.
Enforces strict financial domain constraints without artificial price imputation.
"""

from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator


class StockPriceRecord(BaseModel):
    """
    Validated time-series stock price record (OHLCV).
    Represents a single bar/candle for a given ticker symbol.
    """
    symbol: str = Field(..., description="Ticker symbol, e.g. AAPL")
    timestamp: datetime = Field(..., description="Bar timestamp normalized to UTC")
    open_price: float = Field(..., gt=0, description="Opening price for the period")
    high_price: float = Field(..., gt=0, description="Highest price during the period")
    low_price: float = Field(..., gt=0, description="Lowest price during the period")
    close_price: float = Field(..., gt=0, description="Closing price for the period")
    volume: int = Field(..., ge=0, description="Trading volume during the period")

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, v: str) -> str:
        """Ensure ticker symbol is uppercase and trimmed."""
        if not v or not v.strip():
            raise ValueError("Symbol cannot be empty")
        return v.strip().upper()

    @field_validator("timestamp")
    @classmethod
    def ensure_utc_timestamp(cls, v: datetime) -> datetime:
        """Ensure timestamp is timezone-aware and normalized to UTC."""
        if v.tzinfo is None:
            # Assume UTC if naive
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)

    @model_validator(mode="after")
    def validate_price_boundaries(self) -> "StockPriceRecord":
        """
        Validate financial consistency:
        - High price cannot be lower than Low price.
        - High price must be >= Open and Close prices.
        - Low price must be <= Open and Close prices.
        """
        # Allow tiny epsilon for floating point precision issues
        eps = 1e-6
        if self.high_price + eps < self.low_price:
            raise ValueError(
                f"High price ({self.high_price}) cannot be lower than low price ({self.low_price})"
            )
        if self.high_price + eps < self.open_price:
            raise ValueError(
                f"High price ({self.high_price}) cannot be lower than open price ({self.open_price})"
            )
        if self.high_price + eps < self.close_price:
            raise ValueError(
                f"High price ({self.high_price}) cannot be lower than close price ({self.close_price})"
            )
        if self.low_price - eps > self.open_price:
            raise ValueError(
                f"Low price ({self.low_price}) cannot be higher than open price ({self.open_price})"
            )
        if self.low_price - eps > self.close_price:
            raise ValueError(
                f"Low price ({self.low_price}) cannot be higher than close price ({self.close_price})"
            )
        return self


class AuditLogRecord(BaseModel):
    """Execution audit metrics recorded in pipeline_audit_logs."""
    run_id: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    status: str
    tickers_processed: int = 0
    records_fetched: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_rejected: int = 0
    error_message: Optional[str] = None
