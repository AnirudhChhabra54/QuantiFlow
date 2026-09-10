"""
Configuration management for the Stock Data Pipeline.
Loads parameters from environment variables with fallback defaults.
"""

import os
from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Pipeline and database settings."""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # PostgreSQL connection settings
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_user: str = "airflow"
    postgres_password: str = "airflow"
    stock_db_name: str = "stock_db"
    airflow_db_name: str = "airflow_db"

    # Ingestion parameters
    stock_tickers_raw: str = "AAPL,MSFT,GOOGL"
    fetch_interval: str = "daily"

    # HTTP client configuration
    http_timeout_connect: int = 10
    http_timeout_read: int = 30
    http_max_retries: int = 3

    @property
    def stock_tickers(self) -> List[str]:
        """Parse comma-separated tickers into a clean uppercase list."""
        return [
            ticker.strip().upper() 
            for ticker in self.stock_tickers_raw.split(",") 
            if ticker.strip()
        ]

    @property
    def stock_db_dsn(self) -> str:
        """PostgreSQL DSN string for connecting to stock_db."""
        return (
            f"host={self.postgres_host} "
            f"port={self.postgres_port} "
            f"dbname={self.stock_db_name} "
            f"user={self.postgres_user} "
            f"password={self.postgres_password}"
        )


settings = Settings()
