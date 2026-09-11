"""
PostgreSQL database persistence manager.
Handles connection management, idempotent batch UPSERT operations, and audit logging.
"""

import logging
from datetime import datetime
from typing import List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values

from src.config import settings
from src.models import StockPriceRecord

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages connections and transactions against the stock_db PostgreSQL database."""

    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or settings.stock_db_dsn

    def get_connection(self):
        """Create and return a new PostgreSQL connection."""
        return psycopg2.connect(self.dsn)

    def upsert_stock_quotes(
        self,
        records: List[StockPriceRecord]
    ) -> Tuple[int, int]:
        """
        Idempotently insert or update a batch of stock price records.
        
        Uses PostgreSQL's ON CONFLICT (symbol, timestamp) DO UPDATE clause.
        Utilizes `RETURNING (xmax = 0) AS is_insert` to distinguish between
        newly inserted rows vs existing rows updated in-place.
        
        Args:
            records: List of validated StockPriceRecord objects
            
        Returns:
            Tuple of (inserted_rows_count, updated_rows_count)
        """
        if not records:
            logger.info("No records provided to upsert.")
            return 0, 0

        # Transform Pydantic records into list of tuples for execute_values
        values = [
            (
                r.symbol,
                r.timestamp,
                r.open_price,
                r.high_price,
                r.low_price,
                r.close_price,
                r.volume,
                datetime.utcnow()
            )
            for r in records
        ]

        query = """
            INSERT INTO stock_quotes (
                symbol,
                timestamp,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                updated_at
            )
            VALUES %s
            ON CONFLICT (symbol, timestamp)
            DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume,
                updated_at = CURRENT_TIMESTAMP
            RETURNING (xmax = 0) AS is_insert;
        """

        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                results = execute_values(
                    cur,
                    query,
                    values,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s)",
                    fetch=True
                )
                conn.commit()

            # xmax = 0 is True for INSERT, False for UPDATE in PostgreSQL
            inserted_count = sum(1 for row in results if row[0] is True)
            updated_count = len(results) - inserted_count

            logger.info(
                f"Batch UPSERT complete: {inserted_count} rows inserted, {updated_count} rows updated."
            )
            return inserted_count, updated_count

        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to upsert stock records: {e}")
            raise
        finally:
            conn.close()

    def write_audit_log(
        self,
        run_id: str,
        started_at: datetime,
        finished_at: datetime,
        status: str,
        tickers_processed: int,
        records_fetched: int,
        records_inserted: int,
        records_updated: int,
        records_rejected: int,
        error_message: Optional[str] = None
    ) -> int:
        """
        Record pipeline execution metrics into pipeline_audit_logs.
        
        Returns:
            The primary key ID of the created audit record.
        """
        query = """
            INSERT INTO pipeline_audit_logs (
                run_id,
                started_at,
                finished_at,
                status,
                tickers_processed,
                records_fetched,
                records_inserted,
                records_updated,
                records_rejected,
                error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    query,
                    (
                        run_id,
                        started_at,
                        finished_at,
                        status,
                        tickers_processed,
                        records_fetched,
                        records_inserted,
                        records_updated,
                        records_rejected,
                        error_message
                    )
                )
                audit_id = cur.fetchone()[0]
                conn.commit()
                logger.info(f"Audit log recorded with ID {audit_id} (Status: {status})")
                return audit_id
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to write audit log: {e}")
            raise
        finally:
            conn.close()
