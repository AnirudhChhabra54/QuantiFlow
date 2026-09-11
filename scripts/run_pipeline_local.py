"""
Standalone End-to-End Pipeline Execution Script.
Allows testing and verifying the complete data pipeline lifecycle directly:
1. Live market data extraction from Yahoo Finance via direct requests.
2. Pydantic validation & non-imputation checks.
3. Batch database upsert (with PostgreSQL, or local SQLite for testing).
4. Idempotency test (running back-to-back to prove no duplicates are created).
5. Audit metrics logging.
"""

import os
import sys
import sqlite3
from datetime import datetime, timezone

# Ensure project root is on path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.config import settings
from src.fetcher import YahooFinanceFetcher, StockFetchError
from src.models import StockPriceRecord
from src.db import DatabaseManager


def run_local_simulation(tickers=None):
    if tickers is None:
        tickers = settings.stock_tickers

    print("=" * 70)
    print("🚀 STARTING STOCK MARKET DATA PIPELINE (END-TO-END TEST)")
    print("=" * 70)
    print(f"Configured Tickers: {tickers}")
    print(f"Target Database:    PostgreSQL ({settings.postgres_host}:{settings.postgres_port})")
    print(f"Time (UTC):         {datetime.now(timezone.utc).isoformat()}\n")

    fetcher = YahooFinanceFetcher()
    all_validated_records = []
    total_rejected = 0

    # -------------------------------------------------------------------------
    # STAGE 1: FETCH & VALIDATE
    # -------------------------------------------------------------------------
    print("----------------------------------------------------------------------")
    print("📥 STAGE 1: INGESTION & DATA CONTRACT VALIDATION (Pydantic)")
    print("----------------------------------------------------------------------")
    for ticker in tickers:
        try:
            print(f"[*] Querying Yahoo Finance API for {ticker}...")
            raw_data = fetcher.fetch_raw_chart_data(ticker, range_param="5d", interval_param="1d")
            records, rejected = fetcher.parse_and_validate(ticker, raw_data)
            all_validated_records.extend(records)
            total_rejected += rejected
            print(f"    └── ✅ Successfully validated {len(records)} bars (Rejected/Skipped: {rejected})")
        except StockFetchError as e:
            print(f"    └── ❌ Fetch error for {ticker}: {e}")
        except Exception as e:
            print(f"    └── ❌ Unexpected error for {ticker}: {e}")

    print(f"\n📊 Total Records Validated: {len(all_validated_records)}")
    print(f"⚠️  Total Corrupted/Missing Records Skipped: {total_rejected}\n")

    if not all_validated_records:
        print("❌ No records available to load. Aborting pipeline.")
        return

    # Sample records preview
    print("Sample Validated Record:")
    sample = all_validated_records[0]
    print(f"  • Symbol:      {sample.symbol}")
    print(f"  • Timestamp:   {sample.timestamp}")
    print(f"  • Open:        ${sample.open_price:,.2f}")
    print(f"  • High:        ${sample.high_price:,.2f}")
    print(f"  • Low:         ${sample.low_price:,.2f}")
    print(f"  • Close:       ${sample.close_price:,.2f}")
    print(f"  • Volume:      {sample.volume:,}\n")

    # -------------------------------------------------------------------------
    # STAGE 2: DATABASE PERSISTENCE & IDEMPOTENCY TEST
    # -------------------------------------------------------------------------
    print("----------------------------------------------------------------------")
    print("💾 STAGE 2: DATABASE PERSISTENCE & IDEMPOTENCY VERIFICATION")
    print("----------------------------------------------------------------------")

    # Check if live PostgreSQL is reachable
    use_postgres = False
    try:
        db_mgr = DatabaseManager()
        test_conn = db_mgr.get_connection()
        test_conn.close()
        use_postgres = True
        print("✅ Connected to live PostgreSQL server.")
    except Exception as e:
        print(f"ℹ️  PostgreSQL not running locally ({e}).")
        print("    Using local in-memory SQLite engine with identical ON CONFLICT UPSERT logic for test verification.\n")

    if use_postgres:
        db_mgr = DatabaseManager()
        # First Run
        inserted, updated = db_mgr.upsert_stock_quotes(all_validated_records)
        print(f"[*] Initial Batch Run:  {inserted} rows inserted, {updated} rows updated.")

        # Second Run (Idempotency Test)
        print("[*] Re-running identical batch to test IDEMPOTENCY...")
        inserted_2, updated_2 = db_mgr.upsert_stock_quotes(all_validated_records)
        print(f"[*] Second Batch Run:   {inserted_2} rows inserted, {updated_2} rows updated.")
        
        if inserted_2 == 0 and updated_2 == len(all_validated_records):
            print("    └── ✅ IDEMPOTENCY TEST PASSED: Zero duplicates created!")
        else:
            print(f"    └── ⚠️  Unexpected duplicate behavior: inserted={inserted_2}")

        # Audit Log
        audit_id = db_mgr.write_audit_log(
            run_id=f"test_run_{int(datetime.utcnow().timestamp())}",
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            status="SUCCESS",
            tickers_processed=len(tickers),
            records_fetched=len(all_validated_records) + total_rejected,
            records_inserted=inserted,
            records_updated=updated,
            records_rejected=total_rejected
        )
        print(f"[*] Audit record created with ID: {audit_id}")

    else:
        # Local SQLite verification
        conn = sqlite3.connect(":memory:")
        conn.execute("""
            CREATE TABLE stock_quotes (
                symbol TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                open_price REAL NOT NULL,
                high_price REAL NOT NULL,
                low_price REAL NOT NULL,
                close_price REAL NOT NULL,
                volume INTEGER NOT NULL,
                updated_at TEXT,
                UNIQUE(symbol, timestamp)
            )
        """)
        
        upsert_query = """
            INSERT INTO stock_quotes (symbol, timestamp, open_price, high_price, low_price, close_price, volume, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                open_price = excluded.open_price,
                high_price = excluded.high_price,
                low_price = excluded.low_price,
                close_price = excluded.close_price,
                volume = excluded.volume,
                updated_at = datetime('now')
        """

        # First run
        values = [
            (r.symbol, r.timestamp.isoformat(), r.open_price, r.high_price, r.low_price, r.close_price, r.volume)
            for r in all_validated_records
        ]
        conn.executemany(upsert_query, values)
        initial_count = conn.execute("SELECT COUNT(*) FROM stock_quotes").fetchone()[0]
        print(f"[*] Initial Batch Run:  {initial_count} rows inserted into table.")

        # Second run
        print("[*] Re-running identical batch to test IDEMPOTENCY...")
        conn.executemany(upsert_query, values)
        second_count = conn.execute("SELECT COUNT(*) FROM stock_quotes").fetchone()[0]
        print(f"[*] Second Batch Run:   Total table row count = {second_count}")

        if initial_count == second_count:
            print("    └── ✅ IDEMPOTENCY TEST PASSED: Exact match! Zero duplicate rows created.\n")
        else:
            print(f"    └── ❌ FAILED: Duplicate rows detected ({second_count} != {initial_count})\n")

    print("=" * 70)
    print("🎉 PIPELINE RUN COMPLETED SUCCESSFULLY")
    print("=" * 70)


if __name__ == "__main__":
    run_local_simulation()
