"""
Airflow DAG: Stock Market Data Pipeline
Orchestrates the 3-stage pipeline: fetch_and_validate -> load_to_postgres -> verify_load.
"""

from datetime import datetime, timedelta
import logging
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.config import settings
from src.fetcher import YahooFinanceFetcher, StockFetchError
from src.models import StockPriceRecord
from src.db import DatabaseManager

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def task_fetch_and_validate(**context) -> Dict[str, Any]:
    """
    Fetch market data for configured tickers, parse and validate with Pydantic.
    Pushes validated records and extraction metrics to XCom.
    """
    ti = context["ti"]
    fetcher = YahooFinanceFetcher()
    tickers = settings.stock_tickers
    
    all_validated_dicts: List[Dict[str, Any]] = []
    total_rejected = 0
    tickers_success = 0
    errors: List[str] = []

    logger.info(f"Starting ingestion for tickers: {tickers}")

    for ticker in tickers:
        try:
            raw_payload = fetcher.fetch_raw_chart_data(ticker, range_param="1mo", interval_param="1d")
            records, rejected = fetcher.parse_and_validate(ticker, raw_payload)
            
            # Serialize records for XCom transmission
            for r in records:
                all_validated_dicts.append({
                    "symbol": r.symbol,
                    "timestamp": r.timestamp.isoformat(),
                    "open_price": r.open_price,
                    "high_price": r.high_price,
                    "low_price": r.low_price,
                    "close_price": r.close_price,
                    "volume": r.volume,
                })
            
            total_rejected += rejected
            tickers_success += 1

        except StockFetchError as sfe:
            msg = f"Fetcher error for {ticker}: {sfe}"
            logger.error(msg)
            errors.append(msg)
        except Exception as e:
            msg = f"Unexpected error processing {ticker}: {e}"
            logger.error(msg)
            errors.append(msg)

    # Pass records and metrics to the next task via XCom
    payload = {
        "records": all_validated_dicts,
        "tickers_count": len(tickers),
        "tickers_success": tickers_success,
        "records_fetched": len(all_validated_dicts) + total_rejected,
        "records_rejected": total_rejected,
        "errors": errors,
        "started_at": datetime.utcnow().isoformat(),
    }
    return payload


def task_load_to_postgres(**context) -> Dict[str, Any]:
    """
    Load validated records into PostgreSQL using idempotent UPSERTs.
    Pushes insertion/update counts to XCom.
    """
    ti = context["ti"]
    fetch_result = ti.xcom_pull(task_ids="fetch_and_validate")
    
    if not fetch_result:
        raise ValueError("No data received from fetch_and_validate task.")

    raw_records = fetch_result.get("records", [])
    records: List[StockPriceRecord] = []

    # Rehydrate Pydantic models from serialized dicts
    for r in raw_records:
        records.append(
            StockPriceRecord(
                symbol=r["symbol"],
                timestamp=datetime.fromisoformat(r["timestamp"]),
                open_price=r["open_price"],
                high_price=r["high_price"],
                low_price=r["low_price"],
                close_price=r["close_price"],
                volume=r["volume"],
            )
        )

    db_manager = DatabaseManager()
    inserted_count, updated_count = db_manager.upsert_stock_quotes(records)

    return {
        "records_inserted": inserted_count,
        "records_updated": updated_count,
    }


def task_verify_load(**context) -> str:
    """
    Verify database persistence and log business metrics into pipeline_audit_logs.
    Market-aware: gracefully handles periods with 0 new records (e.g. holidays).
    """
    ti = context["ti"]
    run_id = context.get("run_id", f"manual__{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}")
    
    fetch_result = ti.xcom_pull(task_ids="fetch_and_validate") or {}
    load_result = ti.xcom_pull(task_ids="load_to_postgres") or {}

    started_at_str = fetch_result.get("started_at")
    started_at = datetime.fromisoformat(started_at_str) if started_at_str else datetime.utcnow()
    finished_at = datetime.utcnow()

    tickers_count = fetch_result.get("tickers_count", 0)
    tickers_success = fetch_result.get("tickers_success", 0)
    records_fetched = fetch_result.get("records_fetched", 0)
    records_rejected = fetch_result.get("records_rejected", 0)
    errors = fetch_result.get("errors", [])

    records_inserted = load_result.get("records_inserted", 0)
    records_updated = load_result.get("records_updated", 0)

    # Determine status
    if errors and tickers_success == 0:
        status = "FAILED"
    elif errors:
        status = "PARTIAL"
    else:
        status = "SUCCESS"

    error_msg = "; ".join(errors) if errors else None

    # Write metrics to audit table
    db_manager = DatabaseManager()
    audit_id = db_manager.write_audit_log(
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        status=status,
        tickers_processed=tickers_success,
        records_fetched=records_fetched,
        records_inserted=records_inserted,
        records_updated=records_updated,
        records_rejected=records_rejected,
        error_message=error_msg,
    )

    summary = (
        f"\n================ PIPELINE RUN VERIFICATION ================\n"
        f"Run ID:            {run_id}\n"
        f"Audit Record ID:   {audit_id}\n"
        f"Status:            {status}\n"
        f"Tickers Processed: {tickers_success}/{tickers_count}\n"
        f"Records Fetched:   {records_fetched}\n"
        f"Records Inserted:  {records_inserted}\n"
        f"Records Updated:   {records_updated}\n"
        f"Records Rejected:  {records_rejected}\n"
        f"Errors Encountered:{error_msg or 'None'}\n"
        f"============================================================"
    )
    logger.info(summary)
    print(summary)

    if status == "FAILED":
        raise RuntimeError(f"Pipeline run {run_id} failed: {error_msg}")

    return status


with DAG(
    dag_id="stock_data_pipeline",
    default_args=default_args,
    description="Automated stock market data ingestion pipeline with Pydantic & PostgreSQL UPSERT",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["stock", "finance", "etl", "postgres"],
) as dag:

    fetch_and_validate = PythonOperator(
        task_id="fetch_and_validate",
        python_callable=task_fetch_and_validate,
    )

    load_to_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=task_load_to_postgres,
    )

    verify_load = PythonOperator(
        task_id="verify_load",
        python_callable=task_verify_load,
    )

    fetch_and_validate >> load_to_postgres >> verify_load
