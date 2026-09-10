-- Initialization script for PostgreSQL container
-- Executed on container first run via /docker-entrypoint-initdb.d/

-- 1. Ensure stock_db exists (airflow_db is created as the default POSTGRES_DB)
SELECT 'CREATE DATABASE stock_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'stock_db')\gexec

-- 2. Connect to the application database: stock_db
\c stock_db;

-- 3. Create stock_quotes table
CREATE TABLE IF NOT EXISTS stock_quotes (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open_price NUMERIC(12, 4) NOT NULL,
    high_price NUMERIC(12, 4) NOT NULL,
    low_price NUMERIC(12, 4) NOT NULL,
    close_price NUMERIC(12, 4) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_symbol_timestamp UNIQUE (symbol, timestamp)
);

-- 4. B-Tree index for high-performance time-series queries
CREATE INDEX IF NOT EXISTS idx_stock_quotes_symbol_time 
ON stock_quotes (symbol, timestamp DESC);

-- 5. Create pipeline_audit_logs table to capture pipeline business metrics
CREATE TABLE IF NOT EXISTS pipeline_audit_logs (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL,          -- 'SUCCESS', 'PARTIAL', 'FAILED'
    tickers_processed INT DEFAULT 0,
    records_fetched INT DEFAULT 0,
    records_inserted INT DEFAULT 0,
    records_updated INT DEFAULT 0,
    records_rejected INT DEFAULT 0,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_run_id 
ON pipeline_audit_logs (run_id);
