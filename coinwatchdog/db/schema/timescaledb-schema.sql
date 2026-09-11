CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    product_id TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS pricing_data (
    timestamp TIMESTAMPTZ NOT NULL,
    product_id INTEGER NOT NULL REFERENCES products(id),
    price DOUBLE PRECISION,
    open_24h DOUBLE PRECISION,
    volume_24h DOUBLE PRECISION,
    low_24h DOUBLE PRECISION,
    high_24h DOUBLE PRECISION,
    volume_30d DOUBLE PRECISION,
    best_bid DOUBLE PRECISION,
    best_bid_size DOUBLE PRECISION,
    best_ask DOUBLE PRECISION,
    best_ask_size DOUBLE PRECISION,
    side TEXT,
    trade_id BIGINT,
    last_size DOUBLE PRECISION
);

SELECT create_hypertable('pricing_data', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_pricing_product_timestamp
    ON pricing_data (product_id, timestamp DESC);

ALTER TABLE pricing_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'product_id',
    timescaledb.compress_orderby = 'timestamp',
    timescaledb.compress_chunk_time_interval = '7 days'
);

INSERT INTO products(product_id)
VALUES ('BTC-USD'), ('ADA-USD'), ('POL-USD')
ON CONFLICT (product_id) DO NOTHING;
