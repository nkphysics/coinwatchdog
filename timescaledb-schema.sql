CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    product_id TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS pricing_data (
    timestamp TIMESTAMPTZ NOT NULL,
    product_id INTEGER NOT NULL REFERENCES products(id),
    open_price REAL,
    close_price REAL,
    min_price REAL,
    max_price REAL,
    open_volume_24h REAL,
    close_volume_24h REAL,
    min_volume_24h REAL,
    max_volume_24h REAL,
    open_volume_30d REAL,
    close_volume_30d REAL,
    min_volume_30d REAL,
    max_volume_30d REAL,
    open_best_bid REAL,
    close_best_bid REAL,
    min_best_bid REAL,
    max_best_bid REAL,
    open_best_bid_size REAL,
    close_best_bid_size REAL,
    min_best_bid_size REAL,
    max_best_bid_size REAL,
    open_best_ask REAL,
    close_best_ask REAL,
    min_best_ask REAL,
    max_best_ask REAL,
    open_best_ask_size REAL,
    close_best_ask_size REAL,
    min_best_ask_size REAL,
    max_best_ask_size REAL
);

SELECT create_hypertable('pricing_data', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_pricing_product_timestamp
    ON pricing_data (product_id, timestamp DESC);

ALTER TABLE pricing_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'product_id',
    timescaledb.compress_chunk_time_interval = '1 day'
);

INSERT INTO products(product_id)
VALUES ('BTC-USD'), ('ADA-USD'), ('POL-USD')
ON CONFLICT (product_id) DO NOTHING;
