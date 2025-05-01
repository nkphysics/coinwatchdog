CREATE TABLE products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id TEXT NOT NULL UNIQUE,
    UNIQUE (product_id)
);

CREATE TABLE pricing_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL,
    last_timestamp TEXT NOT NULL,
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
    max_best_ask_size REAL,
    FOREIGN KEY (product_id) REFERENCES products(id)
);

INSERT INTO products(product_id) VALUES ("BTC-USD");
INSERT INTO products(product_id) VALUES ("ADA-USD");
INSERT INTO products(product_id) VALUES ("POL-USD");
