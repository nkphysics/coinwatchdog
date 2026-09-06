import sqlite3
from pathlib import Path


class SQLiteBackend:

    def __init__(self, db_path: str, **kwargs):
        """
        Connect to a SQLite database.

        Parameters:
            db_path: Path to the SQLite database file
        """
        self.conn = sqlite3.connect(Path(db_path).resolve())
        self.pair_db_ids = self.get_product_ids()

    def get_product_ids(self) -> dict:
        """
        Get all product_id to database id mappings.

        Returns:
            dict: {product_id_str: db_id_int}
        """
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM products")
        return {row[1]: row[0] for row in cur.fetchall()}

    async def insert_pricing_data(self, data: dict) -> int:
        """
        Insert a row of aggregated pricing metrics.

        Parameters:
            data: dict containing product_id, last_timestamp, and all
                  open/close/min/max price/volume/bid/ask metrics

        Returns:
            int: id of the inserted row
        """
        cur = self.conn.cursor()
        data["product_id"] = self.pair_db_ids[data["product_id"]]
        cur.execute(
            """
            INSERT INTO pricing_data (
                product_id,
                last_timestamp,
                open_price,
                close_price,
                min_price,
                max_price,
                open_volume_24h,
                close_volume_24h,
                min_volume_24h,
                max_volume_24h,
                open_volume_30d,
                close_volume_30d,
                min_volume_30d,
                max_volume_30d,
                open_best_bid,
                close_best_bid,
                min_best_bid,
                max_best_bid,
                open_best_bid_size,
                close_best_bid_size,
                min_best_bid_size,
                max_best_bid_size,
                open_best_ask,
                close_best_ask,
                min_best_ask,
                max_best_ask,
                open_best_ask_size,
                close_best_ask_size,
                min_best_ask_size,
                max_best_ask_size
            ) VALUES (
                :product_id,
                :last_timestamp,
                :open_price,
                :close_price,
                :min_price,
                :max_price,
                :open_volume_24h,
                :close_volume_24h,
                :min_volume_24h,
                :max_volume_24h,
                :open_volume_30d,
                :close_volume_30d,
                :min_volume_30d,
                :max_volume_30d,
                :open_best_bid,
                :close_best_bid,
                :min_best_bid,
                :max_best_bid,
                :open_best_bid_size,
                :close_best_bid_size,
                :min_best_bid_size,
                :max_best_bid_size,
                :open_best_ask,
                :close_best_ask,
                :min_best_ask,
                :max_best_ask,
                :open_best_ask_size,
                :close_best_ask_size,
                :min_best_ask_size,
                :max_best_ask_size
            )
            """,
            data,
        )
        self.conn.commit()
        return cur.lastrowid

    def is_healthy(self) -> bool:
        """
        SQLite is always healthy (local file).

        Returns:
            bool: True
        """
        return True

    def close(self):
        """Close the database connection."""
        self.conn.close()
