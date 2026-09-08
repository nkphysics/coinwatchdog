import asyncio
import logging
from datetime import datetime
from pathlib import Path

import psycopg2

logger = logging.getLogger(__name__)


class TimescaleBackend:
    """TimescaleDB backend using psycopg2."""

    def __init__(
        self,
        host: str,
        port: int,
        dbname: str,
        user: str,
        password: str,
        **kwargs,
    ):
        """
        Connect to TimescaleDB and initialize schema if needed.

        Parameters:
            host: Database host
            port: Database port
            dbname: Database name
            user: Database user
            password: Database password
        """
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
        )
        self._init_schema()
        self.pair_db_ids = self.get_product_ids()

    def _init_schema(self):
        """
        Initialize the database schema using tsdb_schema.sql.

        All statements use IF NOT EXISTS so this is safe to run
        on every startup.
        """
        schema_path = (
            Path(__file__).parent / "schema" / "timescaledb-schema.sql"
        )
        with open(schema_path, "r") as f:
            schema_sql = f.read()
        cur = self.conn.cursor()
        cur.execute(schema_sql)
        self.conn.commit()

    def get_product_ids(self) -> dict:
        """
        Get all product_id to database id mappings.

        Returns:
            dict: {product_id_str: db_id_int}
        """
        cur = self.conn.cursor()
        cur.execute("SELECT id, product_id FROM products")
        return {row[1]: row[0] for row in cur.fetchall()}

    async def insert_pricing_data(self, data: dict) -> None:
        """
        Insert a row of aggregated pricing metrics.

        Parameters:
            data: dict containing product_id, last_timestamp, and all
                  open/close/min/max price/volume/bid/ask metrics
        """
        await asyncio.to_thread(self._insert_pricing_data_sync, data)

    def _insert_pricing_data_sync(self, data: dict) -> None:
        cur = self.conn.cursor()

        # Convert ISO timestamp string to Python datetime
        ts_str = data["last_timestamp"]
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))

        # Map product_id string to database id
        product_db_id = self.pair_db_ids[data["product_id"]]

        logger.debug(
            f"Inserting pricing data for"
            f" product_db_id={product_db_id}"
            f" timestamp={ts}"
        )

        try:
            cur.execute(
                """
                INSERT INTO pricing_data (
                    timestamp,
                    product_id,
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
                    %(timestamp)s,
                    %(product_id)s,
                    %(open_price)s,
                    %(close_price)s,
                    %(min_price)s,
                    %(max_price)s,
                    %(open_volume_24h)s,
                    %(close_volume_24h)s,
                    %(min_volume_24h)s,
                    %(max_volume_24h)s,
                    %(open_volume_30d)s,
                    %(close_volume_30d)s,
                    %(min_volume_30d)s,
                    %(max_volume_30d)s,
                    %(open_best_bid)s,
                    %(close_best_bid)s,
                    %(min_best_bid)s,
                    %(max_best_bid)s,
                    %(open_best_bid_size)s,
                    %(close_best_bid_size)s,
                    %(min_best_bid_size)s,
                    %(max_best_bid_size)s,
                    %(open_best_ask)s,
                    %(close_best_ask)s,
                    %(min_best_ask)s,
                    %(max_best_ask)s,
                    %(open_best_ask_size)s,
                    %(close_best_ask_size)s,
                    %(min_best_ask_size)s,
                    %(max_best_ask_size)s
                )
                """,
                {
                    "timestamp": ts,
                    "product_id": product_db_id,
                    "open_price": data["open_price"],
                    "close_price": data["close_price"],
                    "min_price": data["min_price"],
                    "max_price": data["max_price"],
                    "open_volume_24h": data["open_volume_24h"],
                    "close_volume_24h": data["close_volume_24h"],
                    "min_volume_24h": data["min_volume_24h"],
                    "max_volume_24h": data["max_volume_24h"],
                    "open_volume_30d": data["open_volume_30d"],
                    "close_volume_30d": data["close_volume_30d"],
                    "min_volume_30d": data["min_volume_30d"],
                    "max_volume_30d": data["max_volume_30d"],
                    "open_best_bid": data["open_best_bid"],
                    "close_best_bid": data["close_best_bid"],
                    "min_best_bid": data["min_best_bid"],
                    "max_best_bid": data["max_best_bid"],
                    "open_best_bid_size": data["open_best_bid_size"],
                    "close_best_bid_size": data["close_best_bid_size"],
                    "min_best_bid_size": data["min_best_bid_size"],
                    "max_best_bid_size": data["max_best_bid_size"],
                    "open_best_ask": data["open_best_ask"],
                    "close_best_ask": data["close_best_ask"],
                    "min_best_ask": data["min_best_ask"],
                    "max_best_ask": data["max_best_ask"],
                    "open_best_ask_size": data["open_best_ask_size"],
                    "close_best_ask_size": data["close_best_ask_size"],
                    "min_best_ask_size": data["min_best_ask_size"],
                    "max_best_ask_size": data["max_best_ask_size"],
                },
            )
            self.conn.commit()
        except psycopg2.Error:
            self.conn.rollback()
            raise

    def is_healthy(self) -> bool:
        """
        Check if the database connection is alive.

        Returns:
            bool: True if connection is alive, False otherwise
        """
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT 1")
            return True
        except Exception:
            return False

    def close(self):
        """Close the database connection."""
        self.conn.close()
