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

    async def insert_ticker_row(self, data: dict) -> None:
        """
        Insert a single raw ticker row immediately upon receipt.

        Parameters:
            data: dict containing raw ticker fields from Coinbase WebSocket
        """
        await asyncio.to_thread(self._insert_ticker_row_sync, data)

    def _insert_ticker_row_sync(self, data: dict) -> None:
        cur = self.conn.cursor()

        ts = datetime.fromisoformat(data["time"].replace("Z", "+00:00"))
        product_db_id = self.pair_db_ids[data["product_id"]]

        logger.debug(
            f"Inserting ticker row for"
            f" product_db_id={product_db_id}"
            f" timestamp={ts}"
        )

        try:
            cur.execute(
                """
                INSERT INTO pricing_data (
                    timestamp,
                    product_id,
                    price,
                    open_24h,
                    volume_24h,
                    low_24h,
                    high_24h,
                    volume_30d,
                    best_bid,
                    best_bid_size,
                    best_ask,
                    best_ask_size,
                    side,
                    trade_id,
                    last_size
                ) VALUES (
                    %(timestamp)s,
                    %(product_id)s,
                    %(price)s,
                    %(open_24h)s,
                    %(volume_24h)s,
                    %(low_24h)s,
                    %(high_24h)s,
                    %(volume_30d)s,
                    %(best_bid)s,
                    %(best_bid_size)s,
                    %(best_ask)s,
                    %(best_ask_size)s,
                    %(side)s,
                    %(trade_id)s,
                    %(last_size)s
                )
                """,
                {
                    "timestamp": ts,
                    "product_id": product_db_id,
                    "price": float(data["price"]),
                    "open_24h": float(data["open_24h"]),
                    "volume_24h": float(data["volume_24h"]),
                    "low_24h": float(data["low_24h"]),
                    "high_24h": float(data["high_24h"]),
                    "volume_30d": float(data["volume_30d"]),
                    "best_bid": float(data["best_bid"]),
                    "best_bid_size": float(data["best_bid_size"]),
                    "best_ask": float(data["best_ask"]),
                    "best_ask_size": float(data["best_ask_size"]),
                    "side": data["side"],
                    "trade_id": data["trade_id"],
                    "last_size": float(data["last_size"]),
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
