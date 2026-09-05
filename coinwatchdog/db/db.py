from abc import ABC, abstractmethod


class DBManager(ABC):

    @abstractmethod
    def get_product_ids(self) -> dict:
        """
        Get all product_id to database id mappings.

        Returns:
            dict: {product_id_str: db_id_int}
        """
        pass

    @abstractmethod
    def insert_pricing_data(self, data: dict) -> int:
        """
        Insert a row of aggregated pricing metrics.

        Parameters:
            data: dict containing product_id, last_timestamp, and all
                  open/close/min/max price/volume/bid/ask metrics

        Returns:
            int: id of the inserted row
        """
        pass

    @abstractmethod
    def is_healthy(self) -> bool:
        """Check if the database connection is alive."""
        pass

    @abstractmethod
    def close(self):
        """Close the database connection."""
        pass


def get_backend(backend: str, **kwargs) -> DBManager:
    """
    Factory function to create the appropriate database backend.

    Parameters:
        backend: One of "timescaledb", "sqlite", or "auto"
        **kwargs: Connection parameters passed to the backend constructor

    Returns:
        DBManager instance
    """
    if backend == "timescaledb":
        from .timescale import TimescaleBackend
        return TimescaleBackend(**kwargs)
    elif backend == "sqlite":
        from .sqlite import SQLiteBackend
        return SQLiteBackend(**kwargs)
    elif backend == "auto":
        # Try TimescaleDB first, fallback to SQLite
        try:
            from .timescale import TimescaleBackend
            tb = TimescaleBackend(**kwargs)
            if tb.is_healthy():
                return tb
        except Exception:
            pass
        from .sqlite import SQLiteBackend
        return SQLiteBackend(**kwargs)
    raise ValueError(f"Unknown backend: {backend}")
