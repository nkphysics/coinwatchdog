def get_backend(backend: str, **kwargs):
    """
    Factory function to create the appropriate database backend.

    Parameters:
        backend: One of "timescaledb", "sqlite", or "auto"
        **kwargs: Connection parameters passed to the backend constructor

    Returns:
        TimescaleBackend or SQLiteBackend instance
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
