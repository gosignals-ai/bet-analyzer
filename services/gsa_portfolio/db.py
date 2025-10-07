import os
import psycopg

# Minimal async "pool" interface compatible with code that does:
#   async with get_pool().connection() as conn: ...
class _ConnCtx:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn: psycopg.AsyncConnection | None = None
    async def __aenter__(self):
        self.conn = await psycopg.AsyncConnection.connect(self.dsn)
        return self.conn
    async def __aexit__(self, exc_type, exc, tb):
        if self.conn is not None:
            await self.conn.close()

class _Pool:
    def __init__(self, dsn: str):
        self.dsn = dsn
    def connection(self) -> _ConnCtx:
        return _ConnCtx(self.dsn)

def _dsn_from_env() -> str:
    dsn = (
        os.environ.get("INTERNAL_DB_URL")
        or os.environ.get("DATABASE_URL")
        or os.environ.get("DB_URL")
        or os.environ.get("DB_URL_EXT")
    )
    if not dsn:
        raise RuntimeError("Database URL not set in env (INTERNAL_DB_URL/DATABASE_URL/DB_URL/DB_URL_EXT)")
    # Ensure sslmode=require if absent
    if "sslmode=" not in dsn:
        joiner = "&" if "?" in dsn else "?"
        dsn = f"{dsn}{joiner}sslmode=require"
    return dsn

_pool: _Pool | None = None

def get_pool() -> _Pool:
    global _pool
    if _pool is None:
        _pool = _Pool(_dsn_from_env())
    return _pool