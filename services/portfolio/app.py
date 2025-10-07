# services/portfolio/app.py
# FastAPI microservice for simple portfolios
# - Storage: Postgres (psycopg v3)
# - Endpoints:
#     GET  /health
#     GET  /portfolio
#     POST /portfolio   (schema-agnostic "upsert": UPDATE latest-by-name, else INSERT)
#
# Notes:
# - Does NOT require a UNIQUE constraint on name.
# - Creates the table on startup if it doesn't exist.
# - Uses psycopg_pool.AsyncConnectionPool (no asyncpg).

import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

app = FastAPI(title="GSA Portfolio (bootstrap)", version="0.1.2")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

pool: Optional[AsyncConnectionPool] = None

CREATE_SQL = """
create table if not exists portfolios (
  id         serial primary key,
  name       text        not null,
  balance    numeric     not null,
  currency   text        not null,
  created_at timestamptz not null default now()
);
"""


# -------------------------------------------------------------------
# Models
# -------------------------------------------------------------------
class PortfolioIn(BaseModel):
    name: str
    balance: float
    currency: str = "USD"


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def _normalize(row: Dict[str, Any]) -> Dict[str, Any]:
    """Make DB row JSON-safe and consistent."""
    d = dict(row)
    # Ensure predictable types
    if isinstance(d.get("created_at"), datetime):
        d["created_at"] = d["created_at"].isoformat()
    # balance may be Decimal -> float
    try:
        d["balance"] = float(d["balance"])
    except Exception:
        pass
    return d


# -------------------------------------------------------------------
# Lifecycle
# -------------------------------------------------------------------
@app.on_event("startup")
async def startup() -> None:
    global pool
    # Create and open the async pool (avoids deprecation warning)
    pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=5)
    await pool.open()

    # Ensure schema exists
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(CREATE_SQL)


@app.on_event("shutdown")
async def shutdown() -> None:
    if pool:
        await pool.close()


# -------------------------------------------------------------------
# Endpoints
# -------------------------------------------------------------------
@app.get("/health")
async def health() -> Dict[str, str]:
    return {"db": "ok"}


@app.get("/portfolio")
async def list_portfolios(limit: int = Query(100, ge=1, le=500)) -> List[Dict[str, Any]]:
    """Return portfolios ordered by most-recent first."""
    sql = """
      select id, name, balance, currency, created_at
        from portfolios
    order by created_at desc
       limit %s
    """
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, (limit,))
            rows = await cur.fetchall()
    return [_normalize(r) for r in rows]


@app.post("/portfolio")
async def create_or_update_portfolio(p: PortfolioIn) -> Dict[str, Any]:
    """
    Schema-agnostic 'upsert':
      1) UPDATE the most recent row with this name (if any), RETURNING row
      2) If none updated, INSERT a new row, RETURNING row

    This avoids needing a UNIQUE(name) constraint and works with existing data.
    """
    update_sql = """
      update portfolios
         set balance = %s,
             currency = %s
       where id = (
           select id
             from portfolios
            where name = %s
         order by created_at desc
            limit 1
       )
     returning id, name, balance, currency, created_at
    """
    insert_sql = """
      insert into portfolios (name, balance, currency)
      values (%s, %s, %s)
      returning id, name, balance, currency, created_at
    """

    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Try UPDATE latest-by-name
            await cur.execute(update_sql, (p.balance, p.currency, p.name))
            row = await cur.fetchone()
            if row:
                return _normalize(row)

            # Else INSERT
            await cur.execute(insert_sql, (p.name, p.balance, p.currency))
            row = await cur.fetchone()
            return _normalize(row)
