import os, decimal
from typing import Optional, Dict, Any, List
from datetime import datetime

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

app = FastAPI(title="GSA Portfolio (psycopg async)", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

pool: Optional[AsyncConnectionPool] = None

CREATE_SQL = """
create table if not exists portfolios (
  id serial primary key,
  name text unique not null,
  balance numeric not null,
  currency text not null default 'USD',
  created_at timestamptz not null default now()
);
"""

@app.on_event("startup")
async def startup():
    global pool
    # autocommit simplifies inserts in pooled connections
    pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=5, kwargs={"autocommit": True})
    async with pool.connection() as aconn:
        async with aconn.cursor() as cur:
            await cur.execute(CREATE_SQL)

@app.on_event("shutdown")
async def shutdown():
    if pool:
        await pool.close()

class PortfolioIn(BaseModel):
    name: str
    balance: float
    currency: str = "USD"

def _row_to_dict(row: Dict[str, Any]) -> Dict[str, Any]:
    d = dict(row)
    if isinstance(d.get("created_at"), datetime):
        d["created_at"] = d["created_at"].isoformat()
    if isinstance(d.get("balance"), decimal.Decimal):
        d["balance"] = float(d["balance"])
    return d

@app.get("/health")
async def health():
    async with pool.connection() as aconn:
        async with aconn.cursor() as cur:
            await cur.execute("select 1")
            await cur.fetchone()
    return {"db": "ok"}

@app.post("/portfolio")
async def create_portfolio(p: PortfolioIn):
    sql = """
    insert into portfolios (name, balance, currency)
    values (%s, %s, %s)
    on conflict (name) do update
      set balance = excluded.balance,
          currency = excluded.currency
    returning id, name, balance, currency, created_at
    """
    async with pool.connection() as aconn:
        async with aconn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, (p.name, p.balance, p.currency))
            row = await cur.fetchone()
    return _row_to_dict(row)

@app.get("/portfolio")
async def list_portfolios(limit: int = Query(100, ge=1, le=500)):
    sql = "select id, name, balance, currency, created_at from portfolios order by created_at desc limit %s"
    async with pool.connection() as aconn:
        async with aconn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, (limit,))
            rows = await cur.fetchall()
    return [_row_to_dict(r) for r in rows]
