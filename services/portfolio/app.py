# services/portfolio/app.py
import os
from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

app = FastAPI(title="GSA Portfolio (psycopg)", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

CREATE_SQL = """
create table if not exists portfolios (
  id         serial primary key,
  name       text unique not null,
  balance    numeric not null,
  currency   text not null,
  created_at timestamptz not null default now()
);
"""

pool: Optional[AsyncConnectionPool] = None

@app.on_event("startup")
async def startup():
    global pool
    # create closed; open explicitly (avoids deprecation warning)
    pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=5, open=False)
    await pool.open()
    # ensure table exists
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(CREATE_SQL)

@app.on_event("shutdown")
async def shutdown():
    if pool:
        await pool.close()

@app.get("/health")
async def health():
    return {"db": "ok"}

class PortfolioIn(BaseModel):
    name: str
    balance: float
    currency: str = "USD"

def _normalize(d: Dict[str, Any]) -> Dict[str, Any]:
    if d is None:
        return d
    if "created_at" in d and isinstance(d["created_at"], datetime):
        d["created_at"] = d["created_at"].isoformat()
    # balance comes back as Decimal -> make it JSON-friendly
    if "balance" in d:
        try:
            d["balance"] = float(d["balance"])
        except Exception:
            pass
    return d

@app.post("/portfolio")
async def create_portfolio(p: PortfolioIn):
    async with pool.connection() as conn:
        conn.row_factory = dict_row
        async with conn.cursor() as cur:
            await cur.execute(
                """
                insert into portfolios (name, balance, currency)
                values (%s, %s, %s)
                on conflict (name) do update
                  set balance = excluded.balance,
                      currency = excluded.currency
                returning id, name, balance, currency, created_at
                """,
                (p.name, p.balance, p.currency),
            )
            row = await cur.fetchone()
    return _normalize(row)

@app.get("/portfolio")
async def list_portfolios() -> List[Dict[str, Any]]:
    async with pool.connection() as conn:
        conn.row_factory = dict_row
        async with conn.cursor() as cur:
            await cur.execute(
                """
                select id, name, balance, currency, created_at
                from portfolios
                order by created_at desc
                """
            )
            rows = await cur.fetchall()
    return [_normalize(r) for r in rows]
