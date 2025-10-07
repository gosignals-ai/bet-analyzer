import os, asyncpg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from datetime import datetime

# (optional) local .env; Render uses env var
try:
    from dotenv import load_dotenv
    load_dotenv(".env.local", override=True)
except Exception:
    pass

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

app = FastAPI(title="GSA Portfolio", version="0.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

pool: Optional[asyncpg.pool.Pool] = None

CREATE_SQL = """
create table if not exists portfolios (
  id          serial primary key,
  name        text unique not null,
  balance     numeric not null,
  currency    text not null,
  created_at  timestamptz not null default now()
);
"""

@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_SQL)

@app.on_event("shutdown")
async def shutdown():
    global pool
    if pool:
        await pool.close()

@app.get("/health")
async def health():
    return {"db":"ok"}

class PortfolioIn(BaseModel):
    name: str
    balance: float
    currency: str = "USD"

def _row_to_dict(r: asyncpg.Record) -> Dict[str, Any]:
    d = dict(r)
    if isinstance(d.get("created_at"), datetime):
        d["created_at"] = d["created_at"].isoformat()
    if d.get("balance") is not None:
        try:
            d["balance"] = float(d["balance"])
        except Exception:
            pass
    return d

@app.post("/portfolio")
async def create_portfolio(p: PortfolioIn):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            insert into portfolios (name, balance, currency)
            values ($1,$2,$3)
            on conflict (name) do update
              set balance=excluded.balance, currency=excluded.currency
            returning id, name, balance, currency, created_at
            """,
            p.name, p.balance, p.currency
        )
        return _row_to_dict(row)

@app.get("/portfolio")
async def list_portfolios():
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "select id, name, balance, currency, created_at from portfolios order by created_at desc"
        )
        return [_row_to_dict(r) for r in rows]
