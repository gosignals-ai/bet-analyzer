import os, json, asyncpg
from typing import Optional, List
from datetime import datetime

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv(".env.local", override=True)
DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI(title="GSA Core", version="0.1.0")

# CORS (open for now)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"]
)

_pool: Optional[asyncpg.pool.Pool] = None

@app.on_event("startup")
async def startup():
    global _pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

@app.on_event("shutdown")
async def shutdown():
    global _pool
    if _pool:
        await _pool.close()

class MoneylineRow(BaseModel):
    sport_key: str
    game_id: str
    away_team: Optional[str] = None
    home_team: Optional[str] = None
    commence_time_utc: Optional[datetime] = None
    away_best_price: Optional[int] = None
    away_book: Optional[str] = None
    home_best_price: Optional[int] = None
    home_book: Optional[str] = None

@app.get("/health")
async def health():
    return {"db": "ok"}

@app.get("/core/metrics")
async def metrics():
    sql = "select count(*) as n from v_moneyline_latest"
    async with _pool.acquire() as conn:
        n = await conn.fetchval(sql)
    return {"moneyline_rows": n}

@app.get("/core/latest-lines", response_model=List[MoneylineRow])
async def latest_lines(
    sport: Optional[str] = Query(None, description="e.g., americanfootball_nfl"),
    limit: int = Query(50, ge=1, le=500)
):
    sql = """
      SELECT sport_key, game_id, away_team, home_team, commence_time_utc,
             away_best_price, away_book, home_best_price, home_book
      FROM v_moneyline_game_best
      WHERE ($1::text IS NULL OR sport_key = $1)
      ORDER BY commence_time_utc NULLS LAST, game_id
      LIMIT $2
    """
    async with _pool.acquire() as conn:
        rows = await conn.fetch(sql, sport, limit)
    # FastAPI + Pydantic will serialize datetime automatically
    return [dict(r) for r in rows]
