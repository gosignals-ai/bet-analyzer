import os
from typing import List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import psycopg

APP_NAME = "gsa_portfolio"
DB_URL = os.getenv("DATABASE_URL")

app = FastAPI(title="GoSignals Portfolio", version="0.1.0")

class PortfolioIn(BaseModel):
    name: str = Field(..., min_length=1, max_length=64)
    balance: float = 0.0
    currency: str = "USD"

@app.get("/")
def root():
    return {"service": APP_NAME, "status": "ready"}

@app.get("/health")
def health():
    try:
        with psycopg.connect(DB_URL, connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"service": APP_NAME, "db": "ok"}
    except Exception as e:
        raise HTTPException(500, f"DB error: {e}")

@app.post("/portfolio")
def create_portfolio(p: PortfolioIn):
    try:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO portfolios (name, balance, currency) VALUES (%s,%s,%s) RETURNING id",
                    (p.name, p.balance, p.currency),
                )
                pid = cur.fetchone()[0]
            conn.commit()
        return {"id": pid, "name": p.name, "balance": float(p.balance), "currency": p.currency}
    except Exception as e:
        raise HTTPException(500, f"create error: {e}")

@app.get("/portfolio")
def list_portfolios():
    try:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, name, balance, currency, created_at FROM portfolios ORDER BY id DESC")
                rows = cur.fetchall()
        return [
            {"id": r[0], "name": r[1], "balance": float(r[2]), "currency": r[3], "created_at": r[4].isoformat()}
            for r in rows
        ]
    except Exception as e:
        raise HTTPException(500, f"list error: {e}")
