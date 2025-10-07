import os
from fastapi import FastAPI, HTTPException
import httpx
import psycopg
from psycopg.types.json import Json

APP_NAME = "gsa_ingestor"
ODDS_API_KEY = os.getenv("ODDS_API_KEY")
DB_URL = os.getenv("DATABASE_URL")

app = FastAPI(title="GoSignals Ingestor", version="0.1.0")

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

@app.get("/ingest/sports")
def ingest_sports(dry_run: int = 1):
    if not ODDS_API_KEY:
        raise HTTPException(500, "ODDS_API_KEY not set")
    url = f"https://api.the-odds-api.com/v4/sports?all=true&apiKey={ODDS_API_KEY}"
    try:
        r = httpx.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        raise HTTPException(502, f"Odds API error: {e}")

    # Optional DB write (audit)
    try:
        if dry_run == 0:
            with psycopg.connect(DB_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO audit_logs (module, event, detail) VALUES (%s, %s, %s)",
                        ("ingestor", "sports_fetch", Json({"count": len(data)})),
                    )
                conn.commit()
        return {"fetched": len(data), "dry_run": bool(dry_run)}
    except Exception as e:
        raise HTTPException(500, f"DB write error: {e}")
# --- auto: attach normalize router ---
try:
    from services.gsa_ingestor.normalize import router as normalize_router
    app.include_router(normalize_router)
    print("[ingestor] normalize router attached")
except Exception as e:
    import sys
    print(f"[ingestor] normalize attach failed: {e}", file=sys.stderr)
# --- end auto ---
