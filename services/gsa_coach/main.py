import os
from fastapi import FastAPI, HTTPException
import psycopg, httpx

APP_NAME = "gsa_coach"
DB_URL   = os.getenv("DATABASE_URL")
CORE_URL = os.getenv("CORE_URL")  # e.g., https://gsa-core.onrender.com

app = FastAPI(title="GoSignals Coach", version="0.1.0")

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

@app.get("/coach/summary")
def summary():
    try:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                      (SELECT COUNT(*) FROM games)   AS games,
                      (SELECT COUNT(*) FROM markets) AS markets,
                      (SELECT COUNT(*) FROM odds)    AS odds,
                      (SELECT COUNT(*) FROM picks)   AS picks
                """)
                g, m, o, p = cur.fetchone()
        note = "System initialized. Ingest data to see recommendations." if (o == 0 and p == 0) else "Data present."
        return {"games": int(g), "markets": int(m), "odds": int(o), "picks": int(p), "note": note}
    except Exception as e:
        raise HTTPException(500, f"summary error: {e}")

@app.get("/coach/ping-core")
def ping_core():
    if not CORE_URL:
        return {"core": "not_configured"}
    try:
        r = httpx.get(CORE_URL.rstrip('/') + "/core/metrics", timeout=15)
        r.raise_for_status()
        return {"core": "ok", "metrics": r.json()}
    except Exception as e:
        raise HTTPException(502, f"core call failed: {e}")
