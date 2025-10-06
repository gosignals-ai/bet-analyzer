import os
from fastapi import FastAPI, HTTPException
import psycopg

APP_NAME = "gsa_core"
DB_URL = os.getenv("DATABASE_URL")

app = FastAPI(title="GoSignals Core", version="0.1.0")

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

@app.get("/core/metrics")
def metrics():
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
        return {"games": int(g), "markets": int(m), "odds": int(o), "picks": int(p)}
    except Exception as e:
        raise HTTPException(500, f"metrics error: {e}")

@app.get("/core/sample-picks")
def sample_picks(limit: int = 5):
    try:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                  SELECT g.league, g.home_team, g.away_team,
                         m.market_key, o.outcome, o.price, o.point, o.fetched_at
                  FROM v_latest_odds o
                  JOIN markets m ON m.id = o.market_id
                  JOIN games   g ON g.id = m.game_id
                  ORDER BY o.fetched_at DESC NULLS LAST
                  LIMIT %s
                """, (limit,))
                rows = cur.fetchall()
        return [
            {
                "league": r[0], "home": r[1], "away": r[2],
                "market": r[3], "outcome": r[4],
                "price": float(r[5]),
                "point": float(r[6]) if r[6] is not None else None,
                "fetched_at": r[7].isoformat() if r[7] else None
            } for r in rows
        ]
    except Exception as e:
        raise HTTPException(500, f"query error: {e}")
