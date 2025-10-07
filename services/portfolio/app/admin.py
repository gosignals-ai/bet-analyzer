import os
from typing import Optional
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from services.gsa_portfolio.db import get_pool

router = APIRouter(prefix="/admin", tags=["admin"])

def _auth(authorization: Optional[str] = Header(None)) -> bool:
    token = os.environ.get("SHARED_TASK_TOKEN") or os.environ.get("PORTFOLIO_ADMIN_TOKEN")
    if not token:
        raise HTTPException(status_code=500, detail="server_token_missing")
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="unauthorized")
    if authorization.split(" ", 1)[1] != token:
        raise HTTPException(status_code=403, detail="forbidden")
    return True

@router.get("/__ping")
async def __ping(ok: bool = Depends(_auth)):
    return {"ok": True, "svc": "portfolio-admin"}

@router.get("/__debug_token")
async def __debug_token():
    import hashlib
    token = os.environ.get("SHARED_TASK_TOKEN") or ""
    sha8 = hashlib.sha256(token.encode("utf-8")).hexdigest()[:8] if token else None
    return {"expected_sha8": sha8, "len_expected": len(token) if token else 0}

@router.post("/retention")
async def run_retention(
    dry_run: bool = Query(False),
    ok: bool = Depends(_auth),
):
    pool = get_pool()
    async with pool.connection() as conn, conn.cursor() as cur:
        if dry_run:
            await cur.execute("SELECT count(*)::bigint FROM portfolios WHERE created_at < now() - interval '395 days'")
            affected = (await cur.fetchone())[0]
            return {"purged": int(affected), "dry_run": True}

        await cur.execute(
            "SELECT retention.purge_portfolios_395d(p_batch := %s, p_hard_cap := %s, p_dry_run := %s)",
            (50000, 5000000, False),
        )
        row = await cur.fetchone()
        affected = row[0] if row else 0
        return {"purged": int(affected), "dry_run": False}

@router.post("/normalize")
async def normalize_from_raw(
    dry_run: bool = Query(True),
    limit: Optional[int] = Query(None, description="optional row cap for diagnostics"),
    ok: bool = Depends(_auth),
):
    """
    Normalize public.odds_raw → odds_norm.* with defensive casts.
    """
    if dry_run:
        return {"normalized": {"games": 0, "markets": 0, "odds_inserts": 0}, "dry_run": True}

    pool = get_pool()
    try:
        async with pool.connection() as conn, conn.cursor() as cur:
            # Ensure schema exists
            await cur.execute("CREATE SCHEMA IF NOT EXISTS odds_norm")

            # ---------- GAMES ----------
            games_sql = """
                INSERT INTO odds_norm.games (game_uid, sport_key, commence_time, home_team, away_team, status)
                SELECT DISTINCT r.game_id, r.sport_key, r.commence_time, r.home_team, r.away_team, NULL
                FROM public.odds_raw r
                WHERE r.game_id IS NOT NULL
            """
            games_params = []
            if limit and limit > 0:
                games_sql += " LIMIT %s"
                games_params.append(limit)
            games_sql += """
                ON CONFLICT (game_uid) DO UPDATE SET
                    sport_key = EXCLUDED.sport_key,
                    commence_time = EXCLUDED.commence_time,
                    home_team = EXCLUDED.home_team,
                    away_team = EXCLUDED.away_team,
                    updated_at = now()
                RETURNING 1
            """
            await cur.execute(games_sql, tuple(games_params))
            g = len(await cur.fetchall())

            # ---------- MARKETS ----------
            markets_sql = """
                WITH agg AS (
                  SELECT r.game_id AS game_uid,
                         r.market_key,
                         r.book_key,
                         MAX(r.last_update) AS last_update
                  FROM public.odds_raw r
                  WHERE r.game_id IS NOT NULL
                        AND r.market_key IS NOT NULL
                        AND r.book_key IS NOT NULL
                        AND r.last_update IS NOT NULL
                  GROUP BY 1,2,3
                )
                INSERT INTO odds_norm.markets (game_uid, market_key, book_key, market_ref, last_update)
                SELECT a.game_uid, a.market_key, a.book_key, NULL, a.last_update
                FROM agg a
                ON CONFLICT (game_uid, market_key, book_key) DO UPDATE
                  SET last_update = GREATEST(EXCLUDED.last_update, odds_norm.markets.last_update),
                      updated_at = now()
                RETURNING 1
            """
            markets_params = []
            if limit and limit > 0:
                # apply a LIMIT by wrapping the final SELECT
                markets_sql = markets_sql.replace("FROM agg a", "FROM agg a ORDER BY a.game_uid, a.market_key, a.book_key LIMIT %s")
                markets_params.append(limit)
            await cur.execute(markets_sql, tuple(markets_params))
            m = len(await cur.fetchall())

            # ---------- ODDS ----------
            odds_sql = """
                INSERT INTO odds_norm.odds
                    (game_uid, market_key, book_key, side, price, point, last_update, observed_at)
                SELECT
                    r.game_id,
                    r.market_key,
                    r.book_key,
                    r.side,
                    CASE
                      WHEN COALESCE(r.price::text,'') ~ '^-?\\d+$' THEN r.price::int
                      ELSE NULL
                    END AS price,
                    NULLIF(r.point::text,'')::numeric AS point,
                    r.last_update,
                    COALESCE(r.observed_at, now())
                FROM public.odds_raw r
                WHERE r.game_id IS NOT NULL
                  AND r.market_key IS NOT NULL
                  AND r.book_key IS NOT NULL
                  AND r.last_update IS NOT NULL
                  AND r.side IN ('home','away','draw')
                  AND COALESCE(r.price::text,'') ~ '^-?\\d+$'
            """
            odds_params = []
            if limit and limit > 0:
                odds_sql += " LIMIT %s"
                odds_params.append(limit)
            odds_sql += """
                ON CONFLICT (game_uid, market_key, book_key, side, last_update) DO NOTHING
                RETURNING 1
            """
            await cur.execute(odds_sql, tuple(odds_params))
            o = len(await cur.fetchall())

        return {"normalized": {"games": g, "markets": m, "odds_inserts": o}, "dry_run": False}

    except Exception as e:
        return {"error": "normalize_failed", "message": str(e)}