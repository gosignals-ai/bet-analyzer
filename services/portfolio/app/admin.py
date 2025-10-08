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
    limit: Optional[int] = Query(None, description="cap odds_raw rows considered"),
    ok: bool = Depends(_auth),
):
    """
    Normalize public.odds_raw.payload (JSONB) â†’ odds_norm.games/markets/odds.
    - De-duplicate per unique target key before INSERT to avoid ON CONFLICT double-update error.
    """
    if dry_run:
        return {"normalized": {"games": 0, "markets": 0, "odds_inserts": 0}, "dry_run": True}

    pool = get_pool()
    try:
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute("CREATE SCHEMA IF NOT EXISTS odds_norm")

            # Bind limit param if provided
            limit_clause = " LIMIT %s" if (limit and limit > 0) else ""
            params = (limit,) if limit_clause else tuple()

            # ---- GAMES (dedup latest by game_id) ----
            games_sql = f"""
                WITH src AS (
                  SELECT game_id, fetched_at, payload
                  FROM public.odds_raw
                  WHERE game_id IS NOT NULL
                  ORDER BY fetched_at DESC
                  {limit_clause}
                ),
                latest AS (
                  SELECT DISTINCT ON (game_id)
                         game_id, fetched_at, payload
                  FROM src
                  ORDER BY game_id, fetched_at DESC
                )
                INSERT INTO odds_norm.games (game_uid, sport_key, commence_time, home_team, away_team, status)
                SELECT
                    l.game_id                                          AS game_uid,
                    l.payload->>'sport_key'                            AS sport_key,
                    NULLIF(l.payload->>'commence_time','')::timestamptz AS commence_time,
                    l.payload->>'home_team'                            AS home_team,
                    l.payload->>'away_team'                            AS away_team,
                    NULL                                               AS status
                FROM latest l
                ON CONFLICT (game_uid) DO UPDATE SET
                    sport_key     = EXCLUDED.sport_key,
                    commence_time = EXCLUDED.commence_time,
                    home_team     = EXCLUDED.home_team,
                    away_team     = EXCLUDED.away_team,
                    updated_at    = now()
                RETURNING 1
            """
            await cur.execute(games_sql, params)
            g = len(await cur.fetchall())

            # ---- MARKETS (dedup by game_id, market_key, book_key) ----
            markets_sql = f"""
                WITH src AS (
                  SELECT game_id, fetched_at, payload
                  FROM public.odds_raw
                  WHERE game_id IS NOT NULL
                  ORDER BY fetched_at DESC
                  {limit_clause}
                ),
                bm AS (
                  SELECT
                    s.game_id,
                    s.fetched_at,
                    (b->>'key') AS book_key,
                    (m->>'key') AS market_key,
                    COALESCE(NULLIF(m->>'last_update','')::timestamptz, s.fetched_at) AS last_update
                  FROM src s
                  CROSS JOIN LATERAL jsonb_array_elements(s.payload->'bookmakers') AS b
                  CROSS JOIN LATERAL jsonb_array_elements(b->'markets')           AS m
                  WHERE b ? 'key' AND m ? 'key'
                ),
                bm_dedup AS (
                  SELECT game_id, market_key, book_key, MAX(last_update) AS last_update
                  FROM bm
                  GROUP BY game_id, market_key, book_key
                )
                INSERT INTO odds_norm.markets (game_uid, market_key, book_key, market_ref, last_update)
                SELECT game_id, market_key, book_key, NULL, last_update
                FROM bm_dedup
                ON CONFLICT (game_uid, market_key, book_key) DO UPDATE
                  SET last_update = GREATEST(odds_norm.markets.last_update, EXCLUDED.last_update),
                      updated_at = now()
                RETURNING 1
            """
            await cur.execute(markets_sql, params)
            m = len(await cur.fetchall())

            # ---- ODDS (filter + dedup by full unique key) ----
            odds_sql = f"""
                WITH src AS (
                  SELECT game_id, fetched_at, payload
                  FROM public.odds_raw
                  WHERE game_id IS NOT NULL
                  ORDER BY fetched_at DESC
                  {limit_clause}
                ),
                bm AS (
                  SELECT
                    s.game_id,
                    s.fetched_at,
                    (b->>'key') AS book_key,
                    (m->>'key') AS market_key,
                    COALESCE(NULLIF(m->>'last_update','')::timestamptz, s.fetched_at) AS market_last_update,
                    (m->'outcomes') AS outcomes
                  FROM src s
                  CROSS JOIN LATERAL jsonb_array_elements(s.payload->'bookmakers') AS b
                  CROSS JOIN LATERAL jsonb_array_elements(b->'markets')           AS m
                  WHERE b ? 'key' AND m ? 'key'
                ),
                rows_base AS (
                  SELECT
                    game_id                                   AS game_uid,
                    market_key,
                    book_key,
                    LOWER(COALESCE(o->>'name',''))            AS side_raw,
                    CASE WHEN (o->>'price') ~ '^-?\\d+$' THEN (o->>'price')::int ELSE NULL END AS price,
                    NULLIF(o->>'point','')::numeric           AS point,
                    COALESCE(NULLIF(o->>'last_update','')::timestamptz, market_last_update) AS last_update,
                    fetched_at                                AS observed_at
                  FROM bm
                  CROSS JOIN LATERAL jsonb_array_elements(outcomes) AS o
                ),
                rows_flt AS (
                  SELECT *
                  FROM rows_base
                  WHERE market_key IS NOT NULL
                    AND book_key IS NOT NULL
                    AND last_update IS NOT NULL
                    AND price IS NOT NULL
                    AND side_raw IN ('home','away','draw','over','under')
                ),
                rows_dedup AS (
                  SELECT DISTINCT ON (game_uid, market_key, book_key, side_raw, last_update)
                         game_uid, market_key, book_key, side_raw, price, point, last_update, observed_at
                  FROM rows_flt
                  ORDER BY game_uid, market_key, book_key, side_raw, last_update DESC, observed_at DESC
                )
                INSERT INTO odds_norm.odds
                  (game_uid, market_key, book_key, side, price, point, last_update, observed_at)
                SELECT
                  game_uid,
                  market_key,
                  book_key,
                  CASE side_raw
                    WHEN 'home' THEN 'home'
                    WHEN 'away' THEN 'away'
                    WHEN 'draw' THEN 'draw'
                    WHEN 'over' THEN 'over'
                    WHEN 'under' THEN 'under'
                    ELSE side_raw
                  END AS side,
                  price,
                  point,
                  last_update,
                  observed_at
                FROM rows_dedup
                ON CONFLICT (game_uid, market_key, book_key, side, last_update) DO NOTHING
                RETURNING 1
            """
            await cur.execute(odds_sql, params)
            o = len(await cur.fetchall())

        return {"normalized": {"games": g, "markets": m, "odds_inserts": o}, "dry_run": False}

    except Exception as e:
        return {"error": "normalize_failed", "message": str(e)}

# --- diagnostic: normalized table counts ---
@router.get("/admin/norm_counts")
async def norm_counts(ok: bool = Depends(_auth)):
    pool = get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT count(*) FROM odds_norm.games")
            g = (await cur.fetchone())[0] or 0
            await cur.execute("SELECT count(*) FROM odds_norm.markets")
            m = (await cur.fetchone())[0] or 0
            await cur.execute("SELECT count(*) FROM odds_norm.odds")
            o = (await cur.fetchone())[0] or 0
    return {"games": int(g), "markets": int(m), "odds": int(o)}