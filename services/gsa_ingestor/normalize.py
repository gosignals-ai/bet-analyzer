import os
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from services.gsa_portfolio.db import get_pool  # shared async psycopg pool shim

router = APIRouter(prefix="/ingest", tags=["ingest"])

def _maybe_auth(authorization: str | None = Header(None)):
    # Optional: if SHARED_TASK_TOKEN is set on the service, enforce it
    token = os.environ.get("SHARED_TASK_TOKEN")
    if not token:
        return True
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="unauthorized")
    if authorization.split(" ", 1)[1] != token:
        raise HTTPException(status_code=403, detail="forbidden")
    return True

@router.post("/normalize")
async def normalize_from_raw(dry_run: int = Query(1, ge=0, le=1), ok: bool = Depends(_maybe_auth)):
    """
    Normalize rows from public.odds_raw into odds_norm.games/markets/odds.
    - dry_run=1: no changes; returns 200 with zeros
    - dry_run=0: performs set-based upserts/inserts and returns affected counts
    """
    if dry_run == 1:
        return {"normalized": {"games": 0, "markets": 0, "odds_inserts": 0}, "dry_run": True}

    pool = get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            # Ensure odds_raw exists
            await cur.execute("select to_regclass('public.odds_raw')")
            if (await cur.fetchone())[0] is None:
                return {"error": "odds_raw_not_found", "normalized": {"games":0,"markets":0,"odds_inserts":0}}

            # Discover columns minimally (expecting standard names)
            await cur.execute("""
                select column_name
                from information_schema.columns
                where table_schema = 'public' and table_name = 'odds_raw'
            """)
            cols = {r[0] for r in await cur.fetchall()}
            required = {"game_id","sport_key","commence_time","home_team","away_team",
                        "book_key","market_key","last_update"}
            missing = required - cols
            if missing:
                return {"error":"missing_columns","missing":sorted(missing)}

            # 1) games upsert (count via RETURNING)
            await cur.execute("""
                insert into odds_norm.games(game_uid, sport_key, commence_time, home_team, away_team, status)
                select distinct r.game_id, r.sport_key, r.commence_time, r.home_team, r.away_team, null
                from public.odds_raw r
                where r.game_id is not null
                on conflict (game_uid) do update set
                    sport_key = excluded.sport_key,
                    commence_time = excluded.commence_time,
                    home_team = excluded.home_team,
                    away_team = excluded.away_team,
                    updated_at = now()
                returning 1
            """)
            g = len(await cur.fetchall())

            # 2) markets upsert (latest last_update per trio)
            await cur.execute("""
                with agg as (
                  select r.game_id as game_uid,
                         r.market_key as market_key,
                         r.book_key as book_key,
                         max(r.last_update) as last_update
                  from public.odds_raw r
                  where r.game_id is not null and r.market_key is not null and r.book_key is not null and r.last_update is not null
                  group by 1,2,3
                )
                insert into odds_norm.markets(game_uid, market_key, book_key, market_ref, last_update)
                select a.game_uid, a.market_key, a.book_key, null, a.last_update
                from agg a
                on conflict (game_uid, market_key, book_key) do update
                  set last_update = greatest(excluded.last_update, odds_norm.markets.last_update),
                      updated_at = now()
                returning 1
            """)
            m = len(await cur.fetchall())

            # 3) odds insert (time-series dedup)
            # require side to be one of home/away/draw
            await cur.execute("""
                insert into odds_norm.odds(game_uid, market_key, book_key, side, price, point, last_update, observed_at)
                select r.game_id, r.market_key, r.book_key,
                       r.side, r.price, r.point, r.last_update,
                       coalesce(r.observed_at, now())
                from public.odds_raw r
                where r.game_id is not null and r.market_key is not null and r.book_key is not null and r.last_update is not null
                      and r.side in ('home','away','draw')
                on conflict (game_uid, market_key, book_key, side, last_update) do nothing
                returning 1
            """)
            o = len(await cur.fetchall())

    return {"normalized": {"games": g, "markets": m, "odds_inserts": o}, "dry_run": False}