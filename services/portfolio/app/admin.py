import os, hashlib
from fastapi import APIRouter, Depends, Header, HTTPException, Query

# pool import (support both layouts)
try:
    from .db import get_pool
except Exception:
    from services.gsa_portfolio.db import get_pool  # shim

router = APIRouter(prefix="/admin", tags=["admin"])

def _auth(
    authorization: str | None = Header(None),
    x_gsa: str | None = Header(None, alias="x-gsa-token"),
):
    """
    Accept either:
      - Authorization: Bearer <token>
      - x-gsa-token: <token>
    Trim quotes/whitespace to avoid silent mismatches.
    """
    expected = (os.environ.get("SHARED_TASK_TOKEN") or "").strip().strip('"').strip("'")
    if not expected:
        return True  # no token configured â†’ open

    presented = None
    if authorization:
        low = authorization.lower()
        presented = authorization[7:] if low.startswith("bearer ") else authorization
    if not presented and x_gsa:
        presented = x_gsa

    if not presented:
        raise HTTPException(status_code=401, detail="unauthorized")

    presented = presented.strip().strip('"').strip("'")
    if presented != expected:
        raise HTTPException(status_code=403, detail="forbidden")
    return True

@router.get("/__ping")
async def ping(ok: bool = Depends(_auth)):
    return {"ok": True, "svc": "portfolio-admin"}

# PUBLIC debug (no auth): returns only hash+length of server token
@router.get("/__debug_token")
async def debug_token():
    t = (os.environ.get("SHARED_TASK_TOKEN") or "").strip().strip('"').strip("'")
    h = hashlib.sha256(t.encode()).hexdigest()[:8] if t else "MISSING"
    return {"expected_sha8": h, "len_expected": len(t)}

@router.post("/retention")
async def run_retention(dry_run: bool = Query(False), ok: bool = Depends(_auth)):
    pool = get_pool()
    async with pool.connection() as conn, conn.cursor() as cur:
        if dry_run:
            await cur.execute("SELECT * FROM retention.purge_portfolios_395d(p_dry_run := TRUE)")
        else:
            await cur.execute(
                "SELECT * FROM retention.purge_portfolios_395d("
                "p_batch := 50000, p_hard_cap := 5000000, p_dry_run := FALSE)"
            )
        row = await cur.fetchone()
    affected = int(row[0]) if row else 0
    return {"purged": affected, "dry_run": bool(dry_run)}

@router\.post\("/normalize"\)\nasync\ def\ normalize_from_raw\(\n\ \ \ \ dry_run:\ bool\ =\ Query\(True\),\n\ \ \ \ limit:\ int\ \|\ None\ =\ Query\(None,\ description="optional\ row\ cap\ for\ diagnostics"\),\n\ \ \ \ ok:\ bool\ =\ Depends\(_auth\),\n\):\n\ \ \ \ """\n\ \ \ \ Normalize\ public\.odds_raw\ →\ odds_norm\.\*\.\n\ \ \ \ When\ an\ error\ occurs,\ return\ structured\ details\ so\ we\ can\ fix\ quickly\.\n\ \ \ \ """\n\ \ \ \ if\ dry_run:\n\ \ \ \ \ \ \ \ return\ \{"normalized":\ \{"games":\ 0,\ "markets":\ 0,\ "odds_inserts":\ 0},\ "dry_run":\ True}\n\n\ \ \ \ pool\ =\ get_pool\(\)\n\ \ \ \ try:\n\ \ \ \ \ \ \ \ async\ with\ pool\.connection\(\)\ as\ conn,\ conn\.cursor\(\)\ as\ cur:\n\ \ \ \ \ \ \ \ \ \ \ \ \#\ basic\ presence\ check\n\ \ \ \ \ \ \ \ \ \ \ \ await\ cur\.execute\("select\ to_regclass\('public\.odds_raw'\)"\)\n\ \ \ \ \ \ \ \ \ \ \ \ if\ \(await\ cur\.fetchone\(\)\)\[0]\ is\ None:\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ return\ \{"error":"odds_raw_not_found","normalized":\{"games":0,"markets":0,"odds_inserts":0}}\n\n\ \ \ \ \ \ \ \ \ \ \ \ \#\ Optional\ limit\ for\ diagnostics\n\ \ \ \ \ \ \ \ \ \ \ \ lim_sql\ =\ "LIMIT\ %s"\ if\ limit\ and\ limit\ >\ 0\ else\ ""\n\ \ \ \ \ \ \ \ \ \ \ \ lim_arg\ =\ \(limit,\)\ if\ limit\ and\ limit\ >\ 0\ else\ tuple\(\)\n\n\ \ \ \ \ \ \ \ \ \ \ \ \#\ GAMES\n\ \ \ \ \ \ \ \ \ \ \ \ await\ cur\.execute\(f"""\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ INSERT\ INTO\ odds_norm\.games\ \(game_uid,\ sport_key,\ commence_time,\ home_team,\ away_team,\ status\)\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ SELECT\ DISTINCT\ r\.game_id,\ r\.sport_key,\ r\.commence_time,\ r\.home_team,\ r\.away_team,\ NULL\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ FROM\ public\.odds_raw\ r\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ WHERE\ r\.game_id\ IS\ NOT\ NULL\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \{lim_sql}\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ ON\ CONFLICT\ \(game_uid\)\ DO\ UPDATE\ SET\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ sport_key\ =\ EXCLUDED\.sport_key,\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ commence_time\ =\ EXCLUDED\.commence_time,\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ home_team\ =\ EXCLUDED\.home_team,\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ away_team\ =\ EXCLUDED\.away_team,\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ updated_at\ =\ now\(\)\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ RETURNING\ 1\n\ \ \ \ \ \ \ \ \ \ \ \ """,\ lim_arg\)\n\ \ \ \ \ \ \ \ \ \ \ \ g\ =\ len\(await\ cur\.fetchall\(\)\)\n\n\ \ \ \ \ \ \ \ \ \ \ \ \#\ MARKETS\n\ \ \ \ \ \ \ \ \ \ \ \ await\ cur\.execute\(f"""\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ WITH\ agg\ AS\ \(\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ SELECT\ r\.game_id\ AS\ game_uid,\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ r\.market_key\ AS\ market_key,\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ r\.book_key\ AS\ book_key,\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ MAX\(r\.last_update\)\ AS\ last_update\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ FROM\ public\.odds_raw\ r\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ WHERE\ r\.game_id\ IS\ NOT\ NULL\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ AND\ r\.market_key\ IS\ NOT\ NULL\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ AND\ r\.book_key\ IS\ NOT\ NULL\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ AND\ r\.last_update\ IS\ NOT\ NULL\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ GROUP\ BY\ 1,2,3\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \{lim_sql}\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \)\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ INSERT\ INTO\ odds_norm\.markets\ \(game_uid,\ market_key,\ book_key,\ market_ref,\ last_update\)\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ SELECT\ a\.game_uid,\ a\.market_key,\ a\.book_key,\ NULL,\ a\.last_update\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ FROM\ agg\ a\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ ON\ CONFLICT\ \(game_uid,\ market_key,\ book_key\)\ DO\ UPDATE\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ SET\ last_update\ =\ GREATEST\(EXCLUDED\.last_update,\ odds_norm\.markets\.last_update\),\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ updated_at\ =\ now\(\)\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ RETURNING\ 1\n\ \ \ \ \ \ \ \ \ \ \ \ """,\ lim_arg\)\n\ \ \ \ \ \ \ \ \ \ \ \ m\ =\ len\(await\ cur\.fetchall\(\)\)\n\n\ \ \ \ \ \ \ \ \ \ \ \ \#\ ODDS\ \(defensive\ casts/filters\ to\ avoid\ bad\ rows\)\n\ \ \ \ \ \ \ \ \ \ \ \ await\ cur\.execute\(f"""\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ INSERT\ INTO\ odds_norm\.odds\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \(game_uid,\ market_key,\ book_key,\ side,\ price,\ point,\ last_update,\ observed_at\)\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ SELECT\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ r\.game_id,\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ r\.market_key,\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ r\.book_key,\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ r\.side,\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ CASE\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ WHEN\ \(CASE\ WHEN\ r\.price\ IS\ NULL\ THEN\ ''\ ELSE\ r\.price::text\ END\)\ ~\ '\^-\?\\\\d\+\

        # games
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

        # markets
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

        # odds
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

    return {"normalized": {"games": g, "markets": m, "odds_inserts": o}, "dry_run": False}\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ THEN\ r\.price::int\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ ELSE\ NULL\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ END\ AS\ price,\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ NULLIF\(r\.point::text,\ ''\)::numeric\ NULLS\ FIRST\ AS\ point,\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ r\.last_update,\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ COALESCE\(r\.observed_at,\ now\(\)\)\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ FROM\ public\.odds_raw\ r\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ WHERE\ r\.game_id\ IS\ NOT\ NULL\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ AND\ r\.market_key\ IS\ NOT\ NULL\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ AND\ r\.book_key\ IS\ NOT\ NULL\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ AND\ r\.last_update\ IS\ NOT\ NULL\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ AND\ r\.side\ IN\ \('home','away','draw'\)\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ AND\ \(CASE\ WHEN\ r\.price\ IS\ NULL\ THEN\ ''\ ELSE\ r\.price::text\ END\)\ ~\ '\^-\?\\\\d\+\

        # games
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

        # markets
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

        # odds
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

    return {"normalized": {"games": g, "markets": m, "odds_inserts": o}, "dry_run": False}\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \{lim_sql}\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ ON\ CONFLICT\ \(game_uid,\ market_key,\ book_key,\ side,\ last_update\)\ DO\ NOTHING\n\ \ \ \ \ \ \ \ \ \ \ \ \ \ \ \ RETURNING\ 1\n\ \ \ \ \ \ \ \ \ \ \ \ """,\ lim_arg\)\n\ \ \ \ \ \ \ \ \ \ \ \ o\ =\ len\(await\ cur\.fetchall\(\)\)\n\n\ \ \ \ \ \ \ \ return\ \{"normalized":\ \{"games":\ g,\ "markets":\ m,\ "odds_inserts":\ o},\ "dry_run":\ False}\n\n\ \ \ \ except\ Exception\ as\ e:\n\ \ \ \ \ \ \ \ \#\ surface\ structured\ details\ so\ we\ can\ fix\ quickly\n\ \ \ \ \ \ \ \ import\ traceback,\ psycopg\n\ \ \ \ \ \ \ \ etype\ =\ type\(e\)\.__name__\n\ \ \ \ \ \ \ \ msg\ =\ str\(e\)\n\ \ \ \ \ \ \ \ tb\ \ =\ traceback\.format_exc\(limit=2\)\n\ \ \ \ \ \ \ \ return\ \{"error":"normalize_failed","etype":etype,"message":msg,"trace":tb}

        # games
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

        # markets
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

        # odds
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
@router.post("/normalize")
async def normalize_from_raw(
    dry_run: bool = Query(True),
    limit: int | None = Query(None, description="optional row cap for diagnostics"),
    ok: bool = Depends(_auth),
):
    """
    Normalize public.odds_raw → odds_norm.*.
    When an error occurs, return structured details so we can fix quickly.
    """
    if dry_run:
        return {"normalized": {"games": 0, "markets": 0, "odds_inserts": 0}, "dry_run": True}

    pool = get_pool()
    try:
        async with pool.connection() as conn, conn.cursor() as cur:
            # basic presence check
            await cur.execute("select to_regclass('public.odds_raw')")
            if (await cur.fetchone())[0] is None:
                return {"error":"odds_raw_not_found","normalized":{"games":0,"markets":0,"odds_inserts":0}}

            # Optional limit for diagnostics
            lim_sql = "LIMIT %s" if limit and limit > 0 else ""
            lim_arg = (limit,) if limit and limit > 0 else tuple()

            # GAMES
            await cur.execute(f"""
                INSERT INTO odds_norm.games (game_uid, sport_key, commence_time, home_team, away_team, status)
                SELECT DISTINCT r.game_id, r.sport_key, r.commence_time, r.home_team, r.away_team, NULL
                FROM public.odds_raw r
                WHERE r.game_id IS NOT NULL
                {lim_sql}
                ON CONFLICT (game_uid) DO UPDATE SET
                    sport_key = EXCLUDED.sport_key,
                    commence_time = EXCLUDED.commence_time,
                    home_team = EXCLUDED.home_team,
                    away_team = EXCLUDED.away_team,
                    updated_at = now()
                RETURNING 1
            """, lim_arg)
            g = len(await cur.fetchall())

            # MARKETS
            await cur.execute(f"""
                WITH agg AS (
                  SELECT r.game_id AS game_uid,
                         r.market_key AS market_key,
                         r.book_key AS book_key,
                         MAX(r.last_update) AS last_update
                  FROM public.odds_raw r
                  WHERE r.game_id IS NOT NULL
                        AND r.market_key IS NOT NULL
                        AND r.book_key IS NOT NULL
                        AND r.last_update IS NOT NULL
                  GROUP BY 1,2,3
                  {lim_sql}
                )
                INSERT INTO odds_norm.markets (game_uid, market_key, book_key, market_ref, last_update)
                SELECT a.game_uid, a.market_key, a.book_key, NULL, a.last_update
                FROM agg a
                ON CONFLICT (game_uid, market_key, book_key) DO UPDATE
                  SET last_update = GREATEST(EXCLUDED.last_update, odds_norm.markets.last_update),
                      updated_at = now()
                RETURNING 1
            """, lim_arg)
            m = len(await cur.fetchall())

            # ODDS (defensive casts/filters to avoid bad rows)
            await cur.execute(f"""
                INSERT INTO odds_norm.odds
                    (game_uid, market_key, book_key, side, price, point, last_update, observed_at)
                SELECT
                    r.game_id,
                    r.market_key,
                    r.book_key,
                    r.side,
                    CASE
                      WHEN (CASE WHEN r.price IS NULL THEN '' ELSE r.price::text END) ~ '^-?\\d+$'
                        THEN r.price::int
                      ELSE NULL
                    END AS price,
                    NULLIF(r.point::text, '')::numeric NULLS FIRST AS point,
                    r.last_update,
                    COALESCE(r.observed_at, now())
                FROM public.odds_raw r
                WHERE r.game_id IS NOT NULL
                  AND r.market_key IS NOT NULL
                  AND r.book_key IS NOT NULL
                  AND r.last_update IS NOT NULL
                  AND r.side IN ('home','away','draw')
                  AND (CASE WHEN r.price IS NULL THEN '' ELSE r.price::text END) ~ '^-?\\d+$'
                {lim_sql}
                ON CONFLICT (game_uid, market_key, book_key, side, last_update) DO NOTHING
                RETURNING 1
            """, lim_arg)
            o = len(await cur.fetchall())

        return {"normalized": {"games": g, "markets": m, "odds_inserts": o}, "dry_run": False}

    except Exception as e:
        # surface structured details so we can fix quickly
        import traceback, psycopg
        etype = type(e).__name__
        msg = str(e)
        tb  = traceback.format_exc(limit=2)
        return {"error":"normalize_failed","etype":etype,"message":msg,"trace":tb}