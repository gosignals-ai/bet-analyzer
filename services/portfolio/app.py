# services/portfolio/app.py
import os, hashlib
from fastapi import FastAPI, APIRouter, Depends, Header, HTTPException, Query
from services.gsa_portfolio.db import get_pool

app = FastAPI(title="GoSignals Portfolio")
admin = APIRouter(prefix="/admin", tags=["admin"])

# ---------- auth ----------
def _auth(authorization: str | None = Header(None)):
    token = os.environ.get("SHARED_TASK_TOKEN")
    if not token:
        raise HTTPException(status_code=401, detail="unauthorized")
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="unauthorized")
    if authorization.split(" ", 1)[1] != token:
        raise HTTPException(status_code=403, detail="forbidden")
    return True

# ---------- health ----------
@app.get("/", summary="Root")
def root():
    return {"service": "gsa_portfolio"}

@app.get("/health", summary="Health")
async def health():
    pool = get_pool()
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute("select 1")
        _ = await cur.fetchone()
    return {"service": "gsa_portfolio", "db": "ok"}

# ---------- admin basics ----------
@admin.get("/__ping", summary=" Ping")
async def __ping(ok: bool = Depends(_auth)):
    return {"ok": True, "svc": "portfolio-admin"}

@admin.get("/__debug_token", summary=" Debug Token")
async def __debug_token():
    tok = os.environ.get("SHARED_TASK_TOKEN", "")
    sha8 = hashlib.sha256(tok.encode()).hexdigest()[:8] if tok else None
    return {"expected_sha8": sha8, "len_expected": len(tok)}

# ---------- retention (optional; keeps previous contract) ----------
@admin.post("/retention", summary="Run Retention")
async def run_retention(dry_run: bool = Query(False), ok: bool = Depends(_auth)):
    pool = get_pool()
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT * FROM retention.purge_portfolios_395d(%s,%s,%s);",
            (None, None, dry_run),
        )
        row = await cur.fetchone()
        affected = int(row[0]) if row else 0
        if dry_run:
            await conn.rollback()
        else:
            await conn.commit()
    return {"purged": affected, "dry_run": bool(dry_run)}

# ---------- diagnostics ----------
@admin.get("/norm_counts", summary="Norm Counts")
async def norm_counts(ok: bool = Depends(_auth)):
    pool = get_pool()
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute("SELECT count(*) FROM odds_norm.games");   g = (await cur.fetchone())[0]
        await cur.execute("SELECT count(*) FROM odds_norm.markets"); m = (await cur.fetchone())[0]
        await cur.execute("SELECT count(*) FROM odds_norm.odds");    o = (await cur.fetchone())[0]
    return {"games": int(g), "markets": int(m), "odds": int(o)}

@admin.get("/raw_counts", summary="Raw Counts")
async def raw_counts(ok: bool = Depends(_auth)):
    pool = get_pool()
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute("SELECT count(*) FROM public.odds_raw")
        n = (await cur.fetchone())[0]
    return {"odds_raw": int(n)}

@admin.get("/db_info", summary="DB Info")
async def db_info(ok: bool = Depends(_auth)):
    env_dsn = os.environ.get("DATABASE_URL", "")
    parsed = {"var": "DATABASE_URL", "host": None, "db": None, "sha8": None}
    if env_dsn.startswith(("postgres://", "postgresql://")):
        try:
            rest = env_dsn.split("://", 1)[1].split("@")[-1]
            host = rest.split("/", 1)[0].split(":")[0]
            db = rest.split("/", 1)[1].split("?")[0]
            parsed = {"var":"DATABASE_URL","host":host,"db":db,
                      "sha8": hashlib.sha256(env_dsn.encode()).hexdigest()[:8]}
        except Exception:
            pass

    pool = get_pool()
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute("select current_database(), inet_server_addr(), inet_server_port()")
        current_database, server_addr, port = await cur.fetchone()
    return {"env_dsns":[parsed], "runtime":{
        "current_database": current_database, "server_addr": str(server_addr), "port": int(port)
    }}

# ---------- normalize ----------
@admin.post("/normalize", summary="Normalize From Raw",
            description=("Normalize public.odds_raw.payload (JSONB) → odds_norm.games/markets/odds. "
                         "Uses DISTINCT to avoid ON CONFLICT double-update."))
async def normalize_from_raw(
    dry_run: bool = Query(True, description="no writes when true"),
    limit: int | None = Query(None, description="cap odds_raw rows considered"),
    ok: bool = Depends(_auth),
):
    pool = get_pool()
    lim_clause = "LIMIT %s" if limit else ""
    lim_arg = (limit,) if limit else tuple()

    async with pool.connection() as conn, conn.cursor() as cur:
        # GAMES
        await cur.execute(
            f"""
            WITH src AS (
              SELECT DISTINCT
                (payload->>'id')::text                   AS game_uid,
                payload->>'sport_key'                    AS sport_key,
                (payload->>'commence_time')::timestamptz AS commence_time,
                payload->>'away_team'                    AS away_team,
                payload->>'home_team'                    AS home_team
              FROM public.odds_raw
              ORDER BY fetched_at DESC
              {lim_clause}
            )
            INSERT INTO odds_norm.games (game_uid, sport_key, commence_time, away_team, home_team)
            SELECT game_uid, sport_key, commence_time, away_team, home_team
            FROM src
            ON CONFLICT (game_uid) DO UPDATE
              SET sport_key = EXCLUDED.sport_key,
                  commence_time = EXCLUDED.commence_time,
                  away_team = EXCLUDED.away_team,
                  home_team = EXCLUDED.home_team
            RETURNING 1
            """,
            lim_arg,
        )
        g = len(await cur.fetchall())

        # MARKETS
        await cur.execute(
            f"""
            WITH src AS (
              SELECT DISTINCT
                (r.payload->>'id')::text AS game_uid,
                m->>'key'                AS market_key,
                b->>'key'                AS book_key
              FROM public.odds_raw r
              CROSS JOIN LATERAL jsonb_array_elements(r.payload->'bookmakers') AS b
              CROSS JOIN LATERAL jsonb_array_elements(b->'markets')           AS m
              ORDER BY r.fetched_at DESC
              {lim_clause}
            )
            INSERT INTO odds_norm.markets (game_uid, market_key, book_key)
            SELECT game_uid, market_key, book_key
            FROM src
            ON CONFLICT (game_uid, market_key, book_key) DO NOTHING
            RETURNING 1
            """,
            lim_arg,
        )
        m = len(await cur.fetchall())

        # ODDS
        await cur.execute(
            f"""
            WITH src AS (
              SELECT DISTINCT
                (r.payload->>'id')::text AS game_uid,
                m->>'key'                AS market_key,
                b->>'key'                AS book_key,
                o->>'name'               AS side_raw,
                o->>'price'              AS price_raw,
                o->>'point'              AS point_raw,
                COALESCE(NULLIF(b->>'last_update',''), to_char(r.fetched_at,'YYYY-MM-DD"T"HH24:MI:SSOF')) AS last_update_raw,
                r.fetched_at             AS observed_at
              FROM public.odds_raw r
              CROSS JOIN LATERAL jsonb_array_elements(r.payload->'bookmakers') AS b
              CROSS JOIN LATERAL jsonb_array_elements(b->'markets')           AS m
              CROSS JOIN LATERAL jsonb_array_elements(m->'outcomes')          AS o
              ORDER BY r.fetched_at DESC
              {lim_clause}
            )
            INSERT INTO odds_norm.odds
              (game_uid, market_key, book_key, side, price, point, last_update, observed_at)
            SELECT
              game_uid,
              market_key,
              book_key,
              CASE
                WHEN lower(side_raw) IN ('home','1','over')  THEN 'home'
                WHEN lower(side_raw) IN ('away','2','under') THEN 'away'
                ELSE NULL
              END AS side,
              CASE WHEN price_raw ~ E'^-?\\d+$'            THEN price_raw::int     ELSE NULL END AS price,
              CASE WHEN point_raw ~ E'^-?\\d+(\\.\\d+)?$'  THEN point_raw::numeric ELSE NULL END AS point,
              COALESCE(NULLIF(last_update_raw,'')::timestamptz, now())          AS last_update,
              observed_at
            FROM src
            WHERE lower(side_raw) IN ('home','1','over','away','2','under')
            ON CONFLICT (game_uid, market_key, book_key, side, last_update) DO NOTHING
            RETURNING 1
            """,
            lim_arg,
        )
        o = len(await cur.fetchall())

        if dry_run:
            await conn.rollback()
        else:
            await conn.commit()

    return {"normalized": {"games": g, "markets": m, "odds_inserts": o}, "dry_run": bool(dry_run)}

# mount admin
app.include_router(admin)
