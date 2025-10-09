from __future__ import annotations
import os, json, hashlib
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, APIRouter, Depends, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN") or os.environ.get("TOKEN")
ADMIN_LEN_EXPECTED = 64

# ——— diagnostic flag so we can see what's deployed
SELECT_STYLE = "distinct_on_v1"

pool = AsyncConnectionPool(conninfo=DATABASE_URL, min_size=1, max_size=5, open=False)
app = FastAPI(title="GSA Portfolio")
router = APIRouter(prefix="/admin", tags=["admin"])

def _sha8(s: str) -> str: return hashlib.sha256(s.encode("utf-8")).hexdigest()[:8]

async def require_admin(
    authorization: Optional[str] = Header(None, convert_underscores=False),
    x_gsa_token: Optional[str] = Header(None),
) -> str:
    if not ADMIN_TOKEN or len(ADMIN_TOKEN) != ADMIN_LEN_EXPECTED:
        raise HTTPException(status_code=500, detail="admin token not configured")
    supplied = None
    if authorization and authorization.lower().startswith("bearer "):
        supplied = authorization.split(" ", 1)[1].strip()
    if not supplied and x_gsa_token:
        supplied = x_gsa_token.strip()
    if not supplied:
        raise HTTPException(status_code=401, detail="unauthorized")
    if supplied == ADMIN_TOKEN:
        return supplied
    raise HTTPException(status_code=401, detail="unauthorized")

async def _ensure_pool_open() -> None:
    if pool.closed: await pool.open()

class Counts(BaseModel):
    games: int; markets: int; odds: int

def _safe_market_side(market_key: str, outcome_name: str, home_team: str, away_team: str) -> Optional[str]:
    mk = (market_key or "").strip().lower()
    nm = (outcome_name or "").strip().lower()
    h = (home_team or "").strip().lower()
    a = (away_team or "").strip().lower()
    if mk in ("h2h","spreads","spread","line"):
        if nm in ("home", h): return "home"
        if nm in ("away", a): return "away"
        return None
    if mk in ("totals","total","over_under"):
        if nm.startswith("over"): return "over"
        if nm.startswith("under"): return "under"
        return None
    return None

@router.get("/__ping")
async def __ping(_: str = Depends(require_admin)) -> Dict[str, Any]:
    return {"ok": True}

@router.get("/__debug_token")
async def __debug_token(_: Request) -> Dict[str, Any]:
    return {"expected_sha8": _sha8(ADMIN_TOKEN) if ADMIN_TOKEN else None, "len_expected": ADMIN_LEN_EXPECTED}

# NEW: prove which module & select style is live
@router.get("/whoami")
async def whoami(_: str = Depends(require_admin)) -> Dict[str, Any]:
    return {"module": __file__, "select_style": SELECT_STYLE}

@router.get("/db_info")
async def db_info(_: str = Depends(require_admin)) -> Dict[str, Any]:
    await _ensure_pool_open()
    async with pool.connection() as ac, ac.cursor(row_factory=dict_row) as cur:
        await cur.execute("select current_database() as db, current_schema() as schema;")
        row = await cur.fetchone()
    dsn = DATABASE_URL
    try:
        pre, rest = dsn.split("://", 1)
        userinfo, hostpart = rest.split("@", 1)
        user = userinfo.split(":")[0]
        dsn = f"{pre}://{user}:***@{hostpart}"
    except Exception:
        pass
    return {"dsn": dsn, "runtime": row}

@router.get("/raw_counts")
async def raw_counts(_: str = Depends(require_admin)) -> Dict[str, int]:
    await _ensure_pool_open()
    async with pool.connection() as ac, ac.cursor() as cur:
        await cur.execute("select count(*) from public.odds_raw;")
        (n,) = await cur.fetchone()
    return {"odds_raw": int(n)}

@router.get("/norm_counts", response_model=Counts)
async def norm_counts(_: str = Depends(require_admin)) -> Counts:
    await _ensure_pool_open()
    async with pool.connection() as ac, ac.cursor() as cur:
        await cur.execute("select count(*) from odds_norm.games;"); (g,) = await cur.fetchone()
        await cur.execute("select count(*) from odds_norm.markets;"); (m,) = await cur.fetchone()
        await cur.execute("select count(*) from odds_norm.odds;"); (o,) = await cur.fetchone()
    return Counts(games=int(g), markets=int(m), odds=int(o))

@router.post("/normalize")
async def normalize_from_raw(
    _: str = Depends(require_admin),
    dry_run: bool = Query(True),
    limit: int = Query(200, ge=1, le=10000),
) -> JSONResponse:
    await _ensure_pool_open()

    # FIXED SOURCE QUERY — DISTINCT ON with proper ORDER BY
    async with pool.connection() as ac, ac.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT DISTINCT ON (game_id, sport_key, payload_hash)
                id, sport_key, game_id, fetched_at, payload, payload_hash
            FROM public.odds_raw
            ORDER BY game_id, sport_key, payload_hash, fetched_at DESC
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )
        rows: List[Dict[str, Any]] = await cur.fetchall()

    if not rows:
        return JSONResponse({"ok": True, "dry_run": dry_run, "limit": limit, "source": {"rows": 0}})

    ins_games = ins_markets = ins_odds = 0

    async with pool.connection() as ac:
        async with ac.cursor() as cur:
            for r in rows:
                sport_key: str = r["sport_key"]
                game_id_raw: str = r["game_id"]
                payload = r["payload"]
                if isinstance(payload, str): payload = json.loads(payload)
                gid = payload.get("id") or game_id_raw
                home_team = (payload.get("home_team") or "").strip()
                away_team = (payload.get("away_team") or "").strip()
                commence_time = payload.get("commence_time")
                game_uid = f"{sport_key}:{gid}"

                if dry_run:
                    ins_games += 1
                else:
                    await cur.execute(
                        """
                        INSERT INTO odds_norm.games (game_uid, sport_key, game_id, home_team, away_team, commence_time)
                        VALUES (%(game_uid)s, %(sport_key)s, %(game_id)s, %(home_team)s, %(away_team)s, %(commence_time)s)
                        ON CONFLICT (game_uid) DO UPDATE
                          SET home_team = EXCLUDED.home_team,
                              away_team = EXCLUDED.away_team,
                              commence_time = EXCLUDED.commence_time
                        """,
                        {"game_uid": game_uid, "sport_key": sport_key, "game_id": gid,
                         "home_team": home_team, "away_team": away_team, "commence_time": commence_time}
                    )

                for bk in (payload.get("bookmakers") or []):
                    book_key = (bk.get("key") or "").strip()
                    book_ts = bk.get("last_update") or bk.get("lastUpdate") or payload.get("fetched_at")
                    for mk in (bk.get("markets") or []):
                        market_key = (mk.get("key") or "").strip().lower()
                        m_ts = mk.get("last_update") or mk.get("lastUpdate") or book_ts

                        if dry_run:
                            ins_markets += 1
                        else:
                            await cur.execute(
                                """
                                INSERT INTO odds_norm.markets (game_uid, market_key, book_key, last_update)
                                VALUES (%(game_uid)s, %(market_key)s, %(book_key)s, %(last_update)s)
                                ON CONFLICT (game_uid, market_key, book_key) DO UPDATE
                                  SET last_update = GREATEST(odds_norm.markets.last_update, EXCLUDED.last_update)
                                """,
                                {"game_uid": game_uid, "market_key": market_key,
                                 "book_key": book_key, "last_update": m_ts}
                            )

                        for oc in (mk.get("outcomes") or []):
                            name = oc.get("name") or ""
                            side = _safe_market_side(market_key, name, home_team, away_team)
                            if not side: continue
                            price = oc.get("price") or oc.get("odds")
                            point = oc.get("point")
                            last_update = oc.get("last_update") or oc.get("lastUpdate") or m_ts
                            if market_key in ("totals","total","over_under") and side not in ("over","under"):
                                continue
                            if dry_run:
                                ins_odds += 1
                            else:
                                await cur.execute(
                                    """
                                    INSERT INTO odds_norm.odds
                                        (game_uid, market_key, book_key, side, price, point, last_update)
                                    VALUES (%(game_uid)s, %(market_key)s, %(book_key)s, %(side)s, %(price)s, %(point)s, %(last_update)s)
                                    ON CONFLICT (game_uid, market_key, book_key, side, last_update) DO NOTHING
                                    """,
                                    {"game_uid": game_uid, "market_key": market_key, "book_key": book_key,
                                     "side": side, "price": price, "point": point, "last_update": last_update}
                                )
            if dry_run: await ac.rollback()
            else:       await ac.commit()

    return JSONResponse({"ok": True, "dry_run": dry_run, "limit": limit,
                         "source": {"rows": len(rows)},
                         "counts": {"games": ins_games, "markets": ins_markets, "odds": ins_odds}})

@app.on_event("startup")
async def _startup() -> None: await _ensure_pool_open()
@app.on_event("shutdown")
async def _shutdown() -> None:
    try: await pool.close()
    except Exception: pass

app.include_router(router)

@app.get("/")
async def root() -> Dict[str, Any]:
    return {"service": "gsa-portfolio", "status": "ok"}
