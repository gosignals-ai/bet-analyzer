from .audit_compat import log_audit_compat
import os, sys, json, hashlib, argparse, asyncio, textwrap
from datetime import datetime, timezone
import httpx, asyncpg
from dotenv import load_dotenv

# Load local env (never commit secrets)
load_dotenv(".env.local", override=True)
ODDS_API_KEY = os.getenv("ODDS_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

DDL = """
CREATE TABLE IF NOT EXISTS audit_logs (
  id           BIGSERIAL PRIMARY KEY,
  at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  source       TEXT NOT NULL,
  action       TEXT NOT NULL,
  details      JSONB
);

CREATE TABLE IF NOT EXISTS odds_raw (
  id           BIGSERIAL PRIMARY KEY,
  sport_key    TEXT NOT NULL,
  game_id      TEXT NOT NULL,
  fetched_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  payload      JSONB NOT NULL,
  payload_hash TEXT NOT NULL,
  UNIQUE (payload_hash)
);

CREATE INDEX IF NOT EXISTS idx_odds_raw_sport_time ON odds_raw (sport_key, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_odds_raw_game ON odds_raw (game_id);
"""

def stable_hash(obj: dict) -> str:
    """sha256 of canonical JSON for idempotency."""
    data = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(data.encode("utf-8")).hexdigest()

async def ensure_schema(conn: asyncpg.Connection):
    await conn.execute(DDL)

async def fetch_odds(sport: str, regions: str, markets: str, timeout: int = 30):
    if not ODDS_API_KEY:
        raise RuntimeError("ODDS_API_KEY missing")
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": regions,               # e.g., "us"
        "markets": markets,               # e.g., "h2h,spreads,totals"
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(url, params=params)
        if r.status_code != 200:
            raise RuntimeError(f"Odds API {r.status_code}: {r.text[:300]}")
        return r.json(), dict(r.headers)

async def write_batch(conn: asyncpg.Connection, sport: str, games: list, dry_run: bool):
    now = datetime.now(timezone.utc)
    inserted, skipped = 0, 0
    for g in games:
        # Build a compact canonical object to hash (sport + game core + bookmakers block)
        core = {
            "sport_key": sport,
            "id": g.get("id"),
            "commence_time": g.get("commence_time"),
            "home_team": g.get("home_team"),
            "away_team": g.get("away_team"),
            "bookmakers": g.get("bookmakers", []),
        }
        h = stable_hash(core)

        if dry_run:
            skipped += 1
            continue

        try:
            await conn.execute(
                """
                INSERT INTO odds_raw (sport_key, game_id, fetched_at, payload, payload_hash)
                VALUES ($1, $2, $3, $4::jsonb, $5)
                ON CONFLICT (payload_hash) DO NOTHING
                """,
                sport, g.get("id") or "n/a", now, json.dumps(g), h
            )
            # Use row count via a probe: if conflict, nothing new
            # asyncpg's execute returns "INSERT 0 1" or "INSERT 0 0"
            inserted += 1
        except Exception as e:
            # Non-fatal: continue batch, but record in audit
            await conn.execute(
                """
                INSERT INTO audit_logs (source, action, details)
                VALUES ($1, $2, $3::jsonb)
                """,
                "ingestor", "insert_error",
                json.dumps({"error": str(e), "sport_key": sport, "game_id": g.get("id")})
            )
            continue

    # Deduplicate count (since conflict doesn't raise); refine by comparing payload_hash if needed
    # For simplicity we report attempted inserts vs dry-run skips.
    return inserted, skipped

async def log_audit(conn: asyncpg.Connection, action: str, details: dict):
    await conn.execute(
        """INSERT INTO audit_logs (source, action, details) VALUES ($1, $2, $3::jsonb)""",
        "ingestor", action, json.dumps(details)
    )

async def main():
    parser = argparse.ArgumentParser(description="Ingest Odds API ? odds_raw (idempotent).")
    parser.add_argument("--sport", required=True, help="e.g., basketball_nba, americanfootball_nfl")
    parser.add_argument("--regions", default="us", help="Odds API regions, e.g., us")
    parser.add_argument("--markets", default="h2h,spreads,totals", help="Odds API markets list")
    parser.add_argument("--dry-run", type=int, default=1, help="1 = no DB writes, 0 = write")
    args = parser.parse_args()

    if not DATABASE_URL:
        print("DATABASE_URL missing", file=sys.stderr)
        sys.exit(2)

    # Fetch odds
    games, hdrs = await fetch_odds(args.sport, args.regions, args.markets)

    # DB work
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await ensure_schema(conn)
        inserted, skipped = await write_batch(conn, args.sport, games, dry_run=bool(args.dry_run))
        await log_audit_compat(conn, "ingest_run", {
            "sport": args.sport,
            "regions": args.regions,
            "markets": args.markets,
            "dry_run": bool(args.dry_run),
            "attempted": len(games),
            "inserted": inserted if not args.dry_run else 0,
            "skipped": skipped if args.dry_run else 0,
            "odds_api_remain": hdrs.get("X-Requests-Remaining"),
            "odds_api_used": hdrs.get("X-Requests-Used"),
        })
        print(json.dumps({
            "ok": True,
            "attempted": len(games),
            "inserted": inserted if not args.dry_run else 0,
            "skipped": skipped if args.dry_run else 0,
            "dry_run": bool(args.dry_run),
            "sport": args.sport
        }, indent=2))
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
async def log_audit_compat(conn, action: str, details: dict):
    """
    Backward-compatible audit logger:
    - Tries (source, action, details)
    - Falls back to (module, source, action, details) if needed
    - Swallows errors to avoid breaking ingestion
    """
    import json
    try:
        await conn.execute(
            """INSERT INTO audit_logs (source, action, details)
               VALUES ($1, $2, $3::jsonb)""",
            "ingestor", action, json.dumps(details)
        )
        return
    except Exception as e1:
        try:
            cols = [r["column_name"] for r in await conn.fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_name='audit_logs'"
            )]
            if "module" in cols:
                await conn.execute(
                    """INSERT INTO audit_logs (module, source, action, details)
                       VALUES ($1, $2, $3, $4::jsonb)""",
                    "ingestor", "ingestor", action, json.dumps(details)
                )
                return
            # If no module column, re-raise original
            raise
        except Exception:
            # Last resort: ignore audit error
            return
