import os, asyncio, json, httpx, asyncpg
from dotenv import load_dotenv

load_dotenv(".env.local")

ODDS_API_KEY = os.getenv("ODDS_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

async def check_db():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        row = await conn.fetchrow("select now() as ts, current_database() as db")
        # table existence probe (won’t fail if missing)
        tables = {}
        for t in ["games","markets","odds","audit_logs","portfolios"]:
            try:
                c = await conn.fetchval(f"select count(*) from {t}")
                tables[t] = c
            except Exception:
                tables[t] = "n/a"
        return {"time": str(row["ts"]), "db": row["db"], "tables": tables}
    finally:
        await conn.close()

async def check_odds_api():
    if not ODDS_API_KEY:
        return {"ok": False, "error": "ODDS_API_KEY missing"}
    url = "https://api.the-odds-api.com/v4/sports"
    params = {"apiKey": ODDS_API_KEY}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, params=params)
        ok = r.status_code == 200
        return {"ok": ok, "status": r.status_code, "len": len(r.json()) if ok else None}

async def main():
    db = await check_db()
    odds = await check_odds_api()
    print(json.dumps({"db": db, "odds_api": odds}, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
