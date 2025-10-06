import os, asyncio, asyncpg
from dotenv import load_dotenv

load_dotenv(".env.local")

async def main():
    db = os.getenv("DATABASE_URL")
    if not db:
        print("DATABASE_URL missing in .env.local"); return
    conn = await asyncpg.connect(db)
    try:
        # Count rows if table exists
        try:
            c = await conn.fetchval("select count(*) from odds_raw")
        except Exception:
            print("Table odds_raw not found."); return
        print("odds_raw count:", c)
        rows = await conn.fetch("""
            select id, sport_key, game_id, fetched_at
            from odds_raw
            order by id desc limit 5
        """)
        for r in rows:
            print(dict(r))
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
