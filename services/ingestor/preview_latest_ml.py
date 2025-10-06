import os, asyncio, asyncpg, json
from dotenv import load_dotenv

load_dotenv(".env.local", override=True)

async def main():
    conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
    try:
        rows = await conn.fetch("""
            SELECT sport_key, game_id, away_team, home_team, commence_time_utc,
                   away_best_price, away_book, home_best_price, home_book
            FROM v_moneyline_game_best
            ORDER BY commence_time_utc NULLS LAST, game_id
            LIMIT 10
        """)
        for r in rows:
            print(json.dumps(dict(r), default=str))
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
