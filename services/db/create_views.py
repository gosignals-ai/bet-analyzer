import os, asyncio, asyncpg
from dotenv import load_dotenv

load_dotenv(".env.local", override=True)

SQL = r"""
-- Latest snapshot per game (by fetched_at)
CREATE OR REPLACE VIEW v_odds_latest_per_game AS
WITH maxes AS (
  SELECT game_id, MAX(fetched_at) AS max_fetched
  FROM odds_raw
  GROUP BY game_id
)
SELECT r.*
FROM odds_raw r
JOIN maxes m
  ON r.game_id = m.game_id AND r.fetched_at = m.max_fetched;

-- Flatten latest snapshot -> one row per bookmaker/market/outcome
CREATE OR REPLACE VIEW v_odds_flat AS
SELECT
  l.sport_key,
  l.game_id,
  (l.payload->>'home_team')              AS home_team,
  (l.payload->>'away_team')              AS away_team,
  (l.payload->>'commence_time')::timestamptz AS commence_time_utc,
  b->>'key'                              AS bookmaker_key,
  b->>'title'                            AS bookmaker_title,
  m->>'key'                              AS market_key,
  o->>'name'                             AS outcome_name,
  NULLIF(o->>'price','')::int            AS price_american,
  NULLIF(o->>'point','')::numeric        AS point,
  l.fetched_at
FROM v_odds_latest_per_game l
CROSS JOIN LATERAL jsonb_array_elements(l.payload->'bookmakers') b
CROSS JOIN LATERAL jsonb_array_elements(b->'markets')    m
CROSS JOIN LATERAL jsonb_array_elements(m->'outcomes')   o;

-- Filter to moneyline (h2h)
CREATE OR REPLACE VIEW v_moneyline_latest AS
SELECT *
FROM v_odds_flat
WHERE market_key = 'h2h';

-- Best (highest) moneyline per team for each game across all books
CREATE OR REPLACE VIEW v_moneyline_latest_best AS
SELECT DISTINCT ON (game_id, outcome_name)
  sport_key, game_id, home_team, away_team, commence_time_utc,
  outcome_name AS team,
  price_american AS best_price_american,
  bookmaker_title,
  fetched_at
FROM v_moneyline_latest
ORDER BY game_id, outcome_name, price_american DESC NULLS LAST, fetched_at DESC;

-- Convenience view to show both sides in one row
CREATE OR REPLACE VIEW v_moneyline_game_best AS
WITH away AS (
  SELECT game_id, away_team, best_price_american AS away_best_price, bookmaker_title AS away_book
  FROM v_moneyline_latest_best WHERE team = away_team
),
home AS (
  SELECT game_id, home_team, best_price_american AS home_best_price, bookmaker_title AS home_book
  FROM v_moneyline_latest_best WHERE team = home_team
)
SELECT
  f.sport_key,
  f.game_id,
  f.away_team,
  f.home_team,
  f.commence_time_utc,
  a.away_best_price,
  a.away_book,
  h.home_best_price,
  h.home_book
FROM (SELECT DISTINCT sport_key, game_id, away_team, home_team, commence_time_utc FROM v_moneyline_latest) f
LEFT JOIN away a ON a.game_id = f.game_id
LEFT JOIN home h ON h.game_id = f.game_id;
"""

async def main():
    db = os.getenv("DATABASE_URL")
    if not db:
        raise SystemExit("DATABASE_URL missing")
    conn = await asyncpg.connect(db)
    try:
        await conn.execute(SQL)
        print("Views created: v_odds_latest_per_game, v_odds_flat, v_moneyline_latest, v_moneyline_latest_best, v_moneyline_game_best")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
