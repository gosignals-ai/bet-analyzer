import os, json, psycopg
dsn = os.environ["postgresql://gsa_admin:kENWrILMAxj0AOyibrLzFnHXo1gozPKG@dpg-d3hi8n0gjchc73ah5eg0-a.oregon-postgres.render.com/gosignals_kg43"]
checks = [
  ("games_count", "SELECT count(*) FROM odds_norm.games"),
  ("markets_count", "SELECT count(*) FROM odds_norm.markets"),
  ("odds_count", "SELECT count(*) FROM odds_norm.odds"),
  ("orphans_markets", "SELECT count(*) FROM odds_norm.markets m LEFT JOIN odds_norm.games g USING (game_uid) WHERE g.game_uid IS NULL"),
  ("orphans_odds", "SELECT count(*) FROM odds_norm.odds o LEFT JOIN odds_norm.games g USING (game_uid) WHERE g.game_uid IS NULL"),
  ("invalid_side", "SELECT count(*) FROM odds_norm.odds WHERE side NOT IN ('home','away','draw') OR side IS NULL"),
  ("latest_count", "SELECT count(*) FROM odds_norm.v_odds_latest"),
  ("latest_distinct", "SELECT count(DISTINCT game_uid, market_key, book_key, side) FROM odds_norm.odds"),
  ("best_rows", "SELECT count(*) FROM odds_norm.v_moneyline_game_best_norm"),
  ("null_best_prices", "SELECT count(*) FROM odds_norm.v_moneyline_game_best_norm WHERE away_best_price IS NULL AND home_best_price IS NULL"),
]
out = {}
with psycopg.connect(dsn) as conn, conn.cursor() as cur:
    for k,q in checks:
        cur.execute(q); out[k] = int(cur.fetchone()[0])
ok = (
    out["orphans_markets"] == 0 and
    out["orphans_odds"] == 0 and
    out["invalid_side"] == 0 and
    out["latest_count"] == out["latest_distinct"]
)
print("INTEGRITY:", json.dumps(out), "OK=", ok)