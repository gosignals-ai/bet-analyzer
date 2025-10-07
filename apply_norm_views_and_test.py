import os, psycopg, json
dsn = os.environ["DB_URL_EXT"]
with psycopg.connect(dsn) as conn, conn.cursor() as cur:
    cur.execute(open("sql_norm_views.sql","r",encoding="utf-8").read())
    # Small sanity checks
    cur.execute("SELECT count(*) FROM odds_norm.v_odds_latest")
    c1 = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM odds_norm.v_moneyline_game_best_norm")
    c2 = cur.fetchone()[0]
    cur.execute("""SELECT sport_key, game_uid, away_team, home_team,
                          away_best_price, away_book, home_best_price, home_book
                   FROM odds_norm.v_moneyline_game_best_norm
                   ORDER BY commence_time_utc NULLS LAST
                   LIMIT 5""")
    rows = cur.fetchall()
print("VIEWS OK;", dict(v_odds_latest=c1, v_moneyline_best=c2))
print("SAMPLE:", json.dumps(rows, default=str))