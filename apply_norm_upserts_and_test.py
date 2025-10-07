import os, psycopg, datetime as dt
dsn = os.environ["DB_URL_EXT"]

sql = open("sql_norm_upserts.sql","r",encoding="utf-8").read()
with psycopg.connect(dsn) as conn, conn.cursor() as cur:
    cur.execute(sql)  # create/replace functions

    game = "sample_game_001"
    now  = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)

    # upsert game/market twice (should not error)
    for _ in (1,2):
        cur.execute("SELECT odds_norm.upsert_game(%s,%s,%s,%s,%s,%s)",
                    (game, "americanfootball_nfl", now, "Home FC", "Away FC", "scheduled"))
        cur.execute("SELECT odds_norm.upsert_market(%s,%s,%s,%s,%s)",
                    (game, "h2h", "draftkings", None, now))

    # insert odds twice with same timestamp -> True then False
    cur.execute("SELECT odds_norm.insert_odds(%s,%s,%s,%s,%s,%s,%s)",
                (game, "h2h", "draftkings", "home", -110, None, now))
    first = cur.fetchone()[0]
    cur.execute("SELECT odds_norm.insert_odds(%s,%s,%s,%s,%s,%s,%s)",
                (game, "h2h", "draftkings", "home", -110, None, now))
    second = cur.fetchone()[0]

print(f"UPSERTS OK; INSERTS: {first} {second}")  # expect: True False