import os, psycopg
dsn = os.environ["DB_URL_EXT"]
with psycopg.connect(dsn) as conn, conn.cursor() as cur:
    cur.execute("select count(*) from odds_norm.games");  g = cur.fetchone()[0]
    cur.execute("select count(*) from odds_norm.markets");m = cur.fetchone()[0]
    cur.execute("select count(*) from odds_norm.odds");   o = cur.fetchone()[0]
print(f"COUNT games={g} markets={m} odds={o}")
