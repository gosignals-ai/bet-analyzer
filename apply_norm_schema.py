import os, pathlib, psycopg
dsn = os.environ["DB_URL_EXT"]
sql = pathlib.Path("sql_norm_schema.sql").read_text()
with psycopg.connect(dsn) as conn, conn.cursor() as cur:
    cur.execute(sql)
print("Normalized schema created/verified.")