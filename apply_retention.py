import os, pathlib, sys, psycopg

dsn = os.environ["DB_URL_EXT"]
sql = pathlib.Path("sql_portfolios_retention.sql").read_text()

def ensure_index():
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_portfolios_created_at ON portfolios (created_at);")
    print("Index ensured: idx_portfolios_created_at")

def create_function():
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql)
    print("Function created.")

def dry_run():
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM retention.purge_portfolios_395d(p_dry_run := TRUE);")
        row = cur.fetchone()
        print("DRY-RUN affected =", (row[0] if row else 0))

def apply_once():
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM retention.purge_portfolios_395d(p_batch := 50000, p_hard_cap := 5000000, p_dry_run := FALSE);")
        row = cur.fetchone()
        print("APPLY affected =", (row[0] if row else 0))

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "--help"
    if cmd == "--ensure-index":
        ensure_index()
    elif cmd == "--create":
        create_function()
    elif cmd == "--dry-run":
        dry_run()
    elif cmd == "--apply":
        apply_once()
    else:
        print("Usage: python apply_retention.py [--ensure-index | --create | --dry-run | --apply]")