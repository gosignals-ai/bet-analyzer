import os, asyncio, asyncpg
from dotenv import load_dotenv

load_dotenv(".env.local", override=True)

SQL = """
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='audit_logs' AND column_name='source'
  ) THEN
    ALTER TABLE audit_logs ADD COLUMN source TEXT;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='audit_logs' AND column_name='action'
  ) THEN
    ALTER TABLE audit_logs ADD COLUMN action TEXT;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='audit_logs' AND column_name='details'
  ) THEN
    ALTER TABLE audit_logs ADD COLUMN details JSONB;
  END IF;
END $$;
"""

async def main():
    db = os.getenv("DATABASE_URL")
    if not db:
        raise SystemExit("DATABASE_URL missing")
    conn = await asyncpg.connect(db)
    try:
        await conn.execute(SQL)
        print("audit_logs patched")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
