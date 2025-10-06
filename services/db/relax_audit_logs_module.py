import os, asyncio, asyncpg
from dotenv import load_dotenv
load_dotenv(".env.local", override=True)

SQL = """
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='audit_logs' AND column_name='module'
  ) THEN
    BEGIN
      EXECUTE 'ALTER TABLE audit_logs ALTER COLUMN module DROP NOT NULL';
    EXCEPTION WHEN others THEN
      -- ignore if already nullable
      NULL;
    END;

    BEGIN
      EXECUTE $$ALTER TABLE audit_logs ALTER COLUMN module SET DEFAULT 'ingestor'$$;
    EXCEPTION WHEN others THEN
      -- ignore if default already set
      NULL;
    END;
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
        print("audit_logs.module: nullable + default set (if present)")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
