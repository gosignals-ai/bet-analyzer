import os
from fastapi import APIRouter, Depends, Header, HTTPException
from .db import get_pool  # existing psycopg pool helper

router = APIRouter(prefix="/admin", tags=["admin"])

def _auth(authorization: str | None = Header(None)):
    token = os.environ.get("SHARED_TASK_TOKEN")
    if not token or not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="unauthorized")
    if authorization.split(" ", 1)[1] != token:
        raise HTTPException(status_code=403, detail="forbidden")
    return True

@router.post("/retention")
async def run_retention(dry_run: bool = False, ok: bool = Depends(_auth)):
    pool = get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            if dry_run:
                await cur.execute("SELECT * FROM retention.purge_portfolios_395d(p_dry_run := TRUE);")
            else:
                await cur.execute("SELECT * FROM retention.purge_portfolios_395d(p_batch := 50000, p_hard_cap := 5000000, p_dry_run := FALSE);")
            row = await cur.fetchone()
            affected = int(row[0]) if row else 0
    return {"purged": affected, "dry_run": bool(dry_run)}