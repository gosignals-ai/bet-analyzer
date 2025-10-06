import json
import asyncpg

async def log_audit_compat(conn: asyncpg.Connection, action: str, details: dict):
    """
    Backward-compatible audit logger:
    - Try (source, action, details)
    - If legacy schemas require `module`, include it
    - Swallow errors so ingestion never crashes on audit
    """
    try:
        await conn.execute(
            """INSERT INTO audit_logs (source, action, details)
               VALUES ($1, $2, $3::jsonb)""",
            "ingestor", action, json.dumps(details)
        )
        return
    except Exception:
        try:
            cols = [r["column_name"] for r in await conn.fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_name='audit_logs'"
            )]
            # if a legacy NOT NULL "module" exists, include it
            if "module" in cols:
                await conn.execute(
                    """INSERT INTO audit_logs (module, source, action, details)
                       VALUES ($1, $2, $3, $4::jsonb)""",
                    "ingestor", "ingestor", action, json.dumps(details)
                )
                return
        except Exception:
            pass  # last resort: ignore audit failure
