import os, re
from typing import Optional, List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, validator
import psycopg
from psycopg.types.json import Json

APP_NAME = "gsa_compliance"
DB_URL   = os.getenv("DATABASE_URL")

app = FastAPI(title="GoSignals Compliance", version="0.1.0")

# --------- Models ----------
class SanitizeIn(BaseModel):
    text: str = Field(..., min_length=1, max_length=5000)

BAD_PATTERNS = [
    r"\b(ssn|social security)\b",
    r"\b(credit\s*card|cc\s*number)\b",
    r"\b(api[_-]?key)\b",
    r"[A-Za-z0-9]{32,}"  # generic long token-ish strings
]
REDACT = "[REDACTED]"

class PickIn(BaseModel):
    game_id: Optional[int] = None
    league: Optional[str] = None
    market_key: str
    outcome: str
    price: float
    point: Optional[float] = None
    stake: float = 0.0

    @validator("market_key")
    def check_market(cls, v):
        allowed = {"h2h","spreads","totals"}
        if v not in allowed:
            raise ValueError(f"market_key must be one of {sorted(allowed)}")
        return v

    @validator("outcome")
    def check_outcome(cls, v):
        allowed = {"home","away","draw","over","under"}
        if v not in allowed:
            raise ValueError(f"outcome must be one of {sorted(allowed)}")
        return v

    @validator("stake")
    def non_negative_stake(cls, v):
        if v < 0:
            raise ValueError("stake must be >= 0")
        return v

    @validator("price")
    def realistic_price(cls, v):
        # Allow common American odds range
        if v == 0 or v < -2000 or v > 2000:
            raise ValueError("price must be within -2000..2000 and non-zero")
        return v

# --------- Routes ----------
@app.get("/")
def root():
    return {"service": APP_NAME, "status": "ready"}

@app.get("/health")
def health():
    try:
        with psycopg.connect(DB_URL, connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"service": APP_NAME, "db": "ok"}
    except Exception as e:
        raise HTTPException(500, f"DB error: {e}")

@app.post("/compliance/sanitize")
def sanitize(payload: SanitizeIn):
    redacted = payload.text
    for pat in BAD_PATTERNS:
        redacted = re.sub(pat, REDACT, redacted, flags=re.IGNORECASE)
    # audit log
    try:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO audit_logs (module, event, detail) VALUES (%s, %s, %s)",
                    ("compliance", "sanitize", Json({"len_in": len(payload.text), "len_out": len(redacted)})),
                )
            conn.commit()
    except Exception as e:
        # don't block response on audit failures
        pass
    return {"sanitized": redacted}

@app.post("/compliance/validate-pick")
def validate_pick(p: PickIn):
    # Pydantic validators handle most checks; we add a few cross-field hints
    hints: List[str] = []
    if p.market_key in {"spreads","totals"} and p.point is None:
        hints.append("point should be provided for spreads/totals")
    return {"valid": True, "hints": hints, "pick": p.dict()}
