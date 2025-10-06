import os, json
from dotenv import load_dotenv

load_dotenv(".env.local", override=True)
k = os.getenv("ODDS_API_KEY") or ""
db = os.getenv("DATABASE_URL") or ""

print(json.dumps({
  "odds_api_key_loaded": (k[:4] + "..." + k[-4:]) if k else "MISSING",
  "has_database_url": bool(db)
}, indent=2))
