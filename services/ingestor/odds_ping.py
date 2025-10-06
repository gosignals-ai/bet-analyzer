import os, json, httpx
from dotenv import load_dotenv

load_dotenv(".env.local", override=True)
k = os.getenv("ODDS_API_KEY") or ""
if not k:
    raise SystemExit("ODDS_API_KEY missing")

url = "https://api.the-odds-api.com/v4/sports"
with httpx.Client(timeout=20) as c:
    r = c.get(url, params={"apiKey": k})
print(json.dumps({"status": r.status_code, "ok": r.status_code == 200, "sample": r.json()[:2] if r.status_code==200 else r.text[:200]}, indent=2))
