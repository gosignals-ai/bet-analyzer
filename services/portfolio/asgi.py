# Minimal ASGI wrapper to avoid circular imports:
# Imports the real FastAPI app and exposes it as "app" for Uvicorn.
from services.gsa_portfolio.main import app as app  # do not import anything else here