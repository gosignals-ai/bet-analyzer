# Expose FastAPI "app" for Render import path: services.portfolio.app:app
# We reuse the existing app from gsa_portfolio and attach the admin router.

from services.gsa_portfolio.main import app as _app  # existing FastAPI app
try:
    from .admin import router as admin_router
    _app.include_router(admin_router)
except Exception as e:
    # Keep the app booting even if admin isn't present; log at import time
    import sys
    print(f"[portfolio-admin] attach failed: {e}", file=sys.stderr)

app = _app