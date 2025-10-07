from services.gsa_portfolio.main import app as _app
try:
    from .admin import router as admin_router
    _app.include_router(admin_router)
except Exception as e:
    import sys
    print(f"[portfolio-admin] attach failed: {e}", file=sys.stderr)
app = _app