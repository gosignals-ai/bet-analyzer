from services.gsa_portfolio.main import app as _app

try:
    # Import AFTER app exists to avoid circular imports
    from services.portfolio.app.admin import router as admin_router
    _app.include_router(admin_router)
    print("[asgi] portfolio-admin attached")
except Exception as e:
    import sys, traceback
    print(f"[asgi] portfolio-admin attach failed: {e}", file=sys.stderr)
    traceback.print_exc()

# uvicorn entry
app = _app