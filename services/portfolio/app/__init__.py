from services.gsa_portfolio.main import app as _app
try:
    from services.portfolio.app.admin import router as admin_router
    _app.include_router(admin_router)
    print("[portfolio-admin] attached")  # <--- look for this in Render logs
except Exception as e:
    import sys, traceback
    print(f"[portfolio-admin] attach failed: {e}", file=sys.stderr)
    traceback.print_exc()
app = _app