from importlib import import_module

_APP_CANDIDATES = [
    "services.gsa_ingestor.main",
    "services.gsa_ingestor.app",
    "services.ingestor.main",
    "services.ingestor.app",
]

_app = None
for modname in _APP_CANDIDATES:
    try:
        mod = import_module(modname)
        if hasattr(mod, "app"):
            _app = getattr(mod, "app")
            print(f"[asgi] loaded base app from {modname}")
            break
    except Exception as e:
        print(f"[asgi] import failed from {modname}: {e}")

if _app is None:
    raise RuntimeError("Could not locate ingestor FastAPI app in known modules.")

# Attach normalize router (either absolute or relative import)
try:
    from services.gsa_ingestor.normalize import router as normalize_router
except Exception:
    from .normalize import router as normalize_router

try:
    _app.include_router(normalize_router)
    print("[asgi] normalize router attached")
except Exception as e:
    import sys
    print(f"[asgi] normalize attach failed: {e}", file=sys.stderr)

app = _app