# services/portfolio/asgi.py
# Minimal shim so Render's start command can import `app` correctly.
from .app import app  # `app` is defined in services/portfolio/app.py
