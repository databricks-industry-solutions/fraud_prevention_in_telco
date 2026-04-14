"""Fraud Engine Chapter 0 — Standalone demo app."""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

app = FastAPI(title="Fraud Engine — Chapter 0", version="1.0")

frontend_dir = os.path.join(os.path.dirname(__file__), "frontend", "dist")

if os.path.exists(frontend_dir):
    app.mount(
        "/assets",
        StaticFiles(directory=os.path.join(frontend_dir, "assets")),
        name="assets",
    )

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        return FileResponse(os.path.join(frontend_dir, "index.html"))
else:

    @app.get("/")
    async def root():
        return {"status": "ok", "app": "Fraud Engine Chapter 0"}
