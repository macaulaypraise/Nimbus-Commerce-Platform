"""Standalone smoke test for the FastAPI app.

Loads the .env file for the requested environment and verifies
the application boots and /health returns 200. Does NOT require
pytest, conftest, or the test fixtures.

Usage:
    python scripts/smoke_app.py
    ENVIRONMENT=test python scripts/smoke_app.py
    ENVIRONMENT=development python scripts/smoke_app.py
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


def _load_dotenv() -> None:
    """Load the right .env file for the current environment."""
    env = os.getenv("ENVIRONMENT", "development")
    try:
        from dotenv import load_dotenv
    except ImportError:
        return  # python-dotenv is optional here
    for candidate in (REPO_ROOT / f".env.{env}", REPO_ROOT / ".env"):
        if candidate.exists():
            load_dotenv(candidate, override=False)
            break


async def main() -> int:
    _load_dotenv()
    from httpx import ASGITransport, AsyncClient

    from src.main import create_app

    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/health")
        if response.status_code != 200:
            print(f"FAIL: /health returned {response.status_code}")
            print(response.text)
            return 1
        body = response.json()
        print("OK: /health returned 200")
        print(f"  app         = {body['app']}")
        print(f"  version     = {body['version']}")
        print(f"  environment = {body['environment']}")
        return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
