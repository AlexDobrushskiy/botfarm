"""Playwright E2E test fixtures for the botfarm dashboard.

Provides session-scoped fixtures that:
- Seed a comprehensive test database
- Start the dashboard on a local port via uvicorn in a daemon thread
- Supply a Playwright page pointed at the test dashboard
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest
import uvicorn

from botfarm.dashboard import create_app
from tests.seed_test_db import seed_comprehensive_db

# All tests in this directory require the playwright marker.
pytestmark = pytest.mark.playwright

TEST_PORT = 8421
TEST_HOST = "127.0.0.1"


@pytest.fixture(scope="session")
def seeded_db(tmp_path_factory):
    """Create a comprehensive test database using the seed script."""
    db_path = tmp_path_factory.mktemp("e2e") / "test_dashboard.db"
    seed_comprehensive_db(db_path)
    return db_path


@pytest.fixture(scope="session")
def live_server(seeded_db):
    """Start the dashboard on port 8421 in a daemon thread.

    Yields the base URL. The server shuts down after the test session.
    """
    app = create_app(db_path=seeded_db)

    config = uvicorn.Config(
        app,
        host=TEST_HOST,
        port=TEST_PORT,
        log_level="warning",
    )
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    # Wait for the server to be ready
    import httpx

    base_url = f"http://{TEST_HOST}:{TEST_PORT}"
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            resp = httpx.get(base_url, timeout=1)
            if resp.status_code == 200:
                break
        except httpx.ConnectError:
            time.sleep(0.1)
    else:
        raise RuntimeError(f"Dashboard did not start within 10s on {base_url}")

    yield base_url

    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture()
def page(live_server, page):
    """Playwright page navigated to the test dashboard."""
    page.goto(live_server)
    return page
