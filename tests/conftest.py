"""Root test conftest — fixes Playwright / pytest-asyncio event loop conflict.

Playwright's sync API keeps an asyncio event loop registered as "running" via
greenlet for the lifetime of the browser session.  When pytest-asyncio later
tries to spin up its own ``asyncio.Runner`` for ``@pytest.mark.asyncio`` tests
(or when sync tests call ``asyncio.get_event_loop().run_until_complete()``),
the stale loop causes ``RuntimeError: Runner.run() cannot be called from a
running event loop``.

The ``_clear_playwright_loop`` fixture below temporarily unsets the running-loop
reference for non-Playwright tests so they can use asyncio normally.
"""

from __future__ import annotations

import asyncio
from typing import Iterator

import pytest


@pytest.fixture(autouse=True)
def _clear_playwright_loop(request: pytest.FixtureRequest) -> Iterator[None]:
    """Temporarily clear a leaked Playwright event loop for non-Playwright tests.

    Playwright's sync API (used by the ``page`` fixture in ``tests/e2e/``)
    registers an event loop as *running* in the main thread.  This is harmless
    for Playwright tests but fatal for any test that needs its own asyncio
    runner or calls ``run_until_complete()``.

    We skip this fixture for Playwright tests (which need the running loop)
    and clear it for everything else.
    """
    is_playwright = request.node.get_closest_marker("playwright") is not None
    stale_loop = None

    if not is_playwright:
        stale_loop = asyncio._get_running_loop()
        if stale_loop is not None:
            asyncio._set_running_loop(None)

    yield

    if stale_loop is not None and not stale_loop.is_closed():
        asyncio._set_running_loop(stale_loop)
