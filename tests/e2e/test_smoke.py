"""Minimal smoke test to validate the Playwright E2E infrastructure."""

import pytest


@pytest.mark.playwright
def test_dashboard_loads(page):
    """Verify the dashboard index page loads and has the expected title."""
    assert page.title() == "Live Status - Botfarm"
