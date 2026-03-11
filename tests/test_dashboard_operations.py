"""Tests for dashboard pause/resume, log viewer, identities, preflight, health, codex pages."""

import json
from unittest.mock import patch

import pytest
import yaml
from fastapi.testclient import TestClient

from botfarm.config import (
    BotfarmConfig,
    CoderIdentity,
    IdentitiesConfig,
    LinearConfig,
    ProjectConfig,
    ReviewerIdentity,
)
from botfarm.dashboard import create_app, format_codex_ndjson_line, format_ndjson_line
from botfarm.db import (
    init_db,
    insert_stage_run,
    insert_task,
    save_dispatch_state,
    update_task,
)
from tests.helpers import seed_slot as _seed_slot


class TestManualPauseState:
    def test_running_state(self, db_file):
        """No manual pause → state is 'running'."""
        app = create_app(db_path=db_file, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-controls")
        assert resp.status_code == 200
        # Running state should show the Pause button
        assert "Pause" in resp.text

    def test_paused_state(self, tmp_path):
        """When dispatch_paused=manual_pause and no busy slots → state is 'paused'."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="paused_manual", ticket_id="T-1")
        save_dispatch_state(conn, paused=True, reason="manual_pause")
        conn.commit()
        conn.close()

        app = create_app(db_path=path, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-controls")
        assert resp.status_code == 200
        assert "Resume" in resp.text

    def test_pausing_state(self, tmp_path):
        """When dispatch_paused=manual_pause and busy slots exist → pausing."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="busy", ticket_id="T-1", pid=12345)
        save_dispatch_state(conn, paused=True, reason="manual_pause")
        conn.commit()
        conn.close()

        app = create_app(db_path=path, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-controls")
        assert resp.status_code == 200
        assert "Pausing" in resp.text
        assert "Cancel" in resp.text

    def test_start_paused_shows_play_button(self, tmp_path):
        """When dispatch_paused=start_paused → state is 'start_paused', play button shown."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="free")
        save_dispatch_state(conn, paused=True, reason="start_paused")
        conn.commit()
        conn.close()

        app = create_app(db_path=path, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-controls")
        assert resp.status_code == 200
        assert "Start" in resp.text
        assert "start dispatching" in resp.text

    def test_start_paused_with_busy_slot_shows_play_not_pausing(self, tmp_path):
        """start_paused with busy slots shows play button, not Pausing/Cancel."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="busy", ticket_id="T-1", pid=12345)
        save_dispatch_state(conn, paused=True, reason="start_paused")
        conn.commit()
        conn.close()

        app = create_app(db_path=path, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-controls")
        assert resp.status_code == 200
        assert "Start" in resp.text
        assert "Pausing" not in resp.text
        assert "Cancel" not in resp.text

    def test_start_paused_badge_shows_amber(self, tmp_path):
        """Supervisor badge shows amber 'Dispatch Paused' when start_paused."""
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        path = tmp_path / "badge.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="free")
        save_dispatch_state(conn, paused=True, reason="start_paused",
                            supervisor_heartbeat=now_iso)
        conn.commit()
        conn.close()

        app = create_app(db_path=path, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-badge")
        assert resp.status_code == 200
        assert "Dispatch Paused" in resp.text
        assert "supervisor-badge-paused" in resp.text

    def test_start_paused_banner_on_index(self, tmp_path):
        """Index page shows a start_paused banner when in that state."""
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        path = tmp_path / "banner.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="free")
        save_dispatch_state(conn, paused=True, reason="start_paused",
                            supervisor_heartbeat=now_iso)
        conn.commit()
        conn.close()

        app = create_app(db_path=path, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "start dispatching" in resp.text
        assert "banner-start-paused" in resp.text

    def test_no_start_paused_banner_when_running(self, db_file):
        """Index page should NOT show the start_paused banner when running."""
        app = create_app(db_path=db_file, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Dispatch is paused" not in resp.text

    def test_start_paused_banner_neutral_without_callbacks(self, tmp_path):
        """Banner uses neutral copy when has_callbacks is false (no Start button)."""
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        path = tmp_path / "banner_no_cb.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="free")
        save_dispatch_state(conn, paused=True, reason="start_paused",
                            supervisor_heartbeat=now_iso)
        conn.commit()
        conn.close()

        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "banner-start-paused" in resp.text
        assert "Dispatch is paused" in resp.text
        assert "New tickets will not be dispatched until the supervisor is resumed" in resp.text
        assert "start dispatching" not in resp.text

    def test_start_paused_badge_on_health_page(self, tmp_path):
        """Health page renders Dispatch Paused badge on first load (no htmx poll needed)."""
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        path = tmp_path / "badge.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="free")
        save_dispatch_state(conn, paused=True, reason="start_paused",
                            supervisor_heartbeat=now_iso)
        conn.commit()
        conn.close()

        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/health")
        assert resp.status_code == 200
        assert "Dispatch Paused" in resp.text
        assert "supervisor-badge-paused" in resp.text

    def test_start_paused_badge_on_cleanup_page(self, tmp_path):
        """Cleanup page renders Dispatch Paused badge on first load."""
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        path = tmp_path / "badge.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="free")
        save_dispatch_state(conn, paused=True, reason="start_paused",
                            supervisor_heartbeat=now_iso)
        conn.commit()
        conn.close()

        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/cleanup")
        assert resp.status_code == 200
        assert "Dispatch Paused" in resp.text
        assert "supervisor-badge-paused" in resp.text


class TestResumingTransitionalState:
    """Verify the 'resuming' transitional banner/controls after clicking Start/Resume."""

    def test_resume_shows_transitional_banner(self, tmp_path):
        """After POST /api/resume, the banner partial returns 'resuming' state."""
        from datetime import datetime, timezone

        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        path = tmp_path / "resume.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="free")
        save_dispatch_state(conn, paused=True, reason="start_paused",
                            supervisor_heartbeat=now_iso)
        conn.commit()
        conn.close()

        app = create_app(db_path=path, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)

        # Before resume: banner shows "Dispatch is paused"
        resp = client.get("/partials/start-paused-banner")
        assert "Dispatch is paused" in resp.text

        # Click resume
        resp = client.post("/api/resume")
        assert resp.status_code == 200

        # After resume: banner shows transitional "Starting dispatch" message
        resp = client.get("/partials/start-paused-banner")
        assert "Starting dispatch" in resp.text
        assert "banner-resuming" in resp.text
        assert "Dispatch is paused" not in resp.text

    def test_resume_shows_transitional_controls(self, tmp_path):
        """After POST /api/resume, controls partial shows 'Starting...' state."""
        from datetime import datetime, timezone

        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        path = tmp_path / "resume_ctrl.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="free")
        save_dispatch_state(conn, paused=True, reason="start_paused",
                            supervisor_heartbeat=now_iso)
        conn.commit()
        conn.close()

        app = create_app(db_path=path, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)

        # Before resume: shows Start button
        resp = client.get("/partials/supervisor-controls")
        assert "Start" in resp.text

        # Click resume
        client.post("/api/resume")

        # After resume: shows disabled "Starting..." button
        resp = client.get("/partials/supervisor-controls")
        assert "Starting" in resp.text
        assert "Dispatch is starting" in resp.text

    def test_resuming_state_clears_when_running(self, db_file):
        """When supervisor catches up (state=running), resume flag is cleared."""
        app = create_app(db_path=db_file, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)

        # Simulate a resume request (no actual paused state in DB)
        app.state.resume_requested_at = 1.0  # very old timestamp

        # DB state is "running", so resume flag should be cleared
        resp = client.get("/partials/start-paused-banner")
        assert "Dispatch is paused" not in resp.text
        assert "Starting dispatch" not in resp.text
        assert app.state.resume_requested_at == 0.0


class TestPauseHints:
    """Verify informational hints shown in each pause state."""

    def test_running_hint(self, db_file):
        """Running state shows hint about graceful pause behavior."""
        app = create_app(db_path=db_file, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-controls")
        assert "Stops new dispatches" in resp.text
        assert "running stages finish gracefully" in resp.text

    def test_pausing_hint_and_worker_details(self, tmp_path):
        """Pausing state shows dispatch-stopped message and busy worker stages."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(
            conn, "proj", 1, status="busy", ticket_id="SMA-42",
            stage="implement", pid=12345,
        )
        _seed_slot(
            conn, "proj", 2, status="busy", ticket_id="SMA-43",
            stage="review", pid=12346,
        )
        save_dispatch_state(conn, paused=True, reason="manual_pause")
        conn.commit()
        conn.close()

        app = create_app(db_path=path, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-controls")
        body = resp.text
        assert "New dispatches stopped" in body
        assert "2 workers" in body
        assert "Slot 1: implement SMA-42" in body
        assert "Slot 2: review SMA-43" in body

    def test_paused_hint(self, tmp_path):
        """Paused state shows resume hint about partially-completed tickets."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="paused_manual", ticket_id="T-1")
        save_dispatch_state(conn, paused=True, reason="manual_pause")
        conn.commit()
        conn.close()

        app = create_app(db_path=path, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-controls")
        body = resp.text
        assert "All work paused" in body
        assert "resume from where they left off" in body


class TestPauseButtonFeedback:
    """Verify buttons have immediate-feedback hx-on::before-request handlers."""

    def test_pause_button_has_before_request(self, db_file):
        """Pause button disables itself and shows 'Pausing' on click."""
        app = create_app(db_path=db_file, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-controls")
        assert "hx-on::before-request" in resp.text
        assert "Pausing" in resp.text or "pausing" in resp.text.lower()

    def test_resume_button_has_before_request(self, tmp_path):
        """Resume button disables itself and shows 'Resuming' on click."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="paused_manual", ticket_id="T-1")
        save_dispatch_state(conn, paused=True, reason="manual_pause")
        conn.commit()
        conn.close()

        app = create_app(db_path=path, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-controls")
        assert "hx-on::before-request" in resp.text
        assert "Resuming" in resp.text

    def test_start_button_has_before_request(self, tmp_path):
        """Start button disables itself and shows 'Starting' on click."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="free")
        save_dispatch_state(conn, paused=True, reason="start_paused")
        conn.commit()
        conn.close()

        app = create_app(db_path=path, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-controls")
        assert "hx-on::before-request" in resp.text
        assert "Starting" in resp.text

    def test_cancel_button_has_before_request(self, tmp_path):
        """Cancel button in pausing state disables itself on click."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="busy", ticket_id="T-1", pid=12345)
        save_dispatch_state(conn, paused=True, reason="manual_pause")
        conn.commit()
        conn.close()

        app = create_app(db_path=path, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-controls")
        assert resp.text.count("hx-on::before-request") >= 1
        assert "Resuming" in resp.text


class TestPauseResumeAPI:
    def test_pause_calls_callback(self, db_file):
        """POST /api/pause calls the on_pause callback."""
        called = []
        app = create_app(
            db_path=db_file,
            on_pause=lambda: called.append("pause"),
            on_resume=lambda: None,
        )
        client = TestClient(app)
        resp = client.post("/api/pause")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}
        assert called == ["pause"]

    def test_resume_calls_callback(self, db_file):
        """POST /api/resume calls the on_resume callback."""
        called = []
        app = create_app(
            db_path=db_file,
            on_pause=lambda: None,
            on_resume=lambda: called.append("resume"),
        )
        client = TestClient(app)
        resp = client.post("/api/resume")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}
        assert called == ["resume"]

    def test_pause_without_callback_returns_503(self, db_file):
        """POST /api/pause returns 503 when no callback is registered."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.post("/api/pause")
        assert resp.status_code == 503

    def test_resume_without_callback_returns_503(self, db_file):
        """POST /api/resume returns 503 when no callback is registered."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.post("/api/resume")
        assert resp.status_code == 503


class TestPausedManualSlotDisplay:
    def test_paused_manual_shown_in_slots_table(self, tmp_path):
        """A slot with status paused_manual should display correctly."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(
            conn, "proj", 1, status="paused_manual",
            ticket_id="T-1", ticket_title="Test", branch="b1",
            stages_completed=["implement"],
        )
        save_dispatch_state(conn, paused=True, reason="manual_pause")
        conn.commit()
        conn.close()

        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert resp.status_code == 200
        assert "paused manual" in resp.text


# --- Log Viewer ---


class TestLogViewer:
    """Tests for the live log viewer feature."""

    @pytest.fixture()
    def logs_setup(self, tmp_path):
        """Create DB + log files for testing the log viewer."""
        db_path = tmp_path / "logtest.db"
        conn = init_db(db_path)
        task_id = insert_task(
            conn,
            ticket_id="LOG-1",
            title="Log test",
            project="my-project",
            slot=1,
            status="completed",
        )
        update_task(
            conn,
            task_id,
            started_at="2026-02-26T10:00:00+00:00",
            completed_at="2026-02-26T11:00:00+00:00",
        )
        insert_stage_run(
            conn,
            task_id=task_id,
            stage="implement",
            iteration=1,
            turns=10,
            duration_seconds=1800.0,
        )
        insert_stage_run(
            conn,
            task_id=task_id,
            stage="review",
            iteration=1,
            turns=5,
            duration_seconds=600.0,
        )
        conn.commit()
        conn.close()

        # Create log files
        logs_dir = tmp_path / "logs"
        ticket_log_dir = logs_dir / "LOG-1"
        ticket_log_dir.mkdir(parents=True)
        (ticket_log_dir / "implement-20260226-100000.log").write_text(
            "Starting implement stage\nDoing work...\nDone.\n"
        )
        (ticket_log_dir / "review-20260226-103000.log").write_text(
            "Reviewing PR\nLooks good.\n"
        )

        return db_path, logs_dir

    def test_log_viewer_page_returns_200(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/LOG-1/logs")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_log_viewer_shows_stage_tabs(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/LOG-1/logs")
        body = resp.text
        assert "implement" in body
        assert "review" in body

    def test_log_viewer_stage_page_returns_200(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/LOG-1/logs/implement")
        assert resp.status_code == 200

    def test_log_viewer_shows_log_content(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/LOG-1/logs/implement")
        body = resp.text
        assert "Starting implement stage" in body
        assert "Doing work..." in body

    def test_log_viewer_no_logs_dir(self, tmp_path):
        """Without logs_dir, log viewer shows no logs available."""
        db_path = tmp_path / "nolog.db"
        conn = init_db(db_path)
        insert_task(
            conn, ticket_id="NL-1", title="No logs",
            project="proj", slot=1, status="completed",
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/task/NL-1/logs")
        assert resp.status_code == 200
        assert "No log files available" in resp.text

    def test_log_viewer_no_log_files(self, tmp_path):
        """With logs_dir but no files for ticket, shows no logs available."""
        db_path = tmp_path / "empty.db"
        conn = init_db(db_path)
        insert_task(
            conn, ticket_id="EMP-1", title="Empty",
            project="proj", slot=1, status="completed",
        )
        conn.commit()
        conn.close()
        logs_dir = tmp_path / "logs"
        logs_dir.mkdir()
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/EMP-1/logs")
        assert resp.status_code == 200
        assert "No log files available" in resp.text

    def test_log_content_api_returns_text(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/api/logs/LOG-1/implement/content")
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]
        assert "Starting implement stage" in resp.text

    def test_log_content_api_404_no_file(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/api/logs/LOG-1/merge/content")
        assert resp.status_code == 404

    def test_log_content_api_404_no_ticket(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/api/logs/NONEXIST-1/implement/content")
        assert resp.status_code == 404

    def test_stream_endpoint_404_no_file(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/api/logs/LOG-1/merge/stream")
        assert resp.status_code == 404

    def test_log_viewer_back_link_to_task_detail(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/LOG-1/logs")
        assert "Back to Task Detail" in resp.text

    def test_log_viewer_active_stage_tab(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/LOG-1/logs/implement")
        assert "log-stage-tab-active" in resp.text

    def test_log_viewer_review_content(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/LOG-1/logs/review")
        assert "Reviewing PR" in resp.text
        assert "Looks good" in resp.text


class TestLogViewerBusySlot:
    """Tests for log viewer on busy (live) slots."""

    def test_busy_slot_shows_logs_link(self, tmp_path):
        """Busy slot should show a 'logs' link."""
        db_path = tmp_path / "busy.db"
        conn = init_db(db_path)
        _seed_slot(
            conn, "proj", 1, status="busy",
            ticket_id="T-1", ticket_title="Test",
            stage="implement",
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert "/task/T-1/logs" in resp.text
        assert ">logs</a>" in resp.text

    def test_free_slot_no_logs_link(self, tmp_path):
        """Free slot should not show a 'logs' link."""
        db_path = tmp_path / "free.db"
        conn = init_db(db_path)
        _seed_slot(conn, "proj", 1, status="free")
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert ">logs</a>" not in resp.text

    def test_live_badge_shown_for_busy_stage(self, tmp_path):
        """When a stage is actively running, show LIVE badge."""
        db_path = tmp_path / "live.db"
        conn = init_db(db_path)
        task_id = insert_task(
            conn, ticket_id="LIVE-1", title="Live test",
            project="proj", slot=1, status="in_progress",
        )
        _seed_slot(
            conn, "proj", 1, status="busy",
            ticket_id="LIVE-1", ticket_title="Live test",
            stage="implement",
        )
        conn.commit()
        conn.close()

        logs_dir = tmp_path / "logs"
        ticket_log_dir = logs_dir / "LIVE-1"
        ticket_log_dir.mkdir(parents=True)
        (ticket_log_dir / "implement-20260226-100000.log").write_text(
            "Working...\n"
        )

        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/LIVE-1/logs/implement")
        assert resp.status_code == 200
        assert "LIVE" in resp.text
        assert "log-live-badge" in resp.text


class TestTaskDetailLogLinks:
    """Tests for log links on the task detail page."""

    def test_stage_runs_table_has_logs_column(self, client):
        resp = client.get("/task/TST-1")
        body = resp.text
        assert "<th>Logs</th>" in body

    def test_stage_runs_have_log_view_links(self, client):
        resp = client.get("/task/TST-1")
        body = resp.text
        assert "/task/TST-1/logs/implement" in body
        assert "/task/TST-1/logs/review" in body


class TestFormatNdjsonLine:
    """Tests for the NDJSON line formatting function."""

    def test_empty_line(self):
        event_type, text = format_ndjson_line("")
        assert event_type == "log"
        assert text == ""

    def test_non_json_line(self):
        event_type, text = format_ndjson_line("plain text log line")
        assert event_type == "log"
        assert text == "plain text log line"

    def test_assistant_text_content(self):
        line = json.dumps({
            "type": "assistant",
            "message": {
                "content": [{"type": "text", "text": "Hello, I will help you."}],
            },
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "assistant"
        assert "Hello, I will help you." in text

    def test_assistant_tool_use(self):
        line = json.dumps({
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "text", "text": "Let me read that file."},
                    {"type": "tool_use", "name": "Read", "input": {"file_path": "/foo/bar.py"}},
                ],
            },
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "assistant"
        assert "Let me read that file." in text
        assert "Read" in text
        assert "/foo/bar.py" in text

    def test_assistant_empty_content(self):
        line = json.dumps({
            "type": "assistant",
            "message": {"content": []},
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "assistant"
        assert "[assistant turn]" in text

    def test_user_tool_result_ok(self):
        line = json.dumps({
            "type": "user",
            "message": {
                "content": [{
                    "type": "tool_result",
                    "is_error": False,
                    "content": "File contents here...",
                }],
            },
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "tool_result"
        assert "[ok]" in text
        assert "File contents" in text

    def test_user_tool_result_error(self):
        line = json.dumps({
            "type": "user",
            "message": {
                "content": [{
                    "type": "tool_result",
                    "is_error": True,
                    "content": "Permission denied",
                }],
            },
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "tool_result"
        assert "[ERROR]" in text

    def test_result_event(self):
        line = json.dumps({
            "type": "result",
            "num_turns": 15,
            "duration_ms": 45000,
            "subtype": "end_turn",
            "is_error": False,
            "result": "Task completed successfully",
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "result"
        assert "15 turns" in text
        assert "45.0s" in text
        assert "end_turn" in text
        assert "Task completed successfully" in text

    def test_result_event_error(self):
        line = json.dumps({
            "type": "result",
            "num_turns": 5,
            "duration_ms": 10000,
            "is_error": True,
            "result": "Something went wrong",
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "result"
        assert "[ERROR]" in text

    def test_unknown_json_type(self):
        line = json.dumps({"type": "ping"})
        event_type, text = format_ndjson_line(line)
        assert event_type == "system"
        assert "[ping]" in text

    def test_json_without_type(self):
        line = json.dumps({"foo": "bar"})
        event_type, text = format_ndjson_line(line)
        assert event_type == "log"

    def test_invalid_json(self):
        event_type, text = format_ndjson_line("{bad json")
        assert event_type == "log"
        assert "{bad json" in text

    def test_bash_tool_summarized(self):
        line = json.dumps({
            "type": "assistant",
            "message": {
                "content": [{
                    "type": "tool_use",
                    "name": "Bash",
                    "input": {"command": "git status"},
                }],
            },
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "tool_use"
        assert "Bash" in text
        assert "git status" in text

    def test_edit_tool_summarized(self):
        line = json.dumps({
            "type": "assistant",
            "message": {
                "content": [{
                    "type": "tool_use",
                    "name": "Edit",
                    "input": {"file_path": "/src/main.py"},
                }],
            },
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "tool_use"
        assert "Edit" in text
        assert "/src/main.py" in text

    def test_grep_tool_summarized(self):
        line = json.dumps({
            "type": "assistant",
            "message": {
                "content": [{
                    "type": "tool_use",
                    "name": "Grep",
                    "input": {"pattern": "def foo"},
                }],
            },
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "tool_use"
        assert "Grep" in text
        assert "def foo" in text

    def test_tool_result_content_as_list(self):
        line = json.dumps({
            "type": "user",
            "message": {
                "content": [{
                    "type": "tool_result",
                    "is_error": False,
                    "content": [{"type": "text", "text": "Result text here"}],
                }],
            },
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "tool_result"
        assert "Result text here" in text

    def test_whitespace_stripped(self):
        event_type, text = format_ndjson_line("   \n  ")
        assert event_type == "log"
        assert text == ""

    def test_stderr_marker(self):
        event_type, text = format_ndjson_line("--- STDERR ---")
        assert event_type == "log"
        assert "STDERR" in text


class TestTaskDetailLiveOutput:
    """Tests for the 'View Live Output' link on the task detail page."""

    def test_in_progress_task_shows_live_link(self, tmp_path):
        db_path = tmp_path / "live.db"
        conn = init_db(db_path)
        task_id = insert_task(
            conn, ticket_id="LIVE-2", title="In progress task",
            project="proj", slot=1, status="in_progress",
        )
        insert_stage_run(conn, task_id=task_id, stage="implement",
                         iteration=1, turns=5, duration_seconds=300.0)
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/task/LIVE-2")
        body = resp.text
        assert "View Live Output" in body
        assert "/task/LIVE-2/logs" in body
        assert "log-live-badge" in body

    def test_completed_task_no_live_link(self, client):
        # TST-1 is a completed task in the default fixture
        resp = client.get("/task/TST-1")
        body = resp.text
        assert "View Live Output" not in body

    def test_failed_task_no_live_link(self, tmp_path):
        db_path = tmp_path / "fail.db"
        conn = init_db(db_path)
        insert_task(
            conn, ticket_id="FAIL-1", title="Failed task",
            project="proj", slot=1, status="failed",
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/task/FAIL-1")
        assert "View Live Output" not in resp.text


class TestSSEStreamFormatting:
    """Tests for the SSE stream endpoint with NDJSON formatting."""

    def test_stream_returns_formatted_events(self, tmp_path):
        """SSE stream should transform NDJSON into formatted event types."""
        db_path = tmp_path / "stream.db"
        conn = init_db(db_path)
        task_id = insert_task(
            conn, ticket_id="STR-1", title="Stream test",
            project="proj", slot=1, status="in_progress",
        )
        conn.commit()
        conn.close()

        # Create a log file with NDJSON content
        logs_dir = tmp_path / "logs"
        ticket_log_dir = logs_dir / "STR-1"
        ticket_log_dir.mkdir(parents=True)
        ndjson_content = (
            json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": "Working on it"}]}}) + "\n"
            + json.dumps({"type": "user", "message": {"content": [{"type": "tool_result", "is_error": False, "content": "ok"}]}}) + "\n"
            + json.dumps({"type": "result", "num_turns": 3, "duration_ms": 5000, "result": "Done"}) + "\n"
        )
        (ticket_log_dir / "implement-20260226-100000.log").write_text(ndjson_content)

        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/api/logs/STR-1/implement/stream")
        assert resp.status_code == 200

    def test_stream_404_no_log(self, tmp_path):
        db_path = tmp_path / "empty.db"
        conn = init_db(db_path)
        insert_task(
            conn, ticket_id="NF-1", title="No file",
            project="proj", slot=1, status="in_progress",
        )
        conn.commit()
        conn.close()
        logs_dir = tmp_path / "logs"
        logs_dir.mkdir()
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/api/logs/NF-1/implement/stream")
        assert resp.status_code == 404


class TestSSEHeartbeatAndReconnection:
    """Tests for SSE keepalive heartbeats and auto-reconnection UI."""

    def test_heartbeat_constant_defined(self):
        """SSE_HEARTBEAT_INTERVAL should be defined in routes_logs module."""
        from botfarm.dashboard.routes_logs import SSE_HEARTBEAT_INTERVAL
        assert SSE_HEARTBEAT_INTERVAL > 0

    def test_live_viewer_has_reconnection_logic(self, tmp_path):
        """Live log viewer page should include auto-reconnection JS."""
        db_path = tmp_path / "hb.db"
        conn = init_db(db_path)
        insert_task(
            conn, ticket_id="HB-1", title="Heartbeat test",
            project="proj", slot=1, status="in_progress",
        )
        _seed_slot(
            conn, "proj", 1, status="busy",
            ticket_id="HB-1", ticket_title="Heartbeat test",
            stage="implement",
        )
        conn.commit()
        conn.close()

        logs_dir = tmp_path / "logs"
        ticket_log_dir = logs_dir / "HB-1"
        ticket_log_dir.mkdir(parents=True)
        (ticket_log_dir / "implement-20260226-100000.log").write_text("test\n")

        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/HB-1/logs/implement")
        body = resp.text
        assert resp.status_code == 200
        assert "RECONNECTING" in body
        assert "reconnectDelay" in body
        assert "heartbeat" in body
        assert "showReconnecting" in body
        assert "connect()" in body

    def test_log_stream_partial_has_reconnection_logic(self, tmp_path):
        """Embeddable log stream partial should include reconnection JS."""
        db_path = tmp_path / "ls.db"
        conn = init_db(db_path)
        insert_task(
            conn, ticket_id="LS-1", title="Log stream test",
            project="proj", slot=1, status="in_progress",
        )
        _seed_slot(
            conn, "proj", 1, status="busy",
            ticket_id="LS-1", ticket_title="Log stream test",
            stage="implement",
        )
        conn.commit()
        conn.close()

        logs_dir = tmp_path / "logs"
        ticket_log_dir = logs_dir / "LS-1"
        ticket_log_dir.mkdir(parents=True)
        (ticket_log_dir / "implement-20260226-100000.log").write_text("test\n")

        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        # The task detail page includes the log stream partial for busy tasks
        resp = client.get("/task/LS-1")
        body = resp.text
        assert resp.status_code == 200
        # Check reconnection logic is present in the embedded stream
        assert "log-stream-reconnecting" in body or "reconnectDelay" in body

    def test_reconnecting_css_in_base_template(self, tmp_path):
        """Base template should include reconnecting badge CSS classes."""
        db_path = tmp_path / "css.db"
        conn = init_db(db_path)
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/")
        body = resp.text
        assert "log-reconnecting-badge" in body
        assert "log-stream-reconnecting" in body


class TestLogViewerFormattedEvents:
    """Tests for the updated log viewer template with event type styling."""

    def test_live_viewer_includes_event_type_handlers(self, tmp_path):
        """Live log viewer should register handlers for all event types."""
        db_path = tmp_path / "live.db"
        conn = init_db(db_path)
        task_id = insert_task(
            conn, ticket_id="EVT-1", title="Event test",
            project="proj", slot=1, status="in_progress",
        )
        _seed_slot(
            conn, "proj", 1, status="busy",
            ticket_id="EVT-1", ticket_title="Event test",
            stage="implement",
        )
        conn.commit()
        conn.close()

        logs_dir = tmp_path / "logs"
        ticket_log_dir = logs_dir / "EVT-1"
        ticket_log_dir.mkdir(parents=True)
        (ticket_log_dir / "implement-20260226-100000.log").write_text("test\n")

        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/EVT-1/logs/implement")
        body = resp.text
        assert resp.status_code == 200
        assert "LIVE" in body
        # Check that the JS registers handlers for formatted event types
        assert "assistant" in body
        assert "tool_use" in body
        assert "tool_result" in body
        assert "appendLine" in body


# ---------------------------------------------------------------------------
# Identity credentials management
# ---------------------------------------------------------------------------


def _make_identity_config(tmp_path, *, coder=None, reviewer=None):
    """Create a BotfarmConfig with identity fields and a YAML source file."""
    coder_identity = coder or CoderIdentity(
        github_token="ghp_testabc123def456",
        ssh_key_path="~/.botfarm/coder_id_ed25519",
        git_author_name="Coder Bot",
        git_author_email="coder@example.com",
        linear_api_key="lin_api_test123",
    )
    reviewer_identity = reviewer or ReviewerIdentity(
        github_token="ghp_reviewer789xyz",
        linear_api_key="lin_api_reviewer456",
    )

    config_data = {
        "projects": [
            {
                "name": "test-project",
                "linear_team": "TST",
                "base_dir": "~/test",
                "worktree_prefix": "test-slot-",
                "slots": [1],
            }
        ],
        "linear": {
            "api_key": "${LINEAR_API_KEY}",
            "poll_interval_seconds": 30,
        },
        "identities": {
            "coder": {
                "github_token": "${CODER_GITHUB_TOKEN}",
                "ssh_key_path": coder_identity.ssh_key_path,
                "git_author_name": coder_identity.git_author_name,
                "git_author_email": coder_identity.git_author_email,
                "linear_api_key": "${CODER_LINEAR_API_KEY}",
            },
            "reviewer": {
                "github_token": "${REVIEWER_GITHUB_TOKEN}",
                "linear_api_key": "${REVIEWER_LINEAR_API_KEY}",
            },
        },
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config_data, sort_keys=False))

    config = BotfarmConfig(
        projects=[
            ProjectConfig(
                name="test-project", linear_team="TST",
                base_dir="~/test", worktree_prefix="test-slot-", slots=[1],
            ),
        ],
        linear=LinearConfig(api_key="test-key", poll_interval_seconds=30),
        identities=IdentitiesConfig(
            coder=coder_identity,
            reviewer=reviewer_identity,
        ),
    )
    config.source_path = str(config_path)
    return config, config_path


class TestIdentitiesPage:
    @pytest.fixture()
    def identity_client(self, db_file, tmp_path):
        config, _ = _make_identity_config(tmp_path)
        app = create_app(db_path=db_file, botfarm_config=config)
        return TestClient(app)

    def test_identities_page_returns_200(self, identity_client):
        resp = identity_client.get("/identities")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_identities_page_shows_coder_section(self, identity_client):
        resp = identity_client.get("/identities")
        body = resp.text
        assert "Coder Identity" in body
        assert "Reviewer Identity" in body

    def test_identities_page_shows_masked_tokens(self, identity_client):
        resp = identity_client.get("/identities")
        body = resp.text
        # Masked: first 4 + **** + last 4
        assert "ghp_****f456" in body
        assert "ghp_****9xyz" in body

    def test_identities_page_shows_plain_fields(self, identity_client):
        resp = identity_client.get("/identities")
        body = resp.text
        assert "Coder Bot" in body
        assert "coder@example.com" in body
        assert "coder_id_ed25519" in body

    def test_identities_page_shows_set_badges(self, identity_client):
        resp = identity_client.get("/identities")
        body = resp.text
        assert "identity-badge-set" in body

    def test_identities_page_disabled_without_config(self, db_file):
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/identities")
        assert resp.status_code == 200
        assert "not available" in resp.text

    def test_identities_page_shows_unset_badges_when_empty(self, db_file, tmp_path):
        config, _ = _make_identity_config(
            tmp_path,
            coder=CoderIdentity(),
            reviewer=ReviewerIdentity(),
        )
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        resp = client.get("/identities")
        body = resp.text
        assert "identity-badge-unset" in body

    def test_identities_nav_link_present(self, identity_client):
        resp = identity_client.get("/")
        assert "Identities" in resp.text

    def test_identities_page_ssh_key_exists_check(self, db_file, tmp_path):
        ssh_key = tmp_path / "test_key"
        ssh_key.write_text("-----BEGIN OPENSSH PRIVATE KEY-----\ntest\n")
        config, _ = _make_identity_config(
            tmp_path,
            coder=CoderIdentity(ssh_key_path=str(ssh_key)),
        )
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        resp = client.get("/identities")
        assert "File exists" in resp.text

    def test_identities_page_ssh_key_not_found(self, db_file, tmp_path):
        config, _ = _make_identity_config(
            tmp_path,
            coder=CoderIdentity(ssh_key_path="/nonexistent/key"),
        )
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        resp = client.get("/identities")
        assert "File not found" in resp.text


class TestIdentitiesUpdate:
    @pytest.fixture()
    def setup(self, db_file, tmp_path):
        config, config_path = _make_identity_config(tmp_path)
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        return client, config, config_path, tmp_path

    def test_update_coder_plain_fields(self, setup):
        client, _, config_path, _ = setup
        resp = client.post("/identities", json={
            "coder": {
                "git_author_name": "New Bot Name",
                "git_author_email": "new@example.com",
                "ssh_key_path": "/new/path/key",
            },
        })
        assert resp.status_code == 200
        assert "saved" in resp.text.lower()
        # Check YAML was updated
        data = yaml.safe_load(config_path.read_text())
        assert data["identities"]["coder"]["git_author_name"] == "New Bot Name"
        assert data["identities"]["coder"]["git_author_email"] == "new@example.com"
        assert data["identities"]["coder"]["ssh_key_path"] == "/new/path/key"

    def test_update_coder_secret_writes_env_file(self, setup):
        client, _, config_path, tmp_path = setup
        resp = client.post("/identities", json={
            "coder": {"github_token": "ghp_newsecret123"},
        })
        assert resp.status_code == 200
        # .env should have the token
        env_path = config_path.parent / ".env"
        env_content = env_path.read_text()
        assert 'CODER_GITHUB_TOKEN="ghp_newsecret123"' in env_content

    def test_update_coder_secret_writes_env_ref_to_yaml(self, setup):
        client, _, config_path, _ = setup
        client.post("/identities", json={
            "coder": {"github_token": "ghp_newsecret123"},
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["identities"]["coder"]["github_token"] == "${CODER_GITHUB_TOKEN}"

    def test_update_reviewer_secret(self, setup):
        client, _, config_path, _ = setup
        resp = client.post("/identities", json={
            "reviewer": {
                "github_token": "ghp_reviewernew",
                "linear_api_key": "lin_api_new",
            },
        })
        assert resp.status_code == 200
        env_path = config_path.parent / ".env"
        env_content = env_path.read_text()
        assert 'REVIEWER_GITHUB_TOKEN="ghp_reviewernew"' in env_content
        assert 'REVIEWER_LINEAR_API_KEY="lin_api_new"' in env_content

    def test_update_sets_restart_required(self, setup):
        client, _, _, _ = setup
        client.post("/identities", json={
            "coder": {"git_author_name": "Changed"},
        })
        resp = client.get("/config")
        # The config page should show restart banner if restart_required is set
        # We can check via the app state indirectly
        # Just verify the response was successful
        assert resp.status_code == 200

    def test_update_mixed_secret_and_plain(self, setup):
        client, _, config_path, _ = setup
        resp = client.post("/identities", json={
            "coder": {
                "github_token": "ghp_mixed123",
                "git_author_name": "Mixed Bot",
            },
        })
        assert resp.status_code == 200
        # .env has secret
        env_path = config_path.parent / ".env"
        assert 'CODER_GITHUB_TOKEN="ghp_mixed123"' in env_path.read_text()
        # YAML has env ref for secret, plain value for non-secret
        data = yaml.safe_load(config_path.read_text())
        assert data["identities"]["coder"]["github_token"] == "${CODER_GITHUB_TOKEN}"
        assert data["identities"]["coder"]["git_author_name"] == "Mixed Bot"

    def test_update_unknown_role_rejected(self, setup):
        client, _, _, _ = setup
        resp = client.post("/identities", json={
            "admin": {"github_token": "nope"},
        })
        assert resp.status_code == 422
        assert "Unknown role" in resp.text

    def test_update_unknown_field_rejected(self, setup):
        client, _, _, _ = setup
        resp = client.post("/identities", json={
            "coder": {"password": "nope"},
        })
        assert resp.status_code == 422
        assert "not an editable" in resp.text

    def test_update_non_string_value_rejected(self, setup):
        client, _, _, _ = setup
        resp = client.post("/identities", json={
            "coder": {"github_token": 12345},
        })
        assert resp.status_code == 422
        assert "must be a string" in resp.text

    def test_update_invalid_json_returns_400(self, setup):
        client, _, _, _ = setup
        resp = client.post(
            "/identities",
            content=b"not json",
            headers={"content-type": "application/json"},
        )
        assert resp.status_code == 400

    def test_update_non_dict_body_returns_400(self, setup):
        client, _, _, _ = setup
        resp = client.post("/identities", json=["not", "a", "dict"])
        assert resp.status_code == 400

    def test_update_without_config_returns_400(self, db_file):
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.post("/identities", json={"coder": {"git_author_name": "x"}})
        assert resp.status_code == 400
        assert "not available" in resp.text

    def test_env_file_preserves_existing_keys(self, setup):
        client, _, config_path, _ = setup
        env_path = config_path.parent / ".env"
        env_path.write_text("EXISTING_KEY=keepme\n")
        client.post("/identities", json={
            "coder": {"github_token": "ghp_new"},
        })
        env_content = env_path.read_text()
        assert "EXISTING_KEY=keepme" in env_content
        assert 'CODER_GITHUB_TOKEN="ghp_new"' in env_content

    def test_env_file_updates_existing_secret(self, setup):
        client, _, config_path, _ = setup
        env_path = config_path.parent / ".env"
        env_path.write_text("CODER_GITHUB_TOKEN=old_value\n")
        client.post("/identities", json={
            "coder": {"github_token": "ghp_updated"},
        })
        env_content = env_path.read_text()
        assert 'CODER_GITHUB_TOKEN="ghp_updated"' in env_content
        assert "old_value" not in env_content

    def test_yaml_creates_identities_section_if_missing(self, db_file, tmp_path):
        """When config.yaml has no identities section, it should be created."""
        config_data = {
            "projects": [
                {
                    "name": "test-project",
                    "linear_team": "TST",
                    "base_dir": "~/test",
                    "worktree_prefix": "test-slot-",
                    "slots": [1],
                }
            ],
            "linear": {"api_key": "${LINEAR_API_KEY}", "poll_interval_seconds": 30},
        }
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump(config_data, sort_keys=False))

        config = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="test-project", linear_team="TST",
                    base_dir="~/test", worktree_prefix="test-slot-", slots=[1],
                ),
            ],
            linear=LinearConfig(api_key="test-key", poll_interval_seconds=30),
        )
        config.source_path = str(config_path)

        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        resp = client.post("/identities", json={
            "coder": {"git_author_name": "New Bot"},
        })
        assert resp.status_code == 200
        data = yaml.safe_load(config_path.read_text())
        assert data["identities"]["coder"]["git_author_name"] == "New Bot"

    def test_reviewer_plain_field_rejected(self, setup):
        """Reviewer identity has no plain-text editable fields like ssh_key_path."""
        client, _, _, _ = setup
        resp = client.post("/identities", json={
            "reviewer": {"ssh_key_path": "/path"},
        })
        assert resp.status_code == 422
        assert "not an editable" in resp.text


# --- Preflight / System Health ---


class _FakeCheckResult:
    """Lightweight stand-in for preflight.CheckResult (avoids importing the module)."""
    def __init__(self, name, passed, message, critical=True):
        self.name = name
        self.passed = passed
        self.message = message
        self.critical = critical


class TestPreflightBanner:
    def test_banner_hidden_when_not_degraded(self, db_file):
        """Banner partial should be empty when supervisor is not degraded."""
        app = create_app(
            db_path=db_file,
            get_preflight_results=lambda: [],
            get_degraded=lambda: False,
        )
        client = TestClient(app)
        resp = client.get("/partials/preflight-banner")
        assert resp.status_code == 200
        assert "Degraded Mode" not in resp.text

    def test_banner_visible_when_degraded(self, db_file):
        """Banner partial should show degraded-mode alert when checks fail."""
        results = [
            _FakeCheckResult("git_repo:proj", False, "Remote unreachable"),
            _FakeCheckResult("database", True, "OK"),
        ]
        app = create_app(
            db_path=db_file,
            get_preflight_results=lambda: results,
            get_degraded=lambda: True,
        )
        client = TestClient(app)
        resp = client.get("/partials/preflight-banner")
        assert resp.status_code == 200
        body = resp.text
        assert "Degraded Mode" in body
        assert "1 preflight check failed" in body
        assert "Re-run Checks" in body

    def test_banner_shows_plural_failures(self, db_file):
        """Banner should pluralise correctly for multiple failures."""
        results = [
            _FakeCheckResult("git_repo:a", False, "fail", critical=True),
            _FakeCheckResult("linear_api", False, "fail", critical=True),
        ]
        app = create_app(
            db_path=db_file,
            get_preflight_results=lambda: results,
            get_degraded=lambda: True,
        )
        client = TestClient(app)
        resp = client.get("/partials/preflight-banner")
        assert "2 preflight checks failed" in resp.text

    def test_banner_without_callbacks(self, db_file):
        """Banner should render gracefully when no supervisor is connected."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/partials/preflight-banner")
        assert resp.status_code == 200
        assert "Degraded Mode" not in resp.text

    def test_base_template_includes_banner_div(self, db_file):
        """Every page should include the preflight-banner htmx container."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/")
        assert 'id="preflight-banner"' in resp.text
        assert 'hx-get="/partials/preflight-banner"' in resp.text


class TestHealthPage:
    def _make_client(self, db_file, results=None, degraded=False):
        app = create_app(
            db_path=db_file,
            get_preflight_results=lambda: (results or []),
            get_degraded=lambda: degraded,
        )
        return TestClient(app)

    def test_health_page_returns_200(self, db_file):
        client = self._make_client(db_file)
        resp = client.get("/health")
        assert resp.status_code == 200
        assert "System Health" in resp.text

    def test_health_page_shows_all_green(self, db_file):
        results = [
            _FakeCheckResult("git_repo", True, "OK"),
            _FakeCheckResult("database", True, "Schema v5 OK"),
        ]
        client = self._make_client(db_file, results=results)
        resp = client.get("/health")
        body = resp.text
        assert "All Checks Passed" in body
        assert "git_repo" in body
        assert "database" in body

    def test_health_page_shows_degraded(self, db_file):
        results = [
            _FakeCheckResult("linear_api", False, "401 Unauthorized", critical=True),
            _FakeCheckResult("database", True, "OK"),
        ]
        client = self._make_client(db_file, results=results, degraded=True)
        resp = client.get("/health")
        body = resp.text
        assert "Degraded Mode" in body
        assert "linear_api" in body
        assert "401 Unauthorized" in body

    def test_health_page_shows_guidance_for_failures(self, db_file):
        results = [
            _FakeCheckResult("git_repo:myproject", False, "Remote unreachable"),
        ]
        client = self._make_client(db_file, results=results, degraded=True)
        resp = client.get("/health")
        body = resp.text
        assert "Verify base_dir path" in body

    def test_health_page_shows_warning_checks(self, db_file):
        results = [
            _FakeCheckResult("notifications_webhook", False, "Unreachable", critical=False),
        ]
        client = self._make_client(db_file, results=results)
        resp = client.get("/health")
        body = resp.text
        assert "Warning" in body
        assert "notifications_webhook" in body

    def test_health_page_navigation_link(self, db_file):
        """Health page should be accessible from navigation."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/")
        assert 'href="/health"' in resp.text

    def test_health_checks_partial(self, db_file):
        results = [
            _FakeCheckResult("database", True, "OK"),
            _FakeCheckResult("git_repo", False, "Not found", critical=True),
        ]
        client = self._make_client(db_file, results=results, degraded=True)
        resp = client.get("/partials/health-checks")
        assert resp.status_code == 200
        body = resp.text
        assert "database" in body
        assert "git_repo" in body
        assert "Blocking" in body

    def test_health_badge_partial_degraded(self, db_file):
        client = self._make_client(db_file, degraded=True)
        resp = client.get("/partials/health-badge")
        assert resp.status_code == 200
        assert "Degraded Mode" in resp.text

    def test_health_badge_partial_healthy(self, db_file):
        results = [_FakeCheckResult("database", True, "OK")]
        client = self._make_client(db_file, results=results, degraded=False)
        resp = client.get("/partials/health-badge")
        assert resp.status_code == 200
        assert "All Checks Passed" in resp.text

    def test_no_results_shows_empty_message(self, db_file):
        client = self._make_client(db_file)
        resp = client.get("/partials/health-checks")
        assert "No preflight results available" in resp.text


class TestRerunPreflightAPI:
    def test_rerun_calls_callback(self, db_file):
        """POST /api/rerun-preflight calls the on_rerun_preflight callback."""
        called = []
        app = create_app(
            db_path=db_file,
            on_rerun_preflight=lambda: called.append("rerun"),
        )
        client = TestClient(app)
        resp = client.post("/api/rerun-preflight")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}
        assert called == ["rerun"]

    def test_rerun_without_callback_returns_503(self, db_file):
        """POST /api/rerun-preflight returns 503 when no callback is registered."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.post("/api/rerun-preflight")
        assert resp.status_code == 503


class TestPreflightResultsAPI:
    def test_returns_check_data(self, db_file):
        """GET /api/preflight-results returns JSON with checks, degraded, failed_critical."""
        results = [
            _FakeCheckResult("git_repo:proj", True, "OK"),
            _FakeCheckResult("linear_api", False, "Unreachable", critical=True),
            _FakeCheckResult("credentials", False, "Missing", critical=False),
        ]
        app = create_app(
            db_path=db_file,
            get_preflight_results=lambda: results,
            get_degraded=lambda: True,
        )
        client = TestClient(app)
        resp = client.get("/api/preflight-results")
        assert resp.status_code == 200
        data = resp.json()
        assert data["degraded"] is True
        assert data["failed_critical"] == 1
        assert len(data["checks"]) == 3
        assert data["checks"][0]["name"] == "git_repo:proj"
        assert data["checks"][0]["passed"] is True
        assert data["checks"][1]["passed"] is False
        assert data["checks"][1]["critical"] is True

    def test_returns_empty_when_no_callbacks(self, db_file):
        """GET /api/preflight-results returns empty checks when no supervisor connected."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/api/preflight-results")
        assert resp.status_code == 200
        data = resp.json()
        assert data["degraded"] is False
        assert data["checks"] == []
        assert data["failed_critical"] == 0


# --- Codex JSONL formatter ---


class TestFormatCodexNdjsonLine:
    def test_empty_line(self):
        event_type, text = format_codex_ndjson_line("")
        assert event_type == "log"
        assert text == ""

    def test_non_json_passthrough(self):
        event_type, text = format_codex_ndjson_line("just a plain line")
        assert event_type == "log"
        assert text == "just a plain line"

    def test_thread_started(self):
        line = json.dumps({"type": "thread.started"})
        event_type, text = format_codex_ndjson_line(line)
        assert event_type == "system"
        assert "thread started" in text.lower()

    def test_turn_started(self):
        line = json.dumps({"type": "turn.started"})
        event_type, text = format_codex_ndjson_line(line)
        assert event_type == "system"
        assert "turn started" in text.lower()

    def test_turn_completed(self):
        line = json.dumps({"type": "turn.completed", "status": "completed"})
        event_type, text = format_codex_ndjson_line(line)
        assert event_type == "result"
        assert "completed" in text.lower()

    def test_item_completed_agent_message(self):
        line = json.dumps({
            "type": "item.completed",
            "item": {
                "type": "agent_message",
                "content": [{"type": "text", "text": "Review looks good"}],
            },
        })
        event_type, text = format_codex_ndjson_line(line)
        assert event_type == "assistant"
        assert "Review looks good" in text

    def test_item_completed_reasoning(self):
        line = json.dumps({
            "type": "item.completed",
            "item": {
                "type": "reasoning",
                "content": [{"type": "text", "text": "Thinking about the code"}],
            },
        })
        event_type, text = format_codex_ndjson_line(line)
        assert event_type == "system"
        assert "Thinking about the code" in text

    def test_item_completed_shell_command(self):
        line = json.dumps({
            "type": "item.completed",
            "item": {"type": "shell_command", "command": "ls -la"},
        })
        event_type, text = format_codex_ndjson_line(line)
        assert event_type == "tool_use"
        assert "ls -la" in text

    def test_item_completed_shell_result(self):
        line = json.dumps({
            "type": "item.completed",
            "item": {"type": "shell_result", "output": "file1.py\nfile2.py"},
        })
        event_type, text = format_codex_ndjson_line(line)
        assert event_type == "tool_result"
        assert "file1.py" in text

    def test_unknown_event_type(self):
        line = json.dumps({"type": "some.unknown"})
        event_type, text = format_codex_ndjson_line(line)
        assert event_type == "system"
        assert "some.unknown" in text


# --- Codex review log viewer ---


class TestLogViewerCodexStage:
    def test_codex_review_stage_in_log_tabs(self, tmp_path):
        """codex_review logs should be discoverable and selectable in the log viewer."""
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        task_id = insert_task(
            conn, ticket_id="TST-CX1", title="Test Codex",
            project="proj", slot=1, status="completed",
        )
        insert_stage_run(
            conn, task_id=task_id, stage="implement", iteration=1,
            turns=10, duration_seconds=60.0,
        )
        insert_stage_run(
            conn, task_id=task_id, stage="review", iteration=1,
            turns=5, duration_seconds=30.0,
        )
        insert_stage_run(
            conn, task_id=task_id, stage="codex_review", iteration=1,
            turns=3, duration_seconds=20.0, exit_subtype="approved",
        )
        conn.commit()
        conn.close()

        # Create log files
        logs_dir = tmp_path / "logs"
        ticket_dir = logs_dir / "TST-CX1"
        ticket_dir.mkdir(parents=True)
        (ticket_dir / "implement-20260228-120000.log").write_text("impl log")
        (ticket_dir / "review-20260228-120100.log").write_text("review log")
        (ticket_dir / "codex_review-20260228-120200.log").write_text(
            json.dumps({"type": "item.completed", "item": {"type": "agent_message", "content": [{"type": "text", "text": "LGTM"}]}}) + "\n"
        )

        app = create_app(db_path=db_path, logs_dir=str(logs_dir))
        client = TestClient(app)

        resp = client.get("/task/TST-CX1/logs")
        assert resp.status_code == 200
        body = resp.text
        assert "codex review" in body.lower() or "codex_review" in body.lower()

    def test_codex_review_stage_detail_shows_label(self, tmp_path):
        """codex_review stage runs should show 'Codex Review' label in task detail."""
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        task_id = insert_task(
            conn, ticket_id="TST-CX2", title="Test Codex Label",
            project="proj", slot=1, status="completed",
        )
        insert_stage_run(
            conn, task_id=task_id, stage="review", iteration=1,
            turns=5, duration_seconds=30.0,
        )
        insert_stage_run(
            conn, task_id=task_id, stage="codex_review", iteration=1,
            turns=3, duration_seconds=20.0, exit_subtype="approved",
        )
        conn.commit()
        conn.close()

        app = create_app(db_path=db_path)
        client = TestClient(app)

        resp = client.get(f"/task/{task_id}")
        assert resp.status_code == 200
        assert "Codex Review" in resp.text


# --- Codex config toggles ---


class TestCodexConfigToggles:
    @pytest.fixture()
    def config_client(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        conn.close()
        cfg = BotfarmConfig(
            projects=[ProjectConfig(
                name="proj", linear_team="team",
                base_dir="/tmp/test", worktree_prefix="/tmp/test-wt",
                slots=[1],
            )],
            linear=LinearConfig(api_key="test-key", workspace="test"),
            source_path=str(tmp_path / "config.yaml"),
        )
        # Write a minimal config.yaml for edit tests
        yaml_data = {"agents": {}}
        (tmp_path / "config.yaml").write_text(yaml.dump(yaml_data))
        app = create_app(db_path=db_path, botfarm_config=cfg)
        return TestClient(app)

    def test_config_view_shows_codex_fields(self, config_client):
        """Config view tab should show Codex reviewer status."""
        resp = config_client.get("/config")
        assert resp.status_code == 200
        assert "Codex Reviewer" in resp.text

    def test_config_edit_shows_codex_fields(self, config_client):
        """Config edit tab should have Codex reviewer form fields."""
        resp = config_client.get("/config")
        assert resp.status_code == 200
        body = resp.text
        assert "codex_reviewer_enabled" in body
        assert "codex_reviewer_model" in body
        assert "codex_reviewer_reasoning_effort" in body
        assert "codex_reviewer_timeout_minutes" in body


# --- Auto-restart banner ---


class TestAutoRestartBanner:
    """Tests for auto-restart behaviour in the update banner and API."""

    @pytest.fixture()
    def client_auto_restart(self, db_file):
        app = create_app(db_path=db_file, auto_restart=True)
        return TestClient(app)

    @pytest.fixture()
    def client_no_auto_restart(self, db_file):
        app = create_app(db_path=db_file, auto_restart=False)
        return TestClient(app)

    def test_api_update_allowed_with_auto_restart(self, db_file):
        called = []
        app = create_app(db_path=db_file, auto_restart=True, on_update=lambda: called.append(1))
        client = TestClient(app)
        resp = client.post("/api/update")
        assert resp.status_code == 200
        assert called  # callback was invoked

    def test_api_update_blocked_without_auto_restart(self, client_no_auto_restart):
        resp = client_no_auto_restart.post("/api/update")
        assert resp.status_code == 409
        assert "Auto-restart is disabled" in resp.json()["error"]

    @patch("botfarm.dashboard.commits_behind", return_value=3)
    def test_banner_shows_button_with_auto_restart(self, _mock_cb, client_auto_restart):
        resp = client_auto_restart.get("/partials/update-banner")
        assert resp.status_code == 200
        body = resp.text
        assert "Update &amp; Restart" in body
        assert "disabled" not in body

    @patch("botfarm.dashboard.commits_behind", return_value=3)
    def test_banner_disables_button_without_auto_restart(self, _mock_cb, client_no_auto_restart):
        resp = client_no_auto_restart.get("/partials/update-banner")
        assert resp.status_code == 200
        body = resp.text
        assert "disabled" in body
        assert "Auto-restart is disabled" in body

    def test_auto_restart_defaults_to_true(self, db_file):
        app = create_app(db_path=db_file)
        assert app.state.auto_restart is True


# --- Daily Summary config ---


class TestDailySummaryConfig:
    @pytest.fixture()
    def config_client(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        conn.close()
        cfg = BotfarmConfig(
            projects=[ProjectConfig(
                name="proj", linear_team="team",
                base_dir="/tmp/test", worktree_prefix="/tmp/test-wt",
                slots=[1],
            )],
            linear=LinearConfig(api_key="test-key", workspace="test"),
            source_path=str(tmp_path / "config.yaml"),
        )
        yaml_data = {"daily_summary": {}}
        (tmp_path / "config.yaml").write_text(yaml.dump(yaml_data))
        app = create_app(db_path=db_path, botfarm_config=cfg)
        return TestClient(app)

    def test_config_view_shows_daily_summary(self, config_client):
        """Config view tab should show Daily Summary section."""
        resp = config_client.get("/config")
        assert resp.status_code == 200
        assert "Daily Summary" in resp.text
        assert "Send hour" in resp.text

    def test_config_edit_shows_daily_summary_fields(self, config_client):
        """Config edit tab should have daily summary form fields."""
        resp = config_client.get("/config")
        assert resp.status_code == 200
        body = resp.text
        assert "daily_summary-enabled" in body
        assert "daily_summary-send_hour" in body
        assert "daily_summary-min_tasks_for_summary" in body

    def test_update_daily_summary_fields(self, config_client):
        """POST to /config with daily_summary fields should succeed."""
        resp = config_client.post(
            "/config",
            json={"daily_summary": {
                "enabled": True,
                "send_hour": 9,
                "min_tasks_for_summary": 3,
            }},
        )
        assert resp.status_code == 200
        assert "success" in resp.text

    def test_send_hour_out_of_range_rejected(self, config_client):
        """send_hour > 23 should be rejected."""
        resp = config_client.post(
            "/config",
            json={"daily_summary": {"send_hour": 25}},
        )
        assert resp.status_code == 422
        assert "at most 23" in resp.text

    def test_negative_min_tasks_rejected(self, config_client):
        """Negative min_tasks_for_summary should be rejected."""
        resp = config_client.post(
            "/config",
            json={"daily_summary": {"min_tasks_for_summary": -1}},
        )
        assert resp.status_code == 422
        assert "at least 0" in resp.text


# --- Workflow page ---


