"""Tests for _parse_qa_report in worker_claude.py."""

from botfarm.worker_claude import _parse_qa_report


class TestParseQaReport:
    """Tests for parsing QA agent output."""

    def test_full_report_with_bugs_and_failed_verdict(self):
        text = (
            "QA_REPORT_START\n"
            "## QA Report\n"
            "Tested login flow and found issues.\n"
            "Verdict: FAILED\n"
            "QA_REPORT_END\n"
            "\n"
            "BUG_START\n"
            "Title: Login button unresponsive on mobile\n"
            "Severity: High\n"
            "Description:\n"
            "The login button does not respond to tap events on iOS Safari.\n"
            "BUG_END\n"
            "\n"
            "BUG_START\n"
            "Title: Missing error message for invalid email\n"
            "Severity: Medium\n"
            "Description:\n"
            "No validation error is shown when an invalid email is entered.\n"
            "BUG_END\n"
        )
        report_text, bugs, passed = _parse_qa_report(text)
        assert report_text is not None
        assert "Tested login flow" in report_text
        assert passed is False
        assert len(bugs) == 2
        assert bugs[0]["title"] == "Login button unresponsive on mobile"
        assert bugs[0]["severity"] == "high"
        assert "iOS Safari" in bugs[0]["description"]
        assert bugs[1]["title"] == "Missing error message for invalid email"
        assert bugs[1]["severity"] == "medium"

    def test_passed_verdict_no_bugs(self):
        text = (
            "QA_REPORT_START\n"
            "All tests passed successfully.\n"
            "Verdict: PASSED\n"
            "QA_REPORT_END\n"
        )
        report_text, bugs, passed = _parse_qa_report(text)
        assert report_text is not None
        assert "All tests passed" in report_text
        assert passed is True
        assert bugs == []

    def test_no_markers_at_all(self):
        text = "Some random agent output with no markers."
        report_text, bugs, passed = _parse_qa_report(text)
        assert report_text is None
        assert bugs == []
        assert passed is None

    def test_empty_string(self):
        report_text, bugs, passed = _parse_qa_report("")
        assert report_text is None
        assert bugs == []
        assert passed is None

    def test_report_markers_without_bug_markers(self):
        text = (
            "QA_REPORT_START\n"
            "Everything looks good.\n"
            "QA_REPORT_END\n"
        )
        report_text, bugs, passed = _parse_qa_report(text)
        assert report_text == "Everything looks good."
        assert bugs == []
        # No explicit verdict → stays ambiguous even with no bugs
        assert passed is None

    def test_no_verdict_with_bugs_stays_ambiguous(self):
        """When report exists but no Verdict: line, passed stays None."""
        text = (
            "QA_REPORT_START\n"
            "Tested the feature.\n"
            "QA_REPORT_END\n"
            "BUG_START\n"
            "Title: Something is broken\n"
            "Severity: Low\n"
            "Description:\n"
            "It breaks in an edge case.\n"
            "BUG_END\n"
        )
        report_text, bugs, passed = _parse_qa_report(text)
        assert report_text is not None
        assert len(bugs) == 1
        # No explicit verdict → stays ambiguous even with bugs present
        assert passed is None

    def test_case_insensitive_markers(self):
        text = (
            "qa_report_start\n"
            "Report content here.\n"
            "Verdict: passed\n"
            "qa_report_end\n"
            "bug_start\n"
            "Title: A bug\n"
            "Severity: Low\n"
            "Description:\n"
            "Details.\n"
            "bug_end\n"
        )
        report_text, bugs, passed = _parse_qa_report(text)
        assert report_text is not None
        assert "Report content here" in report_text
        # Explicit verdict says PASSED
        assert passed is True
        assert len(bugs) == 1
        assert bugs[0]["title"] == "A bug"

    def test_malformed_bug_partial_fields(self):
        """Bug block with only a title (no severity/description)."""
        text = (
            "BUG_START\n"
            "Title: Incomplete bug report\n"
            "BUG_END\n"
        )
        report_text, bugs, passed = _parse_qa_report(text)
        assert report_text is None
        assert len(bugs) == 1
        assert bugs[0] == {"title": "Incomplete bug report"}
        assert "severity" not in bugs[0]
        assert "description" not in bugs[0]

    def test_empty_bug_block(self):
        """Bug block with no parseable fields is skipped."""
        text = (
            "BUG_START\n"
            "\n"
            "BUG_END\n"
        )
        report_text, bugs, passed = _parse_qa_report(text)
        assert bugs == []

    def test_missing_end_marker(self):
        """QA_REPORT_START without QA_REPORT_END — no report extracted."""
        text = (
            "QA_REPORT_START\n"
            "This report never ends.\n"
        )
        report_text, bugs, passed = _parse_qa_report(text)
        assert report_text is None
        assert passed is None

    def test_verdict_in_full_text_fallback(self):
        """When no report markers, verdict is searched in full text."""
        text = (
            "I tested everything.\n"
            "Verdict: FAILED\n"
            "Please see the bugs below.\n"
        )
        report_text, bugs, passed = _parse_qa_report(text)
        assert report_text is None
        assert passed is False

    def test_multiple_bugs_various_severities(self):
        text = (
            "QA_REPORT_START\n"
            "Found three bugs.\n"
            "Verdict: FAILED\n"
            "QA_REPORT_END\n"
            "BUG_START\n"
            "Title: Critical crash\n"
            "Severity: Critical\n"
            "Description:\n"
            "App crashes on start.\n"
            "BUG_END\n"
            "BUG_START\n"
            "Title: Typo in header\n"
            "Severity: Low\n"
            "Description:\n"
            "Minor typo.\n"
            "BUG_END\n"
            "BUG_START\n"
            "Title: Slow response\n"
            "Severity: Medium\n"
            "Description:\n"
            "API takes 5s to respond.\n"
            "BUG_END\n"
        )
        report_text, bugs, passed = _parse_qa_report(text)
        assert passed is False
        assert len(bugs) == 3
        assert bugs[0]["severity"] == "critical"
        assert bugs[1]["severity"] == "low"
        assert bugs[2]["severity"] == "medium"

    def test_description_multiline(self):
        """Description field captures multiple lines."""
        text = (
            "BUG_START\n"
            "Title: Complex bug\n"
            "Severity: High\n"
            "Description:\n"
            "Line one of description.\n"
            "Line two of description.\n"
            "\n"
            "Line four after blank.\n"
            "BUG_END\n"
        )
        _, bugs, _ = _parse_qa_report(text)
        assert len(bugs) == 1
        desc = bugs[0]["description"]
        assert "Line one" in desc
        assert "Line two" in desc
        assert "Line four" in desc
