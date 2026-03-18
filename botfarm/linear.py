# Backward-compat — will be removed in SMA-466
from botfarm.bugtracker.linear.client import LINEAR_API_URL, LinearAPIError, LinearClient
from botfarm.bugtracker.linear.poller import LinearPoller, create_pollers
from botfarm.bugtracker.linear.queries import (
    ACTIVE_ISSUES_COUNT_QUERY,
    ACTIVE_ISSUES_FOR_PROJECT_COUNT_QUERY,
    ISSUE_DETAILS_QUERY,
    ISSUES_QUERY,
    ISSUES_WITH_PROJECT_QUERY,
)
from botfarm.bugtracker.types import ActiveIssuesCount, Issue as LinearIssue, PollResult
