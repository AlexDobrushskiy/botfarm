"""Jira REST API v2 client implementing the abstract BugtrackerClient."""

from __future__ import annotations

import base64
import logging
import time

import httpx

from botfarm.bugtracker.base import BugtrackerClient
from botfarm.bugtracker.errors import BugtrackerError
from botfarm.bugtracker.types import Comment, CreatedIssue, Issue, IssueDetails

logger = logging.getLogger(__name__)


class JiraAPIError(BugtrackerError):
    """Raised when a Jira REST API call fails."""


class JiraClient(BugtrackerClient):
    """Low-level HTTP client for the Jira REST API v2.

    Supports both Jira Cloud (Basic auth with email + API token) and
    Jira Server/Data Center (Bearer auth with personal access token).
    """

    def __init__(
        self,
        url: str,
        email: str = "",
        api_token: str = "",
    ) -> None:
        self._base_url = url.rstrip("/")
        self._email = email
        self._api_token = api_token
        self._rank_field_id: str | None = None
        self._rank_field_checked = False

        # Build auth header
        if email:
            # Jira Cloud: Basic auth
            creds = base64.b64encode(f"{email}:{api_token}".encode()).decode()
            self._auth_header = f"Basic {creds}"
        else:
            # Jira Server/DC: Bearer auth
            self._auth_header = f"Bearer {api_token}"

    _MAX_RETRIES = 3

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict | None = None,
        params: dict | None = None,
        _retry_count: int = 0,
    ) -> httpx.Response:
        """Execute an HTTP request against the Jira API.

        Handles 429 rate limiting with Retry-After header (up to _MAX_RETRIES).
        Raises JiraAPIError on failures.
        """
        url = f"{self._base_url}/rest/api/2{path}"
        headers = {
            "Authorization": self._auth_header,
            "Content-Type": "application/json",
        }

        try:
            response = httpx.request(
                method,
                url,
                json=json,
                params=params,
                headers=headers,
                timeout=30.0,
            )
        except httpx.HTTPError as exc:
            raise JiraAPIError(f"HTTP request failed: {exc}") from exc

        if response.status_code == 429:
            if _retry_count >= self._MAX_RETRIES:
                raise JiraAPIError(
                    f"Rate limited after {self._MAX_RETRIES} retries"
                )
            retry_after = response.headers.get("Retry-After", "5")
            try:
                wait = int(retry_after)
            except ValueError:
                wait = 5
            logger.warning("Jira rate limit hit, retrying after %ds", wait)
            time.sleep(wait)
            return self._request(
                method, path, json=json, params=params,
                _retry_count=_retry_count + 1,
            )

        if response.status_code >= 400:
            raise JiraAPIError(
                f"HTTP {response.status_code}: {response.text[:300]}"
            )

        return response

    def _get_json(self, path: str, *, params: dict | None = None) -> dict:
        """GET request returning parsed JSON."""
        resp = self._request("GET", path, params=params)
        return resp.json()

    def _post_json(self, path: str, *, json: dict) -> dict:
        """POST request with JSON body returning parsed JSON."""
        resp = self._request("POST", path, json=json)
        return resp.json()

    def _ensure_rank_field(self) -> None:
        """Discover the Rank custom field ID on first use."""
        if self._rank_field_checked:
            return
        self._rank_field_checked = True
        try:
            fields = self._request("GET", "/field").json()
            for f in fields:
                if f.get("name") == "Rank":
                    self._rank_field_id = f["id"]
                    logger.debug("Discovered Rank field: %s", self._rank_field_id)
                    return
            logger.info("Rank field not found; using fallback ordering")
        except JiraAPIError:
            logger.warning("Failed to discover Rank field; using fallback ordering")

    # --- Required BugtrackerClient methods ---

    def fetch_team_issues(
        self,
        team_key: str,
        status_name: str = "Todo",
        first: int = 50,
        project_name: str = "",
    ) -> list[Issue]:
        if project_name:
            logger.warning(
                "Jira does not support sub-project filtering; "
                "project_name=%r is ignored (team_key=%r already selects the project)",
                project_name,
                team_key,
            )

        self._ensure_rank_field()

        jql = f'project = "{team_key}" AND status = "{status_name}"'
        if self._rank_field_id:
            order_by = "ORDER BY Rank ASC"
        else:
            order_by = "ORDER BY priority DESC, created ASC"
        jql = f"{jql} {order_by}"

        fields_list = ["summary", "status", "labels", "assignee", "priority",
                       "issuelinks", "parent", "subtasks"]
        if self._rank_field_id:
            fields_list.append(self._rank_field_id)

        issues: list[Issue] = []
        next_page_token: str | None = None
        while True:
            body: dict = {
                "jql": jql,
                "fields": fields_list,
                "maxResults": first,
            }
            if next_page_token is not None:
                body["nextPageToken"] = next_page_token
            data = self._post_json("/search/jql", json=body)
            page_issues = data.get("issues", [])
            for idx, item in enumerate(page_issues):
                fields_data = item.get("fields", {})

                # Parse blocked_by from issuelinks
                blocked_by: list[str] = []
                for link in fields_data.get("issuelinks", []):
                    link_type = link.get("type", {})
                    # "Blocks" link type: inwardIssue blocks outwardIssue
                    # If inwardIssue exists, it means something blocks this issue
                    if link_type.get("name") == "Blocks" and "inwardIssue" in link:
                        inward = link["inwardIssue"]
                        inward_status = (
                            inward.get("fields", {})
                            .get("status", {})
                            .get("statusCategory", {})
                            .get("key", "")
                        )
                        if inward_status != "done":
                            blocked_by.append(inward.get("key", ""))

                # Parse children (subtasks)
                children_states: list[tuple[str, str]] | None = None
                subtasks = fields_data.get("subtasks")
                if subtasks:
                    children_states = []
                    for sub in subtasks:
                        sub_key = sub.get("key", "")
                        sub_status = sub.get("fields", {}).get("status", {})
                        sub_cat = (
                            sub_status
                            .get("statusCategory", {})
                            .get("key", "")
                        )
                        sub_name = sub_status.get("name", "").lower()
                        children_states.append(
                            (sub_key, self._map_status_category(sub_cat, sub_name))
                        )

                sort_order = float(len(issues) + idx)

                assignee = fields_data.get("assignee") or {}
                priority_obj = fields_data.get("priority") or {}
                # Jira priorities: Highest=1, High=2, Medium=3, Low=4, Lowest=5
                # Map to 0-4 scale: missing/unknown=4 (lowest)
                priority_id = priority_obj.get("id", "3")
                try:
                    priority = int(priority_id)
                except (ValueError, TypeError):
                    priority = 3

                labels = fields_data.get("labels", [])

                issues.append(
                    Issue(
                        id=item.get("id", ""),
                        identifier=item.get("key", ""),
                        title=fields_data.get("summary", ""),
                        priority=priority,
                        url=f"{self._base_url}/browse/{item.get('key', '')}",
                        assignee_id=assignee.get("accountId") or assignee.get("name"),
                        assignee_email=assignee.get("emailAddress"),
                        labels=labels if labels else None,
                        sort_order=sort_order,
                        blocked_by=blocked_by if blocked_by else None,
                        children_states=children_states,
                    )
                )

            next_page_token = data.get("nextPageToken")
            if not page_issues or not next_page_token:
                break

        return issues

    def update_issue_state(self, issue_id: str, state_id: str) -> None:
        """Move an issue via Jira's transition-based workflow.

        ``state_id`` here is the target status *name* (not a numeric ID),
        because the base interface uses opaque string IDs and
        ``get_team_states()`` returns ``{name: name}`` for Jira.
        """
        target_status = state_id

        # Discover available transitions (expand fields to check screen config)
        data = self._get_json(
            f"/issue/{issue_id}/transitions",
            params={"expand": "transitions.fields"},
        )
        transitions = data.get("transitions", [])

        matching = None
        for t in transitions:
            if t.get("to", {}).get("name", "").lower() == target_status.lower():
                matching = t
                break

        if not matching:
            available = [
                t.get("to", {}).get("name", "?") for t in transitions
            ]
            raise JiraAPIError(
                f"No transition to '{target_status}' for issue {issue_id}. "
                f"Available transitions: {available}. "
                f"The service account may lack permission for this transition."
            )

        body: dict = {"transition": {"id": matching["id"]}}

        # Set resolution when transitioning to Done-category status, but only
        # if the resolution field is on the transition screen.  Some Jira
        # projects (especially team-managed ones) don't expose resolution on
        # the screen — Jira auto-sets it.  Sending it anyway causes HTTP 400:
        # "Field 'resolution' cannot be set."
        to_category = matching.get("to", {}).get("statusCategory", {}).get("key", "")
        transition_fields = matching.get("fields", {})
        if to_category == "done" and "resolution" in transition_fields:
            body["fields"] = {"resolution": {"name": "Done"}}

        self._request("POST", f"/issue/{issue_id}/transitions", json=body)

    def get_team_states(self, team_key: str) -> dict[str, str]:
        """Return ``{status_name: status_name}`` for a Jira project.

        Unlike Linear (where state IDs are UUIDs), Jira transitions are
        discovered dynamically. We return name→name so callers can pass the
        name as the "ID" to ``update_issue_state``.
        """
        data = self._get_json(f"/project/{team_key}/statuses")
        states: dict[str, str] = {}
        for issue_type in data:
            for status in issue_type.get("statuses", []):
                name = status.get("name", "")
                if name and name not in states:
                    states[name] = name
        return states

    def add_comment(self, issue_id: str, body: str) -> None:
        self._request(
            "POST",
            f"/issue/{issue_id}/comment",
            json={"body": body},
        )

    def get_viewer_id(self) -> str:
        data = self._get_json("/myself")
        # Cloud uses accountId, Server/DC uses name
        viewer_id = data.get("accountId") or data.get("name")
        if not viewer_id:
            raise JiraAPIError("Failed to retrieve viewer ID from /myself")
        return viewer_id

    def assign_issue(self, issue_id: str, assignee_id: str) -> None:
        if self._email:
            assignee_field = {"accountId": assignee_id}
        else:
            assignee_field = {"name": assignee_id}
        self._request(
            "PUT",
            f"/issue/{issue_id}",
            json={"fields": {"assignee": assignee_field}},
        )

    def add_labels(self, issue_id: str, label_ids: list[str]) -> None:
        """Add labels to an issue. In Jira, label_ids are label name strings."""
        update_ops = [{"add": label} for label in label_ids]
        self._request(
            "PUT",
            f"/issue/{issue_id}",
            json={"update": {"labels": update_ops}},
        )

    def fetch_issue_labels(self, identifier: str) -> tuple[str, list[str]] | None:
        try:
            data = self._get_json(
                f"/issue/{identifier}",
                params={"fields": "summary,labels"},
            )
        except JiraAPIError:
            logger.warning("Failed to fetch labels for issue %s", identifier)
            return None
        fields = data.get("fields", {})
        title = fields.get("summary", "")
        labels = fields.get("labels", [])
        return title, labels

    def fetch_issue_state_type(self, identifier: str) -> str | None:
        try:
            data = self._get_json(
                f"/issue/{identifier}",
                params={"fields": "status"},
            )
        except JiraAPIError:
            logger.warning("Failed to fetch state for issue %s", identifier)
            return None
        fields = data.get("fields", {})
        status = fields.get("status", {})
        status_name = status.get("name", "").lower()
        category_key = status.get("statusCategory", {}).get("key", "")
        return self._map_status_category(category_key, status_name)

    def fetch_issue_details(self, identifier: str) -> IssueDetails:
        data = self._get_json(f"/issue/{identifier}")
        if not data or "fields" not in data:
            raise JiraAPIError(f"Issue '{identifier}' not found")

        fields = data.get("fields", {})
        status = fields.get("status", {})
        assignee = fields.get("assignee") or {}
        creator = fields.get("creator") or {}
        project = fields.get("project") or {}
        parent = fields.get("parent") or {}
        priority_obj = fields.get("priority") or {}
        try:
            priority = int(priority_obj.get("id", "3"))
        except (ValueError, TypeError):
            priority = 3

        # Parse blocked_by / blocks from issuelinks
        blocked_by: list[str] = []
        blocks: list[str] = []
        for link in fields.get("issuelinks", []):
            link_type = link.get("type", {})
            if link_type.get("name") == "Blocks":
                if "inwardIssue" in link:
                    blocked_by.append(link["inwardIssue"].get("key", ""))
                if "outwardIssue" in link:
                    blocks.append(link["outwardIssue"].get("key", ""))

        # Parse subtasks for children_ids
        children_ids = [
            sub.get("key", "") for sub in fields.get("subtasks", [])
        ]

        # Parse comments
        comment_data = fields.get("comment", {})
        comment_nodes = comment_data.get("comments", []) if isinstance(comment_data, dict) else []
        comments = [
            Comment(
                body=c.get("body", ""),
                author=(c.get("author") or {}).get("displayName", ""),
                created_at=c.get("created", ""),
            )
            for c in comment_nodes
        ]

        labels = fields.get("labels", [])
        key = data.get("key", identifier)

        return IssueDetails(
            id=data.get("id", ""),
            ticket_id=key,
            title=fields.get("summary", ""),
            description=fields.get("description"),
            status=status.get("name"),
            priority=priority,
            url=f"{self._base_url}/browse/{key}",
            assignee_name=assignee.get("displayName"),
            assignee_email=assignee.get("emailAddress"),
            creator_name=creator.get("displayName"),
            project_name=project.get("name"),
            team_name=project.get("name"),
            estimate=fields.get("timeoriginalestimate"),
            due_date=fields.get("duedate"),
            parent_id=parent.get("key"),
            children_ids=children_ids,
            blocked_by=blocked_by,
            blocks=blocks,
            labels=labels,
            comments=comments,
            created_at=fields.get("created"),
            updated_at=fields.get("updated"),
            completed_at=fields.get("resolutiondate"),
            raw=data,
        )

    def get_team_id(self, team_key: str) -> str:
        data = self._get_json(f"/project/{team_key}")
        project_id = data.get("id")
        if not project_id:
            raise JiraAPIError(f"Project '{team_key}' not found")
        return project_id

    def get_label_id(self, team_key: str, label_name: str) -> str | None:
        """In Jira, labels are plain strings — return the name as-is if it exists."""
        # Labels are global strings in Jira, no lookup needed
        return label_name

    def get_or_create_label(self, team_key: str, label_name: str) -> str:
        """Labels are auto-created in Jira — return the name as the "ID"."""
        return label_name

    def create_issue(
        self,
        *,
        team_id: str,
        title: str,
        description: str = "",
        priority: int | None = None,
        label_ids: list[str] | None = None,
        project_id: str | None = None,
        state_id: str | None = None,
    ) -> CreatedIssue:
        fields: dict = {
            "project": {"id": team_id},
            "summary": title,
            "issuetype": {"name": "Task"},
        }
        if description:
            fields["description"] = description
        if priority is not None:
            fields["priority"] = {"id": str(priority)}
        if label_ids:
            fields["labels"] = label_ids
        # state_id is not used at creation in Jira — issues start in the
        # project's default status

        data = self._request("POST", "/issue", json={"fields": fields}).json()
        issue_id = data.get("id", "")
        issue_key = data.get("key", "")
        return CreatedIssue(
            id=issue_id,
            identifier=issue_key,
            url=f"{self._base_url}/browse/{issue_key}",
        )

    # --- Helpers ---

    @staticmethod
    def _map_status_category(
        category_key: str, status_name: str = ""
    ) -> str:
        """Map Jira statusCategory.key to the canonical state type.

        Jira has no native "canceled" state — detect it by checking
        the status name for common canceled variants.
        """
        if category_key == "done":
            if status_name and status_name.lower() in ("canceled", "cancelled"):
                return "canceled"
            return "completed"
        if category_key == "new":
            return "unstarted"
        if category_key == "indeterminate":
            return "started"
        return category_key
