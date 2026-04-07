"""GraphQL query and mutation strings for the Linear API."""

_ISSUE_FIELDS = """
      id
      identifier
      title
      priority
      sortOrder
      url
      assignee {
        id
        email
      }
      labels {
        nodes {
          name
        }
      }
      relations {
        nodes {
          type
          relatedIssue {
            identifier
            state { type }
          }
        }
      }
      inverseRelations {
        nodes {
          type
          issue {
            identifier
            state { type }
          }
        }
      }
      children {
        nodes {
          identifier
          state { type name }
        }
      }
"""

ISSUES_QUERY = """
query TeamTodoIssues($teamKey: String!, $statusName: String!, $first: Int!) {
  issues(
    filter: {
      team: { key: { eq: $teamKey } }
      state: { name: { eq: $statusName } }
    }
    first: $first
    orderBy: createdAt
  ) {
    nodes {
""" + _ISSUE_FIELDS + """
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

ISSUES_WITH_PROJECT_QUERY = """
query TeamProjectTodoIssues($teamKey: String!, $statusName: String!, $projectName: String!, $first: Int!) {
  issues(
    filter: {
      team: { key: { eq: $teamKey } }
      state: { name: { eq: $statusName } }
      project: { name: { eq: $projectName } }
    }
    first: $first
    orderBy: createdAt
  ) {
    nodes {
""" + _ISSUE_FIELDS + """
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

UPDATE_STATE_MUTATION = """
mutation UpdateIssueState($issueId: String!, $stateId: String!) {
  issueUpdate(id: $issueId, input: { stateId: $stateId }) {
    success
    issue {
      id
      identifier
      state {
        name
      }
    }
  }
}
"""

TEAM_STATES_QUERY = """
query TeamStates($teamKey: String!) {
  teams(filter: { key: { eq: $teamKey } }) {
    nodes {
      id
      key
      states {
        nodes {
          id
          name
          type
        }
      }
    }
  }
}
"""

ADD_COMMENT_MUTATION = """
mutation AddComment($issueId: String!, $body: String!) {
  commentCreate(input: { issueId: $issueId, body: $body }) {
    success
  }
}
"""

VIEWER_QUERY = """
query Viewer {
  viewer {
    id
    name
  }
}
"""

ASSIGN_ISSUE_MUTATION = """
mutation AssignIssue($issueId: String!, $assigneeId: String!) {
  issueUpdate(id: $issueId, input: { assigneeId: $assigneeId }) {
    success
    issue {
      id
      identifier
      assignee {
        id
        name
      }
    }
  }
}
"""

ISSUE_LABELS_QUERY = """
query IssueLabels($issueId: String!) {
  issue(id: $issueId) {
    labels {
      nodes {
        id
        name
      }
    }
  }
}
"""

ADD_LABELS_MUTATION = """
mutation AddLabels($issueId: String!, $labelIds: [String!]!) {
  issueUpdate(id: $issueId, input: { labelIds: $labelIds }) {
    success
  }
}
"""

ISSUE_STATE_QUERY = """
query IssueState($identifier: String!) {
  issue(id: $identifier) {
    id
    identifier
    state {
      name
      type
    }
  }
}
"""

ISSUE_LABELS_BY_IDENTIFIER_QUERY = """
query IssueLabelsByIdentifier($identifier: String!) {
  issue(id: $identifier) {
    title
    labels {
      nodes {
        name
      }
    }
  }
}
"""

ACTIVE_ISSUES_COUNT_QUERY = """
query ActiveIssuesCount($first: Int!, $after: String) {
  issues(
    first: $first
    after: $after
    includeArchived: false
  ) {
    nodes {
      id
      project {
        name
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

ISSUE_DETAILS_QUERY = """
query IssueDetails($identifier: String!) {
  issue(id: $identifier) {
    id
    identifier
    title
    description
    priority
    url
    estimate
    dueDate
    createdAt
    updatedAt
    completedAt
    state { name }
    creator { name }
    assignee { name, email }
    project { name }
    team { name, key }
    parent { identifier, title }
    children { nodes { identifier } }
    labels { nodes { name } }
    relations {
      nodes {
        type
        relatedIssue { identifier }
      }
    }
    inverseRelations {
      nodes {
        type
        issue { identifier }
      }
    }
    comments(first: 50) {
      nodes {
        body
        user { name }
        createdAt
      }
    }
  }
}
"""

TEAM_LABELS_QUERY = """
query TeamLabels($teamKey: String!) {
  issueLabels(
    filter: {
      or: [
        { team: { key: { eq: $teamKey } } }
        { team: { null: true } }
      ]
    }
    first: 250
  ) {
    nodes { id name }
  }
}
"""

CREATE_LABEL_MUTATION = """
mutation CreateLabel($input: IssueLabelCreateInput!) {
  issueLabelCreate(input: $input) {
    success
    issueLabel { id name }
  }
}
"""

CREATE_ISSUE_MUTATION = """
mutation CreateIssue($input: IssueCreateInput!) {
  issueCreate(input: $input) {
    success
    issue {
      id
      identifier
      url
    }
  }
}
"""

ISSUES_BY_LABEL_QUERY = """
query IssuesByLabel($teamKey: String!, $labelName: String!, $first: Int!) {
  issues(
    first: $first
    includeArchived: false
    filter: {
      team: { key: { eq: $teamKey } }
      labels: { name: { eq: $labelName } }
      state: { type: { nin: ["completed", "canceled"] } }
    }
  ) {
    nodes {
      id
      identifier
      title
      state { type name }
    }
  }
}
"""

PROJECT_BY_NAME_QUERY = """
query ProjectByName($name: String!) {
  projects(filter: { name: { eq: $name } }, first: 1) {
    nodes {
      id
      name
    }
  }
}
"""

LIST_TEAMS_QUERY = """
query ListTeams {
  teams {
    nodes {
      id
      name
      key
    }
  }
}
"""

LIST_TEAM_PROJECTS_QUERY = """
query ListTeamProjects($teamId: String!) {
  team(id: $teamId) {
    projects {
      nodes {
        id
        name
      }
    }
  }
}
"""

ORGANIZATION_QUERY = """
query Organization {
  organization {
    urlKey
    name
  }
}
"""

CREATE_PROJECT_MUTATION = """
mutation CreateProject($input: ProjectCreateInput!) {
  projectCreate(input: $input) {
    success
    project {
      id
      name
    }
  }
}
"""

ACTIVE_ISSUES_FOR_PROJECT_COUNT_QUERY = """
query ActiveIssuesForProjectCount($first: Int!, $after: String, $projectName: String!) {
  issues(
    first: $first
    after: $after
    includeArchived: false
    filter: {
      project: { name: { eq: $projectName } }
    }
  ) {
    nodes {
      id
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""
