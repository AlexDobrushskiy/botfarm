# Archiving Linear Tickets

## When to Archive

Archive Done tickets periodically to keep the Linear workspace clean. Typical request: "archive the N oldest Done tickets".

## How to Find the Oldest Done Tickets

Use the Linear MCP `list_issues` tool with these parameters:
- `project`: `"Bot farm"` — filter to the specific project, not the entire team
- `state`: `"Done"` — only completed tickets
- `orderBy`: `"createdAt"` — sort by creation date
- `limit`: `250` — fetch all (the API default sort may be descending)

**Important**: The API returns results in descending order (newest first) regardless of `orderBy`. You must sort the results client-side in ascending order by `createdAt` to get the truly oldest tickets. Extract all results, then sort ascending and take the first N.

Example flow:
```
1. list_issues(project="Bot farm", state="Done", orderBy="createdAt", limit=250)
2. Parse results, sort by createdAt ascending (oldest first)
3. Take first N issues
4. Present the list to the user for confirmation before archiving
```

## How to Archive

Linear's MCP plugin does not expose an archive mutation. Use the Linear GraphQL API directly:

```bash
curl -s -X POST https://api.linear.app/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: $LINEAR_API_KEY" \
  -d '{"query": "mutation { issueArchive(id: \"<issue-uuid>\") { success } }"}'
```

The API key is stored in `~/.botfarm/.env` as `LINEAR_API_KEY`.

Loop through all issue UUIDs and call the mutation for each one. Always confirm the full list with the user before executing.

## Notes

- Archived issues are still searchable and restorable in Linear's team archive
- Linear also auto-archives based on team settings, but manual archiving via API is supported
- Always filter by **project** (not just team) to avoid archiving tickets from other projects
