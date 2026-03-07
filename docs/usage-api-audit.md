# Usage API Audit Log

Botfarm instruments every Anthropic usage API call to detect key blocking, measure recovery times, and understand call patterns that lead to blocks.

## Problem Statement

Anthropic's usage API blocks OAuth keys after excessive requests. When a key is blocked:

- The dashboard shows stale utilization data
- Usage history has gaps, making limit decisions unreliable
- No visibility into how long blocks last or what triggers them

The audit log answers three questions:

1. **Call frequency before blocking** — how many calls/hour trigger a block?
2. **Block duration** — how long does a block last before the key recovers?
3. **Recovery patterns** — does the key recover on its own, or must it be rotated?

## Data Model

### `usage_api_calls` — Raw Event Log

Every HTTP request attempt is logged, including retries within `_fetch_with_retry`. One API poll cycle may produce 1–3 rows (1 success, or up to 3 retry attempts for transient errors).

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | INTEGER PK | Auto-increment | `42` |
| `created_at` | TEXT | ISO-8601 timestamp | `2026-03-07T14:30:00.000Z` |
| `token_fingerprint` | TEXT | SHA-256 of last 8 chars (16 hex) | `a1b2c3d4e5f67890` |
| `status_code` | INTEGER | HTTP status. NULL for connection errors | `200`, `429`, `401`, `NULL` |
| `success` | INTEGER | 1 for 2xx, 0 otherwise | `1` |
| `error_type` | TEXT | Error category (NULL on success) | `rate_limit`, `auth_error` |
| `error_detail` | TEXT | Error message (truncated to 500 chars) | `Client error '429 ...'` |
| `response_time_ms` | REAL | Wall-clock HTTP request time | `245.3` |
| `retry_after` | TEXT | `Retry-After` header value | `60` |
| `caller` | TEXT | What triggered this call | `poll` |

**Indexes:**

| Index | Column | Purpose |
|-------|--------|---------|
| `idx_usage_api_calls_created_at` | `created_at` | Time-range queries and retention purge |
| `idx_usage_api_calls_token` | `token_fingerprint` | Filter calls by key |
| `idx_usage_api_calls_success` | `success` | Quickly count failures |

**`caller` values:**

| Value | Trigger |
|-------|---------|
| `poll` | Regular 5-minute supervisor poll cycle |
| `force_poll` | Startup/recovery — supervisor needs fresh data immediately |
| `force_poll_bypass` | Safety-critical paths (limit checks, resume decisions) that bypass cooldown |
| `cli_refresh` | `botfarm status` command |
| `dashboard_refresh` | Web dashboard refresh |

**`error_type` mapping:**

| `error_type` | HTTP Status / Exception |
|--------------|------------------------|
| `rate_limit` | 429 |
| `auth_error` | 401 |
| `server_error` | 5xx |
| `timeout` | `httpx.ConnectTimeout`, `httpx.PoolTimeout` |
| `connection_error` | `httpx.ConnectError` |
| `other` | Any other exception |

### `usage_api_key_sessions` — Key Lifecycle

One row per distinct token fingerprint. Tracks the key from first use through blocking/recovery/replacement.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | INTEGER PK | Auto-increment | `1` |
| `token_fingerprint` | TEXT UNIQUE | SHA-256 fingerprint | `a1b2c3d4e5f67890` |
| `first_seen_at` | TEXT | When this token was first used | `2026-03-01T10:00:00Z` |
| `last_success_at` | TEXT | Last successful API call | `2026-03-07T14:25:00Z` |
| `first_error_at` | TEXT | First error with this token | `2026-03-07T14:30:00Z` |
| `last_error_at` | TEXT | Most recent error | `2026-03-07T14:40:00Z` |
| `consecutive_errors` | INTEGER | Current streak of consecutive errors | `5` |
| `total_errors` | INTEGER | Lifetime error count | `12` |
| `total_successes` | INTEGER | Lifetime success count | `1847` |
| `status` | TEXT | Current state (see state machine below) | `blocked` |
| `blocked_at` | TEXT | When status became `blocked` | `2026-03-07T14:40:00Z` |
| `unblocked_at` | TEXT | When status became `recovered` | `2026-03-07T16:10:00Z` |
| `replaced_at` | TEXT | When a new token was detected | `NULL` |
| `block_duration_seconds` | REAL | Seconds from block start to resolution | `5400.0` |
| `created_at` | TEXT | Row creation timestamp | `2026-03-01T10:00:00Z` |

**Indexes:**

| Index | Column | Purpose |
|-------|--------|---------|
| `idx_usage_api_key_sessions_status` | `status` | Find active/blocked keys |

**Relationship:** Both tables link via `token_fingerprint`. A session row summarizes the aggregate state of all call rows sharing the same fingerprint.

**`block_duration_seconds` calculation:**

- On recovery (success after block): `unblocked_at - (blocked_at or first_error_at)`
- On replacement (new key detected): `replaced_at - (blocked_at or first_error_at)`
- Computed automatically by `upsert_usage_api_key_session` and `mark_key_session_replaced`

## Token Fingerprinting

```python
hashlib.sha256(token[-8:].encode()).hexdigest()[:16]
```

- **Last 8 characters**: sufficient entropy for rotation detection — OAuth tokens have high per-character entropy in the suffix
- **SHA-256 truncated to 16 hex chars**: non-reversible (cannot recover the token from the fingerprint), compact for storage and display in logs
- **Use case**: detect when `~/.claude/.credentials.json` gets a new token (user re-authenticates, token refresh) without storing the token itself

## Key Lifecycle State Machine

```
active ──(error)──> erroring ──(3rd consecutive error)──> blocked
                       |                                     |
                       |──(success)──> recovered             |──(success)──> recovered
                       |                                     |
                       └──(new key)──> replaced              └──(new key)──> replaced

(new key appears → old key marked replaced)
```

**State definitions:**

| Status | Meaning |
|--------|---------|
| `active` | Key is working normally (0 consecutive errors) |
| `erroring` | 1–2 consecutive errors — transient issue or early warning |
| `blocked` | 3+ consecutive errors — Anthropic is blocking this key |
| `recovered` | Was blocked/erroring, then a success came through |
| `replaced` | A different token fingerprint appeared — this key is no longer in use |

**Threshold:** The blocked threshold is 3 consecutive errors, hardcoded in `db.py:upsert_usage_api_key_session` (line `if new_consecutive >= 3`).

**Recovery resets:** When a `recovered` key starts erroring again, `first_error_at`, `blocked_at`, `unblocked_at`, and `block_duration_seconds` are reset so the next block cycle is tracked independently.

## How Data Flows

### Instrumentation Points

1. **`UsagePoller._fetch_with_retry()`** — logs each attempt within the retry loop. Each attempt (success or failure) is appended to `self._attempt_records` with status code, error type, response time, and retry-after header.

2. **`UsagePoller._do_poll()`** — orchestrates a poll cycle:
   - Calls `_check_key_rotation()` to detect token changes before fetching
   - Clears `_attempt_records`, then calls `_fetch()` (which calls `_fetch_with_retry()`)
   - Calls `_flush_audit_records()` to write all attempt records to the DB
   - On success: parses response, stores snapshot, purges old data

3. **`UsagePoller._flush_audit_records()`** — writes collected attempt records to both audit tables:
   - `insert_usage_api_call()` — one row per attempt
   - `upsert_usage_api_key_session()` — updates the session state machine

4. **`UsagePoller._purge_old_snapshots()`** — also purges old audit rows via `purge_old_usage_api_calls()` (same retention period as usage snapshots)

5. **`refresh_usage_snapshot()`** — creates a temporary `UsagePoller`, calls `force_poll()` with the appropriate caller tag (`cli_refresh` or `dashboard_refresh`), then closes. Audit records flow through the same `_flush_audit_records()` path.

### Call Flow Diagram

```
poll() / force_poll()
  └─> _do_poll(conn)
        ├─> _check_key_rotation(conn, fp)     # detect new token
        ├─> _fetch(token)
        │     └─> _fetch_with_retry(token)     # 1-3 attempts, records each
        ├─> _flush_audit_records(conn, fp)     # write to usage_api_calls + usage_api_key_sessions
        ├─> _parse_and_store(data, conn)       # update usage_snapshots
        └─> _purge_old_snapshots(conn)         # retention cleanup
```

## Analysis Queries

Ready-to-use SQL queries for common analyses against `~/.botfarm/botfarm.db`.

### Calls per hour (detect over-polling)

```sql
SELECT strftime('%Y-%m-%d %H:00', created_at) AS hour,
       COUNT(*) AS calls,
       SUM(success) AS successes,
       COUNT(*) - SUM(success) AS failures
FROM usage_api_calls
GROUP BY hour ORDER BY hour DESC;
```

### Call frequency in the 30 minutes before each block

```sql
SELECT ks.token_fingerprint, ks.blocked_at,
       COUNT(c.id) AS calls_before_block,
       AVG(c.response_time_ms) AS avg_response_time
FROM usage_api_key_sessions ks
JOIN usage_api_calls c ON c.token_fingerprint = ks.token_fingerprint
  AND c.created_at BETWEEN datetime(ks.blocked_at, '-30 minutes') AND ks.blocked_at
WHERE ks.blocked_at IS NOT NULL
GROUP BY ks.token_fingerprint, ks.blocked_at;
```

### Block duration history

```sql
SELECT token_fingerprint, status, blocked_at,
       COALESCE(unblocked_at, replaced_at) AS resolved_at,
       block_duration_seconds,
       total_successes, total_errors
FROM usage_api_key_sessions
WHERE blocked_at IS NOT NULL
ORDER BY blocked_at DESC;
```

### Caller breakdown (who makes the most calls?)

```sql
SELECT caller, COUNT(*) AS total_calls,
       SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) AS errors
FROM usage_api_calls
GROUP BY caller ORDER BY total_calls DESC;
```

### Time from first use to first error per key

```sql
SELECT token_fingerprint,
       total_successes AS successes_before_trouble,
       first_seen_at, first_error_at,
       (julianday(first_error_at) - julianday(first_seen_at)) * 24 AS hours_until_first_error
FROM usage_api_key_sessions
WHERE first_error_at IS NOT NULL;
```

### Error rate by hour (spot degradation trends)

```sql
SELECT strftime('%Y-%m-%d %H:00', created_at) AS hour,
       COUNT(*) AS total,
       SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) AS errors,
       ROUND(100.0 * SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS error_pct
FROM usage_api_calls
GROUP BY hour
HAVING errors > 0
ORDER BY hour DESC;
```

## Configuration

| Setting | Value | Notes |
|---------|-------|-------|
| Retention | Same as usage snapshots (default 30 days, configurable via `usage.retention_days` in config) | Old audit rows are purged alongside old snapshots |
| Blocked threshold | 3 consecutive errors | Hardcoded in `db.py:upsert_usage_api_key_session` |
| Always-on | Yes | No config key to disable — audit logging is built into the poll path |
| New config keys | None | No changes to `config.yaml` needed |

## Related Files

| File | Role |
|------|------|
| `botfarm/usage.py` | `UsagePoller` — poll logic, retry, fingerprinting, audit record collection |
| `botfarm/db.py` | `insert_usage_api_call`, `upsert_usage_api_key_session`, `mark_key_session_replaced`, `purge_old_usage_api_calls` |
| `botfarm/migrations/028_usage_api_audit.sql` | Schema for both audit tables |
| `botfarm/credentials.py` | `CredentialManager`, `fetch_usage` — underlying HTTP call |
| `docs/database.md` | Table schema reference (columns, indexes, migration history) |
