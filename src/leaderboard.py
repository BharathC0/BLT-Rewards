import asyncio
import calendar
import json
import time
from typing import Optional, Tuple
from urllib.parse import quote


LEADERBOARD_MARKER = "<!-- leaderboard-bot -->"
REVIEWER_LEADERBOARD_MARKER = "<!-- reviewer-leaderboard-bot -->"
MERGED_PR_COMMENT_MARKER = "<!-- merged-pr-comment-bot -->"
LEADERBOARD_COMMAND = "/leaderboard"
MAX_OPEN_PRS_PER_AUTHOR = 50


def month_key(ts: Optional[int] = None) -> str:
    """Return YYYY-MM month key for UTC timestamp (or now)."""
    if ts is None:
        ts = int(time.time())
    return time.strftime("%Y-%m", time.gmtime(ts))


def month_window(mk: str) -> Tuple[int, int]:
    """Return start/end timestamps (UTC) for a YYYY-MM key."""
    year, month = mk.split("-")
    y = int(year)
    m = int(month)
    start_struct = time.struct_time((y, m, 1, 0, 0, 0, 0, 0, 0))
    start_ts = int(calendar.timegm(start_struct))
    if m == 12:
        next_struct = time.struct_time((y + 1, 1, 1, 0, 0, 0, 0, 0, 0))
    else:
        next_struct = time.struct_time((y, m + 1, 1, 0, 0, 0, 0, 0, 0))
    end_ts = int(calendar.timegm(next_struct)) - 1
    return start_ts, end_ts


def parse_github_timestamp(ts_str: str) -> int:
    """Parse a GitHub ISO 8601 timestamp (e.g. '2024-03-05T12:34:56Z') to a
    Unix timestamp integer.  Returns 0 for any invalid or empty input.
    """
    if not ts_str:
        return 0
    try:
        from datetime import datetime, timezone
        dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except (ValueError, TypeError):
        return 0


def safe_event_ts(ts_str: Optional[str]) -> int:
    """Parse a GitHub timestamp, falling back to now() for invalid/empty values.

    Fix #8: parse_github_timestamp returns 0 for malformed strings, which
    would produce month_key "1970-01" and silently corrupt leaderboard data.
    Always return a positive timestamp.
    """
    ts = parse_github_timestamp(ts_str) if ts_str else 0
    return ts if ts > 0 else int(time.time())


from html import escape as html_escape

def avatar_img_tag(login: str, size: int = 20) -> str:
    """Return a fixed-size GitHub avatar image tag safe for markdown tables."""
    safe_login = quote(str(login), safe="")
    safe_alt = html_escape(str(login))
    return (
        f"<img src=\"https://avatars.githubusercontent.com/{safe_login}?size={size}&v=4\" "
        f"width=\"{size}\" height=\"{size}\" alt=\"{safe_alt}\" />"
    )


def d1_binding(env):
    """Return D1 binding object if configured, otherwise None."""
    return getattr(env, "LEADERBOARD_DB", None) if env else None


def _to_py(value):
    """Best-effort conversion for JS proxy values returned by Workers runtime."""
    try:
        from pyodide.ffi import to_py
        return to_py(value)
    except Exception:
        return value


async def d1_run(db, sql: str, params: tuple = ()):
    """Execute a D1 write statement and return the result."""
    from js import console
    try:
        stmt = db.prepare(sql)
        if params:
            stmt = stmt.bind(*params)
        result = await stmt.run()
        return result
    except Exception as e:
        console.error(f"[D1.run] Error executing {sql[:60]}: {e}")
        raise


async def d1_all(db, sql: str, params: tuple = ()) -> list:
    """Execute a D1 SELECT and return rows as a list of dicts.

    Fix #5: Prefer the direct _to_py path (avoids JSON stringify round-trip)
    and fall back to the JSON approach only if _to_py does not yield a list.
    """
    stmt = db.prepare(sql)
    if params:
        stmt = stmt.bind(*params)
    raw_result = await stmt.all()

    # Primary path: direct pyodide proxy conversion (cheaper than JSON)
    try:
        result = _to_py(raw_result)
        rows = None
        if isinstance(result, dict):
            rows = result.get("results")
        if rows is None:
            try:
                rows = getattr(result, "results", None)
            except (TypeError, AttributeError):
                pass
        rows = _to_py(rows)
        if isinstance(rows, list):
            return rows
        if rows is not None:
            return list(rows)
    except Exception:
        pass

    # Fallback path: JSON stringify (last resort)
    try:
        from js import JSON as JS_JSON
        js_json = JS_JSON.stringify(raw_result)
        parsed = json.loads(str(js_json))
        rows = parsed.get("results") if isinstance(parsed, dict) else None
        if isinstance(rows, list):
            return rows
    except Exception:
        pass

    return []


async def d1_first(db, sql: str, params: tuple = ()):
    """Return the first row of a D1 SELECT, or None if no rows."""
    rows = await d1_all(db, sql, params)
    return rows[0] if rows else None


_VALID_TABLES = frozenset({
    "leaderboard_monthly_stats",
    "leaderboard_open_prs",
    "leaderboard_pr_state",
    "leaderboard_review_credits",
    "leaderboard_backfill_state",
    "leaderboard_backfill_repo_done",
})

async def d1_has_column(db, table_name: str, column_name: str) -> bool:
    """Return True when the table already contains the given column."""
    if table_name not in _VALID_TABLES:
        return False
    try:
        rows = await d1_all(db, f"PRAGMA table_info({table_name})")
    except Exception:
        return False
    normalized = (column_name or "").strip().lower()
    for row in rows:
        if str(row.get("name") or "").strip().lower() == normalized:
            return True
    return False


# Fix #4: module-level flag so ensure_leaderboard_schema() is only executed
# once per isolate lifetime instead of on every webhook event.
_schema_initialized = False


async def ensure_leaderboard_schema(db) -> None:
    """Create leaderboard tables if they do not exist.

    Fix #4: guarded by _schema_initialized so DDL is only issued once per
    isolate.  In a multi-isolate Cloudflare Workers environment the flag
    won't survive cross-isolate restarts, but it eliminates the 6 x DDL
    round-trips in the common case within a single isolate's lifetime.
    """
    global _schema_initialized
    if _schema_initialized:
        return
    await d1_run(
        db,
        """
        CREATE TABLE IF NOT EXISTS leaderboard_monthly_stats (
            org TEXT NOT NULL,
            month_key TEXT NOT NULL,
            user_login TEXT NOT NULL,
            merged_prs INTEGER NOT NULL DEFAULT 0,
            closed_prs INTEGER NOT NULL DEFAULT 0,
            reviews INTEGER NOT NULL DEFAULT 0,
            comments INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (org, month_key, user_login)
        )
        """,
    )
    await d1_run(
        db,
        """
        CREATE TABLE IF NOT EXISTS leaderboard_open_prs (
            org TEXT NOT NULL,
            user_login TEXT NOT NULL,
            open_prs INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (org, user_login)
        )
        """,
    )
    await d1_run(
        db,
        """
        CREATE TABLE IF NOT EXISTS leaderboard_pr_state (
            org TEXT NOT NULL,
            repo TEXT NOT NULL,
            pr_number INTEGER NOT NULL,
            author_login TEXT NOT NULL,
            state TEXT NOT NULL,
            merged INTEGER NOT NULL DEFAULT 0,
            closed_at INTEGER,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (org, repo, pr_number)
        )
        """,
    )
    await d1_run(
        db,
        """
        CREATE TABLE IF NOT EXISTS leaderboard_review_credits (
            org TEXT NOT NULL,
            repo TEXT NOT NULL,
            pr_number INTEGER NOT NULL,
            month_key TEXT NOT NULL,
            reviewer_login TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (org, repo, pr_number, month_key, reviewer_login)
        )
        """,
    )
    await d1_run(
        db,
        """
        CREATE TABLE IF NOT EXISTS leaderboard_backfill_state (
            org TEXT NOT NULL,
            month_key TEXT NOT NULL,
            next_page INTEGER NOT NULL DEFAULT 1,
            completed INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (org, month_key)
        )
        """,
    )
    await d1_run(
        db,
        """
        CREATE TABLE IF NOT EXISTS leaderboard_backfill_repo_done (
            org TEXT NOT NULL,
            month_key TEXT NOT NULL,
            repo TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (org, month_key, repo)
        )
        """,
    )
    _schema_initialized = True


async def inc_open_pr(db, org: str, user_login: str, delta: int) -> None:
    """Increment or decrement the open PR counter for a user."""
    from js import console
    now = int(time.time())
    try:
        await d1_run(
            db,
            """
            INSERT INTO leaderboard_open_prs (org, user_login, open_prs, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(org, user_login) DO UPDATE SET
                open_prs = CASE
                    WHEN leaderboard_open_prs.open_prs + excluded.open_prs < 0 THEN 0
                    ELSE leaderboard_open_prs.open_prs + excluded.open_prs
                END,
                updated_at = excluded.updated_at
            """,
            (org, user_login, delta, now),
        )
        console.log(f"[D1] Updated open PR count org={org} user={user_login} delta={delta}")
    except Exception as e:
        console.error(f"[D1] Failed to update open PRs org={org} user={user_login}: {e}")
        raise
        raise


async def inc_monthly(db, org: str, mk: str, user_login: str, field: str, delta: int = 1) -> None:
    """Add delta to a monthly leaderboard stat field for a user.

    SECURITY: field is validated against a whitelist before interpolation
    into SQL to prevent injection. Only allowed values are:
    merged_prs, closed_prs, reviews, comments.

    Fix #7: delta is now referenced by a single named binding (:delta) in the
    params tuple rather than being repeated 4 times, making it impossible for
    a future SQL edit to silently use the wrong positional value.
    """
    from js import console
    now = int(time.time())
    # SECURITY: field is interpolated into SQL — only these whitelisted column
    # names are permitted. Any other value is rejected to prevent SQL injection.
    if field not in {"merged_prs", "closed_prs", "reviews", "comments"}:
        return
    # delta appears once in INSERT and once in the ON CONFLICT update expression.
    # Passing it twice (as insert_delta, update_delta) makes the param ordering
    # explicit and avoids the previous 4-copy pattern that was a bug magnet.
    insert_delta = max(delta, 0)  # clamp negative deltas to 0 on INSERT
    try:
        await d1_run(
            db,
            f"""
            INSERT INTO leaderboard_monthly_stats (org, month_key, user_login, {field}, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(org, month_key, user_login) DO UPDATE SET
                {field} = CASE
                    WHEN leaderboard_monthly_stats.{field} + ? < 0 THEN 0
                    ELSE leaderboard_monthly_stats.{field} + ?
                END,
                updated_at = excluded.updated_at
            """,
            (org, mk, user_login, insert_delta, now, delta, delta),
        )
        console.log(f"[D1] Updated {field} org={org} month={mk} user={user_login} +{delta}")
    except Exception as e:
        console.error(f"[D1] Failed to update {field} org={org} month={mk} user={user_login}: {e}")
        raise
        raise


async def track_pr_opened(payload: dict, env, is_bot_fn, d1_binding_fn) -> None:
    """Record a PR-opened event in D1 and increment the author open-PR counter.

    Fix #1: The previous read-then-write pattern was vulnerable to a TOCTOU
    race when GitHub retried a webhook delivery.  Two concurrent deliveries
    could both read existing=None and both call inc_open_pr(+1), doubling the
    counter.

    Fix: upsert leaderboard_pr_state first using ON CONFLICT DO UPDATE, then
    derive whether to increment the open-PR counter from the change that just
    happened (rows_written > 0 means a new row was inserted, i.e. first time
    we see this PR as open).  The upsert is atomic in D1/SQLite so only one
    concurrent writer can insert; the other will conflict and update.
    """
    from js import console
    db = d1_binding_fn(env)
    if not db:
        return
    pr = payload.get("pull_request") or {}
    author = pr.get("user") or {}
    if is_bot_fn(author):
        return
    org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
    repo = (payload.get("repository") or {}).get("name", "")
    pr_number = pr.get("number")
    author_login = author.get("login", "")
    if not (org and repo and pr_number and author_login):
        return
    await ensure_leaderboard_schema(db)
    now = int(time.time())
    # Atomically upsert the PR state row.  We detect a genuine state
    # transition by checking whether the previous state was NOT already 'open'.
    result = await d1_run(
        db,
        """
        INSERT INTO leaderboard_pr_state (org, repo, pr_number, author_login, state, merged, closed_at, updated_at)
        VALUES (?, ?, ?, ?, 'open', 0, NULL, ?)
        ON CONFLICT(org, repo, pr_number) DO UPDATE SET
            author_login = excluded.author_login,
            state        = 'open',
            merged       = 0,
            closed_at    = NULL,
            updated_at   = excluded.updated_at
            WHERE leaderboard_pr_state.state != 'open'
        """,
        (org, repo, pr_number, author_login, now),
    )
    # rows_written > 0  → row was inserted or the WHERE guard matched (state changed)
    rows_written = 0
    try:
        rows_written = int(getattr(result, "rowsWritten", None) or 0)
    except Exception:
        pass
    if rows_written > 0:
        await inc_open_pr(db, org, author_login, 1)


async def track_pr_closed(payload: dict, env, is_bot_fn, d1_binding_fn) -> None:
    """Record a PR-closed event in D1 and update merged/closed monthly counters.

    Fix #1: Same TOCTOU fix as track_pr_opened — upsert first, then check
    rows_written to decide whether counters need updating.
    """
    from js import console
    db = d1_binding_fn(env)
    if not db:
        return
    pr = payload.get("pull_request") or {}
    author = pr.get("user") or {}
    if is_bot_fn(author):
        return
    org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
    repo = (payload.get("repository") or {}).get("name", "")
    pr_number = pr.get("number")
    author_login = author.get("login", "")
    closed_at = pr.get("closed_at")
    merged_at = pr.get("merged_at")
    merged = bool(pr.get("merged"))
    # Fix #8: use safe_event_ts so malformed timestamps don't produce "1970-01"
    closed_ts = safe_event_ts(closed_at)
    if not (org and repo and pr_number and author_login):
        return
    await ensure_leaderboard_schema(db)
    event_ts = safe_event_ts(merged_at) if merged and merged_at else closed_ts
    mk = month_key(event_ts)
    now = int(time.time())
    # Atomically upsert; only proceed when the row genuinely transitions to closed.
    result = await d1_run(
        db,
        """
        INSERT INTO leaderboard_pr_state (org, repo, pr_number, author_login, state, merged, closed_at, updated_at)
        VALUES (?, ?, ?, ?, 'closed', ?, ?, ?)
        ON CONFLICT(org, repo, pr_number) DO UPDATE SET
            author_login = excluded.author_login,
            state        = 'closed',
            merged       = excluded.merged,
            closed_at    = excluded.closed_at,
            updated_at   = excluded.updated_at
            WHERE leaderboard_pr_state.state != 'closed'
               OR leaderboard_pr_state.merged != excluded.merged
               OR leaderboard_pr_state.closed_at != excluded.closed_at
        """,
        (org, repo, pr_number, author_login, 1 if merged else 0, closed_ts, now),
    )
    rows_written = 0
    try:
        rows_written = int(getattr(result, "rowsWritten", None) or 0)
    except Exception:
        pass
    if rows_written > 0:
        # Decrement open-PR counter (was previously open)
        await inc_open_pr(db, org, author_login, -1)
        if merged:
            await inc_monthly(db, org, mk, author_login, "merged_prs", 1)
        else:
            await inc_monthly(db, org, mk, author_login, "closed_prs", 1)


async def track_pr_reopened(payload: dict, env, is_bot_fn, d1_binding_fn) -> None:
    """Reverse any closed-state counters when a PR is reopened.

    Fix #1: Same TOCTOU fix — upsert first, use rows_written to gate
    the counter adjustments.
    """
    from js import console
    db = d1_binding_fn(env)
    if not db:
        return
    pr = payload.get("pull_request") or {}
    author = pr.get("user") or {}
    if is_bot_fn(author):
        return
    org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
    repo = (payload.get("repository") or {}).get("name", "")
    pr_number = pr.get("number")
    author_login = author.get("login", "")
    if not (org and repo and pr_number and author_login):
        return
    await ensure_leaderboard_schema(db)
    # Read the previous state so we can reverse the right monthly counter.
    existing = await d1_first(
        db,
        "SELECT state, merged, closed_at FROM leaderboard_pr_state WHERE org = ? AND repo = ? AND pr_number = ?",
        (org, repo, pr_number),
    )
    now = int(time.time())
    result = await d1_run(
        db,
        """
        INSERT INTO leaderboard_pr_state (org, repo, pr_number, author_login, state, merged, closed_at, updated_at)
        VALUES (?, ?, ?, ?, 'open', 0, NULL, ?)
        ON CONFLICT(org, repo, pr_number) DO UPDATE SET
            author_login = excluded.author_login,
            state        = 'open',
            merged       = 0,
            closed_at    = NULL,
            updated_at   = excluded.updated_at
            WHERE leaderboard_pr_state.state != 'open'
        """,
        (org, repo, pr_number, author_login, now),
    )
    rows_written = 0
    try:
        rows_written = int(getattr(result, "rowsWritten", None) or 0)
    except Exception:
        pass
    if rows_written > 0:
        # Reverse the previous closed/merged monthly counter
        if existing and existing.get("state") == "closed":
            prev_merged = int(existing.get("merged") or 0)
            prev_closed_at = int(existing.get("closed_at") or 0)
            prev_mk = month_key(prev_closed_at) if prev_closed_at else month_key()
            field = "merged_prs" if prev_merged else "closed_prs"
            await inc_monthly(db, org, prev_mk, author_login, field, -1)
        # Increment open-PR counter
        await inc_open_pr(db, org, author_login, 1)


async def track_comment(payload: dict, env, is_bot_fn, is_coderabbit_ping_fn, extract_command_fn, d1_binding_fn) -> None:
    """Increment the comment counter for a user, skipping bots and CodeRabbit pings."""
    db = d1_binding_fn(env)
    if not db:
        return
    comment = payload.get("comment") or {}
    user = comment.get("user") or {}
    if is_bot_fn(user):
        return
    body = comment.get("body", "")
    if is_coderabbit_ping_fn(body):
        return
    if extract_command_fn(body):
        return
    org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
    login = user.get("login", "")
    created_at = comment.get("created_at")
    if not (org and login):
        return
    await ensure_leaderboard_schema(db)
    # Fix #8: use safe_event_ts to avoid "1970-01" on malformed timestamps
    mk = month_key(safe_event_ts(created_at))
    await inc_monthly(db, org, mk, login, "comments", 1)


async def track_review(payload: dict, env, is_bot_fn, d1_binding_fn) -> None:
    """Award up to two review credits per PR per month using an atomic INSERT.

    Fix #2: The previous pre-check SELECT 1 was redundant and introduced a
    race window.  The PK constraint already prevents duplicate rows for the
    same reviewer, and the WHERE COUNT < 2 subquery already gates different
    reviewers.  Removing the pre-check eliminates both the extra round-trip
    and the race window between the check and the insert.
    """
    from js import console
    db = d1_binding_fn(env)
    if not db:
        console.log("[D1] REVIEW: No DB binding")
        return
    review = payload.get("review") or {}
    reviewer = review.get("user") or {}
    if is_bot_fn(reviewer):
        return
    pr = payload.get("pull_request") or {}
    org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
    repo = (payload.get("repository") or {}).get("name", "")
    pr_number = pr.get("number")
    reviewer_login = reviewer.get("login", "")
    submitted_at = review.get("submitted_at")
    if not (org and repo and pr_number and reviewer_login):
        return
    await ensure_leaderboard_schema(db)
    # Fix #8: use safe_event_ts to avoid "1970-01" on malformed timestamps
    mk = month_key(safe_event_ts(submitted_at))
    # Fix #2: single atomic INSERT — no pre-check SELECT needed.
    # The PK constraint handles duplicate reviewer rows (INSERT OR IGNORE),
    # the WHERE COUNT < 2 subquery caps distinct reviewers per PR per month.
    result = await d1_run(
        db,
        """
        INSERT OR IGNORE INTO leaderboard_review_credits (org, repo, pr_number, month_key, reviewer_login, created_at)
        SELECT ?, ?, ?, ?, ?, ?
        WHERE (
            SELECT COUNT(*) FROM leaderboard_review_credits
            WHERE org = ? AND repo = ? AND pr_number = ? AND month_key = ?
        ) < 2
        """,
        (org, repo, pr_number, mk, reviewer_login, int(time.time()),
         org, repo, pr_number, mk),
    )
    rows_written = 0
    try:
        rows_written = int(getattr(result, "rowsWritten", None) or 0)
    except Exception:
        pass
    if rows_written > 0:
        await inc_monthly(db, org, mk, reviewer_login, "reviews", 1)


async def calculate_stats_from_d1(owner: str, env) -> Optional[dict]:
    """Read current-month leaderboard stats from D1 if configured.

    Fix #6: The two independent SELECT queries are now issued concurrently
    via asyncio.gather() instead of sequentially.
    """
    from js import console
    db = d1_binding(env)
    if not db:
        console.error("[D1] No D1 binding available")
        return None
    await ensure_leaderboard_schema(db)
    mk = month_key()
    start_timestamp, end_timestamp = month_window(mk)
    # Fix #6: run both queries in parallel
    monthly_rows, open_rows = await asyncio.gather(
        d1_all(
            db,
            """
            SELECT user_login, merged_prs, closed_prs, reviews, comments
            FROM leaderboard_monthly_stats
            WHERE org = ? AND month_key = ?
            """,
            (owner, mk),
        ),
        d1_all(
            db,
            """
            SELECT user_login, open_prs
            FROM leaderboard_open_prs
            WHERE org = ?
            """,
            (owner,),
        ),
    )
    user_stats = {}

    def ensure(login: str):
        if login not in user_stats:
            user_stats[login] = {
                "openPrs": 0, "mergedPrs": 0, "closedPrs": 0,
                "reviews": 0, "comments": 0, "total": 0,
            }

    for row in monthly_rows:
        login = row.get("user_login")
        if not login:
            continue
        ensure(login)
        user_stats[login]["mergedPrs"] = int(row.get("merged_prs") or 0)
        user_stats[login]["closedPrs"] = int(row.get("closed_prs") or 0)
        user_stats[login]["reviews"] = int(row.get("reviews") or 0)
        user_stats[login]["comments"] = int(row.get("comments") or 0)
    for row in open_rows:
        login = row.get("user_login")
        if not login:
            continue
        ensure(login)
        user_stats[login]["openPrs"] = int(row.get("open_prs") or 0)
    for login in user_stats:
        s = user_stats[login]
        s["total"] = (s["openPrs"] * 1) + (s["mergedPrs"] * 10) + (s["closedPrs"] * -2) + (s["reviews"] * 5) + (s["comments"] * 2)
    sorted_users = sorted(
        [{"login": login, **stats} for login, stats in user_stats.items()],
        key=lambda u: (-u["total"], -u["mergedPrs"], -u["reviews"], u["login"].lower()),
    )
    return {
        "users": user_stats,
        "sorted": sorted_users,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
    }


async def get_backfill_state(db, owner: str, mk: str) -> dict:
    """Return the current incremental backfill cursor for an org and month."""
    row = await d1_first(
        db,
        "SELECT next_page, completed FROM leaderboard_backfill_state WHERE org = ? AND month_key = ?",
        (owner, mk),
    )
    if row:
        return {"next_page": int(row.get("next_page") or 1), "completed": bool(int(row.get("completed") or 0))}
    return {"next_page": 1, "completed": False}


async def set_backfill_state(db, owner: str, mk: str, next_page: int, completed: bool) -> None:
    """Persist the incremental backfill cursor so the next cron run can resume."""
    from js import console
    try:
        await d1_run(
            db,
            """
            INSERT INTO leaderboard_backfill_state (org, month_key, next_page, completed, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(org, month_key) DO UPDATE SET
                next_page = excluded.next_page,
                completed = excluded.completed,
                updated_at = excluded.updated_at
            """,
            (owner, mk, next_page, 1 if completed else 0, int(time.time())),
        )
    except Exception as e:
        console.error(f"[Backfill] Failed to update state: {e}")
        raise
        raise


async def reset_leaderboard_month(org: str, mk: str, db) -> dict:
    """Clear all leaderboard data for an org/month so a fresh backfill can re-populate it.

    Fix #3: All DELETEs are now executed inside a db.batch() call so they
    succeed or fail atomically — no more partially-reset leaderboard state
    if one DELETE fails midway.
    """
    await ensure_leaderboard_schema(db)
    start_ts, end_ts = month_window(mk)
    from js import console

    # Build all statements for atomic batch execution
    stmts_by_key: list[tuple[str, str, tuple]] = [
        ("leaderboard_monthly_stats",    "DELETE FROM leaderboard_monthly_stats WHERE org = ? AND month_key = ?",    (org, mk)),
        ("leaderboard_backfill_repo_done", "DELETE FROM leaderboard_backfill_repo_done WHERE org = ? AND month_key = ?", (org, mk)),
        ("leaderboard_review_credits",   "DELETE FROM leaderboard_review_credits WHERE org = ? AND month_key = ?",   (org, mk)),
        ("leaderboard_backfill_state",   "DELETE FROM leaderboard_backfill_state WHERE org = ? AND month_key = ?",   (org, mk)),
        ("leaderboard_pr_state",
         """
         DELETE FROM leaderboard_pr_state
         WHERE org = ?
           AND (
             closed_at BETWEEN ? AND ?
             OR (state = 'open' AND closed_at IS NULL AND updated_at BETWEEN ? AND ?)
           )
         """,
         (org, start_ts, end_ts, start_ts, end_ts)),
        # leaderboard_open_prs is intentionally excluded — it holds current-state
        # data with no month_key and must not be wiped by a month-scoped reset.
    ]

    deleted: dict = {}
    try:
        # Fix #3: batch all DELETEs atomically so a mid-flight failure does
        # not leave the leaderboard in a partially-reset state.
        batch_stmts = []
        for _key, sql, params in stmts_by_key:
            stmt = db.prepare(sql)
            if params:
                stmt = stmt.bind(*params)
            batch_stmts.append(stmt)
        await db.batch(batch_stmts)
        for key, _sql, _params in stmts_by_key:
            deleted[key] = "cleared"
    except Exception as e:
        console.error(f"[AdminReset] Batch DELETE failed: {e}")
        # Do not fall back to individual deletes — that reintroduces partial resets.
        for key, _sql, _params in stmts_by_key:
            deleted[key] = "error"
        return deleted
    return deleted


def format_leaderboard_comment(author_login: str, leaderboard_data: dict, owner: str, note: str = "") -> str:
    """Format a leaderboard comment for a specific user."""
    sorted_users = leaderboard_data["sorted"]
    start_ts = leaderboard_data["start_timestamp"]
    author_index = -1
    for i, user in enumerate(sorted_users):
        if user["login"] == author_login:
            author_index = i
            break
    month_struct = time.gmtime(start_ts)
    display_month = time.strftime("%B %Y", month_struct)
    comment = LEADERBOARD_MARKER + "\n"
    comment += "## 📊 Monthly Leaderboard\n\n"
    comment += f"Hi @{author_login}! Here's how you rank for {display_month}:\n\n"
    comment += "| Rank | User | Open PRs | PRs (merged) | PRs (closed) | Reviews | Comments | Total |\n"
    comment += "| --- | --- | --- | --- | --- | --- | --- | --- |\n"

    def row_for(rank: int, u: dict, bold: bool = False, medal: str = "") -> str:
        av = avatar_img_tag(u["login"])
        user_cell = f"{av} **`@{u['login']}`** ✨" if bold else f"{av} `@{u['login']}`"
        rank_cell = f"{medal} {rank}" if medal else f"{rank}"
        return (f"| {rank_cell} | {user_cell} | {u['openPrs']} | {u['mergedPrs']} | "
                f"{u['closedPrs']} | {u['reviews']} | {u['comments']} | **{u['total']}** |")

    if not sorted_users:
        av = avatar_img_tag(author_login)
        comment += f"| - | {av} **`@{author_login}`** ✨ | 0 | 0 | 0 | 0 | 0 | **0** |\n"
        comment += "\n_No leaderboard activity has been recorded for this month yet._\n"
    elif author_index == -1:
        for i in range(min(5, len(sorted_users))):
            medal = ["🥇", "🥈", "🥉"][i] if i < 3 else ""
            comment += row_for(i + 1, sorted_users[i], False, medal) + "\n"
    else:
        if author_index > 0:
            medal = ["🥇", "🥈", "🥉"][author_index - 1] if author_index - 1 < 3 else ""
            comment += row_for(author_index, sorted_users[author_index - 1], False, medal) + "\n"
        medal = ["🥇", "🥈", "🥉"][author_index] if author_index < 3 else ""
        comment += row_for(author_index + 1, sorted_users[author_index], True, medal) + "\n"
        if author_index < len(sorted_users) - 1:
            comment += row_for(author_index + 2, sorted_users[author_index + 1]) + "\n"
    comment += "\n---\n"
    comment += (
        f"**Scoring this month** (across {owner} org): Open PRs (+1 each), Merged PRs (+10), "
        "Closed (not merged) (-2), Reviews (+5; first two per PR in-month), "
        "Comments (+2, excludes CodeRabbit). Run `/leaderboard` on any issue or PR to see your rank!\n"
    )
    if note:
        comment += f"\n> Note: {note}\n"
    return comment


def format_reviewer_leaderboard_comment(leaderboard_data: dict, owner: str, pr_reviewers: Optional[list] = None) -> str:
    """Format a reviewer leaderboard comment showing top reviewers for the month."""
    sorted_users = leaderboard_data["sorted"]
    start_ts = leaderboard_data["start_timestamp"]
    reviewer_sorted = sorted(
        [u for u in sorted_users if u["reviews"] > 0],
        key=lambda u: (-u["reviews"], u["login"].lower()),
    )
    month_struct = time.gmtime(start_ts)
    display_month = time.strftime("%B %Y", month_struct)
    comment = REVIEWER_LEADERBOARD_MARKER + "\n"
    comment += "## 🔍 Reviewer Leaderboard\n\n"
    comment += f"Top reviewers for {display_month} (across the {owner} org):\n\n"
    medals = ["🥇", "🥈", "🥉"]

    def row_for(rank: int, u: dict, highlight: bool = False) -> str:
        medal = medals[rank - 1] if rank <= 3 else ""
        rank_cell = f"{medal} {rank}" if medal else f"{rank}"
        av = avatar_img_tag(u["login"])
        user_cell = f"{av} **`@{u['login']}`** ⭐" if highlight else f"{av} `@{u['login']}`"
        return f"| {rank_cell} | {user_cell} | {u['reviews']} |"

    comment += "| Rank | Reviewer | Reviews this month |\n"
    comment += "| --- | --- | --- |\n"
    pr_reviewer_set = set(pr_reviewers or [])
    if not reviewer_sorted:
        comment += "| - | _No review activity recorded yet_ | 0 |\n"
    else:
        total = len(reviewer_sorted)
        center_idx = None
        if pr_reviewer_set:
            for i, u in enumerate(reviewer_sorted):
                if u["login"] in pr_reviewer_set:
                    center_idx = i
                    break
        if center_idx is not None:
            start_idx = center_idx - 2
            end_idx = center_idx + 2
            if start_idx < 0:
                end_idx -= start_idx
                start_idx = 0
            if end_idx >= total:
                shift = end_idx - total + 1
                start_idx = max(0, start_idx - shift)
                end_idx = total - 1
            if start_idx > 0:
                comment += "| … | … | … |\n"
            for i in range(start_idx, end_idx + 1):
                u = reviewer_sorted[i]
                highlight = u["login"] in pr_reviewer_set
                comment += row_for(i + 1, u, highlight) + "\n"
            if end_idx < total - 1:
                comment += "| … | … | … |\n"
        else:
            for i, u in enumerate(reviewer_sorted[:5]):
                highlight = u["login"] in pr_reviewer_set
                comment += row_for(i + 1, u, highlight) + "\n"
    comment += "\n---\n"
    comment += (
        "Reviews earn **+5 points** each in the monthly leaderboard "
        "(first two reviewers per PR). Thank you to everyone who helps review PRs!\n"
    )
    return comment
