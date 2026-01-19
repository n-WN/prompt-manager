"""Sync prompts from all sources to the database with incremental updates."""

from __future__ import annotations

import duckdb
from pathlib import Path
from dataclasses import dataclass
from typing import Callable, Optional

from .db import get_connection, insert_prompt
from .parsers.claude_code import ClaudeCodeParser
from .parsers.cursor import CursorParser
from .parsers.aider import AiderParser
from .parsers.codex import CodexParser
from .parsers.gemini_cli import GeminiCliParser


@dataclass(frozen=True)
class SyncProgress:
    phase: str
    source: str
    file_path: Path
    files_checked: int
    files_total: int
    files_updated: int
    new_prompts_total: int
    new_prompts_in_file: int
    skipped: bool = False
    error: Optional[str] = None


ProgressCallback = Callable[[SyncProgress], None]


def _init_file_state_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create file state tracking table."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS file_sync_state (
            file_path VARCHAR PRIMARY KEY,
            source VARCHAR NOT NULL,
            file_size BIGINT,
            mtime DOUBLE,
            last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def _get_file_state(conn: duckdb.DuckDBPyConnection, file_path: str) -> Optional[dict]:
    """Get the stored state for a file."""
    result = conn.execute("""
        SELECT file_size, mtime, last_sync FROM file_sync_state WHERE file_path = ?
    """, [file_path]).fetchone()
    if result:
        return {"file_size": result[0], "mtime": result[1], "last_sync": result[2]}
    return None


def _update_file_state(conn: duckdb.DuckDBPyConnection, file_path: str, source: str, file_size: int, mtime: float) -> None:
    """Update the stored state for a file."""
    conn.execute("""
        INSERT INTO file_sync_state (file_path, source, file_size, mtime, last_sync)
        VALUES (?, ?, ?, ?, NOW())
        ON CONFLICT (file_path) DO UPDATE SET
            file_size = EXCLUDED.file_size,
            mtime = EXCLUDED.mtime,
            last_sync = NOW()
    """, [file_path, source, file_size, mtime])


def _file_needs_sync(conn: duckdb.DuckDBPyConnection, file_path: Path) -> bool:
    """Check if a file needs to be synced based on size/mtime changes."""
    try:
        stat = file_path.stat()
        current_size = stat.st_size
        current_mtime = stat.st_mtime
    except OSError:
        return False

    state = _get_file_state(conn, str(file_path))
    if state is None:
        return True  # Never synced

    # Check if file changed (size or mtime different)
    return state["file_size"] != current_size or abs(state["mtime"] - current_mtime) > 0.001


def _sync_file(conn: duckdb.DuckDBPyConnection, parser: "BaseParser", file_path: Path) -> int:
    """Sync a single file and return number of prompts inserted.

    Returns:
        Number of new prompts inserted, or -1 on error
    """
    count = 0
    try:
        for prompt in parser.parse_file(file_path):
            inserted = insert_prompt(
                conn,
                id=prompt.id,
                source=prompt.source,
                content=prompt.content,
                project_path=prompt.project_path,
                session_id=prompt.session_id,
                timestamp=prompt.timestamp,
                response=prompt.response,
                turn_json=prompt.turn_json,
            )
            if inserted:
                count += 1

        # Update file state after successful parse
        stat = file_path.stat()
        _update_file_state(conn, str(file_path), parser.source_name, stat.st_size, stat.st_mtime)
        return count

    except Exception as e:
        # Log error but don't crash - return -1 to indicate failure
        print(f"Error syncing {file_path}: {e}")
        return -1


def sync_all(
    conn: Optional[duckdb.DuckDBPyConnection] = None,
    force: bool = False,
    progress_callback: Optional[ProgressCallback] = None,
    parsers: Optional[list["BaseParser"]] = None,
) -> dict:
    """Sync prompts from all sources.

    Args:
        conn: Database connection
        force: If True, re-sync all files regardless of state
        progress_callback: Optional callback invoked after each file is processed
        parsers: Optional parser instances (primarily for tests)

    Returns:
        Dict with counts per source and files_checked/files_updated stats
    """
    if conn is None:
        conn = get_connection()

    _init_file_state_table(conn)

    counts = {
        "claude_code": 0,
        "cursor": 0,
        "aider": 0,
        "codex": 0,
        "gemini_cli": 0,
        "total": 0,
        "files_checked": 0,
        "files_updated": 0,
    }

    active_parsers = parsers or [
        ClaudeCodeParser(),
        CursorParser(),
        AiderParser(),
        CodexParser(),
        GeminiCliParser(),
    ]

    # Ensure all sources exist in the counts dict even when custom parsers are provided.
    for parser in active_parsers:
        counts.setdefault(parser.source_name, 0)

    file_jobs: list[tuple["BaseParser", Path]] = []
    for parser in active_parsers:
        for file_path in parser.find_log_files():
            file_jobs.append((parser, file_path))

    files_total = len(file_jobs)
    if progress_callback:
        progress_callback(
            SyncProgress(
                phase="starting",
                source="",
                file_path=Path(),
                files_checked=0,
                files_total=files_total,
                files_updated=0,
                new_prompts_total=0,
                new_prompts_in_file=0,
            )
        )

    for idx, (parser, file_path) in enumerate(file_jobs, 1):
        counts["files_checked"] = idx

        needs_sync = force or _file_needs_sync(conn, file_path)
        inserted_in_file = 0
        skipped = not needs_sync
        error: Optional[str] = None

        if needs_sync:
            counts["files_updated"] += 1
            result = _sync_file(conn, parser, file_path)
            if result >= 0:
                inserted_in_file = result
                counts[parser.source_name] = counts.get(parser.source_name, 0) + result
                counts["total"] += result
            else:
                error = "sync failed"

        if progress_callback:
            progress_callback(
                SyncProgress(
                    phase="syncing",
                    source=parser.source_name,
                    file_path=file_path,
                    files_checked=idx,
                    files_total=files_total,
                    files_updated=counts["files_updated"],
                    new_prompts_total=counts["total"],
                    new_prompts_in_file=inserted_in_file,
                    skipped=skipped,
                    error=error,
                )
            )

    return counts


def rebuild_database(
    conn: Optional[duckdb.DuckDBPyConnection] = None,
    progress_callback: Optional[ProgressCallback] = None,
    preserve_metadata: bool = True,
) -> dict:
    """Rebuild the prompt database by re-parsing all known log files.

    Notes:
      - This clears the `prompts` table then runs a forced sync of all files.
      - When `preserve_metadata` is true, starred/tags/use_count are restored by prompt id.
    """
    if conn is None:
        conn = get_connection()

    _init_file_state_table(conn)

    preserved: list[tuple[str, bool, object, int]] = []
    if preserve_metadata:
        try:
            preserved = conn.execute(
                "SELECT id, starred, tags, use_count FROM prompts"
            ).fetchall()
        except Exception:
            preserved = []

    if progress_callback:
        progress_callback(
            SyncProgress(
                phase="resetting",
                source="",
                file_path=Path(),
                files_checked=0,
                files_total=0,
                files_updated=0,
                new_prompts_total=0,
                new_prompts_in_file=0,
            )
        )

    conn.execute("DELETE FROM prompts")
    try:
        conn.execute("DELETE FROM file_sync_state")
    except Exception:
        pass
    try:
        conn.execute("DELETE FROM sync_state")
    except Exception:
        pass

    counts = sync_all(conn, force=True, progress_callback=progress_callback)

    if preserve_metadata and preserved:
        if progress_callback:
            progress_callback(
                SyncProgress(
                    phase="restoring",
                    source="",
                    file_path=Path(),
                    files_checked=counts.get("files_checked", 0),
                    files_total=counts.get("files_checked", 0),
                    files_updated=counts.get("files_updated", 0),
                    new_prompts_total=counts.get("total", 0),
                    new_prompts_in_file=0,
                )
            )

        for prompt_id, starred, tags, use_count in preserved:
            try:
                conn.execute(
                    """
                    UPDATE prompts
                    SET starred = ?,
                        tags = ?,
                        use_count = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    [bool(starred), tags, int(use_count or 0), prompt_id],
                )
            except Exception:
                continue

    return counts


def sync_source(source: str, conn: Optional[duckdb.DuckDBPyConnection] = None, force: bool = False) -> int:
    """Sync prompts from a specific source."""
    if conn is None:
        conn = get_connection()

    _init_file_state_table(conn)

    parser_map = {
        "claude_code": ClaudeCodeParser,
        "cursor": CursorParser,
        "aider": AiderParser,
        "codex": CodexParser,
        "gemini_cli": GeminiCliParser,
    }

    if source not in parser_map:
        raise ValueError(f"Unknown source: {source}")

    parser = parser_map[source]()
    count = 0

    for file_path in parser.find_log_files():
        if not force and not _file_needs_sync(conn, file_path):
            continue

        result = _sync_file(conn, parser, file_path)
        if result >= 0:
            count += result

    return count


def check_updates(conn: Optional[duckdb.DuckDBPyConnection] = None) -> dict:
    """Check which sources have updates without syncing.

    Returns:
        Dict mapping source names to number of files that need sync
    """
    if conn is None:
        conn = get_connection()

    _init_file_state_table(conn)

    updates = {
        "claude_code": 0,
        "cursor": 0,
        "aider": 0,
        "codex": 0,
        "gemini_cli": 0,
    }

    parsers = [
        ClaudeCodeParser(),
        CursorParser(),
        AiderParser(),
        CodexParser(),
        GeminiCliParser(),
    ]

    for parser in parsers:
        for file_path in parser.find_log_files():
            if _file_needs_sync(conn, file_path):
                updates[parser.source_name] += 1

    return updates
