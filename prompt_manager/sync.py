"""Sync prompts from all sources to the database with incremental updates."""

from __future__ import annotations

import duckdb
from pathlib import Path
from dataclasses import dataclass
import logging
import time
from typing import Callable, Optional, TYPE_CHECKING

from .db import get_connection, insert_prompt
from .parsers.claude_code import ClaudeCodeParser
from .parsers.cursor import CursorParser
from .parsers.aider import AiderParser
from .parsers.codex import CodexParser
from .parsers.gemini_cli import GeminiCliParser

if TYPE_CHECKING:
    from .parsers import BaseParser


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
    file_items_done: int = 0
    file_items_total: Optional[int] = None
    skipped: bool = False
    skip_reason: Optional[str] = None
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
            sync_version INTEGER DEFAULT 1,
            last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info('file_sync_state')").fetchall()}
    except duckdb.Error:
        logging.exception("failed to read table_info for file_sync_state")
        raise

    if "sync_version" not in columns:
        try:
            conn.execute("ALTER TABLE file_sync_state ADD COLUMN sync_version INTEGER DEFAULT 1")
        except duckdb.Error:
            logging.exception("failed to add sync_version column")
            raise

    conn.execute("UPDATE file_sync_state SET sync_version = 1 WHERE sync_version IS NULL")


def _get_file_state(conn: duckdb.DuckDBPyConnection, file_path: str) -> Optional[dict]:
    """Get the stored state for a file."""
    result = conn.execute("""
        SELECT file_size, mtime, sync_version, last_sync
        FROM file_sync_state
        WHERE file_path = ?
    """, [file_path]).fetchone()
    if result:
        return {"file_size": result[0], "mtime": result[1], "sync_version": result[2], "last_sync": result[3]}
    return None


def _update_file_state(
    conn: duckdb.DuckDBPyConnection,
    file_path: str,
    source: str,
    file_size: int,
    mtime: float,
    sync_version: int,
) -> None:
    """Update the stored state for a file."""
    conn.execute("""
        INSERT INTO file_sync_state (file_path, source, file_size, mtime, sync_version, last_sync)
        VALUES (?, ?, ?, ?, ?, NOW())
        ON CONFLICT (file_path) DO UPDATE SET
            file_size = EXCLUDED.file_size,
            mtime = EXCLUDED.mtime,
            sync_version = EXCLUDED.sync_version,
            last_sync = NOW()
    """, [file_path, source, file_size, mtime, sync_version])


def _file_sync_status(
    conn: duckdb.DuckDBPyConnection, parser: BaseParser, file_path: Path
) -> tuple[bool, Optional[str]]:
    """Return (needs_sync, reason) for a file."""
    try:
        stat = file_path.stat()
        current_size = stat.st_size
        current_mtime = stat.st_mtime
    except OSError:
        return False, "missing"

    state = _get_file_state(conn, str(file_path))
    if state is None:
        return True, "new"

    stored_version = int(state.get("sync_version") or 1)
    parser_version = int(getattr(parser, "sync_version", 1) or 1)
    if stored_version != parser_version:
        return True, f"parser v{stored_version}→{parser_version}"

    # Check if file changed (size or mtime different)
    if state["file_size"] != current_size or abs(state["mtime"] - current_mtime) > 0.001:
        return True, "modified"
    return False, "up-to-date"


def _file_needs_sync(conn: duckdb.DuckDBPyConnection, parser: BaseParser, file_path: Path) -> bool:
    needs_sync, _ = _file_sync_status(conn, parser, file_path)
    return needs_sync


def _sync_file(
    conn: duckdb.DuckDBPyConnection,
    parser: BaseParser,
    file_path: Path,
    *,
    progress: Optional[Callable[[int, int], None]] = None,
) -> int:
    """Sync a single file and return number of prompts inserted.

    Returns:
        Number of new prompts inserted, or -1 on error
    """
    count = 0
    items_done = 0
    last_emit = 0.0
    backfill_rows: list[tuple[str, Optional[str], Optional[str]]] = []
    backfill_batch_size = 250

    try:
        file_size = file_path.stat().st_size
    except OSError:
        file_size = 0

    batch_row_limit = 1000
    batch_byte_limit = 64 * 1024 * 1024
    if parser.source_name == "codex" or file_size >= 20 * 1024 * 1024:
        batch_row_limit = 200
        batch_byte_limit = 16 * 1024 * 1024

    batch_rows = 0
    batch_bytes = 0

    def flush_backfill_rows() -> None:
        nonlocal backfill_rows
        if not backfill_rows:
            return
        conn.execute("DROP TABLE IF EXISTS tmp_backfill")
        conn.execute("CREATE TEMP TABLE tmp_backfill(id VARCHAR, response TEXT, turn_json TEXT)")
        conn.executemany("INSERT INTO tmp_backfill VALUES (?,?,?)", backfill_rows)
        conn.execute(
            """
            UPDATE prompts
            SET response = COALESCE(prompts.response, tmp_backfill.response),
                turn_json = COALESCE(prompts.turn_json, tmp_backfill.turn_json),
                updated_at = CURRENT_TIMESTAMP
            FROM tmp_backfill
            WHERE prompts.id = tmp_backfill.id
              AND (prompts.response IS NULL OR prompts.turn_json IS NULL)
            """
        )
        conn.execute("DROP TABLE tmp_backfill")
        backfill_rows = []

    try:
        # DuckDB autocommits each statement by default, which becomes extremely slow
        # when syncing large sessions (hundreds/thousands of prompts). Wrap a file
        # sync in a single transaction so we pay fsync/commit cost once per file.
        conn.execute("BEGIN")
        for prompt in parser.parse_file(file_path):
            items_done += 1
            batch_rows += 1

            approx = len(prompt.content)
            if prompt.response:
                approx += len(prompt.response)
            if prompt.turn_json:
                approx += len(prompt.turn_json)
            batch_bytes += approx

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
                backfill_missing_fields=False,
            )
            if inserted:
                count += 1
            elif prompt.response or prompt.turn_json:
                # Backfill missing large fields in a single set-based UPDATE (fast),
                # instead of per-prompt UPDATEs (can be very slow in DuckDB).
                needs_backfill = conn.execute(
                    "SELECT response IS NULL, turn_json IS NULL FROM prompts WHERE id = ?",
                    [prompt.id],
                ).fetchone()
                needs_response = bool(needs_backfill and needs_backfill[0] and prompt.response is not None)
                needs_turn_json = bool(needs_backfill and needs_backfill[1] and prompt.turn_json is not None)
                if needs_response or needs_turn_json:
                    backfill_rows.append((prompt.id, prompt.response, prompt.turn_json))

            if len(backfill_rows) >= backfill_batch_size:
                flush_backfill_rows()

            if batch_rows >= batch_row_limit or batch_bytes >= batch_byte_limit:
                flush_backfill_rows()
                conn.execute("COMMIT")
                conn.execute("BEGIN")
                batch_rows = 0
                batch_bytes = 0

            if progress:
                now = time.monotonic()
                if items_done == 1 or (now - last_emit) > 0.2:
                    progress(items_done, count)
                    last_emit = now

        flush_backfill_rows()

        # Update file state after successful parse
        stat = file_path.stat()
        _update_file_state(
            conn,
            str(file_path),
            parser.source_name,
            stat.st_size,
            stat.st_mtime,
            int(getattr(parser, "sync_version", 1) or 1),
        )
        conn.execute("COMMIT")
        if parser.source_name == "codex" or file_size >= 20 * 1024 * 1024:
            try:
                conn.execute("CHECKPOINT")
            except Exception:
                pass
        if progress:
            progress(items_done, count)
        return count

    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
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

        needs_sync, reason = _file_sync_status(conn, parser, file_path)
        if force:
            needs_sync, reason = True, "forced"

        if progress_callback:
            progress_callback(
                SyncProgress(
                    phase="checking",
                    source=parser.source_name,
                    file_path=file_path,
                    files_checked=idx,
                    files_total=files_total,
                    files_updated=counts["files_updated"],
                    new_prompts_total=counts["total"],
                    new_prompts_in_file=0,
                    skip_reason=reason,
                )
            )

        inserted_in_file = 0
        file_items_done = 0
        skipped = not needs_sync
        error: Optional[str] = None

        if needs_sync:
            counts["files_updated"] += 1
            current_files_updated = counts["files_updated"]
            current_source = parser.source_name
            current_file_path = file_path
            current_idx = idx

            if progress_callback:
                progress_callback(
                    SyncProgress(
                        phase="syncing",
                        source=current_source,
                        file_path=current_file_path,
                        files_checked=current_idx,
                        files_total=files_total,
                        files_updated=current_files_updated,
                        new_prompts_total=counts["total"],
                        new_prompts_in_file=0,
                        file_items_done=0,
                        skipped=False,
                    )
                )

            progress = progress_callback

            def emit_file_progress(
                items_done: int,
                inserted_so_far: int,
                *,
                _source: str = current_source,
                _file_path: Path = current_file_path,
                _idx: int = current_idx,
                _progress: Optional[ProgressCallback] = progress,
                _files_updated: int = current_files_updated,
            ) -> None:
                nonlocal file_items_done
                file_items_done = items_done
                if _progress is None:
                    return
                _progress(
                    SyncProgress(
                        phase="syncing",
                        source=_source,
                        file_path=_file_path,
                        files_checked=_idx,
                        files_total=files_total,
                        files_updated=_files_updated,
                        new_prompts_total=counts["total"] + inserted_so_far,
                        new_prompts_in_file=inserted_so_far,
                        file_items_done=items_done,
                        skipped=False,
                    )
                )

            result = _sync_file(conn, parser, file_path, progress=emit_file_progress)
            if result >= 0:
                inserted_in_file = result
                counts[parser.source_name] = counts.get(parser.source_name, 0) + result
                counts["total"] += result
            else:
                error = "sync failed"

        if progress_callback:
            progress_callback(
                SyncProgress(
                    phase="skipping" if skipped else "syncing",
                    source=parser.source_name,
                    file_path=file_path,
                    files_checked=idx,
                    files_total=files_total,
                    files_updated=counts["files_updated"],
                    new_prompts_total=counts["total"],
                    new_prompts_in_file=inserted_in_file,
                    file_items_done=file_items_done,
                    skipped=skipped,
                    skip_reason=reason,
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

    preserved_table = "pm_preserved_prompt_metadata"
    if preserve_metadata:
        try:
            conn.execute(f"DROP TABLE IF EXISTS {preserved_table}")
            conn.execute(
                f"""
                CREATE TABLE {preserved_table} AS
                SELECT id, starred, tags, use_count
                FROM prompts
                """
            )
        except Exception:
            preserve_metadata = False

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

    if preserve_metadata:
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

        try:
            conn.execute(
                f"""
                UPDATE prompts
                SET starred = {preserved_table}.starred,
                    tags = {preserved_table}.tags,
                    use_count = {preserved_table}.use_count,
                    updated_at = CURRENT_TIMESTAMP
                FROM {preserved_table}
                WHERE prompts.id = {preserved_table}.id
                """
            )
        finally:
            try:
                conn.execute(f"DROP TABLE IF EXISTS {preserved_table}")
            except Exception:
                pass

    if progress_callback:
        progress_callback(
            SyncProgress(
                phase="compacting",
                source="",
                file_path=Path(),
                files_checked=counts.get("files_checked", 0),
                files_total=counts.get("files_checked", 0),
                files_updated=counts.get("files_updated", 0),
                new_prompts_total=counts.get("total", 0),
                new_prompts_in_file=0,
            )
        )

    try:
        conn.execute("CHECKPOINT")
    except Exception:
        pass
    try:
        conn.execute("VACUUM")
    except Exception:
        pass

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
        if not force and not _file_needs_sync(conn, parser, file_path):
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
            if _file_needs_sync(conn, parser, file_path):
                updates[parser.source_name] += 1

    return updates
