"""Sync prompts from all sources to the database with incremental updates."""

import duckdb
import os
from pathlib import Path
from typing import Optional
from datetime import datetime

from .db import get_connection, insert_prompt
from .parsers.claude_code import ClaudeCodeParser
from .parsers.cursor import CursorParser
from .parsers.aider import AiderParser
from .parsers.codex import CodexParser
from .parsers.gemini_cli import GeminiCliParser


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


def sync_all(conn: Optional[duckdb.DuckDBPyConnection] = None, force: bool = False) -> dict:
    """Sync prompts from all sources.

    Args:
        conn: Database connection
        force: If True, re-sync all files regardless of state

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

    parsers = [
        ClaudeCodeParser(),
        CursorParser(),
        AiderParser(),
        CodexParser(),
        GeminiCliParser(),
    ]

    for parser in parsers:
        source_count = 0

        for file_path in parser.find_log_files():
            counts["files_checked"] += 1

            # Skip unchanged files unless forced
            if not force and not _file_needs_sync(conn, file_path):
                continue

            counts["files_updated"] += 1

            # Parse and insert prompts from this file
            result = _sync_file(conn, parser, file_path)
            if result >= 0:
                source_count += result

        counts[parser.source_name] = source_count
        counts["total"] += source_count

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
