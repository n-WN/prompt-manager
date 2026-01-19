"""DuckDB database operations for prompt storage."""

import duckdb
import os
import sys
import zlib
from pathlib import Path
from typing import Optional
from datetime import datetime

_DEFAULT_DB_PATH = Path.home() / ".prompt-manager" / "prompts.duckdb"


_INLINE_TEXT_BYTES = int(os.environ.get("PROMPT_MANAGER_INLINE_TEXT_BYTES", "8192"))
_RESPONSE_PREVIEW_CHARS = int(os.environ.get("PROMPT_MANAGER_RESPONSE_PREVIEW_CHARS", "4000"))
_COMPRESS_LEVEL = int(os.environ.get("PROMPT_MANAGER_COMPRESS_LEVEL", "1"))


def get_default_db_path() -> Path:
    return _DEFAULT_DB_PATH


def get_db_path() -> Path:
    """Return the configured database path.

    Supports overriding via `PROMPT_MANAGER_DB_PATH`.
    """
    env = os.environ.get("PROMPT_MANAGER_DB_PATH")
    if env:
        return Path(env).expanduser()
    return _DEFAULT_DB_PATH


def _recovered_db_path(db_path: Path) -> Path:
    return db_path.with_name(f"{db_path.stem}.recovered{db_path.suffix}")


def get_recovered_db_path(db_path: Optional[Path] = None) -> Path:
    return _recovered_db_path(db_path or get_default_db_path())


def _is_wal_replay_error(error: Exception) -> bool:
    msg = str(error).lower()
    return "replaying wal file" in msg or ("wal" in msg and "replay" in msg and "failure" in msg)


def _wants_store_blobs() -> bool:
    value = os.environ.get("PROMPT_MANAGER_STORE_BLOBS", "1").strip().lower()
    return value not in {"0", "false", "no", "off"}

def _decompress_text(blob: bytes) -> Optional[str]:
    try:
        raw = zlib.decompress(blob)
    except Exception:
        return None
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("utf-8", errors="replace")


def _load_jsonl_range_as_array(path: str, start: int, end: int) -> Optional[str]:
    try:
        with open(path, "rb") as f:
            f.seek(int(start))
            chunk = f.read(int(end) - int(start))
    except Exception:
        return None

    try:
        text = chunk.decode("utf-8")
    except UnicodeDecodeError:
        text = chunk.decode("utf-8", errors="replace")

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return None
    return "[" + ",".join(lines) + "]"


def pack_large_text(
    text: Optional[str],
    *,
    keep_preview: bool = False,
) -> tuple[Optional[str], Optional[bytes]]:
    """Pack a potentially large text field into (inline_text, compressed_blob)."""
    if text is None:
        return None, None

    wants_blobs = _wants_store_blobs()
    if not wants_blobs:
        if not keep_preview:
            return None, None
        preview = text[:_RESPONSE_PREVIEW_CHARS]
        return preview, None

    raw = text.encode("utf-8")
    if len(raw) <= _INLINE_TEXT_BYTES:
        return text, None

    inline = text[:_RESPONSE_PREVIEW_CHARS] if keep_preview else None
    return inline, zlib.compress(raw, level=_COMPRESS_LEVEL)


def get_connection() -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection, creating the database if needed."""
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        conn = duckdb.connect(str(db_path))
    except Exception as exc:
        if os.environ.get("PROMPT_MANAGER_DB_PATH") is None and _is_wal_replay_error(exc):
            recovered = _recovered_db_path(db_path)
            print(
                (
                    "WARNING: DuckDB failed to replay the WAL for the prompt-manager database.\n"
                    f"  db:  {db_path}\n"
                    f"  wal: {db_path}.wal\n"
                    "Falling back to a recovered database file:\n"
                    f"  db:  {recovered}\n"
                    "Run `pm rebuild` to re-import logs into the recovered database."
                ),
                file=sys.stderr,
            )
            conn = duckdb.connect(str(recovered))
        else:
            raise

    _init_schema(conn)
    return conn


def _init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Initialize the database schema."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS prompts (
            id VARCHAR PRIMARY KEY,
            source VARCHAR NOT NULL,           -- 'claude_code', 'cursor', 'aider', 'codex', 'gemini_cli'
            project_path VARCHAR,              -- Original project path
            session_id VARCHAR,                -- Session/conversation ID
            origin_path VARCHAR,               -- Source log file path (read-only reference)
            origin_offset_start BIGINT,        -- Optional byte offset start within origin_path
            origin_offset_end BIGINT,          -- Optional byte offset end within origin_path
            content TEXT NOT NULL,             -- The actual prompt text
            response TEXT,                     -- LLM response text
            response_blob BLOB,                -- Optional compressed response
            turn_json TEXT,                    -- Per-turn raw timeline (JSON)
            turn_json_blob BLOB,               -- Optional compressed turn JSON
            timestamp TIMESTAMP,               -- When the prompt was created
            tags VARCHAR[],                    -- User-defined tags
            starred BOOLEAN DEFAULT FALSE,     -- User favorites
            use_count INTEGER DEFAULT 0,       -- How many times reused
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Add response column if not exists (for existing databases)
    try:
        conn.execute("ALTER TABLE prompts ADD COLUMN response TEXT")
    except Exception:
        pass  # Column already exists

    # Add turn_json column if not exists (for existing databases)
    try:
        conn.execute("ALTER TABLE prompts ADD COLUMN turn_json TEXT")
    except Exception:
        pass  # Column already exists

    try:
        conn.execute("ALTER TABLE prompts ADD COLUMN response_blob BLOB")
    except Exception:
        pass

    try:
        conn.execute("ALTER TABLE prompts ADD COLUMN turn_json_blob BLOB")
    except Exception:
        pass

    try:
        conn.execute("ALTER TABLE prompts ADD COLUMN origin_path VARCHAR")
    except Exception:
        pass

    try:
        conn.execute("ALTER TABLE prompts ADD COLUMN origin_offset_start BIGINT")
    except Exception:
        pass

    try:
        conn.execute("ALTER TABLE prompts ADD COLUMN origin_offset_end BIGINT")
    except Exception:
        pass

    conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_state (
            source VARCHAR PRIMARY KEY,
            last_sync TIMESTAMP,
            last_file_path VARCHAR,
            last_position INTEGER
        )
    """)

    # Avoid indexing raw prompt content:
    # DuckDB ART indexes have a maximum key size (~120KB). Real-world agent logs
    # can easily exceed this (e.g. large diffs / tool outputs), causing commits to
    # fail. Drop legacy index if present.
    try:
        conn.execute("DROP INDEX IF EXISTS idx_prompts_content")
    except Exception:
        pass

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_prompts_source
        ON prompts(source)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_prompts_timestamp
        ON prompts(timestamp DESC)
    """)


def insert_prompt(
    conn: duckdb.DuckDBPyConnection,
    id: str,
    source: str,
    content: str,
    project_path: Optional[str] = None,
    session_id: Optional[str] = None,
    origin_path: Optional[str] = None,
    origin_offset_start: Optional[int] = None,
    origin_offset_end: Optional[int] = None,
    timestamp: Optional[datetime] = None,
    response: Optional[str] = None,
    turn_json: Optional[str] = None,
    backfill_missing_fields: bool = True,
) -> bool:
    """Insert a prompt if it doesn't exist.

    Returns:
        True if a new row was inserted, False if it already existed.
    """
    inline_response, response_blob = pack_large_text(response, keep_preview=True)
    inline_turn_json, turn_blob = pack_large_text(turn_json, keep_preview=False)
    inserted = conn.execute(
        """
        INSERT INTO prompts (
            id, source, project_path, session_id, origin_path,
            origin_offset_start, origin_offset_end,
            content, timestamp,
            response, response_blob,
            turn_json, turn_json_blob
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO NOTHING
        RETURNING id
        """,
        [
            id,
            source,
            project_path,
            session_id,
            origin_path,
            origin_offset_start,
            origin_offset_end,
            content,
            timestamp,
            inline_response,
            response_blob,
            inline_turn_json,
            turn_blob,
        ],
    ).fetchone()

    if inserted:
        return True

    # Existing prompt: opportunistically fill in missing fields without
    # counting it as a "new prompt" for sync stats.
    if backfill_missing_fields and (response or turn_json or origin_path):
        needs_backfill = conn.execute(
            """
            SELECT
                (response IS NULL AND response_blob IS NULL) AS response_missing,
                (turn_json IS NULL AND turn_json_blob IS NULL) AS turn_missing,
                origin_path IS NULL AS origin_missing,
                origin_offset_start IS NULL AS offset_start_missing,
                origin_offset_end IS NULL AS offset_end_missing
            FROM prompts
            WHERE id = ?
            """,
            [id],
        ).fetchone()

        needs_response = bool(needs_backfill and needs_backfill[0] and (inline_response or response_blob))
        needs_turn_json = bool(needs_backfill and needs_backfill[1] and (inline_turn_json or turn_blob))
        needs_origin = bool(needs_backfill and needs_backfill[2] and origin_path)
        needs_offset_start = bool(needs_backfill and needs_backfill[3] and origin_offset_start is not None)
        needs_offset_end = bool(needs_backfill and needs_backfill[4] and origin_offset_end is not None)

        if needs_response or needs_turn_json or needs_origin or needs_offset_start or needs_offset_end:
            conn.execute(
                """
                UPDATE prompts
                SET response = COALESCE(response, ?),
                    response_blob = COALESCE(response_blob, ?),
                    turn_json = COALESCE(turn_json, ?),
                    turn_json_blob = COALESCE(turn_json_blob, ?),
                    origin_path = COALESCE(origin_path, ?),
                    origin_offset_start = COALESCE(origin_offset_start, ?),
                    origin_offset_end = COALESCE(origin_offset_end, ?),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                [
                    inline_response,
                    response_blob,
                    inline_turn_json,
                    turn_blob,
                    origin_path,
                    origin_offset_start,
                    origin_offset_end,
                    id,
                ],
            )

    return False


def search_prompt_summaries(
    conn: duckdb.DuckDBPyConnection,
    query: Optional[str] = None,
    source: Optional[str] = None,
    starred_only: bool = False,
    limit: int = 1000,
    offset: int = 0,
    snippet_len: int = 400,
) -> list[dict]:
    """Search prompt rows for list views (returns truncated content, no response).

    This avoids loading potentially huge prompt/response bodies just to populate the
    tree/list UI. Use `get_prompt()` to fetch full content for a selected row.
    """
    conditions = []
    params = []

    if query:
        conditions.append("content ILIKE ?")
        params.append(f"%{query}%")

    if source:
        conditions.append("source = ?")
        params.append(source)

    if starred_only:
        conditions.append("starred = TRUE")

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    result = conn.execute(
        f"""
        SELECT id, source, project_path, session_id,
               SUBSTR(content, 1, ?) AS content,
               timestamp, tags, starred, use_count, created_at
        FROM prompts
        WHERE {where_clause}
        ORDER BY timestamp DESC NULLS LAST
        LIMIT ? OFFSET ?
        """,
        [int(snippet_len)] + params + [limit, offset],
    ).fetchall()

    columns = [
        "id",
        "source",
        "project_path",
        "session_id",
        "content",
        "timestamp",
        "tags",
        "starred",
        "use_count",
        "created_at",
    ]
    return [dict(zip(columns, row)) for row in result]


def search_prompts(
    conn: duckdb.DuckDBPyConnection,
    query: Optional[str] = None,
    source: Optional[str] = None,
    starred_only: bool = False,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    """Search prompts with optional filters."""
    conditions = []
    params = []

    if query:
        conditions.append("content ILIKE ?")
        params.append(f"%{query}%")

    if source:
        conditions.append("source = ?")
        params.append(source)

    if starred_only:
        conditions.append("starred = TRUE")

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    result = conn.execute(f"""
        SELECT id, source, project_path, session_id, content, timestamp,
               tags, starred, use_count, created_at, response
        FROM prompts
        WHERE {where_clause}
        ORDER BY timestamp DESC NULLS LAST
        LIMIT ? OFFSET ?
    """, params + [limit, offset]).fetchall()

    columns = ['id', 'source', 'project_path', 'session_id', 'content',
               'timestamp', 'tags', 'starred', 'use_count', 'created_at', 'response']
    return [dict(zip(columns, row)) for row in result]


def get_prompt(conn: duckdb.DuckDBPyConnection, prompt_id: str) -> Optional[dict]:
    """Get a single prompt by ID (includes turn_json when available)."""
    result = conn.execute(
        """
        SELECT id, source, project_path, session_id, content, timestamp,
               tags, starred, use_count, created_at, response, turn_json
             , response_blob, turn_json_blob, origin_path
             , origin_offset_start, origin_offset_end
        FROM prompts
        WHERE id = ?
        """,
        [prompt_id],
    ).fetchone()

    if not result:
        return None

    columns = [
        "id",
        "source",
        "project_path",
        "session_id",
        "content",
        "timestamp",
        "tags",
        "starred",
        "use_count",
        "created_at",
        "response",
        "turn_json",
        "response_blob",
        "turn_json_blob",
        "origin_path",
        "origin_offset_start",
        "origin_offset_end",
    ]
    row = dict(zip(columns, result))

    if row.get("response_blob") is not None:
        restored = _decompress_text(row["response_blob"])
        if restored is not None:
            row["response"] = restored

    if row.get("turn_json_blob") is not None:
        restored = _decompress_text(row["turn_json_blob"])
        if restored is not None:
            row["turn_json"] = restored

    if (
        row.get("turn_json") is None
        and row.get("source") == "codex"
        and row.get("origin_path")
        and row.get("origin_offset_start") is not None
        and row.get("origin_offset_end") is not None
    ):
        restored = _load_jsonl_range_as_array(
            str(row["origin_path"]),
            int(row["origin_offset_start"]),
            int(row["origin_offset_end"]),
        )
        if restored is not None:
            row["turn_json"] = restored

    row.pop("response_blob", None)
    row.pop("turn_json_blob", None)
    return row


def get_prompt_preview(conn: duckdb.DuckDBPyConnection, prompt_id: str) -> Optional[dict]:
    """Get prompt fields needed for list/preview views (does not hydrate turn_json)."""
    result = conn.execute(
        """
        SELECT id, source, project_path, session_id, content, timestamp,
               tags, starred, use_count, created_at, response
        FROM prompts
        WHERE id = ?
        """,
        [prompt_id],
    ).fetchone()
    if not result:
        return None

    columns = [
        "id",
        "source",
        "project_path",
        "session_id",
        "content",
        "timestamp",
        "tags",
        "starred",
        "use_count",
        "created_at",
        "response",
    ]
    return dict(zip(columns, result))


def toggle_star(conn: duckdb.DuckDBPyConnection, prompt_id: str) -> bool:
    """Toggle the starred status of a prompt."""
    conn.execute("""
        UPDATE prompts
        SET starred = NOT starred, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, [prompt_id])
    result = conn.execute("SELECT starred FROM prompts WHERE id = ?", [prompt_id]).fetchone()
    return result[0] if result else False


def increment_use_count(conn: duckdb.DuckDBPyConnection, prompt_id: str) -> None:
    """Increment the use count when a prompt is copied/reused."""
    conn.execute("""
        UPDATE prompts
        SET use_count = use_count + 1, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, [prompt_id])


def get_stats(conn: duckdb.DuckDBPyConnection) -> dict:
    """Get statistics about stored prompts."""
    result = conn.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN source = 'claude_code' THEN 1 END) as claude_code,
            COUNT(CASE WHEN source = 'cursor' THEN 1 END) as cursor,
            COUNT(CASE WHEN source = 'aider' THEN 1 END) as aider,
            COUNT(CASE WHEN source = 'codex' THEN 1 END) as codex,
            COUNT(CASE WHEN source = 'gemini_cli' THEN 1 END) as gemini_cli,
            COUNT(CASE WHEN starred THEN 1 END) as starred,
            SUM(use_count) as total_uses
        FROM prompts
    """).fetchone()

    return {
        'total': result[0],
        'claude_code': result[1],
        'cursor': result[2],
        'aider': result[3],
        'codex': result[4],
        'gemini_cli': result[5],
        'starred': result[6],
        'total_uses': result[7] or 0,
    }


def delete_prompt(conn: duckdb.DuckDBPyConnection, prompt_id: str) -> None:
    """Delete a prompt by ID."""
    conn.execute("DELETE FROM prompts WHERE id = ?", [prompt_id])
