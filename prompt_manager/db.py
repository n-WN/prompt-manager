"""DuckDB database operations for prompt storage."""

import duckdb
from pathlib import Path
from typing import Optional
from datetime import datetime

DB_PATH = Path.home() / ".prompt-manager" / "prompts.duckdb"


def get_connection() -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection, creating the database if needed."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DB_PATH))
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
            content TEXT NOT NULL,             -- The actual prompt text
            response TEXT,                     -- LLM response text
            turn_json TEXT,                    -- Per-turn raw timeline (JSON)
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
    timestamp: Optional[datetime] = None,
    response: Optional[str] = None,
    turn_json: Optional[str] = None,
) -> bool:
    """Insert a prompt if it doesn't exist.

    Returns:
        True if a new row was inserted, False if it already existed.
    """
    inserted = conn.execute(
        """
        INSERT INTO prompts (id, source, project_path, session_id, content, timestamp, response, turn_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO NOTHING
        RETURNING id
        """,
        [id, source, project_path, session_id, content, timestamp, response, turn_json],
    ).fetchone()

    if inserted:
        return True

    # Existing prompt: opportunistically fill in missing fields without
    # counting it as a "new prompt" for sync stats.
    if response or turn_json:
        conn.execute(
            """
            UPDATE prompts
            SET response = COALESCE(response, ?),
                turn_json = COALESCE(turn_json, ?),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            [response, turn_json, id],
        )

    return False


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
