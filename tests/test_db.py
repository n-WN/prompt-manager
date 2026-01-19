import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from prompt_manager.db import (
    _init_schema,
    get_prompt,
    insert_prompt,
    search_prompt_summaries,
    search_prompt_summaries_balanced,
)


class TestDuckDbSchema(unittest.TestCase):
    def test_drops_content_index_and_allows_long_content(self) -> None:
        conn = duckdb.connect(":memory:")
        try:
            _init_schema(conn)

            # Simulate legacy installations that created a content index.
            conn.execute("CREATE INDEX idx_prompts_content ON prompts(content)")
            _init_schema(conn)

            index_names = [
                row[0]
                for row in conn.execute(
                    "SELECT index_name FROM duckdb_indexes() WHERE table_name='prompts'"
                ).fetchall()
            ]
            self.assertNotIn("idx_prompts_content", index_names)

            long_content = "x" * 130_000
            inserted = insert_prompt(conn, id="p1", source="codex", content=long_content)
            self.assertTrue(inserted)
        finally:
            conn.close()

    def test_search_prompt_summaries_truncates_and_omits_response(self) -> None:
        conn = duckdb.connect(":memory:")
        try:
            _init_schema(conn)
            insert_prompt(
                conn,
                id="p1",
                source="codex",
                content="hello world",
                response="should not be returned",
            )
            insert_prompt(
                conn,
                id="p2",
                source="codex",
                content="x" * 1000,
                response="also hidden",
            )

            rows = search_prompt_summaries(conn, query="hello", limit=10, snippet_len=5)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["id"], "p1")
            self.assertLessEqual(len(rows[0]["content"]), 5)
            self.assertNotIn("response", rows[0])
        finally:
            conn.close()

    def test_balanced_summaries_include_each_source(self) -> None:
        conn = duckdb.connect(":memory:")
        try:
            _init_schema(conn)
            now = datetime.now(tz=timezone.utc)

            # Many codex rows dominate recency.
            for idx in range(200):
                insert_prompt(
                    conn,
                    id=f"cx{idx}",
                    source="codex",
                    content=f"codex {idx}",
                    timestamp=now + timedelta(seconds=idx),
                )

            insert_prompt(
                conn,
                id="cu1",
                source="cursor",
                content="cursor prompt",
                timestamp=now - timedelta(days=1),
            )
            insert_prompt(
                conn,
                id="cc1",
                source="claude_code",
                content="claude prompt",
                timestamp=now - timedelta(days=2),
            )
            insert_prompt(
                conn,
                id="gm1",
                source="gemini_cli",
                content="gemini prompt",
                timestamp=now - timedelta(days=3),
            )

            recent = search_prompt_summaries(conn, limit=50)
            self.assertEqual({row["source"] for row in recent}, {"codex"})

            balanced = search_prompt_summaries_balanced(
                conn,
                sources=["claude_code", "cursor", "codex", "gemini_cli"],
                per_source_limit=10,
            )
            sources = {row["source"] for row in balanced}
            self.assertIn("codex", sources)
            self.assertIn("cursor", sources)
            self.assertIn("claude_code", sources)
            self.assertIn("gemini_cli", sources)
        finally:
            conn.close()

    def test_large_fields_are_stored_compressed_and_roundtrip(self) -> None:
        conn = duckdb.connect(":memory:")
        try:
            _init_schema(conn)
            response = "r" * 50_000
            turn_json = "[" + ",".join(["{\"k\":\"v\"}"] * 20_000) + "]"

            inserted = insert_prompt(
                conn,
                id="p1",
                source="codex",
                content="hello world",
                response=response,
                turn_json=turn_json,
            )
            self.assertTrue(inserted)

            stored = conn.execute(
                "SELECT response, response_blob, turn_json, turn_json_blob FROM prompts WHERE id = ?",
                ["p1"],
            ).fetchone()
            self.assertIsNotNone(stored)
            self.assertIsNotNone(stored[1])  # response_blob
            self.assertIsNotNone(stored[3])  # turn_json_blob

            row = get_prompt(conn, "p1")
            self.assertIsNotNone(row)
            self.assertEqual(row["response"], response)
            self.assertEqual(row["turn_json"], turn_json)
        finally:
            conn.close()

    def test_amp_turn_json_hydrates_from_origin_indices(self) -> None:
        conn = duckdb.connect(":memory:")
        try:
            _init_schema(conn)
            with tempfile.TemporaryDirectory() as tmp:
                thread_path = Path(tmp) / "T-1.json"
                thread_path.write_text(
                    json.dumps(
                        {
                            "id": "T-1",
                            "messages": [
                                {"role": "user", "messageId": 0, "content": [{"type": "text", "text": "hi"}]},
                                {"role": "assistant", "messageId": 1, "content": [{"type": "text", "text": "ok"}]},
                                {"role": "user", "messageId": 2, "content": [{"type": "tool_result"}]},
                                {"role": "assistant", "messageId": 3, "content": [{"type": "text", "text": "done"}]},
                            ],
                        },
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )

                insert_prompt(
                    conn,
                    id="p1",
                    source="amp",
                    content="hi",
                    session_id="T-1",
                    origin_path=str(thread_path),
                    origin_offset_start=0,
                    origin_offset_end=3,
                    response="ok",
                    turn_json=None,
                )

                row = get_prompt(conn, "p1")
                self.assertIsNotNone(row)
                turn = json.loads(row.get("turn_json") or "null")
                self.assertIsInstance(turn, list)
                self.assertEqual(len(turn), 3)
                self.assertEqual(turn[0].get("role"), "user")
                self.assertEqual(turn[1].get("role"), "assistant")
        finally:
            conn.close()
