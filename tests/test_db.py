import unittest

import duckdb

from prompt_manager.db import _init_schema, get_prompt, insert_prompt, search_prompt_summaries


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
