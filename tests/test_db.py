import unittest

import duckdb

from prompt_manager.db import _init_schema, insert_prompt, search_prompt_summaries


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
