import unittest

import duckdb

from prompt_manager.db import _init_schema, insert_prompt


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

