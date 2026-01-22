import tempfile
import unittest
from pathlib import Path

import duckdb

from prompt_manager.db import _init_schema
from prompt_manager.parsers import BaseParser, ParsedPrompt
from prompt_manager.sync import sync_all


class _DummyParser(BaseParser):
    source_name = "dummy"

    def __init__(self, file_path: Path, *, sync_version: int):
        self._file_path = file_path
        self.sync_version = sync_version

    def find_log_files(self):
        yield self._file_path

    def parse_file(self, _file_path: Path):
        yield ParsedPrompt(
            id=self.generate_id(self.source_name, "hello", "sess", "t0"),
            source=self.source_name,
            content="hello",
            session_id="sess",
        )


class TestSyncVersion(unittest.TestCase):
    def test_sync_version_triggers_resync_for_unchanged_files(self) -> None:
        conn = duckdb.connect(":memory:")
        try:
            _init_schema(conn)
            with tempfile.TemporaryDirectory() as tmp:
                file_path = Path(tmp) / "log.txt"
                file_path.write_text("data", encoding="utf-8")

                counts1 = sync_all(conn, parsers=[_DummyParser(file_path, sync_version=1)])
                self.assertEqual(counts1["files_updated"], 1)

                counts2 = sync_all(conn, parsers=[_DummyParser(file_path, sync_version=1)])
                self.assertEqual(counts2["files_updated"], 0)

                counts3 = sync_all(conn, parsers=[_DummyParser(file_path, sync_version=2)])
                self.assertEqual(counts3["files_updated"], 1)

                counts4 = sync_all(conn, parsers=[_DummyParser(file_path, sync_version=2)])
                self.assertEqual(counts4["files_updated"], 0)
        finally:
            conn.close()

    def test_file_sync_state_schema_migrates_sync_version(self) -> None:
        conn = duckdb.connect(":memory:")
        try:
            _init_schema(conn)
            conn.execute(
                """
                CREATE TABLE file_sync_state (
                    file_path VARCHAR PRIMARY KEY,
                    source VARCHAR NOT NULL,
                    file_size BIGINT,
                    mtime DOUBLE,
                    last_sync TIMESTAMP
                )
                """
            )
            with tempfile.TemporaryDirectory() as tmp:
                file_path = Path(tmp) / "log.txt"
                file_path.write_text("data", encoding="utf-8")
                conn.execute(
                    "INSERT INTO file_sync_state (file_path, source, file_size, mtime, last_sync) VALUES (?,?,?,?,NOW())",
                    [str(file_path), "dummy", 0, 0.0],
                )
                sync_all(conn, parsers=[_DummyParser(file_path, sync_version=1)])

            columns = {row[1] for row in conn.execute("PRAGMA table_info('file_sync_state')").fetchall()}
            self.assertIn("sync_version", columns)
        finally:
            conn.close()

    def test_force_does_not_sync_missing_files(self) -> None:
        class MissingParser(BaseParser):
            source_name = "missing"

            def __init__(self, path: Path):
                self._path = path

            def find_log_files(self):
                yield self._path

            def parse_file(self, _file_path: Path):
                # Should never be called when the file doesn't exist.
                raise AssertionError("parse_file should not be called for missing files")

        conn = duckdb.connect(":memory:")
        try:
            _init_schema(conn)
            missing = Path("/tmp/pm-missing-file-does-not-exist-12345.log")
            counts = sync_all(conn, force=True, parsers=[MissingParser(missing)])
            self.assertEqual(int(counts.get("files_failed") or 0), 0)
            self.assertEqual(int(counts.get("files_skipped") or 0), 1)
        finally:
            conn.close()
