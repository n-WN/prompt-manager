import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from prompt_manager.parsers.claude_code import ClaudeCodeParser
from prompt_manager.parsers.cursor import CursorParser
from prompt_manager.parsers.gemini_cli import GeminiCliParser


class TestGeminiCliParser(unittest.TestCase):
    def test_parses_session_json(self) -> None:
        parser = GeminiCliParser()
        with tempfile.TemporaryDirectory() as tmp:
            session_path = Path(tmp) / "session.json"
            session_path.write_text(
                json.dumps(
                    {
                        "projectHash": "projhash",
                        "sessionId": "sess1",
                        "startTime": "2025-10-14T12:00:00.000Z",
                        "lastUpdated": "2025-10-14T12:01:00.000Z",
                        "messages": [
                            {
                                "id": "m1",
                                "timestamp": "2025-10-14T12:00:00.000Z",
                                "type": "user",
                                "content": "User prompt long enough",
                            },
                            {
                                "id": "m2",
                                "timestamp": "2025-10-14T12:00:01.000Z",
                                "type": "gemini",
                                "content": "First answer",
                            },
                            {
                                "id": "m3",
                                "timestamp": "2025-10-14T12:00:02.000Z",
                                "type": "gemini",
                                "content": "Second answer",
                            },
                            {
                                "id": "m4",
                                "timestamp": "2025-10-14T12:00:03.000Z",
                                "type": "user",
                                "content": "Second user prompt long enough",
                            },
                            {
                                "id": "m5",
                                "timestamp": "2025-10-14T12:00:04.000Z",
                                "type": "gemini",
                                "content": "Second reply",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            prompts = list(parser.parse_file(session_path))
            self.assertEqual(len(prompts), 2)
            self.assertEqual(prompts[0].source, "gemini_cli")
            self.assertEqual(prompts[0].session_id, "sess1")
            self.assertEqual(prompts[0].content, "User prompt long enough")
            self.assertEqual(prompts[0].response, "First answer\nSecond answer")


class TestClaudeCodeParser(unittest.TestCase):
    def test_user_boundary_with_list_content(self) -> None:
        parser = ClaudeCodeParser()
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "my-project"
            log_dir.mkdir(parents=True)
            log_path = log_dir / "session.jsonl"

            lines = [
                {
                    "type": "user",
                    "message": {
                        "role": "user",
                        "content": [{"type": "text", "text": "First prompt long enough"}],
                    },
                    "timestamp": "2025-10-14T12:00:00.000Z",
                    "sessionId": "s",
                    "cwd": "/tmp",
                },
                {
                    "type": "assistant",
                    "message": {
                        "role": "assistant",
                        "content": [{"type": "text", "text": "First response"}],
                    },
                },
                {
                    "type": "user",
                    "message": {
                        "role": "user",
                        "content": [{"type": "text", "text": "Second prompt long enough"}],
                    },
                    "timestamp": "2025-10-14T12:00:10.000Z",
                    "sessionId": "s",
                    "cwd": "/tmp",
                },
                {
                    "type": "assistant",
                    "message": {
                        "role": "assistant",
                        "content": [{"type": "text", "text": "Second response"}],
                    },
                },
            ]
            log_path.write_text("\n".join(json.dumps(x) for x in lines), encoding="utf-8")

            prompts = list(parser.parse_file(log_path))
            self.assertEqual(len(prompts), 2)
            self.assertEqual(prompts[0].content, "First prompt long enough")
            self.assertEqual(prompts[0].response, "First response")
            self.assertEqual(prompts[1].content, "Second prompt long enough")
            self.assertEqual(prompts[1].response, "Second response")


class TestCursorStateVscdbParser(unittest.TestCase):
    def test_parses_state_vscdb_bubbles(self) -> None:
        parser = CursorParser()
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            repo_root = tmp_path / "repo"
            (repo_root / ".git").mkdir(parents=True)
            file_path = repo_root / "src" / "file.py"
            file_path.parent.mkdir(parents=True)
            file_path.write_text("print('hi')\n", encoding="utf-8")

            db_path = tmp_path / "state.vscdb"
            conn = sqlite3.connect(db_path)
            try:
                conn.execute("CREATE TABLE cursorDiskKV (key TEXT, value BLOB)")
                composer_id = "composer-1"
                composer = {
                    "composerId": composer_id,
                    "createdAt": "2025-10-14T12:00:00.000Z",
                    "codeBlockData": {
                        "file:///dummy": {"uri": {"fsPath": str(file_path)}}
                    },
                }
                conn.execute(
                    "INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)",
                    (f"composerData:{composer_id}", json.dumps(composer).encode("utf-8")),
                )

                bubbles = [
                    (
                        f"bubbleId:{composer_id}:b1",
                        {
                            "bubbleId": "b1",
                            "type": 1,
                            "text": "User prompt long enough",
                            "createdAt": "2025-10-14T12:00:00.000Z",
                        },
                    ),
                    (
                        f"bubbleId:{composer_id}:b2",
                        {
                            "bubbleId": "b2",
                            "type": 2,
                            "text": "Assistant reply",
                            "createdAt": "2025-10-14T12:00:01.000Z",
                        },
                    ),
                    (
                        f"bubbleId:{composer_id}:b3",
                        {
                            "bubbleId": "b3",
                            "type": 2,
                            "text": "More reply",
                            "createdAt": "2025-10-14T12:00:02.000Z",
                        },
                    ),
                ]
                conn.executemany(
                    "INSERT INTO cursorDiskKV (key, value) VALUES (?, ?)",
                    [(k, json.dumps(v).encode("utf-8")) for k, v in bubbles],
                )
                conn.commit()
            finally:
                conn.close()

            prompts = list(parser.parse_file(db_path))
            self.assertEqual(len(prompts), 1)
            prompt = prompts[0]
            self.assertEqual(prompt.session_id, composer_id)
            self.assertTrue((prompt.project_path or "").startswith("cursor:"))
            self.assertEqual(prompt.content, "User prompt long enough")
            self.assertEqual(prompt.response, "Assistant reply\nMore reply")

