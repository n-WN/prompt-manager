import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from prompt_manager.parsers.claude_code import ClaudeCodeParser
from prompt_manager.parsers.codex import CodexParser
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
            self.assertIsNotNone(prompts[0].turn_json)
            turn0 = json.loads(prompts[0].turn_json or "[]")
            self.assertEqual([m.get("id") for m in turn0], ["m1", "m2", "m3"])
            turn1 = json.loads(prompts[1].turn_json or "[]")
            self.assertEqual([m.get("id") for m in turn1], ["m4", "m5"])


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
            self.assertIsNotNone(prompts[0].turn_json)
            self.assertIsNotNone(prompts[1].turn_json)
            turn0 = json.loads(prompts[0].turn_json or "[]")
            turn1 = json.loads(prompts[1].turn_json or "[]")
            self.assertEqual(len(turn0), 2)
            self.assertEqual(len(turn1), 2)
            self.assertNotIn("Second prompt long enough", prompts[0].turn_json or "")
            self.assertIn("Second prompt long enough", prompts[1].turn_json or "")

    def test_filters_local_command_transcripts_and_decodes_project_path(self) -> None:
        parser = ClaudeCodeParser()
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "-Users-test-project"
            log_dir.mkdir(parents=True)
            log_path = log_dir / "session.jsonl"

            lines = [
                {
                    "type": "user",
                    "isMeta": True,
                    "message": {
                        "role": "user",
                        "content": "<local-command-caveat>ignore</local-command-caveat>",
                    },
                },
                {
                    "type": "user",
                    "message": {
                        "role": "user",
                        "content": "<command-name>ls</command-name>",
                    },
                },
                {
                    "type": "user",
                    "message": {
                        "role": "user",
                        "content": "Actual prompt long enough",
                    },
                    "timestamp": "2025-10-14T12:00:00.000Z",
                    "sessionId": "s",
                    "cwd": "/tmp",
                },
                {
                    "type": "assistant",
                    "message": {
                        "role": "assistant",
                        "content": "Assistant response",
                    },
                },
            ]
            log_path.write_text("\n".join(json.dumps(x) for x in lines), encoding="utf-8")

            prompts = list(parser.parse_file(log_path))
            self.assertEqual(len(prompts), 1)
            self.assertEqual(prompts[0].content, "Actual prompt long enough")
            self.assertEqual(prompts[0].response, "Assistant response")
            self.assertFalse((prompts[0].project_path or "").startswith("//"))


class TestCodexParser(unittest.TestCase):
    def test_parses_short_user_messages_and_preserves_turn_timeline(self) -> None:
        parser = CodexParser(base_path=Path("/does/not/matter"))
        with tempfile.TemporaryDirectory() as tmp:
            rollout = Path(tmp) / "rollout.jsonl"
            lines = [
                {
                    "timestamp": "2026-01-19T01:45:34.000Z",
                    "type": "session_meta",
                    "payload": {"id": "sess", "cwd": "/proj"},
                },
                {
                    "timestamp": "2026-01-19T01:45:39.000Z",
                    "type": "response_item",
                    "payload": {
                        "type": "message",
                        "role": "user",
                        "content": [{"type": "input_text", "text": "short1"}],
                    },
                },
                {
                    "timestamp": "2026-01-19T01:45:39.100Z",
                    "type": "event_msg",
                    "payload": {
                        "type": "user_message",
                        "message": "short1",
                        "images": [],
                        "local_images": [],
                        "text_elements": [],
                    },
                },
                {
                    "timestamp": "2026-01-19T01:45:39.500Z",
                    "type": "event_msg",
                    "payload": {"type": "agent_message", "message": "reply1"},
                },
                {
                    "timestamp": "2026-01-19T01:45:40.000Z",
                    "type": "response_item",
                    "payload": {
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "reply1"}],
                    },
                },
                {
                    "timestamp": "2026-01-19T01:45:55.000Z",
                    "type": "response_item",
                    "payload": {
                        "type": "message",
                        "role": "user",
                        "content": [{"type": "input_text", "text": "short2"}],
                    },
                },
                {
                    "timestamp": "2026-01-19T01:45:55.100Z",
                    "type": "event_msg",
                    "payload": {
                        "type": "user_message",
                        "message": "short2",
                        "images": [],
                        "local_images": [],
                        "text_elements": [],
                    },
                },
                {
                    "timestamp": "2026-01-19T01:45:56.000Z",
                    "type": "event_msg",
                    "payload": {"type": "agent_message", "message": "reply2"},
                },
                {
                    "timestamp": "2026-01-19T01:45:56.100Z",
                    "type": "response_item",
                    "payload": {
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "reply2"}],
                    },
                },
            ]
            rollout.write_text("\n".join(json.dumps(x) for x in lines), encoding="utf-8")

            prompts = list(parser.parse_file(rollout))
            self.assertEqual(len(prompts), 2)
            self.assertEqual(prompts[0].content, "short1")
            self.assertEqual(prompts[0].response, "reply1")
            self.assertEqual(prompts[1].content, "short2")
            self.assertEqual(prompts[1].response, "reply2")

            self.assertIsNotNone(prompts[0].origin_offset_start)
            self.assertIsNotNone(prompts[0].origin_offset_end)
            self.assertIsNotNone(prompts[1].origin_offset_start)
            self.assertIsNotNone(prompts[1].origin_offset_end)

            raw = rollout.read_bytes()
            seg0 = raw[prompts[0].origin_offset_start : prompts[0].origin_offset_end].decode("utf-8")
            seg1 = raw[prompts[1].origin_offset_start : prompts[1].origin_offset_end].decode("utf-8")
            self.assertNotIn("short2", seg0)
            self.assertIn("short2", seg1)


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


class TestCursorLegacyStoreDbParser(unittest.TestCase):
    def test_inferrs_unknown_roles_and_builds_turn_json(self) -> None:
        parser = CursorParser()
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "store.db"
            conn = sqlite3.connect(db_path)
            try:
                conn.execute("CREATE TABLE meta (key TEXT, value TEXT)")
                conn.execute("CREATE TABLE blobs (id TEXT, data BLOB)")

                meta = {"name": "Chat", "createdAt": "2025-10-14T12:00:00.000Z"}
                conn.execute(
                    "INSERT INTO meta (key, value) VALUES (?, ?)",
                    ("meta", json.dumps(meta).encode("utf-8").hex()),
                )

                def enc_varint(n: int) -> bytes:
                    out = bytearray()
                    while True:
                        b = n & 0x7F
                        n >>= 7
                        if n:
                            out.append(b | 0x80)
                        else:
                            out.append(b)
                            break
                    return bytes(out)

                def pb_string(field_num: int, text: str) -> bytes:
                    tag = (field_num << 3) | 2
                    raw = text.encode("utf-8")
                    return bytes([tag]) + enc_varint(len(raw)) + raw

                def pb_text_message(text: str, msg_id: str) -> bytes:
                    return pb_string(1, text) + pb_string(2, msg_id)

                blobs = [
                    ("b1", pb_text_message("User prompt long enough", "id1")),
                    ("b2", pb_text_message("Assistant response", "id2")),
                    ("b3", pb_text_message("Second user prompt long enough", "id3")),
                    ("b4", pb_text_message("Second assistant response", "id4")),
                ]
                conn.executemany("INSERT INTO blobs (id, data) VALUES (?, ?)", blobs)
                conn.commit()
            finally:
                conn.close()

            prompts = list(parser.parse_file(db_path))
            self.assertEqual(len(prompts), 2)
            self.assertEqual(prompts[0].content, "User prompt long enough")
            self.assertEqual(prompts[0].response, "Assistant response")
            self.assertEqual(prompts[1].content, "Second user prompt long enough")
            self.assertEqual(prompts[1].response, "Second assistant response")

            self.assertIsNotNone(prompts[0].turn_json)
            turn0 = json.loads(prompts[0].turn_json or "[]")
            self.assertEqual([m.get("blob_id") for m in turn0], ["b1", "b2"])
