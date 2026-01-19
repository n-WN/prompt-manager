import json
import tempfile
import unittest
from pathlib import Path

from prompt_manager.codex_transcript import format_codex_rollout_transcript


class TestCodexTranscript(unittest.TestCase):
    def test_formats_like_codex_cli(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            rollout = Path(tmp) / "rollout.jsonl"
            lines = [
                {
                    "timestamp": "2026-01-19T01:45:34.000Z",
                    "type": "session_meta",
                    "payload": {"id": "sess-123", "cwd": "/proj"},
                },
                {
                    "timestamp": "2026-01-19T01:45:39.000Z",
                    "type": "event_msg",
                    "payload": {"type": "user_message", "message": "message1"},
                },
                {
                    "timestamp": "2026-01-19T01:45:39.100Z",
                    "type": "event_msg",
                    "payload": {
                        "type": "agent_reasoning",
                        "text": "**Interpreting user intent**\n\nThe user provided AGENTS.md instructions.",
                    },
                },
                {
                    "timestamp": "2026-01-19T01:45:39.200Z",
                    "type": "event_msg",
                    "payload": {
                        "type": "agent_reasoning",
                        "text": "**Responding concisely**\n\nI should ask for clarification.",
                    },
                },
                {
                    "timestamp": "2026-01-19T01:45:40.000Z",
                    "type": "event_msg",
                    "payload": {"type": "agent_message", "message": "Answer 1"},
                },
                {
                    "timestamp": "2026-01-19T01:45:55.000Z",
                    "type": "event_msg",
                    "payload": {"type": "user_message", "message": "message2"},
                },
                {
                    "timestamp": "2026-01-19T01:45:55.100Z",
                    "type": "event_msg",
                    "payload": {
                        "type": "agent_reasoning",
                        "text": "**Asking for clarification**\n\nSecond turn reasoning.",
                    },
                },
                {
                    "timestamp": "2026-01-19T01:45:56.000Z",
                    "type": "event_msg",
                    "payload": {"type": "agent_message", "message": "Answer 2"},
                },
                {
                    "timestamp": "2026-01-19T01:46:06.000Z",
                    "type": "event_msg",
                    "payload": {"type": "user_message", "message": "message3"},
                },
                {
                    "timestamp": "2026-01-19T01:46:07.000Z",
                    "type": "event_msg",
                    "payload": {"type": "agent_message", "message": "Answer 3"},
                },
                {
                    "timestamp": "2026-01-19T01:46:14.000Z",
                    "type": "event_msg",
                    "payload": {
                        "type": "token_count",
                        "info": {
                            "total_token_usage": {
                                "input_tokens": 31353,
                                "cached_input_tokens": 20480,
                                "output_tokens": 675,
                                "reasoning_output_tokens": 497,
                                "total_tokens": 32028,
                            }
                        },
                    },
                },
            ]
            rollout.write_text("\n".join(json.dumps(x) for x in lines), encoding="utf-8")

            out = format_codex_rollout_transcript(rollout, width=80)

            self.assertIn("› message1", out)
            self.assertIn("› message2", out)
            self.assertIn("› message3", out)

            # First reasoning segment title is suppressed (Codex CLI shows only body).
            self.assertNotIn("Interpreting user intent", out)
            self.assertNotIn("Asking for clarification", out)

            # Subsequent reasoning titles are shown as indented headings.
            self.assertIn("\n  Responding concisely\n", out)

            self.assertIn("• Answer 1", out)
            self.assertIn("• Answer 2", out)
            self.assertIn("• Answer 3", out)

            self.assertIn(
                "Token usage: total=11,548 input=10,873 (+ 20,480 cached) output=675 (reasoning 497)",
                out,
            )
            self.assertIn("To continue this session, run codex resume sess-123", out)
            self.assertTrue(out.endswith("\n"))

