"""Parser for Codex CLI (OpenAI Codex) logs."""

import json
from pathlib import Path
from typing import Iterator, Optional, List, Tuple

from . import BaseParser, ParsedPrompt


class CodexParser(BaseParser):
    """Parser for Codex CLI session logs.

    Log location: ~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl
    Format: JSONL with types:
        - session_meta: {id, cwd, model, ...}
        - event_msg with user_message: user prompts
        - response_item with role=assistant: assistant responses
    """

    source_name = "codex"

    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = base_path or Path.home() / ".codex"

    def find_log_files(self) -> Iterator[Path]:
        """Find Codex session files."""
        sessions_dir = self.base_path / "sessions"
        if sessions_dir.exists():
            for rollout_file in sessions_dir.rglob("rollout-*.jsonl"):
                yield rollout_file

    def parse_file(self, file_path: Path) -> Iterator[ParsedPrompt]:
        """Parse Codex session rollout file."""
        # Read all lines
        lines = []
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    lines.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

        # Extract session metadata
        session_id = None
        project_path = None
        for data in lines:
            if data.get("type") == "session_meta":
                payload = data.get("payload", {})
                session_id = payload.get("id", "")
                project_path = payload.get("cwd")
                break

        if not session_id:
            # Fallback: extract from filename
            session_id = file_path.stem.split("-", 1)[-1] if "-" in file_path.stem else file_path.stem

        # Collect user messages and assistant responses
        messages: List[Tuple[str, str, Optional[str]]] = []  # (content, timestamp, response)

        i = 0
        while i < len(lines):
            data = lines[i]

            # Find user message
            user_content = None
            user_ts = None

            if data.get("type") == "event_msg":
                payload = data.get("payload", {})
                if payload.get("type") == "user_message":
                    user_content = payload.get("message", "")
                    user_ts = data.get("timestamp")

            if not user_content or len(user_content.strip()) < 10:
                i += 1
                continue

            # Look ahead for assistant response
            response = None
            j = i + 1
            while j < len(lines):
                next_data = lines[j]

                # Stop at next user message
                if next_data.get("type") == "event_msg":
                    next_payload = next_data.get("payload", {})
                    if next_payload.get("type") == "user_message":
                        break

                # Extract assistant response
                if next_data.get("type") == "response_item":
                    next_payload = next_data.get("payload", {})
                    if next_payload.get("role") == "assistant":
                        content_list = next_payload.get("content", [])
                        for item in content_list:
                            if isinstance(item, dict) and item.get("type") == "output_text":
                                text = item.get("text", "")
                                if text:
                                    response = text
                                    break
                        if response:
                            break
                j += 1

            # Parse timestamp
            timestamp = self.parse_timestamp(user_ts)

            prompt_id = self.generate_id(
                self.source_name,
                user_content,
                session_id,
                user_ts or ""
            )

            yield ParsedPrompt(
                id=prompt_id,
                source=self.source_name,
                content=user_content,
                project_path=project_path,
                session_id=session_id,
                timestamp=timestamp,
                response=response,
            )

            i += 1
