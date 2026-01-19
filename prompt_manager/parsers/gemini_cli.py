"""Parser for Gemini CLI session logs."""

import json
from pathlib import Path
from typing import Iterator, Optional

from . import BaseParser, ParsedPrompt


class GeminiCliParser(BaseParser):
    """Parser for Gemini CLI JSON session files.

    Default location (per project hash):
        ~/.gemini/tmp/<project_hash>/chats/session-*.json

    Format (observed):
        {
          "projectHash": "...",
          "sessionId": "...",
          "startTime": "2025-10-14T12:03:29.821Z",
          "lastUpdated": "...",
          "messages": [
            {"id": "...", "timestamp": "...", "type": "user"|"gemini", "content": "..."}
          ]
        }
    """

    source_name = "gemini_cli"

    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = base_path or Path.home() / ".gemini" / "tmp"

    def find_log_files(self) -> Iterator[Path]:
        if not self.base_path.exists():
            return

        for project_dir in self.base_path.iterdir():
            if not project_dir.is_dir():
                continue
            chats_dir = project_dir / "chats"
            if not chats_dir.is_dir():
                continue
            for session_file in chats_dir.glob("session-*.json"):
                yield session_file

    def parse_file(self, file_path: Path) -> Iterator[ParsedPrompt]:
        try:
            data = json.loads(file_path.read_text("utf-8"))
        except Exception:
            return

        if not isinstance(data, dict):
            return

        project_hash = data.get("projectHash") or file_path.parent.parent.name
        session_id = data.get("sessionId") or file_path.stem

        project_path = f"gemini_cli:{project_hash}" if project_hash else "gemini_cli"

        messages = data.get("messages", [])
        if not isinstance(messages, list):
            return

        i = 0
        while i < len(messages):
            msg = messages[i]
            if not isinstance(msg, dict):
                i += 1
                continue

            if msg.get("type") != "user":
                i += 1
                continue

            content = msg.get("content") or ""
            if not isinstance(content, str) or len(content.strip()) < 10:
                i += 1
                continue

            ts_str = msg.get("timestamp") or ""
            timestamp = self.parse_timestamp(ts_str)

            response_parts: list[str] = []
            j = i + 1
            while j < len(messages):
                next_msg = messages[j]
                if not isinstance(next_msg, dict):
                    j += 1
                    continue
                if next_msg.get("type") == "user":
                    break
                next_content = next_msg.get("content") or ""
                if isinstance(next_content, str) and next_content.strip():
                    response_parts.append(next_content)
                j += 1

            response = "\n".join(response_parts) if response_parts else None
            turn_json = json.dumps(messages[i:j], ensure_ascii=False)

            unique = msg.get("id") or ts_str
            prompt_id = self.generate_id(
                self.source_name,
                content,
                session_id,
                unique,
            )

            yield ParsedPrompt(
                id=prompt_id,
                source=self.source_name,
                content=content,
                project_path=project_path,
                session_id=session_id,
                timestamp=timestamp,
                response=response,
                turn_json=turn_json,
            )

            i += 1
