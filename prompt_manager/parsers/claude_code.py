"""Parser for Claude Code logs."""

import json
from pathlib import Path
from typing import Any, Iterator, Optional

from . import BaseParser, ParsedPrompt


class ClaudeCodeParser(BaseParser):
    """Parser for Claude Code JSONL logs.

    Log location: ~/.claude/projects/<project-path>/<session-id>.jsonl
    Format: JSONL with fields:
        - type: 'user' | 'assistant' | 'file-history-snapshot'
        - message.role: 'user' | 'assistant'
        - message.content: string or array
        - uuid: unique message ID
        - timestamp: ISO timestamp
        - sessionId: session ID
        - cwd: working directory
    """

    source_name = "claude_code"

    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = base_path or Path.home() / ".claude" / "projects"

    def find_log_files(self) -> Iterator[Path]:
        """Find all Claude Code JSONL log files."""
        if not self.base_path.exists():
            return

        for project_dir in self.base_path.iterdir():
            if not project_dir.is_dir():
                continue
            for log_file in project_dir.glob("*.jsonl"):
                # Skip agent files, we want main conversation logs
                if not log_file.name.startswith("agent-"):
                    yield log_file

    def parse_file(self, file_path: Path) -> Iterator[ParsedPrompt]:
        """Parse a Claude Code JSONL file."""
        project_name = file_path.parent.name
        session_id = file_path.stem

        # Convert project name back to path
        project_path = "/" + project_name.replace("-", "/")

        pending_content: Optional[str] = None
        pending_ts_str: str = ""
        pending_timestamp = None
        pending_response_parts: list[str] = []
        pending_turn_lines: list[dict[str, Any]] = []

        def extract_text(value: Any) -> Optional[str]:
            if isinstance(value, str):
                text = value.strip()
                return text if text else None
            if isinstance(value, list):
                parts: list[str] = []
                for item in value:
                    if not isinstance(item, dict):
                        continue
                    if item.get("type") != "text":
                        continue
                    text = item.get("text", "")
                    if isinstance(text, str) and text:
                        parts.append(text)
                joined = "\n".join(parts).strip()
                return joined if joined else None
            return None

        def is_user_prompt(event: dict[str, Any]) -> Optional[str]:
            if event.get("type") != "user":
                return None
            msg = event.get("message") or {}
            if not isinstance(msg, dict) or msg.get("role") != "user":
                return None
            text = extract_text(msg.get("content"))
            if text and len(text.strip()) >= 10:
                return text
            return None

        def extract_assistant_text(event: dict[str, Any]) -> list[str]:
            if event.get("type") != "assistant":
                return []
            msg = event.get("message") or {}
            if not isinstance(msg, dict):
                return []
            content = msg.get("content")
            if not isinstance(content, list):
                return []
            parts: list[str] = []
            for item in content:
                if not isinstance(item, dict):
                    continue
                if item.get("type") != "text":
                    continue
                text = item.get("text", "")
                if isinstance(text, str) and text and len(text) > 5:
                    parts.append(text)
            return parts

        def flush_pending() -> Optional[ParsedPrompt]:
            nonlocal pending_content, pending_ts_str, pending_timestamp, pending_response_parts, pending_turn_lines
            if pending_content is None:
                return None

            content = pending_content
            ts_str = pending_ts_str
            timestamp = pending_timestamp
            response = "\n".join(pending_response_parts) if pending_response_parts else None
            turn_json = (
                json.dumps(pending_turn_lines, ensure_ascii=False)
                if pending_turn_lines
                else None
            )

            prompt_id = self.generate_id(self.source_name, content, session_id, ts_str or "")
            pending_content = None
            pending_ts_str = ""
            pending_timestamp = None
            pending_response_parts = []
            pending_turn_lines = []

            return ParsedPrompt(
                id=prompt_id,
                source=self.source_name,
                content=content,
                project_path=project_path,
                session_id=session_id,
                timestamp=timestamp,
                response=response,
                turn_json=turn_json,
            )

        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(data, dict):
                    continue

                user_text = is_user_prompt(data)
                if user_text is not None:
                    flushed = flush_pending()
                    if flushed is not None:
                        yield flushed

                    pending_content = user_text
                    pending_ts_str = data.get("timestamp") or ""
                    pending_timestamp = self.parse_timestamp(pending_ts_str)
                    pending_response_parts = []
                    pending_turn_lines = [data]
                    continue

                if pending_content is None:
                    continue

                pending_turn_lines.append(data)
                pending_response_parts.extend(extract_assistant_text(data))

        flushed = flush_pending()
        if flushed is not None:
            yield flushed
