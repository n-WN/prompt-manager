"""Parser for Claude Code logs."""

import json
from pathlib import Path
from typing import Iterator, Optional

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

        # Read all lines first
        lines = []
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    lines.append(data)
                except json.JSONDecodeError:
                    continue

        # Process user messages and find their responses
        i = 0
        while i < len(lines):
            data = lines[i]

            # Only process user messages
            if data.get("type") != "user":
                i += 1
                continue

            message = data.get("message", {})
            if message.get("role") != "user":
                i += 1
                continue

            # Extract user content
            content = message.get("content")
            if not content:
                i += 1
                continue

            # Handle array content (tool results)
            if isinstance(content, list):
                text_parts = [
                    item.get("text", "")
                    for item in content
                    if isinstance(item, dict) and item.get("type") == "text"
                ]
                if not text_parts:
                    i += 1
                    continue
                content = "\n".join(text_parts)

            # Skip very short prompts
            if len(content.strip()) < 10:
                i += 1
                continue

            # Parse timestamp
            ts_str = data.get("timestamp") or ""
            timestamp = self.parse_timestamp(ts_str)

            # Find assistant response (look ahead)
            response = None
            j = i + 1
            while j < len(lines):
                next_data = lines[j]
                next_type = next_data.get("type")

                # Stop if we hit another user message
                if next_type == "user":
                    next_msg = next_data.get("message", {})
                    if next_msg.get("role") == "user":
                        next_content = next_msg.get("content")
                        # Check if it's actual user input (text blocks), not tool results
                        next_text = None
                        if isinstance(next_content, str):
                            next_text = next_content
                        elif isinstance(next_content, list):
                            text_parts = [
                                item.get("text", "")
                                for item in next_content
                                if isinstance(item, dict) and item.get("type") == "text"
                            ]
                            if text_parts:
                                next_text = "\n".join(text_parts)

                        if next_text and len(next_text.strip()) >= 10:
                            break

                # Extract assistant text response
                if next_type == "assistant":
                    next_msg = next_data.get("message", {})
                    next_content = next_msg.get("content", [])
                    if isinstance(next_content, list):
                        for item in next_content:
                            if isinstance(item, dict) and item.get("type") == "text":
                                text = item.get("text", "")
                                if text and len(text) > 5:
                                    if response:
                                        response += "\n" + text
                                    else:
                                        response = text
                j += 1

            prompt_id = self.generate_id(
                self.source_name,
                content,
                session_id,
                ts_str or ""
            )

            yield ParsedPrompt(
                id=prompt_id,
                source=self.source_name,
                content=content,
                project_path=project_path,
                session_id=session_id,
                timestamp=timestamp,
                response=response,
            )

            i += 1
