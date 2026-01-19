"""Parser for Aider chat history."""

import re
from pathlib import Path
from typing import Iterator, Optional
from datetime import datetime

from . import BaseParser, ParsedPrompt


class AiderParser(BaseParser):
    """Parser for Aider Markdown chat history.

    Log location: <project>/.aider.chat.history.md
    Format: Markdown with sections like:
        # aider chat started at 2024-01-01 12:00:00

        > user message here
        > continues...

        assistant response here
    """

    source_name = "aider"

    def __init__(self, search_paths: Optional[list[Path]] = None):
        self.search_paths = search_paths or [
            Path.home(),
            Path.home() / "projects",
            Path.home() / "code",
            Path.home() / "dev",
            Path.home() / "work",
            Path.home() / "my",
        ]

    def find_log_files(self) -> Iterator[Path]:
        """Find all Aider chat history files."""
        seen = set()

        for base_path in self.search_paths:
            if not base_path.exists():
                continue

            # Search up to 4 levels deep
            for depth in range(4):
                pattern = "/".join(["*"] * depth) + "/.aider.chat.history.md" if depth > 0 else ".aider.chat.history.md"
                for log_file in base_path.glob(pattern):
                    if log_file not in seen:
                        seen.add(log_file)
                        yield log_file

    def parse_file(self, file_path: Path) -> Iterator[ParsedPrompt]:
        """Parse an Aider chat history Markdown file."""
        project_path = str(file_path.parent)
        session_id = None
        current_timestamp = None

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
        except OSError:
            return

        # Split by chat sessions
        sessions = re.split(r'^# aider chat started at (.+)$', content, flags=re.MULTILINE)

        # First element is empty or pre-content, then alternating: timestamp, content
        for i in range(1, len(sessions), 2):
            if i + 1 >= len(sessions):
                break

            timestamp_str = sessions[i].strip()
            session_content = sessions[i + 1]

            # Parse timestamp
            try:
                current_timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    current_timestamp = datetime.fromisoformat(timestamp_str)
                except ValueError:
                    current_timestamp = None

            session_id = f"{file_path.stem}_{timestamp_str.replace(' ', '_').replace(':', '-')}"

            # Extract user messages (lines starting with >)
            user_messages = self._extract_user_messages(session_content)

            for msg_content in user_messages:
                if len(msg_content.strip()) < 10:
                    continue

                prompt_id = self.generate_id(
                    self.source_name,
                    msg_content,
                    session_id or "",
                    str(current_timestamp) if current_timestamp else ""
                )

                yield ParsedPrompt(
                    id=prompt_id,
                    source=self.source_name,
                    content=msg_content,
                    project_path=project_path,
                    session_id=session_id,
                    timestamp=current_timestamp,
                )

    def _extract_user_messages(self, content: str) -> list[str]:
        """Extract user messages from session content."""
        messages = []
        current_message = []
        in_user_message = False

        for line in content.split('\n'):
            if line.startswith('> '):
                in_user_message = True
                current_message.append(line[2:])  # Remove '> ' prefix
            elif line.startswith('>'):
                # Empty quoted line
                if in_user_message:
                    current_message.append('')
            else:
                if in_user_message and current_message:
                    messages.append('\n'.join(current_message))
                    current_message = []
                in_user_message = False

        # Don't forget the last message
        if current_message:
            messages.append('\n'.join(current_message))

        return messages
