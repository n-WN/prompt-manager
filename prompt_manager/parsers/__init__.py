"""Parsers for various Code Agent log formats."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator, Optional
from dataclasses import dataclass
from datetime import datetime
import json
import hashlib


@dataclass
class ParsedPrompt:
    """A parsed prompt from a log file."""
    id: str
    source: str
    content: str
    project_path: Optional[str] = None
    session_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    response: Optional[str] = None
    turn_json: Optional[str] = None


class BaseParser(ABC):
    """Base class for log parsers."""

    source_name: str = "unknown"
    # Bump this when a parser's output schema changes (e.g. improved response
    # extraction, new `turn_json` capture) so sync can re-process unchanged files
    # once to backfill missing fields.
    sync_version: int = 1

    @abstractmethod
    def find_log_files(self) -> Iterator[Path]:
        """Find all log files for this source."""
        pass

    @abstractmethod
    def parse_file(self, file_path: Path) -> Iterator[ParsedPrompt]:
        """Parse a single log file and yield prompts."""
        pass

    def parse_all(self) -> Iterator[ParsedPrompt]:
        """Parse all log files and yield prompts."""
        for file_path in self.find_log_files():
            try:
                yield from self.parse_file(file_path)
            except Exception as e:
                print(f"Error parsing {file_path}: {e}")

    @staticmethod
    def generate_id(source: str, content: str, session_id: str = "", timestamp: str = "") -> str:
        """Generate a unique ID for a prompt."""
        data = f"{source}:{session_id}:{timestamp}:{content}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    @staticmethod
    def parse_timestamp(ts_value, formats: Optional[list[str]] = None) -> Optional[datetime]:
        """Parse timestamp from various formats.

        Args:
            ts_value: String timestamp, Unix timestamp (int/float), or None
            formats: List of strptime format strings to try

        Returns:
            datetime object or None if parsing fails
        """
        if ts_value is None:
            return None

        # Handle Unix timestamps
        if isinstance(ts_value, (int, float)):
            try:
                # Handle milliseconds vs seconds
                if ts_value > 1e12:
                    ts_value = ts_value / 1000
                return datetime.fromtimestamp(ts_value)
            except (ValueError, OSError):
                return None

        # Handle string timestamps
        if isinstance(ts_value, str):
            ts_str = ts_value.strip()
            if not ts_str:
                return None

            # Try ISO format with timezone normalization
            try:
                return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except ValueError:
                pass

            # Try custom formats
            default_formats = formats or [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H-%M-%S",
                "%Y-%m-%d",
            ]
            for fmt in default_formats:
                try:
                    return datetime.strptime(ts_str, fmt)
                except ValueError:
                    continue

        return None
