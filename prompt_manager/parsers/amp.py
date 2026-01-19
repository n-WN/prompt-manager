"""Parser for Amp CLI thread logs."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterator, Optional
from urllib.parse import unquote, urlparse

from . import BaseParser, ParsedPrompt


def _default_amp_data_root() -> Path:
    data_home = os.environ.get("XDG_DATA_HOME")
    if data_home:
        return Path(data_home).expanduser() / "amp"
    return Path.home() / ".local" / "share" / "amp"


def _file_uri_to_path(uri: str) -> Optional[str]:
    if not uri.startswith("file://"):
        return None
    parsed = urlparse(uri)
    path = unquote(parsed.path or "")
    if parsed.netloc:
        path = f"//{parsed.netloc}{path}"
    # Handle Windows file URIs like file:///C:/path
    if len(path) >= 3 and path[0] == "/" and path[2] == ":":
        path = path[1:]
    return path or None


def _extract_amp_project_path(data: dict[str, Any]) -> Optional[str]:
    env = data.get("env")
    if not isinstance(env, dict):
        return None
    initial = env.get("initial")
    if not isinstance(initial, dict):
        return None
    trees = initial.get("trees")
    if not isinstance(trees, list) or not trees:
        return None
    tree0 = trees[0]
    if not isinstance(tree0, dict):
        return None
    uri = tree0.get("uri")
    if not isinstance(uri, str) or not uri:
        return None
    return _file_uri_to_path(uri) or uri


def _extract_text_blocks(content: Any) -> list[str]:
    if not isinstance(content, list):
        return []
    parts: list[str] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") != "text":
            continue
        text = block.get("text")
        if isinstance(text, str):
            stripped = text.strip()
            if stripped:
                parts.append(stripped)
    return parts


class AmpParser(BaseParser):
    """Parser for Amp CLI thread JSON files.

    Default location:
        $XDG_DATA_HOME/amp/threads/T-*.json
        (fallback: ~/.local/share/amp/threads/T-*.json)

    Format (observed):
        {
          "v": 123,
          "id": "T-....",
          "created": 1766934709176,
          "title": "optional",
          "env": {"initial": {"trees": [{"uri": "file:///..."}]}},
          "messages": [
            {"role": "user"|"assistant", "messageId": 0, "content": [{"type":"text",...}|{"type":"tool_use",...}|...]},
          ]
        }
    """

    source_name = "amp"
    sync_version = 1

    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = base_path or _default_amp_data_root()

    def find_log_files(self) -> Iterator[Path]:
        threads_dir = self.base_path / "threads"
        if not threads_dir.exists():
            return
        yield from threads_dir.glob("T-*.json")

    def parse_file(self, file_path: Path) -> Iterator[ParsedPrompt]:
        try:
            data = json.loads(file_path.read_text("utf-8"))
        except Exception:
            return

        if not isinstance(data, dict):
            return

        thread_id = data.get("id") if isinstance(data.get("id"), str) else file_path.stem
        project_path = _extract_amp_project_path(data) or "amp"

        messages = data.get("messages", [])
        if not isinstance(messages, list):
            return

        pending_content: Optional[str] = None
        pending_unique: str = ""
        pending_timestamp = None
        pending_response_parts: list[str] = []
        pending_start_idx: Optional[int] = None

        def is_user_prompt(msg: dict[str, Any]) -> Optional[str]:
            if msg.get("role") != "user":
                return None
            text_parts = _extract_text_blocks(msg.get("content"))
            if not text_parts:
                return None
            return "\n".join(text_parts).strip() or None

        def extract_assistant_text(msg: dict[str, Any]) -> list[str]:
            if msg.get("role") != "assistant":
                return []
            return _extract_text_blocks(msg.get("content"))

        def flush(end_idx: int) -> Optional[ParsedPrompt]:
            nonlocal pending_content, pending_unique, pending_timestamp, pending_response_parts, pending_start_idx
            if pending_content is None:
                return None

            content = pending_content
            timestamp = pending_timestamp
            start_idx = pending_start_idx
            response = "\n".join(pending_response_parts).strip() if pending_response_parts else None
            prompt_id = self.generate_id(self.source_name, content, thread_id, pending_unique)

            pending_content = None
            pending_unique = ""
            pending_timestamp = None
            pending_response_parts = []
            pending_start_idx = None

            return ParsedPrompt(
                id=prompt_id,
                source=self.source_name,
                content=content,
                project_path=project_path,
                session_id=thread_id,
                timestamp=timestamp,
                response=response or None,
                turn_json=None,
                origin_offset_start=start_idx,
                origin_offset_end=end_idx,
            )

        for idx, raw in enumerate(messages):
            if not isinstance(raw, dict):
                continue

            user_text = is_user_prompt(raw)
            if user_text is not None:
                flushed = flush(idx)
                if flushed is not None:
                    yield flushed

                meta = raw.get("meta")
                sent_at = meta.get("sentAt") if isinstance(meta, dict) else None
                pending_timestamp = self.parse_timestamp(sent_at or data.get("created"))
                unique = raw.get("messageId", idx)
                pending_unique = str(unique)
                pending_content = user_text
                pending_response_parts = []
                pending_start_idx = idx
                continue

            if pending_content is None:
                continue

            pending_response_parts.extend(extract_assistant_text(raw))

        flushed = flush(len(messages))
        if flushed is not None:
            yield flushed
