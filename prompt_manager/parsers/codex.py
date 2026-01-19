"""Parser for Codex CLI (OpenAI Codex) logs."""

import json
import re
from pathlib import Path
from typing import Iterator, Optional

from . import BaseParser, ParsedPrompt
from ..codex_schema import (
    AgentMessageEvent,
    EventMsgItem,
    ResponseItemItem,
    SessionMetaItem,
    TurnContextItem,
    UserMessageEvent,
    iter_rollout_lines,
)

_UUID_RE = re.compile(
    r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$"
)


class CodexParser(BaseParser):
    """Parser for Codex session rollouts.

    Current Codex CLI format:
      - JSONL under `~/.codex/sessions/**/rollout-*.jsonl`
      - Each line is a JSON object with `timestamp`, `type`, and `payload`

    Legacy format (older Codex builds):
      - Single JSON document `~/.codex/sessions/rollout-*.json`
      - Top-level `{session, items}` where `items` includes message/function call objects
    """

    source_name = "codex"
    sync_version = 2

    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = base_path or Path.home() / ".codex"

    def find_log_files(self) -> Iterator[Path]:
        """Find Codex session files."""
        sessions_dir = self.base_path / "sessions"
        if not sessions_dir.exists():
            return
        yield from sessions_dir.rglob("rollout-*.jsonl")
        yield from sessions_dir.rglob("rollout-*.json")

    def parse_file(self, file_path: Path) -> Iterator[ParsedPrompt]:
        """Parse a Codex rollout file."""
        if file_path.suffix == ".json":
            yield from self._parse_json_rollout(file_path)
            return
        yield from self._parse_jsonl_rollout(file_path)

    def _parse_jsonl_rollout(self, file_path: Path) -> Iterator[ParsedPrompt]:
        """Parse Codex JSONL rollout file."""
        has_user_events = self._jsonl_has_user_events(file_path)

        session_id: Optional[str] = None
        project_path: Optional[str] = None

        carryover_lines: list[str] = []

        pending_content: Optional[str] = None
        pending_ts: Optional[str] = None
        pending_response_parts: list[str] = []
        pending_has_structured_response = False
        pending_turn_lines: list[str] = []

        for line in iter_rollout_lines(file_path):
            item = line.item
            raw_line = line.raw_line or json.dumps(line.raw, ensure_ascii=False)

            if session_id is None and isinstance(item, SessionMetaItem):
                session_id = item.payload.id or None
                project_path = item.payload.cwd or project_path
                continue

            if project_path is None and isinstance(item, TurnContextItem):
                cwd = item.raw.get("cwd")
                if isinstance(cwd, str):
                    project_path = cwd

            # In sessions with `event_msg` user markers, `response_item` user messages can
            # appear *before* the corresponding `event_msg` user_message. Treat those as
            # carryover so per-turn timelines don't accidentally include the next prompt.
            if has_user_events and isinstance(item, ResponseItemItem) and item.message and item.message.role == "user":
                carryover_lines.append(raw_line)
                continue

            if has_user_events:
                is_user_marker = isinstance(item, EventMsgItem) and isinstance(item.event, UserMessageEvent)
                user_message = item.event.message if is_user_marker else None
            else:
                is_user_marker = isinstance(item, ResponseItemItem) and item.message and item.message.role == "user"
                user_message = (
                    self._extract_text_blocks(item.message.content, {"input_text", "text"})
                    if is_user_marker and item.message
                    else None
                )

            if is_user_marker:
                if pending_content and pending_content.strip():
                    timestamp = self.parse_timestamp(pending_ts)
                    prompt_id = self.generate_id(
                        self.source_name,
                        pending_content,
                        session_id or self._extract_session_id_from_path(file_path),
                        pending_ts or "",
                    )
                    yield ParsedPrompt(
                        id=prompt_id,
                        source=self.source_name,
                        content=pending_content,
                        project_path=project_path,
                        session_id=session_id,
                        timestamp=timestamp,
                        response="\n".join(pending_response_parts) if pending_response_parts else None,
                        turn_json=f"[{','.join(pending_turn_lines)}]" if pending_turn_lines else None,
                    )

                pending_content = user_message if isinstance(user_message, str) else ""
                pending_ts = line.timestamp
                pending_response_parts = []
                pending_has_structured_response = False
                pending_turn_lines = carryover_lines + [raw_line]
                carryover_lines = []
                continue

            if pending_content is None:
                continue

            pending_turn_lines.append(raw_line)

            # Prefer structured assistant response items; fall back to event msg if needed.
            if isinstance(item, ResponseItemItem) and item.message and item.message.role == "assistant":
                text = self._extract_text_blocks(item.message.content, {"output_text", "text"})
                if text:
                    pending_response_parts.append(text)
                    pending_has_structured_response = True
            elif (
                isinstance(item, EventMsgItem)
                and not pending_has_structured_response
                and isinstance(item.event, AgentMessageEvent)
            ):
                if item.event.message.strip():
                    pending_response_parts.append(item.event.message)

        if pending_content and pending_content.strip():
            timestamp = self.parse_timestamp(pending_ts)
            prompt_id = self.generate_id(
                self.source_name,
                pending_content,
                session_id or self._extract_session_id_from_path(file_path),
                pending_ts or "",
            )
            yield ParsedPrompt(
                id=prompt_id,
                source=self.source_name,
                content=pending_content,
                project_path=project_path,
                session_id=session_id,
                timestamp=timestamp,
                response="\n".join(pending_response_parts) if pending_response_parts else None,
                turn_json=f"[{','.join(pending_turn_lines)}]" if pending_turn_lines else None,
            )

    def _jsonl_has_user_events(self, file_path: Path) -> bool:
        try:
            for line in iter_rollout_lines(file_path):
                if isinstance(line.item, EventMsgItem) and isinstance(line.item.event, UserMessageEvent):
                    return True
        except OSError:
            return False
        return False

    def _parse_json_rollout(self, file_path: Path) -> Iterator[ParsedPrompt]:
        """Parse legacy Codex rollouts stored as a single JSON document."""
        try:
            data = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            return

        if not isinstance(data, dict):
            return

        session = data.get("session") or {}
        items = data.get("items") or []
        if not isinstance(session, dict) or not isinstance(items, list):
            return

        session_id = session.get("id") or self._extract_session_id_from_path(file_path)
        project_path = session.get("cwd")
        session_ts = session.get("timestamp")
        session_dt = self.parse_timestamp(session_ts)

        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            if item.get("type") != "message" or item.get("role") != "user":
                continue

            user_content = self._extract_text_blocks(item.get("content"), {"input_text", "text"})
            if not user_content or not user_content.strip():
                continue

            response_parts: list[str] = []
            turn_items: list[dict] = [item]
            for next_item in items[idx + 1 :]:
                if not isinstance(next_item, dict):
                    continue
                if next_item.get("type") == "message" and next_item.get("role") == "user":
                    break
                turn_items.append(next_item)
                if next_item.get("type") == "message" and next_item.get("role") == "assistant":
                    response_text = self._extract_text_blocks(
                        next_item.get("content"), {"output_text", "text"}
                    )
                    if response_text:
                        response_parts.append(response_text)

            # This format lacks per-item timestamps; include the index to keep IDs stable/unique.
            unique_ts_key = f"{session_ts or ''}:{idx}"
            prompt_id = self.generate_id(
                self.source_name,
                user_content,
                session_id,
                unique_ts_key,
            )

            yield ParsedPrompt(
                id=prompt_id,
                source=self.source_name,
                content=user_content,
                project_path=project_path,
                session_id=session_id,
                timestamp=session_dt,
                response="\n".join(response_parts) if response_parts else None,
                turn_json=json.dumps(turn_items, ensure_ascii=False),
            )

    def _extract_text_blocks(self, content, block_types: set[str]) -> Optional[str]:
        if not isinstance(content, list):
            return None
        parts: list[str] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get("type") not in block_types:
                continue
            text = block.get("text", "")
            if isinstance(text, str) and text:
                parts.append(text)
        joined = "\n".join(parts).strip()
        return joined or None

    def _extract_session_id_from_path(self, file_path: Path) -> str:
        m = _UUID_RE.search(file_path.stem)
        return m.group(1) if m else file_path.stem

    def _load_json_line(self, line: str) -> Optional[dict]:
        line = line.strip()
        if not line:
            return None
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            return None
        return obj if isinstance(obj, dict) else None
