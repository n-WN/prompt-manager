"""Helpers to render Codex rollout files in a user-visible transcript format."""

from __future__ import annotations

import json
import re
import shutil
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .codex_schema import (
    AgentMessageEvent,
    AgentReasoningEvent,
    EventMsgItem,
    RolloutLine,
    SessionMetaItem,
    TokenCountEvent,
    UserMessageEvent,
    iter_rollout_lines as iter_codex_rollout_lines,
)


_REASONING_TITLE_RE = re.compile(r"^\*\*(.+?)\*\*\s*$")


@dataclass
class CodexTurnView:
    user_message: str
    reasoning_segments: list[str]
    agent_messages: list[str]


def extract_turn_views_from_rollout(path: Path) -> tuple[Optional[str], list[CodexTurnView], Optional[dict]]:
    """Parse a rollout JSONL into user-visible turns plus last token usage."""
    session_id: Optional[str] = None
    turns: list[CodexTurnView] = []
    current: Optional[CodexTurnView] = None
    last_total_token_usage: Optional[dict] = None

    for line in iter_codex_rollout_lines(path):
        if isinstance(line.item, SessionMetaItem):
            sid = line.item.payload.id
            if sid:
                session_id = sid
            continue

        if not isinstance(line.item, EventMsgItem):
            continue

        event = line.item.event
        if isinstance(event, UserMessageEvent):
            if current is not None:
                turns.append(current)
            current = CodexTurnView(
                user_message=event.message,
                reasoning_segments=[],
                agent_messages=[],
            )
            continue

        if isinstance(event, TokenCountEvent):
            info = event.info or {}
            total_usage = info.get("total_token_usage")
            if isinstance(total_usage, dict):
                last_total_token_usage = total_usage
            continue

        if current is None:
            continue

        if isinstance(event, AgentReasoningEvent) and event.text.strip():
            current.reasoning_segments.append(event.text)
        elif isinstance(event, AgentMessageEvent) and event.message.strip():
            current.agent_messages.append(event.message)

    if current is not None:
        turns.append(current)

    return session_id, turns, last_total_token_usage


def extract_turn_view_from_turn_json(turn_json: str) -> Optional[CodexTurnView]:
    """Build a CodexTurnView from a per-turn `turn_json` timeline."""
    user_message: Optional[str] = None
    reasoning_segments: list[str] = []
    agent_messages: list[str] = []

    # turn_json is a JSON array of raw RolloutLine dicts.
    try:
        timeline_raw = json.loads(turn_json)
    except Exception:
        return None
    if not isinstance(timeline_raw, list):
        return None

    for raw in timeline_raw:
        if not isinstance(raw, dict):
            continue
        line = RolloutLine.from_dict(raw)
        if line is None or not isinstance(line.item, EventMsgItem):
            continue
        event = line.item.event
        if isinstance(event, UserMessageEvent) and user_message is None:
            user_message = event.message
        elif isinstance(event, AgentReasoningEvent) and event.text.strip():
            reasoning_segments.append(event.text)
        elif isinstance(event, AgentMessageEvent) and event.message.strip():
            agent_messages.append(event.message)

    if user_message is None:
        return None

    return CodexTurnView(
        user_message=user_message,
        reasoning_segments=reasoning_segments,
        agent_messages=agent_messages,
    )


def _split_reasoning_title(text: str) -> tuple[Optional[str], str]:
    text = text.strip()
    if not text:
        return None, ""

    lines = text.splitlines()
    first = lines[0].strip()
    m = _REASONING_TITLE_RE.match(first)
    if not m:
        return None, text

    title = m.group(1).strip() or None
    body = "\n".join(lines[1:]).lstrip("\n").strip()
    return title, body


def _wrap_paragraphs(text: str, *, width: int, initial_indent: str, subsequent_indent: str) -> str:
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text.strip()) if p.strip()]
    if not paragraphs:
        return ""

    wrapped: list[str] = []
    for paragraph in paragraphs:
        wrapped.append(
            textwrap.fill(
                paragraph,
                width=width,
                initial_indent=initial_indent,
                subsequent_indent=subsequent_indent,
                break_long_words=False,
                break_on_hyphens=False,
            )
        )
    return "\n\n".join(wrapped)


def format_codex_turn_json(turn_json: str, *, width: int) -> Optional[str]:
    """Format a `turn_json` blob into the Codex CLI visible text for that turn."""
    view = extract_turn_view_from_turn_json(turn_json)
    if view is None:
        return None
    return format_codex_turn_view(view, width=width)


def format_codex_turn_view(turn: CodexTurnView, *, width: int) -> str:
    """Format a single turn in the same shape as the Codex CLI output."""
    out: list[str] = []
    out.append("")
    out.append(f"› {turn.user_message}")
    out.append("")

    if turn.reasoning_segments:
        title0, body0 = _split_reasoning_title(turn.reasoning_segments[0])
        first_body = body0 or (title0 or "")
        out.append(
            _wrap_paragraphs(
                first_body,
                width=width,
                initial_indent="• ",
                subsequent_indent="  ",
            )
        )

        for seg in turn.reasoning_segments[1:]:
            title, body = _split_reasoning_title(seg)
            if title:
                out.append("")
                out.append(f"  {title}")
                out.append("")
            if body:
                out.append(
                    _wrap_paragraphs(
                        body,
                        width=width,
                        initial_indent="  ",
                        subsequent_indent="  ",
                    )
                )

        out.append("")

    assistant_text = "\n".join(x for x in turn.agent_messages if x.strip()).strip()
    if assistant_text:
        out.append(
            _wrap_paragraphs(
                assistant_text,
                width=width,
                initial_indent="• ",
                subsequent_indent="  ",
            )
        )
        out.append("")

    return "\n".join(out).lstrip("\n")


def format_token_usage(total_usage: dict) -> Optional[str]:
    try:
        input_tokens = int(total_usage.get("input_tokens", 0) or 0)
        cached_input_tokens = int(total_usage.get("cached_input_tokens", 0) or 0)
        output_tokens = int(total_usage.get("output_tokens", 0) or 0)
        reasoning_output_tokens = int(total_usage.get("reasoning_output_tokens", 0) or 0)
    except (TypeError, ValueError):
        return None

    non_cached_input = max(0, input_tokens - cached_input_tokens)
    total = non_cached_input + output_tokens

    cached_part = f" (+ {cached_input_tokens:,} cached)" if cached_input_tokens else ""
    return (
        f"Token usage: total={total:,} input={non_cached_input:,}"
        f"{cached_part} output={output_tokens:,} (reasoning {reasoning_output_tokens:,})"
    )


def format_codex_rollout_transcript(path: Path, *, width: Optional[int] = None) -> str:
    """Render a rollout file into a transcript matching the Codex CLI view."""
    resolved_width = width or shutil.get_terminal_size(fallback=(100, 20)).columns
    session_id, turns, last_total_usage = extract_turn_views_from_rollout(path)

    out: list[str] = []
    for turn in turns:
        out.append(format_codex_turn_view(turn, width=resolved_width))

    if last_total_usage:
        line = format_token_usage(last_total_usage)
        if line:
            out.append(line)

    if session_id:
        out.append(f"To continue this session, run codex resume {session_id}")

    return "\n".join(out).rstrip() + "\n"
