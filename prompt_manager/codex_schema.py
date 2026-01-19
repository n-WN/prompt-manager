"""Typed schema helpers for Codex rollout (rollout-*.jsonl) parsing.

This mirrors the key shapes from codex-rs:
  - RolloutLine: {timestamp, type, payload}
  - RolloutItem: session_meta / turn_context / response_item / event_msg
  - EventMsg: user_message / agent_message / agent_reasoning / token_count / ...

We keep parsing strict enough to be reliable, but tolerant of unknown variants
by falling back to `Unknown*` wrappers that preserve the raw payload.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterator, Optional


JsonDict = dict[str, Any]


@dataclass(frozen=True)
class RolloutLine:
    timestamp: str
    item: "RolloutItem"
    raw: JsonDict
    raw_line: Optional[str] = None

    @classmethod
    def from_json_line(cls, line: str) -> Optional["RolloutLine"]:
        line = line.strip()
        if not line:
            return None
        try:
            raw = json.loads(line)
        except json.JSONDecodeError:
            return None
        if not isinstance(raw, dict):
            return None
        return cls.from_dict(raw, raw_line=line)

    @classmethod
    def from_dict(cls, raw: JsonDict, *, raw_line: Optional[str] = None) -> Optional["RolloutLine"]:
        timestamp = raw.get("timestamp")
        item_type = raw.get("type")
        payload = raw.get("payload")

        if not isinstance(timestamp, str) or not isinstance(item_type, str):
            return None

        item = RolloutItem.parse(item_type, payload)
        return cls(timestamp=timestamp, item=item, raw=raw, raw_line=raw_line)


def iter_rollout_lines(path) -> Iterator[RolloutLine]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            parsed = RolloutLine.from_json_line(line)
            if parsed is not None:
                yield parsed


class RolloutItem:
    """Discriminated union over top-level rollout items."""

    @staticmethod
    def parse(item_type: str, payload: Any) -> "RolloutItem":
        if item_type == "session_meta" and isinstance(payload, dict):
            return SessionMetaItem.from_payload(payload)
        if item_type == "turn_context" and isinstance(payload, dict):
            return TurnContextItem(raw=payload)
        if item_type == "response_item" and isinstance(payload, dict):
            return ResponseItemItem.from_payload(payload)
        if item_type == "event_msg" and isinstance(payload, dict):
            return EventMsgItem.from_payload(payload)
        return UnknownRolloutItem(item_type=item_type, raw=payload)


@dataclass(frozen=True)
class SessionMetaPayload:
    id: Optional[str]
    cwd: Optional[str]
    timestamp: Optional[str]
    originator: Optional[str]
    cli_version: Optional[str]
    instructions: Optional[str]
    source: Optional[str]
    model_provider: Optional[str]
    git: Optional[JsonDict]
    extra: JsonDict

    @classmethod
    def from_dict(cls, payload: JsonDict) -> "SessionMetaPayload":
        known_keys = {
            "id",
            "cwd",
            "timestamp",
            "originator",
            "cli_version",
            "instructions",
            "source",
            "model_provider",
            "git",
        }
        extra = {k: v for k, v in payload.items() if k not in known_keys}
        git = payload.get("git")
        return cls(
            id=payload.get("id") if isinstance(payload.get("id"), str) else None,
            cwd=str(payload.get("cwd")) if payload.get("cwd") is not None else None,
            timestamp=payload.get("timestamp") if isinstance(payload.get("timestamp"), str) else None,
            originator=payload.get("originator") if isinstance(payload.get("originator"), str) else None,
            cli_version=payload.get("cli_version") if isinstance(payload.get("cli_version"), str) else None,
            instructions=payload.get("instructions") if isinstance(payload.get("instructions"), str) else None,
            source=payload.get("source") if isinstance(payload.get("source"), str) else None,
            model_provider=payload.get("model_provider") if isinstance(payload.get("model_provider"), str) else None,
            git=git if isinstance(git, dict) else None,
            extra=extra,
        )


@dataclass(frozen=True)
class SessionMetaItem(RolloutItem):
    payload: SessionMetaPayload
    raw: JsonDict

    @classmethod
    def from_payload(cls, payload: JsonDict) -> "SessionMetaItem":
        return cls(payload=SessionMetaPayload.from_dict(payload), raw=payload)


@dataclass(frozen=True)
class TurnContextItem(RolloutItem):
    raw: JsonDict


@dataclass(frozen=True)
class ResponseMessage:
    role: str
    content: list[Any]
    raw: JsonDict


@dataclass(frozen=True)
class ResponseItemItem(RolloutItem):
    """ResponseItem persisted into rollout history (subset)."""

    item_type: str
    message: Optional[ResponseMessage]
    raw: JsonDict

    @classmethod
    def from_payload(cls, payload: JsonDict) -> "ResponseItemItem":
        item_type = payload.get("type")
        item_type_str = item_type if isinstance(item_type, str) else "unknown"

        message: Optional[ResponseMessage] = None
        if item_type_str == "message":
            role = payload.get("role")
            content = payload.get("content")
            if isinstance(role, str) and isinstance(content, list):
                message = ResponseMessage(role=role, content=content, raw=payload)

        return cls(item_type=item_type_str, message=message, raw=payload)


class EventMsg:
    """Discriminated union over persisted EventMsg variants."""

    type: str

    @staticmethod
    def parse(payload: JsonDict) -> "EventMsg":
        ev_type = payload.get("type")
        ev_type_str = ev_type if isinstance(ev_type, str) else "unknown"

        if ev_type_str == "user_message":
            return UserMessageEvent.from_payload(payload)
        if ev_type_str == "agent_message":
            return AgentMessageEvent.from_payload(payload)
        if ev_type_str == "agent_reasoning":
            return AgentReasoningEvent.from_payload(payload)
        if ev_type_str == "token_count":
            return TokenCountEvent.from_payload(payload)

        return UnknownEventMsg(type=ev_type_str, raw=payload)


@dataclass(frozen=True)
class UserMessageEvent(EventMsg):
    type: str
    message: str
    images: list[str]
    local_images: list[str]
    text_elements: list[Any]
    raw: JsonDict

    @classmethod
    def from_payload(cls, payload: JsonDict) -> "UserMessageEvent":
        msg = payload.get("message")
        images = payload.get("images")
        local_images = payload.get("local_images")
        text_elements = payload.get("text_elements")
        return cls(
            type="user_message",
            message=msg if isinstance(msg, str) else "",
            images=[x for x in images if isinstance(x, str)] if isinstance(images, list) else [],
            local_images=[x for x in local_images if isinstance(x, str)]
            if isinstance(local_images, list)
            else [],
            text_elements=text_elements if isinstance(text_elements, list) else [],
            raw=payload,
        )


@dataclass(frozen=True)
class AgentMessageEvent(EventMsg):
    type: str
    message: str
    raw: JsonDict

    @classmethod
    def from_payload(cls, payload: JsonDict) -> "AgentMessageEvent":
        msg = payload.get("message")
        return cls(type="agent_message", message=msg if isinstance(msg, str) else "", raw=payload)


@dataclass(frozen=True)
class AgentReasoningEvent(EventMsg):
    type: str
    text: str
    raw: JsonDict

    @classmethod
    def from_payload(cls, payload: JsonDict) -> "AgentReasoningEvent":
        text = payload.get("text")
        return cls(type="agent_reasoning", text=text if isinstance(text, str) else "", raw=payload)


@dataclass(frozen=True)
class TokenCountEvent(EventMsg):
    type: str
    info: Optional[JsonDict]
    rate_limits: Optional[JsonDict]
    raw: JsonDict

    @classmethod
    def from_payload(cls, payload: JsonDict) -> "TokenCountEvent":
        info = payload.get("info")
        rate_limits = payload.get("rate_limits")
        return cls(
            type="token_count",
            info=info if isinstance(info, dict) else None,
            rate_limits=rate_limits if isinstance(rate_limits, dict) else None,
            raw=payload,
        )


@dataclass(frozen=True)
class UnknownEventMsg(EventMsg):
    type: str
    raw: JsonDict


@dataclass(frozen=True)
class EventMsgItem(RolloutItem):
    event: EventMsg
    raw: JsonDict

    @classmethod
    def from_payload(cls, payload: JsonDict) -> "EventMsgItem":
        return cls(event=EventMsg.parse(payload), raw=payload)


@dataclass(frozen=True)
class UnknownRolloutItem(RolloutItem):
    item_type: str
    raw: Any
