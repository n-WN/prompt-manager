"""Parser for Cursor chat logs."""

import base64
import sqlite3
import binascii
import json
import os
from pathlib import Path
from typing import Iterator, Optional, List, Tuple, Set, Any, Dict
from collections import Counter, defaultdict

from . import BaseParser, ParsedPrompt


class CursorParser(BaseParser):
    """Parser for Cursor SQLite chat logs.

    Supported locations:
        - Legacy: ~/.cursor/chats/<workspace-id>/<chat-id>/store.db
        - Modern (VS Code globalStorage):
            - macOS: ~/Library/Application Support/Cursor/User/globalStorage/state.vscdb
            - Linux: ~/.config/Cursor/User/globalStorage/state.vscdb

    Legacy format: SQLite with tables:
        - blobs: id (TEXT), data (BLOB) - conversation data (JSON or Protobuf)
        - meta: key (TEXT), value (TEXT) - hex-encoded JSON metadata

    globalStorage format: SQLite with tables:
        - cursorDiskKV: key (TEXT), value (BLOB/TEXT) - JSON (sometimes base64 JSON)
          Keys of interest:
            - composerData:<composerId>
            - bubbleId:<composerId>:<bubbleId>
    """

    source_name = "cursor"

    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = base_path or Path.home() / ".cursor" / "chats"

    def find_log_files(self) -> Iterator[Path]:
        """Find all Cursor SQLite database files."""
        yielded: Set[Path] = set()

        # Legacy store.db locations
        if self.base_path.exists():
            for workspace_dir in self.base_path.iterdir():
                if not workspace_dir.is_dir():
                    continue
                for chat_dir in workspace_dir.iterdir():
                    if not chat_dir.is_dir():
                        continue
                    db_file = chat_dir / "store.db"
                    if db_file.exists() and db_file not in yielded:
                        yielded.add(db_file)
                        yield db_file

        # Modern globalStorage DB (Cursor is VS Code-based)
        for candidate in self._candidate_state_vscdb_paths():
            if candidate.exists() and candidate not in yielded:
                yielded.add(candidate)
                yield candidate

    def _candidate_state_vscdb_paths(self) -> list[Path]:
        candidates: list[Path] = []
        home = Path.home()
        # macOS
        candidates.append(home / "Library" / "Application Support" / "Cursor" / "User" / "globalStorage" / "state.vscdb")
        # Linux (common)
        candidates.append(home / ".config" / "Cursor" / "User" / "globalStorage" / "state.vscdb")
        # Windows (Roaming)
        appdata = os.environ.get("APPDATA")
        if appdata:
            candidates.append(Path(appdata) / "Cursor" / "User" / "globalStorage" / "state.vscdb")
        return candidates

    def parse_file(self, file_path: Path) -> Iterator[ParsedPrompt]:
        """Parse a Cursor SQLite database."""
        if file_path.name == "state.vscdb":
            yield from self._parse_state_vscdb(file_path)
            return

        yield from self._parse_legacy_store_db(file_path)

    def _parse_legacy_store_db(self, file_path: Path) -> Iterator[ParsedPrompt]:
        """Parse legacy Cursor chat DB at ~/.cursor/chats/**/store.db."""
        workspace_id = file_path.parent.parent.name
        chat_id = file_path.parent.name

        try:
            conn = sqlite3.connect(f"file:{file_path}?mode=ro", uri=True)
        except sqlite3.Error:
            return

        try:
            # Get metadata
            cursor = conn.execute("SELECT key, value FROM meta")
            meta_row = cursor.fetchone()

            chat_name = "Unknown"
            created_at = None

            if meta_row:
                try:
                    hex_value = meta_row[1]
                    meta_json = binascii.unhexlify(hex_value).decode('utf-8')
                    meta_data = json.loads(meta_json)
                    chat_name = meta_data.get("name", "Unknown")
                    if "createdAt" in meta_data:
                        created_at = self.parse_timestamp(meta_data["createdAt"])
                except (binascii.Error, json.JSONDecodeError, ValueError):
                    pass

            # Parse ALL blobs and collect messages in order
            cursor = conn.execute("SELECT id, data FROM blobs")
            messages: List[Tuple[str, str, str]] = []  # (role, content, blob_id)
            seen_content: Set[str] = set()  # For deduplication

            for blob_id, blob_data in cursor:
                if not isinstance(blob_data, bytes):
                    continue

                # Try JSON first (more reliable)
                parsed = self._try_parse_json(blob_data)

                # Try Protobuf if JSON fails
                if parsed is None:
                    parsed = self._try_parse_protobuf(blob_data)

                if parsed is None:
                    continue

                role, content = parsed
                if not role or not content:
                    continue

                # Deduplicate based on content hash (first 200 chars)
                content_key = content[:200].strip()
                if content_key in seen_content:
                    continue
                seen_content.add(content_key)

                messages.append((role, content, blob_id))

            # Generate prompts: pair user messages with following assistant responses
            i = 0
            while i < len(messages):
                role, content, blob_id = messages[i]

                if role != "user":
                    i += 1
                    continue

                # Clean up content (remove XML tags like <user_query>)
                clean_content = self._clean_user_content(content)

                if len(clean_content.strip()) < 10:
                    i += 1
                    continue

                # Look ahead for assistant response (skip tool messages)
                response = None
                j = i + 1
                while j < len(messages):
                    next_role, next_content, _ = messages[j]

                    if next_role == "user":
                        # Hit next user message, stop looking
                        break

                    if next_role == "assistant":
                        # Found assistant response
                        response = next_content
                        break

                    # Skip tool/system messages
                    j += 1

                prompt_id = self.generate_id(
                    self.source_name,
                    clean_content,
                    chat_id,
                    blob_id
                )

                yield ParsedPrompt(
                    id=prompt_id,
                    source=self.source_name,
                    content=clean_content,
                    project_path=f"cursor:{workspace_id}/{chat_name}",
                    session_id=chat_id,
                    timestamp=created_at,
                    response=response,
                )

                i += 1

        finally:
            conn.close()

    def _parse_state_vscdb(self, file_path: Path) -> Iterator[ParsedPrompt]:
        """Parse Cursor globalStorage DB (state.vscdb)."""
        try:
            conn = sqlite3.connect(f"file:{file_path}?mode=ro", uri=True)
        except sqlite3.Error:
            return

        try:
            # Ensure expected table exists
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "cursorDiskKV" not in tables:
                return

            composers: Dict[str, Dict[str, Any]] = {}
            for key, value in conn.execute(
                "SELECT key, value FROM cursorDiskKV WHERE key LIKE 'composerData:%'"
            ):
                composer_id = self._parse_composer_id(key)
                if not composer_id:
                    continue
                obj = self._decode_kv_json(value)
                if isinstance(obj, dict):
                    composers[composer_id] = obj

            bubbles_by_composer: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for key, value in conn.execute(
                "SELECT key, value FROM cursorDiskKV WHERE key LIKE 'bubbleId:%'"
            ):
                composer_id, bubble_id = self._parse_bubble_key(key)
                if not composer_id or not bubble_id:
                    continue

                obj = self._decode_kv_json(value)
                if not isinstance(obj, dict):
                    continue

                # Normalize bubble id
                if not obj.get("bubbleId"):
                    obj["bubbleId"] = bubble_id
                obj["_composerId"] = composer_id

                bubbles_by_composer[composer_id].append(obj)

            for composer_id, bubbles in bubbles_by_composer.items():
                composer = composers.get(composer_id, {})

                project_path = self._infer_project_path(composer)
                if project_path:
                    project_label = f"cursor:{project_path}"
                else:
                    project_label = "cursor"

                # Sort bubbles by a stable numeric time key
                prepared: list[tuple[float, dict[str, Any]]] = []
                for bubble in bubbles:
                    sort_key = self._bubble_sort_key(bubble)
                    prepared.append((sort_key, bubble))
                prepared.sort(key=lambda x: (x[0], str(x[1].get("bubbleId", ""))))

                # Create prompts by pairing user bubbles (type=1) with subsequent assistant bubbles (type=2)
                idx = 0
                while idx < len(prepared):
                    _, bubble = prepared[idx]
                    if bubble.get("type") != 1:
                        idx += 1
                        continue

                    content = bubble.get("text") or ""
                    if not isinstance(content, str):
                        idx += 1
                        continue

                    content = self._clean_user_content(content)
                    if len(content.strip()) < 10:
                        idx += 1
                        continue

                    timestamp = self._bubble_timestamp(bubble) or self.parse_timestamp(
                        composer.get("createdAt")
                    )

                    response_parts: list[str] = []
                    j = idx + 1
                    while j < len(prepared):
                        _, next_bubble = prepared[j]
                        if next_bubble.get("type") == 1:
                            break
                        if next_bubble.get("type") == 2:
                            text = next_bubble.get("text") or ""
                            if isinstance(text, str) and text.strip():
                                response_parts.append(text)
                        j += 1

                    response = "\n".join(response_parts) if response_parts else None

                    bubble_id = str(bubble.get("bubbleId") or "")
                    prompt_id = self.generate_id(
                        self.source_name,
                        content,
                        composer_id,
                        bubble_id,
                    )

                    yield ParsedPrompt(
                        id=prompt_id,
                        source=self.source_name,
                        content=content,
                        project_path=project_label,
                        session_id=composer_id,
                        timestamp=timestamp,
                        response=response,
                    )

                    idx += 1

        finally:
            conn.close()

    def _parse_composer_id(self, key: str) -> Optional[str]:
        if not isinstance(key, str):
            return None
        if not key.startswith("composerData:"):
            return None
        return key.split(":", 1)[1] or None

    def _parse_bubble_key(self, key: str) -> tuple[Optional[str], Optional[str]]:
        if not isinstance(key, str) or not key.startswith("bubbleId:"):
            return None, None
        parts = key.split(":", 2)
        if len(parts) != 3:
            return None, None
        composer_id = parts[1] or None
        bubble_id = parts[2] or None
        return composer_id, bubble_id

    def _decode_kv_json(self, value: Any) -> Optional[Any]:
        """Decode a cursorDiskKV/ItemTable value into JSON when possible."""
        if value is None:
            return None

        if isinstance(value, memoryview):
            raw = value.tobytes()
        elif isinstance(value, (bytes, bytearray)):
            raw = bytes(value)
        elif isinstance(value, str):
            raw = value.encode("utf-8", errors="ignore")
        else:
            try:
                raw = bytes(value)
            except Exception:
                return None

        # Fast path: UTF-8 JSON
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = None

        if text is not None:
            stripped = text.lstrip()
            if stripped.startswith("{") or stripped.startswith("["):
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    pass
            # base64 JSON stored as text
            try:
                decoded = base64.b64decode(text, validate=True)
                try:
                    return json.loads(decoded.decode("utf-8"))
                except Exception:
                    pass
            except Exception:
                pass

        # base64 JSON stored as bytes
        try:
            decoded = base64.b64decode(raw, validate=True)
            try:
                return json.loads(decoded.decode("utf-8"))
            except Exception:
                return None
        except Exception:
            return None

    def _bubble_timestamp(self, bubble: dict[str, Any]):
        created_at = bubble.get("createdAt")
        if created_at:
            return self.parse_timestamp(created_at)
        timing = bubble.get("timingInfo")
        if isinstance(timing, dict):
            end_time = timing.get("clientEndTime")
            if end_time:
                return self.parse_timestamp(end_time)
        return None

    def _bubble_sort_key(self, bubble: dict[str, Any]) -> float:
        ts = self._bubble_timestamp(bubble)
        if ts is None:
            # Put unknown timestamps last; still deterministic with bubbleId tie-breaker.
            return float("inf")
        try:
            return ts.timestamp()
        except Exception:
            return float("inf")

    def _infer_project_path(self, composer: dict[str, Any]) -> Optional[str]:
        """Try to infer a stable project/root path from composerData."""
        if not isinstance(composer, dict) or not composer:
            return None

        paths: list[str] = []

        # codeBlockData contains lots of URI metadata; fsPath is the most useful.
        code_block_data = composer.get("codeBlockData")
        if isinstance(code_block_data, dict):
            for entry in code_block_data.values():
                if not isinstance(entry, dict):
                    continue
                uri = entry.get("uri")
                if isinstance(uri, dict):
                    fs_path = uri.get("fsPath")
                    if isinstance(fs_path, str) and fs_path:
                        paths.append(fs_path)

        # Some versions store a plain list of URIs.
        uris = composer.get("allAttachedFileCodeChunksUris")
        if isinstance(uris, list):
            for u in uris:
                if isinstance(u, str) and u.startswith("file://"):
                    paths.append(u.replace("file://", ""))

        if not paths:
            return None

        # Prefer a Git root if available.
        roots = Counter()
        for p in paths:
            try:
                path_obj = Path(p)
            except Exception:
                continue
            for parent in [path_obj] + list(path_obj.parents):
                if (parent / ".git").exists():
                    roots[str(parent)] += 1
                    break

        if roots:
            return roots.most_common(1)[0][0]

        # Fall back to common path prefix.
        try:
            import os as _os
            common = _os.path.commonpath(paths)
            if common and common != "/":
                return common
        except Exception:
            return None

        return None

    def _clean_user_content(self, content: str) -> str:
        """Clean up user content by removing XML tags."""
        import re
        # Remove <user_query>, <user_info>, etc. tags
        content = re.sub(r'<user_query>\s*', '', content)
        content = re.sub(r'\s*</user_query>', '', content)
        content = re.sub(r'<user_info>.*?</user_info>\s*', '', content, flags=re.DOTALL)
        content = re.sub(r'<environment_context>.*?</environment_context>\s*', '', content, flags=re.DOTALL)
        return content.strip()

    def _try_parse_json(self, blob_data: bytes) -> Optional[Tuple[str, str]]:
        """Try to parse blob as JSON."""
        try:
            data = json.loads(blob_data.decode('utf-8'))
            if not isinstance(data, dict):
                return None

            role = data.get("role")
            content = data.get("content")

            if role == "user" and content:
                text = self._extract_text_content(content)
                if text:
                    return ("user", text)

            elif role == "assistant" and content:
                text = self._extract_assistant_text(content)
                if text:
                    return ("assistant", text)

            elif role == "tool":
                return ("tool", "")  # Mark as tool but no content needed

        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
        return None

    def _try_parse_protobuf(self, blob_data: bytes) -> Optional[Tuple[str, str]]:
        """Try to parse blob as Protobuf and extract message."""
        strings = self._parse_protobuf_strings(blob_data)
        if not strings:
            return None

        # Look for patterns in the extracted strings
        for field_num, text in strings:
            # Field 4 often contains embedded JSON with role/content
            if field_num == 4 and text.startswith("{"):
                try:
                    embedded = json.loads(text)
                    if isinstance(embedded, dict):
                        role = embedded.get("role")
                        content = embedded.get("content")
                        if role == "assistant" and content:
                            extracted = self._extract_assistant_text(content)
                            if extracted:
                                return ("assistant", extracted)
                        elif role == "user" and content:
                            extracted = self._extract_text_content(content)
                            if extracted:
                                return ("user", extracted)
                except json.JSONDecodeError:
                    pass

            # Field 1 might contain user message text (but only if no JSON found)
            if field_num == 1 and len(text) > 20:
                if not text.startswith(("file://", "http://", "https://", "{")):
                    if any(c.isalpha() for c in text):
                        return ("user", text)

        return None

    def _parse_protobuf_strings(self, data: bytes) -> List[Tuple[int, str]]:
        """Extract strings from Protobuf-encoded data."""
        strings = []
        pos = 0

        while pos < len(data):
            try:
                tag_byte, pos = self._decode_varint(data, pos)
                if tag_byte is None:
                    break

                field_num = tag_byte >> 3
                wire_type = tag_byte & 0x7

                if wire_type == 0:  # Varint
                    _, pos = self._decode_varint(data, pos)
                elif wire_type == 2:  # Length-delimited
                    length, pos = self._decode_varint(data, pos)
                    if length is None or pos + length > len(data):
                        break
                    chunk = data[pos:pos + length]
                    pos += length

                    # Try to decode as UTF-8 string
                    try:
                        s = chunk.decode('utf-8')
                        if len(s) > 5 and s.isprintable():
                            strings.append((field_num, s))
                    except UnicodeDecodeError:
                        # Try recursive parse for nested messages
                        nested = self._parse_protobuf_strings(chunk)
                        strings.extend(nested)
                elif wire_type == 5:  # 32-bit
                    pos += 4
                elif wire_type == 1:  # 64-bit
                    pos += 8
                else:
                    break
            except Exception:
                break

        return strings

    def _decode_varint(self, data: bytes, pos: int) -> Tuple[Optional[int], int]:
        """Decode a Protobuf varint."""
        result = 0
        shift = 0
        while True:
            if pos >= len(data):
                return None, pos
            b = data[pos]
            result |= (b & 0x7f) << shift
            pos += 1
            if not (b & 0x80):
                break
            shift += 7
            if shift > 63:
                return None, pos
        return result, pos

    def _extract_text_content(self, content) -> Optional[str]:
        """Extract text from user content (string or list of blocks)."""
        if isinstance(content, str):
            return content

        if isinstance(content, list):
            text_parts = []
            for block in content:
                if isinstance(block, dict):
                    if block.get("type") in ("text", "input_text"):
                        text = block.get("text", "")
                        if text:
                            text_parts.append(text)
                elif isinstance(block, str):
                    text_parts.append(block)
            return "\n".join(text_parts) if text_parts else None

        return None

    def _extract_assistant_text(self, content) -> Optional[str]:
        """Extract readable text from assistant content blocks."""
        if isinstance(content, str):
            return content

        if not isinstance(content, list):
            return None

        text_parts = []
        for block in content:
            if not isinstance(block, dict):
                continue

            block_type = block.get("type")

            if block_type in ("text", "output_text"):
                text = block.get("text", "")
                if text:
                    text_parts.append(text)

            elif block_type == "reasoning":
                text = block.get("text", "")
                if text:
                    text_parts.append(f"[Reasoning] {text}")

        return "\n".join(text_parts) if text_parts else None
