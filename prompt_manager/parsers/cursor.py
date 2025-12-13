"""Parser for Cursor chat logs."""

import sqlite3
import binascii
import json
from pathlib import Path
from typing import Iterator, Optional, List, Tuple, Set

from . import BaseParser, ParsedPrompt


class CursorParser(BaseParser):
    """Parser for Cursor SQLite chat logs.

    Log location: ~/.cursor/chats/<workspace-id>/<chat-id>/store.db
    Format: SQLite with tables:
        - blobs: id (TEXT), data (BLOB) - conversation data (JSON or Protobuf)
        - meta: key (TEXT), value (TEXT) - hex-encoded JSON metadata
    """

    source_name = "cursor"

    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = base_path or Path.home() / ".cursor" / "chats"

    def find_log_files(self) -> Iterator[Path]:
        """Find all Cursor SQLite database files."""
        if not self.base_path.exists():
            return

        for workspace_dir in self.base_path.iterdir():
            if not workspace_dir.is_dir():
                continue
            for chat_dir in workspace_dir.iterdir():
                if not chat_dir.is_dir():
                    continue
                db_file = chat_dir / "store.db"
                if db_file.exists():
                    yield db_file

    def parse_file(self, file_path: Path) -> Iterator[ParsedPrompt]:
        """Parse a Cursor SQLite database."""
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
