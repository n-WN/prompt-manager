#!/usr/bin/env python3
"""Main entry point for Prompt Manager CLI."""

import argparse
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description="Prompt Manager - Manage and reuse prompts from Code Agent logs"
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # TUI command (default)
    tui_parser = subparsers.add_parser("tui", help="Launch the TUI interface")

    # Sync command
    sync_parser = subparsers.add_parser("sync", help="Sync prompts from all sources")
    sync_parser.add_argument(
        "--source",
        choices=["claude_code", "cursor", "aider", "codex", "gemini_cli"],
        help="Only sync from specific source"
    )

    # Rebuild command
    rebuild_parser = subparsers.add_parser("rebuild", help="Rebuild database (force re-import)")
    rebuild_parser.add_argument(
        "--no-preserve-metadata",
        action="store_true",
        help="Do not preserve starred/tags/use_count",
    )

    # Search command
    search_parser = subparsers.add_parser("search", help="Search prompts")
    search_parser.add_argument("query", nargs="?", help="Search query")
    search_parser.add_argument(
        "--source",
        choices=["claude_code", "cursor", "aider", "codex", "gemini_cli"],
        help="Filter by source"
    )
    search_parser.add_argument(
        "--starred",
        action="store_true",
        help="Only show starred prompts"
    )
    search_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Limit results (default: 20)"
    )

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show statistics")

    # Database info / diagnostics
    db_info_parser = subparsers.add_parser("db-info", help="Show database file sizes and status")

    # Codex transcript (from rollout jsonl)
    codex_transcript_parser = subparsers.add_parser(
        "codex-transcript",
        help="Print a Codex rollout transcript (matches Codex CLI view)",
    )
    codex_transcript_parser.add_argument("path", help="Path to rollout-*.jsonl")
    codex_transcript_parser.add_argument(
        "--width",
        type=int,
        default=None,
        help="Wrap width (defaults to terminal width)",
    )

    args = parser.parse_args()

    if args.command is None or args.command == "tui":
        from .tui import main as tui_main
        tui_main()

    elif args.command == "sync":
        from .db import get_connection
        from .sync import sync_all, sync_source

        conn = get_connection()

        if args.source:
            count = sync_source(args.source, conn)
            print(f"Synced {count} new prompts from {args.source}")
        else:
            counts = sync_all(conn)
            print(f"Synced {counts['total']} new prompts:")
            print(f"  Claude Code: {counts['claude_code']}")
            print(f"  Cursor: {counts['cursor']}")
            print(f"  Aider: {counts['aider']}")
            print(f"  Codex: {counts['codex']}")
            print(f"  Gemini CLI: {counts['gemini_cli']}")

    elif args.command == "rebuild":
        from .db import get_connection
        from .sync import rebuild_database, SyncProgress

        conn = get_connection()

        last_line = ""

        def on_progress(p: SyncProgress) -> None:
            nonlocal last_line
            if p.phase in {"starting", "resetting", "restoring"}:
                line = p.phase
            else:
                suffix = ""
                if p.phase == "syncing":
                    if p.file_items_total is not None:
                        suffix = f" | items={p.file_items_done}/{p.file_items_total}"
                    else:
                        suffix = f" | items={p.file_items_done}"
                if p.skipped and p.skip_reason:
                    suffix += f" | skipped={p.skip_reason}"
                line = f"{p.phase} {p.files_checked}/{p.files_total} | updated={p.files_updated} | new={p.new_prompts_total}{suffix}"
            if line != last_line:
                print(line)
                last_line = line

        counts = rebuild_database(
            conn,
            progress_callback=on_progress,
            preserve_metadata=not args.no_preserve_metadata,
        )
        print(f"Rebuilt database: {counts.get('total', 0)} prompts")

    elif args.command == "search":
        from .db import get_connection, search_prompt_summaries

        conn = get_connection()
        prompts = search_prompt_summaries(
            conn,
            query=args.query,
            source=args.source,
            starred_only=args.starred,
            limit=args.limit,
        )

        if not prompts:
            print("No prompts found.")
            return

        for i, prompt in enumerate(prompts, 1):
            star = "*" if prompt["starred"] else " "
            source = prompt["source"]
            content = prompt["content"].replace("\n", " ")[:80]
            timestamp = prompt["timestamp"]
            ts_str = timestamp.strftime("%Y-%m-%d") if timestamp else "N/A"

            print(f"{i:3}. [{star}] [{source:10}] [{ts_str}] {content}...")

    elif args.command == "stats":
        from .db import get_connection, get_stats

        conn = get_connection()
        stats = get_stats(conn)

        print("Prompt Manager Statistics")
        print("=" * 40)
        print(f"Total prompts:    {stats['total']}")
        print(f"  Claude Code:    {stats['claude_code']}")
        print(f"  Cursor:         {stats['cursor']}")
        print(f"  Aider:          {stats['aider']}")
        print(f"  Codex:          {stats['codex']}")
        print(f"  Gemini CLI:     {stats['gemini_cli']}")
        print(f"Starred:          {stats['starred']}")
        print(f"Total uses:       {stats['total_uses']}")

    elif args.command == "db-info":
        import os
        import duckdb

        from .db import get_db_path, get_default_db_path, get_recovered_db_path
        from .db import get_stats

        def fmt_bytes(n: int) -> str:
            units = ["B", "KiB", "MiB", "GiB", "TiB"]
            v = float(n)
            for u in units:
                if v < 1024.0 or u == units[-1]:
                    if u == "B":
                        return f"{int(v)} {u}"
                    return f"{v:.1f} {u}"
                v /= 1024.0
            return f"{n} B"

        def file_size(path: str) -> int:
            try:
                return os.path.getsize(path)
            except OSError:
                return 0

        default_path = get_default_db_path()
        recovered_path = get_recovered_db_path(default_path)
        active_path = get_db_path()

        candidates = []
        if os.environ.get("PROMPT_MANAGER_DB_PATH"):
            candidates.append(("active", active_path))
        else:
            candidates.append(("default", default_path))
            candidates.append(("recovered", recovered_path))
            if active_path not in {default_path, recovered_path}:
                candidates.append(("active", active_path))

        print("Prompt Manager DB Info")
        print("=" * 40)
        print(f"Active DB: {active_path}")
        print("")

        for label, path in candidates:
            db_file = str(path)
            wal_file = f"{db_file}.wal"
            db_bytes = file_size(db_file)
            wal_bytes = file_size(wal_file)
            exists = os.path.exists(db_file)
            print(f"[{label}] {db_file}{'' if exists else ' (missing)'}")
            if exists:
                print(f"  size: {fmt_bytes(db_bytes)}")
            if wal_bytes:
                print(f"  wal:  {fmt_bytes(wal_bytes)}")
            print("")

        # Try to query stats (best-effort; avoid creating new files).
        if os.path.exists(str(active_path)):
            try:
                conn = duckdb.connect(str(active_path), read_only=True)
            except Exception as e:
                print(f"Failed to open active DB read-only: {e}")
            else:
                try:
                    stats = get_stats(conn)
                    print("Row counts")
                    print(f"  total:      {stats['total']}")
                    print(f"  claude:     {stats['claude_code']}")
                    print(f"  cursor:     {stats['cursor']}")
                    print(f"  aider:      {stats['aider']}")
                    print(f"  codex:      {stats['codex']}")
                    print(f"  gemini:     {stats['gemini_cli']}")
                    print(f"  starred:    {stats['starred']}")
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass

    elif args.command == "codex-transcript":
        from .codex_transcript import format_codex_rollout_transcript

        path = Path(args.path).expanduser()
        if not path.exists():
            print(f"File not found: {path}", file=sys.stderr)
            sys.exit(2)

        sys.stdout.write(format_codex_rollout_transcript(path, width=args.width))


if __name__ == "__main__":
    main()
