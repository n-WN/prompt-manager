#!/usr/bin/env python3
"""Main entry point for Prompt Manager CLI."""

import argparse
import sys


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
        choices=["claude_code", "cursor", "aider", "codex"],
        help="Only sync from specific source"
    )

    # Search command
    search_parser = subparsers.add_parser("search", help="Search prompts")
    search_parser.add_argument("query", nargs="?", help="Search query")
    search_parser.add_argument(
        "--source",
        choices=["claude_code", "cursor", "aider", "codex"],
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

    elif args.command == "search":
        from .db import get_connection, search_prompts

        conn = get_connection()
        prompts = search_prompts(
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
        print(f"Starred:          {stats['starred']}")
        print(f"Total uses:       {stats['total_uses']}")


if __name__ == "__main__":
    main()
