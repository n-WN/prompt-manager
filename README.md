# Prompt Manager

A TUI tool to manage and search prompts from various AI coding assistants.

![Screenshot](./imgs/screenshot.png)

## Features

- Parse and index conversation logs from:
  - Claude Code (JSONL logs)
  - Cursor (SQLite with Protobuf)
  - Codex CLI (session rollouts)
  - Aider (markdown logs)
  - Gemini CLI (session JSON)
  - Amp CLI (thread JSON)
- Full-text search across all prompts
- Tree-based navigation by source/project/session
- Preview prompts and responses with Markdown rendering
- Star favorite prompts for quick access
- Fork sessions to continue conversations
- Incremental sync (only processes changed files)

## Development

Requires Python 3.11+ and [uv](https://github.com/astral-sh/uv).

```bash
git clone https://github.com/gengdz/prompt-manager.git
cd prompt-manager
uv sync
```

### Launch or run directly

```bash
# Launch TUI
uv run pm

# Or run directly
uv run python -m prompt_manager.tui
```


## Usage

### By uvx

```bash
uvx git+https://github.com/n-WN/prompt-manager.git
```

### By uv

```bash
uv tool install git+https://github.com/n-WN/prompt-manager.git

pm 

#or
prompt-manager
```

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `ctrl+p` | Command palette |
| `?` | Help |
| `/` | Focus search |
| `1-5` | Filter by source (All/Claude/Cursor/Aider/Codex) |
| `m` | Filter Amp |
| `g` | Filter Gemini CLI |
| `6` | Filter starred |
| `s` | Sync new prompts |
| `r` | Rebuild database (force re-import) |
| `ctrl+r` | Refresh view |
| `c` / `y` | Copy selected prompt |
| `f` | Fork session |
| `Enter` | View full detail |
| `q` | Quit |

## Upgrading

Prompt Manager keeps a local DuckDB index at `~/.prompt-manager/prompts.duckdb`. When you upgrade to a newer
version (especially one that changes parsing or storage), rebuild the index so existing logs are re-imported:

- In the TUI: press `r` and confirm to rebuild
- Or via CLI: `pm rebuild`

Notes:
- `s` is incremental sync (usually seconds; only changed files).
- `r` is a full rebuild (can take minutes; clears and re-imports everything).
- Seeing many `skipped` files during `s` is normal — it just means those logs didn't change.

If startup becomes slow after upgrading, you may have an old / very large database (or a large `.wal` file).
You can inspect and clean it with:

- `pm db-info`
- `pm db-clean` (dry-run)
- `pm db-clean --yes` (delete old DB/WAL in `~/.prompt-manager`)

## Data Storage

- Database: `~/.prompt-manager/prompts.duckdb`
- Parsed from:
  - `~/.claude/projects/` (Claude Code)
  - `~/.cursor/chats/` and Cursor globalStorage `state.vscdb` (Cursor)
  - `~/.codex/sessions/` (Codex)
  - `~/.aider.chat.history.md` (Aider)
  - `~/.gemini/tmp/**/chats/` (Gemini CLI)
  - `~/.local/share/amp/threads/` (Amp)

## Tech Stack

- DuckDB for fast local storage
- Textual for terminal UI
- Rich for Markdown rendering

## License

MIT
