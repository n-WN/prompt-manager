# Prompt Manager

A TUI tool to manage and search prompts from various AI coding assistants.

![Screenshot](./imgs/screenshot.png)

## Features

- Parse and index conversation logs from:
  - Claude Code (JSONL logs)
  - Cursor (SQLite with Protobuf)
  - Codex CLI (session rollouts)
  - Aider (markdown logs)
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
| `/` | Focus search |
| `1-5` | Filter by source |
| `s` | Sync new prompts |
| `c` | Copy selected prompt |
| `f` | Fork session |
| `Enter` | View full detail |
| `q` | Quit |

## Data Storage

- Database: `~/.prompt-manager/prompts.duckdb`
- Parsed from:
  - `~/.claude/projects/` (Claude Code)
  - `~/.cursor/chats/` (Cursor)
  - `~/.codex/sessions/` (Codex)
  - `~/.aider.chat.history.md` (Aider)

## Tech Stack

- DuckDB for fast local storage
- Textual for terminal UI
- Rich for Markdown rendering

## License

MIT
