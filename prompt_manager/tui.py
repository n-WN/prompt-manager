"""Textual TUI for Prompt Manager - Tree-based UI."""

from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.widgets import (
    Header, Footer, Static, Input, Button, Label, Tree, TextArea,
    Rule, OptionList, ContentSwitcher, Markdown, ProgressBar
)
from textual.widgets.tree import TreeNode
from textual.widgets.option_list import Option
from textual.binding import Binding
from textual import on
from textual.screen import ModalScreen
from textual.timer import Timer

from datetime import datetime
from typing import Optional
from collections import defaultdict
import platform
import pyperclip
from rich.markup import escape as escape_markup
import shlex
import shutil
import subprocess
import os
import threading

from .db import (
    get_connection,
    get_prompt,
    get_prompt_preview,
    get_stats,
    increment_use_count,
    search_prompt_summaries,
    search_prompt_summaries_balanced,
    toggle_star,
)
from .codex_transcript import format_codex_turn_json
from .sync import SyncProgress, rebuild_database, sync_all


class ForkConfirmScreen(ModalScreen):
    """Confirmation dialog for forking a session."""

    BINDINGS = [
        Binding("escape", "dismiss", "Cancel"),
        Binding("enter", "confirm", "Confirm"),
    ]

    def __init__(self, prompt: dict):
        super().__init__()
        self.prompt = prompt

    def compose(self) -> ComposeResult:
        source = self.prompt.get("source", "unknown")
        project = self.prompt.get("project_path") or "N/A"
        session_id = self.prompt.get("session_id") or "N/A"

        # Determine launch command and description
        if source == "claude_code":
            if session_id and session_id != "N/A":
                cmd = f"claude --resume {session_id} --fork-session"
                desc = "Create new session with conversation history (fork)"
            else:
                cmd = "claude"
                desc = "Launch Claude Code CLI"
        elif source == "codex":
            if session_id and session_id != "N/A":
                cmd = f"codex fork {session_id}"
                desc = "Fork a new Codex session from saved history"
            else:
                cmd = "codex"
                desc = "Launch Codex CLI (new session)"
        elif source == "aider":
            cmd = "aider"
            desc = "Launch Aider (new chat in project directory)"
        elif source == "cursor":
            cmd = "cursor ."
            desc = "Open Cursor IDE in project directory"
        elif source == "gemini_cli":
            cmd = "gemini"
            desc = "Launch Gemini CLI (new session)"
        elif source == "amp":
            if session_id and session_id != "N/A":
                cmd = f"amp threads handoff {session_id}"
                desc = "Create a new Amp thread from an existing thread (handoff)"
            else:
                cmd = "amp"
                desc = "Launch Amp CLI (new thread)"
        else:
            cmd = "N/A"
            desc = "Unknown agent type"

        with Container(id="fork-dialog"):
            yield Label("[b]Fork Session[/]", id="fork-title")
            yield Rule()
            yield Static(f"[b]Source:[/] {source}")
            yield Static(f"[b]Project:[/] {project}")
            yield Static(f"[b]Session:[/] {session_id[:20]}..." if len(session_id) > 20 else f"[b]Session:[/] {session_id}")
            yield Static(f"[b]Command:[/] {cmd}")
            yield Rule()
            yield Static(f"[cyan]{desc}[/]")
            yield Static("\nContinue from this point with a new session?", id="fork-question")
            with Horizontal(id="fork-actions"):
                yield Button("Fork [Enter]", id="btn-fork-confirm", variant="primary")
                yield Button("Cancel [Esc]", id="btn-fork-cancel", variant="default")

    def action_confirm(self) -> None:
        self.dismiss(True)

    @on(Button.Pressed, "#btn-fork-confirm")
    def on_confirm(self) -> None:
        self.dismiss(True)

    @on(Button.Pressed, "#btn-fork-cancel")
    def on_cancel(self) -> None:
        self.dismiss(False)


class SyncProgressScreen(ModalScreen):
    """Modal progress UI for sync / rebuild operations."""

    DEFAULT_CSS = """
    SyncProgressScreen {
        align: center middle;
    }

    #sync-dialog {
        width: 80%;
        max-width: 100;
        padding: 1 2;
        border: thick $primary;
        background: $surface;
    }

    #sync-title {
        text-style: bold;
        content-align: center middle;
        height: 1;
    }

    #sync-status {
        height: auto;
        color: $text-muted;
        margin: 1 0;
    }

    #sync-subprogress {
        height: 1;
        margin: 0 0 1 0;
    }

    #sync-progress {
        height: 1;
    }
    """

    def __init__(self, title: str):
        super().__init__()
        self._title = title
        self._sub_indeterminate = False
        self._sub_pulse_value = 0
        self._sub_pulse_timer: Optional[Timer] = None

    def on_mount(self) -> None:
        self._sub_pulse_timer = self.set_interval(0.12, self._pulse_subprogress)

    def on_unmount(self) -> None:
        if self._sub_pulse_timer is not None:
            self._sub_pulse_timer.stop()

    def compose(self) -> ComposeResult:
        with Container(id="sync-dialog"):
            yield Label(self._title, id="sync-title")
            yield Rule()
            yield Static("", id="sync-status")
            yield ProgressBar(total=1, show_percentage=False, show_eta=False, id="sync-subprogress")
            yield ProgressBar(total=1, id="sync-progress")

    def _pulse_subprogress(self) -> None:
        if not self._sub_indeterminate:
            return
        self._sub_pulse_value = (self._sub_pulse_value + 3) % 100
        try:
            self.query_one("#sync-subprogress", ProgressBar).update(
                total=100, progress=self._sub_pulse_value
            )
        except Exception:
            return

    def update_progress(self, progress: SyncProgress) -> None:
        total = max(progress.files_total, 1)
        self.query_one("#sync-progress", ProgressBar).update(
            total=total, progress=min(progress.files_checked, total)
        )

        sub_bar = self.query_one("#sync-subprogress", ProgressBar)
        if progress.phase == "syncing" and not progress.skipped:
            if progress.file_items_total is not None:
                self._sub_indeterminate = False
                sub_total = max(int(progress.file_items_total), 1)
                sub_bar.update(total=sub_total, progress=min(progress.file_items_done, sub_total))
            else:
                if not self._sub_indeterminate:
                    self._sub_pulse_value = 0
                self._sub_indeterminate = True
                sub_bar.update(total=100, progress=self._sub_pulse_value)
        else:
            self._sub_indeterminate = False
            self._sub_pulse_value = 0
            sub_bar.update(total=1, progress=0)

        file_label = ""
        if progress.file_path and str(progress.file_path) not in {".", ""}:
            file_label = str(progress.file_path)

        phase = progress.phase
        items = ""
        if progress.file_items_done:
            if progress.file_items_total:
                items = f" | items={progress.file_items_done}/{progress.file_items_total}"
            else:
                items = f" | items={progress.file_items_done}"

        reason = f": {progress.skip_reason}" if progress.skip_reason else ""
        skipped = f" (skipped{reason})" if progress.skipped else ""
        error = f" [error: {progress.error}]" if progress.error else ""

        status = (
            f"{phase} | {progress.files_checked}/{progress.files_total} files | "
            f"updated={progress.files_updated} | new_prompts={progress.new_prompts_total}{items}"
        )
        if progress.source:
            status += f" | source={progress.source}"
        if file_label:
            status += f"\n{file_label}{skipped}{error}"
        self.query_one("#sync-status", Static).update(status)


class RebuildConfirmScreen(ModalScreen):
    """Confirmation dialog for rebuilding the database."""

    BINDINGS = [
        Binding("escape", "dismiss", "Cancel"),
        Binding("enter", "confirm", "Confirm"),
    ]

    def compose(self) -> ComposeResult:
        md = """\
## Rebuild database

This will clear the prompt index and re-import all logs.

- Preserves: `starred`, `tags`, `use_count` (matched by prompt id)
- Rebuilds: prompts/responses/timelines from sources on disk

Continue?
"""
        with Container(id="fork-dialog"):
            yield Label("[b]Rebuild Database[/]", id="fork-title")
            yield Rule()
            yield Markdown(md)
            with Horizontal(id="fork-actions"):
                yield Button("Rebuild [Enter]", id="btn-rebuild-confirm", variant="warning")
                yield Button("Cancel [Esc]", id="btn-rebuild-cancel", variant="default")

    def action_confirm(self) -> None:
        self.dismiss(True)

    @on(Button.Pressed, "#btn-rebuild-confirm")
    def on_confirm(self) -> None:
        self.dismiss(True)

    @on(Button.Pressed, "#btn-rebuild-cancel")
    def on_cancel(self) -> None:
        self.dismiss(False)

class CommandPaletteScreen(ModalScreen):
    """Command palette similar to OpenCode's command list."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
    ]

    def __init__(self, commands: list[tuple[str, str]]):
        super().__init__()
        self.commands = commands

    def compose(self) -> ComposeResult:
        with Container(id="command-dialog"):
            yield Label("[b]Commands[/]", id="command-title")
            yield Rule()
            yield Input(placeholder="Filter...", id="command-filter")
            yield OptionList(id="command-list")

    def on_mount(self) -> None:
        self._set_options("")
        self.query_one("#command-filter", Input).focus()

    def _set_options(self, query: str) -> None:
        q = query.strip().lower()
        options = []
        for command_id, description in self.commands:
            if q and q not in command_id.lower() and q not in description.lower():
                continue
            options.append(Option(f"{command_id} — {description}", id=command_id))
        self.query_one("#command-list", OptionList).set_options(options)

    @on(Input.Changed, "#command-filter")
    def on_filter_changed(self, event: Input.Changed) -> None:
        self._set_options(event.value)

    @on(OptionList.OptionSelected, "#command-list")
    def on_command_selected(self, event: OptionList.OptionSelected) -> None:
        if event.option_id:
            self.dismiss(event.option_id)


class HelpScreen(ModalScreen):
    """Simple help screen for keybinds and actions."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("q", "dismiss", "Close"),
    ]

    def compose(self) -> ComposeResult:
        help_md = """\
## Prompt Manager

- `/` Focus search
- `ctrl+p` Command palette
- `1-5` Filter source (All/Claude/Cursor/Aider/Codex)
- `m` Filter Amp
- `g` Filter Gemini CLI
- `6` Starred only
- `s` Sync new prompts
- `r` Rebuild database (re-index)
- `ctrl+r` Refresh view
- `c`/`y` Copy selected prompt
- `f` Fork (launch agent)
- `Enter` View full detail
- `q` Quit
"""
        with Container(id="help-dialog"):
            yield Label("[b]Help[/]", id="help-title")
            yield Rule()
            yield Markdown(help_md)


class PromptDetailScreen(ModalScreen):
    """Modal screen to show prompt details."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("c", "copy", "Copy"),
        Binding("s", "star", "Star"),
    ]

    def __init__(self, prompt: dict):
        super().__init__()
        self.prompt = prompt

    def compose(self) -> ComposeResult:
        source = self.prompt.get("source", "unknown")
        project = self.prompt.get("project_path", "N/A")
        timestamp = self.prompt.get("timestamp")
        starred = self.prompt.get("starred", False)
        session = self.prompt.get("session_id", "N/A")
        prompt_id = self.prompt.get("id")

        content = self.prompt.get("content", "")
        response = self.prompt.get("response") or ""
        turn_json = None
        if prompt_id:
            conn = getattr(self.app, "conn", None)
            created_conn = False
            if conn is None:
                conn = get_connection()
                created_conn = True
            try:
                full_prompt = get_prompt(conn, prompt_id)
                if full_prompt:
                    content = full_prompt.get("content") or content
                    response = full_prompt.get("response") or response
                    turn_json = full_prompt.get("turn_json")
            except Exception:
                turn_json = None
            finally:
                if created_conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

        ts_str = timestamp.strftime("%Y-%m-%d %H:%M:%S") if timestamp else "N/A"
        star_icon = "[yellow]*[/]" if starred else ""

        with Container(id="detail-dialog"):
            yield Label(f"{star_icon} Prompt Detail", id="detail-title")
            yield Rule()
            with Horizontal(id="detail-meta-row"):
                yield Static(f"[b]Source:[/] {source}", classes="meta-item")
                yield Static(f"[b]Time:[/] {ts_str}", classes="meta-item")
            yield Static(f"[b]Project:[/] {project}", id="detail-project")
            yield Static(f"[b]Session:[/] {session[:20]}..." if session else "", id="detail-session")
            yield Rule()
            with VerticalScroll(id="detail-content"):
                yield Static("[b]Prompt:[/]", classes="section-label")
                yield Markdown(content)
                if response:
                    yield Rule()
                    yield Static("[b]Response:[/]", classes="section-label")
                    yield Markdown(response)
                if turn_json:
                    yield Rule()
                    if source == "codex":
                        yield Static("[b]Codex output (turn):[/]", classes="section-label")
                        transcript = format_codex_turn_json(turn_json, width=100)
                        if transcript:
                            yield Markdown(transcript)
                            yield Rule()
                    yield Static("[b]Turn timeline (raw JSON):[/]", classes="section-label")
                    yield Markdown(f"```json\n{turn_json}\n```")
            with Horizontal(id="detail-actions"):
                yield Button("Copy", id="btn-copy", variant="primary")
                yield Button("Star" if not starred else "Unstar", id="btn-star", variant="warning")
                yield Button("Close", id="btn-close", variant="default")

    def action_copy(self) -> None:
        try:
            pyperclip.copy(self.prompt["content"])
            conn = getattr(self.app, "conn", None)
            created_conn = False
            if conn is None:
                conn = get_connection()
                created_conn = True
            try:
                increment_use_count(conn, self.prompt["id"])
            finally:
                if created_conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
            self.notify("Copied!", severity="information")
        except Exception as e:
            self.notify(f"Failed: {e}", severity="error")

    def action_star(self) -> None:
        conn = getattr(self.app, "conn", None)
        created_conn = False
        if conn is None:
            conn = get_connection()
            created_conn = True
        try:
            new_status = toggle_star(conn, self.prompt["id"])
        finally:
            if created_conn:
                try:
                    conn.close()
                except Exception:
                    pass
        self.prompt["starred"] = new_status
        self.notify("Starred!" if new_status else "Unstarred")
        self.dismiss(True)  # Signal to refresh

    @on(Button.Pressed, "#btn-copy")
    def on_copy(self) -> None:
        self.action_copy()

    @on(Button.Pressed, "#btn-star")
    def on_star(self) -> None:
        self.action_star()

    @on(Button.Pressed, "#btn-close")
    def on_close(self) -> None:
        self.dismiss()


class PromptManagerApp(App):
    """Main TUI application for Prompt Manager."""

    TITLE = "Prompt Manager"
    SUB_TITLE = "Your AI conversation history"

    CSS = """
    Screen {
        background: $surface;
    }

    #app-grid {
        layout: grid;
        grid-size: 2;
        grid-columns: 1fr 2fr;
        height: 100%;
    }

    #left-panel {
        height: 100%;
        border-right: solid $primary-darken-2;
        padding: 0;
    }

    #right-panel {
        height: 100%;
        padding: 1;
    }

    #search-container {
        height: auto;
        padding: 1;
        background: $surface-darken-1;
    }

    #search-input {
        width: 100%;
    }

    #filter-row {
        height: 3;
        padding: 0 1;
        background: $surface-darken-2;
    }

    #filter-row Button {
        min-width: 8;
        height: 3;
    }

    .filter-active {
        background: $primary;
    }

    #stats-bar {
        height: 1;
        background: $primary-darken-3;
        color: $text-muted;
        padding: 0 1;
        text-style: italic;
    }

    #prompt-tree {
        height: 1fr;
        scrollbar-gutter: stable;
    }

    Tree {
        padding: 0 1;
    }

    Tree > .tree--guides {
        color: $primary-darken-2;
    }

    Tree > .tree--cursor {
        background: $primary;
        color: $text;
    }

    #preview-container {
        height: 100%;
        border: round $primary-darken-1;
        padding: 1;
    }

    .preview-inner {
        height: 100%;
    }

    .preview-title {
        text-style: bold;
        color: $primary-lighten-1;
    }

    .preview-meta {
        color: $text-muted;
    }

    .preview-content {
        height: 1fr;
        background: $surface-darken-1;
        margin: 1 0;
        overflow-y: auto;
    }

    .preview-actions {
        height: 3;
        align: center middle;
    }

    .preview-actions Button {
        margin: 0 1;
    }

    .section-label {
        padding: 0;
        color: $primary-lighten-1;
    }

    .response-content {
        height: 1fr;
        background: $surface-darken-2;
        margin: 1 0;
        overflow-y: auto;
    }

    Markdown {
        padding: 0 1;
    }

    .empty-hint {
        height: 100%;
        content-align: center middle;
        color: $text-muted;
    }

    /* Detail Dialog */
    #detail-dialog {
        width: 80%;
        height: 85%;
        background: $surface;
        border: thick $primary;
        padding: 1 2;
    }

    #detail-title {
        text-style: bold;
        text-align: center;
        padding: 1;
        color: $primary-lighten-1;
    }

    #detail-meta-row {
        height: auto;
    }

    .meta-item {
        width: 1fr;
        padding: 0 1;
    }

    #detail-project, #detail-session {
        padding: 0 1;
        color: $text-muted;
    }

    #detail-content {
        height: 1fr;
        margin: 1 0;
    }

    #detail-actions {
        height: 3;
        align: center middle;
    }

    #detail-actions Button {
        margin: 0 1;
        min-width: 10;
    }

    /* Fork Dialog */
    #fork-dialog {
        width: 60%;
        height: auto;
        max-height: 50%;
        background: $surface;
        border: thick $primary;
        padding: 1 2;
    }

    #fork-title {
        text-style: bold;
        text-align: center;
        color: $primary-lighten-1;
    }

    #fork-question {
        text-align: center;
        padding: 1;
    }

    #fork-actions {
        height: 3;
        align: center middle;
    }

    #fork-actions Button {
        margin: 0 1;
    }

    /* Command Dialog */
    #command-dialog {
        width: 70%;
        height: 70%;
        background: $surface;
        border: thick $primary;
        padding: 1 2;
    }

    #command-filter {
        width: 100%;
        margin: 1 0;
    }

    /* Help Dialog */
    #help-dialog {
        width: 70%;
        height: 70%;
        background: $surface;
        border: thick $primary;
        padding: 1 2;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "rebuild", "Rebuild"),
        Binding("ctrl+r", "refresh", "Refresh"),
        Binding("s", "sync", "Sync"),
        Binding("/", "focus_search", "Search"),
        Binding("ctrl+p", "command_palette", "Commands"),
        Binding("?", "help", "Help"),
        Binding("escape", "clear_filter", "Clear"),
        Binding("c", "copy_selected", "Copy"),
        Binding("y", "copy_selected", "Copy"),
        Binding("f", "fork_session", "Fork"),
        Binding("enter", "view_detail", "View"),
        Binding("1", "filter_all", "All"),
        Binding("2", "filter_claude", "Claude"),
        Binding("3", "filter_cursor", "Cursor"),
        Binding("4", "filter_aider", "Aider"),
        Binding("m", "filter_amp", "Amp"),
        Binding("5", "filter_codex", "Codex"),
        Binding("g", "filter_gemini", "Gemini"),
        Binding("6", "filter_starred", "Starred"),
    ]

    def __init__(self):
        super().__init__()
        self.conn = get_connection()
        self.current_filter: Optional[str] = None
        self.starred_only = False
        self.search_query = ""
        self.prompts: list[dict] = []
        self.prompt_map: dict[str, dict] = {}  # id -> prompt
        self.selected_prompt: Optional[dict] = None
        self._search_timer: Optional[Timer] = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="app-grid"):
            with Vertical(id="left-panel"):
                with Container(id="search-container"):
                    yield Input(placeholder="Search...", id="search-input")
                with Horizontal(id="filter-row"):
                    yield Button("All", id="btn-all", variant="primary", classes="filter-active")
                    yield Button("Claude", id="btn-claude")
                    yield Button("Cursor", id="btn-cursor")
                    yield Button("Aider", id="btn-aider")
                    yield Button("Amp", id="btn-amp")
                    yield Button("Codex", id="btn-codex")
                    yield Button("Gemini", id="btn-gemini")
                    yield Button("★", id="btn-starred")
                yield Static("", id="stats-bar")
                yield Tree("Sessions", id="prompt-tree")
            with Vertical(id="right-panel"):
                yield Container(
                    Static("Select a prompt to preview", classes="empty-hint"),
                    id="preview-container"
                )
        yield Footer()

    def on_mount(self) -> None:
        self.load_prompts()
        self.update_stats()
        tree = self.query_one("#prompt-tree", Tree)
        tree.focus()

    def on_unmount(self) -> None:
        """Clean up resources when app closes."""
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass

    def _display_project_label(self, source: str, project: str) -> str:
        if source == "gemini_cli":
            if project.startswith("gemini_cli:"):
                project_hash = project.split(":", 1)[1]
                if project_hash:
                    return f"Gemini {project_hash[:8]}"
            return "Gemini"
        return project.split("/")[-1] if "/" in project else project

    def _display_session_label(self, source: str, session: str, prompts_in_session: list[dict]) -> str:
        session_short = session[:12] + "..." if len(session) > 12 else session
        if source not in {"gemini_cli", "amp"}:
            return session_short

        if not prompts_in_session:
            sid = session[:8] if session else ""
            return f"[dim]{sid}[/]" if sid else session_short

        # Gemini/Amp sessions are keyed by UUID/hash. Use an early user prompt as a human title.
        oldest = prompts_in_session[-1]
        content = (oldest.get("content") or "").strip()
        title = content.splitlines()[0].strip() if content else ""
        if not title:
            title = content.replace("\n", " ").strip()

        title = escape_markup(title[:60])
        sid = session[:8] if session else ""
        if title and sid:
            return f"{title} [dim]{sid}[/]"
        return title or session_short

    def load_prompts(self) -> None:
        """Load prompts and build tree by session."""
        query = (self.search_query or "").strip() or None

        if self.current_filter is None and not self.starred_only and query is None:
            sources = ["claude_code", "cursor", "aider", "amp", "codex", "gemini_cli"]
            per_source = max(50, 1000 // max(len(sources), 1))
            self.prompts = search_prompt_summaries_balanced(
                self.conn,
                sources=sources,
                per_source_limit=per_source,
                snippet_len=400,
            )
        else:
            self.prompts = search_prompt_summaries(
                self.conn,
                query=query,
                source=self.current_filter,
                starred_only=self.starred_only,
                limit=1000,
            )

        # Build prompt map
        self.prompt_map = {p["id"]: p for p in self.prompts}

        # Group by source -> project -> session
        grouped = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        for prompt in self.prompts:
            source = prompt.get("source", "unknown")
            project = prompt.get("project_path") or "No Project"
            session = prompt.get("session_id") or "No Session"
            grouped[source][project][session].append(prompt)

        # Build tree
        tree = self.query_one("#prompt-tree", Tree)
        tree.clear()
        tree.root.expand()

        source_icons = {
            "claude_code": "[cyan]C[/]",
            "cursor": "[magenta]Cu[/]",
            "codex": "[green]Cx[/]",
            "aider": "[yellow]A[/]",
            "gemini_cli": "[blue]Gm[/]",
            "amp": "[red]Am[/]",
        }
        source_labels = {
            "claude_code": "Claude",
            "cursor": "Cursor",
            "codex": "Codex",
            "aider": "Aider",
            "gemini_cli": "Gemini",
            "amp": "Amp",
        }

        for source in sorted(grouped.keys()):
            icon = source_icons.get(source, "[white]?[/]")
            source_label = source_labels.get(source, source)
            source_count = sum(
                len(prompts)
                for projects in grouped[source].values()
                for prompts in projects.values()
            )
            source_node = tree.root.add(
                f"{icon} {source_label} ({source_count})",
                expand=len(grouped) == 1  # Auto-expand if single source
            )

            for project in sorted(grouped[source].keys()):
                project_short = self._display_project_label(source, project)
                project_count = sum(len(p) for p in grouped[source][project].values())
                project_node = source_node.add(
                    f"[blue]{project_short}[/] ({project_count})"
                )

                # Collect ALL prompts in this project for common prefix detection
                all_project_prompts = []
                for session_prompts in grouped[source][project].values():
                    all_project_prompts.extend(session_prompts)

                # Detect common prefix across entire project
                project_common_prefix = self._find_common_prefix(all_project_prompts)

                for session in sorted(grouped[source][project].keys(), reverse=True):
                    prompts_in_session = grouped[source][project][session]
                    # Sort by timestamp
                    prompts_in_session.sort(
                        key=lambda x: x.get("timestamp") or datetime.min,
                        reverse=True
                    )

                    session_label = self._display_session_label(source, session, prompts_in_session)
                    first_ts = prompts_in_session[0].get("timestamp")
                    ts_str = first_ts.strftime("%m/%d") if first_ts else ""

                    session_node = project_node.add(
                        f"[dim]{ts_str}[/] {session_label} ({len(prompts_in_session)})"
                    )

                    for prompt in prompts_in_session:
                        star = "[yellow]*[/]" if prompt.get("starred") else ""
                        content = prompt.get("content", "")
                        ts = prompt.get("timestamp")
                        time_str = ts.strftime("%H:%M") if ts else ""

                        # Generate smart label using project-level common prefix
                        label = self._make_smart_label(content, project_common_prefix, star, time_str)
                        session_node.add_leaf(label, data=prompt["id"])

    def _find_common_prefix(self, prompts: list[dict]) -> str:
        """Find common prefix among prompts in a project."""
        if len(prompts) < 2:
            return ""

        contents = [p.get("content", "").replace("\n", " ") for p in prompts]
        if not contents:
            return ""

        # Find common prefix
        prefix = contents[0]
        for content in contents[1:]:
            while prefix and not content.startswith(prefix):
                prefix = prefix[:-1]
            if not prefix:
                break

        # Only use prefix if it's significant (>30 chars)
        if len(prefix) < 30:
            return ""

        # Check that at least 50% of prompts have meaningful unique content
        unique_parts = [c[len(prefix):].strip() for c in contents]
        meaningful_count = sum(1 for u in unique_parts if len(u) >= 5)
        if meaningful_count < len(prompts) * 0.5:
            return ""

        return prefix

    def _make_smart_label(self, content: str, common_prefix: str, star: str, time_str: str) -> str:
        """Create smart label with optional two-line display."""
        flat = content.replace("\n", " ")

        if common_prefix and flat.startswith(common_prefix):
            # Show truncated prefix + unique part
            prefix_display = common_prefix[:20] + "..." if len(common_prefix) > 20 else common_prefix
            unique_part = flat[len(common_prefix):].strip()[:35]
            if unique_part:
                return f"{star}[dim]{time_str}[/] [dim]{prefix_display}[/]\n    → {unique_part}"

        # Standard single-line preview
        preview = flat[:40]
        return f"{star}[dim]{time_str}[/] {preview}"

    def update_stats(self) -> None:
        stats = get_stats(self.conn)
        stats_text = (
            f"{stats['total']} prompts | "
            f"C:{stats['claude_code']} Cu:{stats['cursor']} A:{stats['aider']} "
            f"Am:{stats['amp']} Cx:{stats['codex']} Gm:{stats['gemini_cli']} | "
            f"★:{stats['starred']}"
        )
        self.query_one("#stats-bar", Static).update(stats_text)

    def update_preview(self, prompt: Optional[dict]) -> None:
        """Update the preview panel with prompt and response."""
        self.selected_prompt = prompt
        container = self.query_one("#preview-container", Container)

        # Remove old content safely
        for child in list(container.children):
            child.remove()

        if prompt is None:
            container.mount(Static("Select a prompt to preview", classes="empty-hint"))
            return

        content = prompt.get("content") or ""
        response = prompt.get("response") or ""
        source = prompt.get("source") or "unknown"
        timestamp = prompt.get("timestamp")
        starred = bool(prompt.get("starred"))
        project = prompt.get("project_path") or "N/A"

        # Defensive timestamp formatting
        ts_str = "N/A"
        if timestamp:
            try:
                ts_str = timestamp.strftime("%Y-%m-%d %H:%M")
            except (AttributeError, ValueError):
                ts_str = str(timestamp)[:16]

        star_str = "[yellow]*[/] " if starred else ""

        # Build preview with optional response section
        widgets = [
            Static(f"{star_str}[b]{source}[/]", classes="preview-title"),
            Static(f"{ts_str} | {project}", classes="preview-meta"),
            Rule(),
            Static("[b]Prompt:[/]", classes="section-label"),
            VerticalScroll(Markdown(content), classes="preview-content"),
        ]

        if response:
            widgets.extend([
                Rule(),
                Static("[b]Response:[/]", classes="section-label"),
                VerticalScroll(Markdown(response), classes="response-content"),
            ])

        widgets.append(
            Horizontal(
                Button("Copy [c/y]", classes="btn-copy", variant="primary"),
                Button("Unstar" if starred else "Star", classes="btn-star", variant="warning"),
                Button("Fork [f]", classes="btn-fork", variant="success"),
                Button("Full [Enter]", classes="btn-full"),
                classes="preview-actions"
            )
        )

        container.mount(Vertical(*widgets, classes="preview-inner"))

    def action_refresh(self) -> None:
        self.load_prompts()
        self.update_stats()
        self.update_preview(None)
        self.notify("Refreshed")

    def action_sync(self) -> None:
        self._start_sync_job(rebuild=False)

    def action_rebuild(self) -> None:
        self.push_screen(RebuildConfirmScreen(), callback=self._on_rebuild_confirm)

    def _on_rebuild_confirm(self, confirmed: bool) -> None:
        if confirmed:
            self._start_sync_job(rebuild=True)

    def _start_sync_job(self, rebuild: bool) -> None:
        title = "Rebuilding database..." if rebuild else "Syncing..."
        screen = SyncProgressScreen(title)
        self.push_screen(screen)

        def on_progress(progress: SyncProgress) -> None:
            self.call_from_thread(screen.update_progress, progress)

        def worker() -> None:
            conn = get_connection()
            try:
                if rebuild:
                    counts = rebuild_database(conn, progress_callback=on_progress)
                else:
                    counts = sync_all(conn, progress_callback=on_progress)
                self.call_from_thread(self._on_sync_complete, screen, counts, rebuild)
            except Exception as e:
                self.call_from_thread(self._on_sync_failed, screen, str(e))
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        threading.Thread(target=worker, daemon=True).start()

    def _on_sync_complete(self, screen: SyncProgressScreen, counts: dict, rebuild: bool) -> None:
        try:
            screen.dismiss()
        except Exception:
            pass

        # Refresh connection so the UI sees all changes.
        try:
            self.conn.close()
        except Exception:
            pass
        self.conn = get_connection()

        self.load_prompts()
        self.update_stats()
        self.update_preview(None)

        if rebuild:
            self.notify(f"Rebuilt database: {counts.get('total', 0)} prompts")
        else:
            self.notify(f"Synced {counts.get('total', 0)} new prompts")

    def _on_sync_failed(self, screen: SyncProgressScreen, error: str) -> None:
        try:
            screen.dismiss()
        except Exception:
            pass
        self.notify(f"Sync failed: {error}", severity="error")

    def action_command_palette(self) -> None:
        commands = [
            ("sync", "Sync new prompts"),
            ("rebuild", "Rebuild database (force re-import)"),
            ("refresh", "Refresh view"),
            ("focus_search", "Focus search"),
            ("clear_filter", "Clear search and filters"),
            ("filter_all", "Show all sources"),
            ("filter_claude", "Filter Claude Code"),
            ("filter_cursor", "Filter Cursor"),
            ("filter_aider", "Filter Aider"),
            ("filter_amp", "Filter Amp"),
            ("filter_codex", "Filter Codex"),
            ("filter_gemini", "Filter Gemini CLI"),
            ("filter_starred", "Starred only"),
            ("quit", "Quit app"),
        ]
        self.push_screen(
            CommandPaletteScreen(commands),
            callback=self._on_command_palette_selected,
        )

    def _on_command_palette_selected(self, command_id: Optional[str]) -> None:
        if not command_id:
            return
        method = getattr(self, f"action_{command_id}", None)
        if callable(method):
            method()
        else:
            self.notify(f"Unknown command: {command_id}", severity="error")

    def action_help(self) -> None:
        self.push_screen(HelpScreen())

    def action_focus_search(self) -> None:
        self.query_one("#search-input", Input).focus()

    def action_clear_filter(self) -> None:
        self.search_query = ""
        self.query_one("#search-input", Input).value = ""
        self.current_filter = None
        self.starred_only = False
        self._update_filter_buttons()
        self.load_prompts()
        self.update_preview(None)

    def action_copy_selected(self) -> None:
        if self.selected_prompt:
            try:
                pyperclip.copy(self.selected_prompt["content"])
                increment_use_count(self.conn, self.selected_prompt["id"])
                self.notify("Copied!")
            except Exception as e:
                self.notify(f"Failed: {e}", severity="error")

    def action_view_detail(self) -> None:
        if self.selected_prompt:
            self.push_screen(
                PromptDetailScreen(self.selected_prompt),
                callback=self._on_detail_close
            )

    def _on_detail_close(self, refresh: bool = False) -> None:
        if refresh:
            self.load_prompts()
            self.update_stats()
            self.update_preview(self.selected_prompt)

    def _set_filter(self, source: Optional[str], starred: bool = False) -> None:
        self.current_filter = source
        self.starred_only = starred
        self._update_filter_buttons()
        self.load_prompts()
        self.update_preview(None)

    def _update_filter_buttons(self) -> None:
        buttons = {
            "btn-all": (None, False),
            "btn-claude": ("claude_code", False),
            "btn-cursor": ("cursor", False),
            "btn-aider": ("aider", False),
            "btn-amp": ("amp", False),
            "btn-codex": ("codex", False),
            "btn-gemini": ("gemini_cli", False),
            "btn-starred": (None, True),
        }

        for btn_id, (source, starred) in buttons.items():
            btn = self.query_one(f"#{btn_id}", Button)
            is_active = (
                (starred and self.starred_only) or
                (not starred and not self.starred_only and self.current_filter == source)
            )
            btn.variant = "primary" if is_active else "default"

    def action_filter_all(self) -> None:
        self._set_filter(None)

    def action_filter_claude(self) -> None:
        self._set_filter("claude_code")

    def action_filter_cursor(self) -> None:
        self._set_filter("cursor")

    def action_filter_aider(self) -> None:
        self._set_filter("aider")

    def action_filter_amp(self) -> None:
        self._set_filter("amp")

    def action_filter_codex(self) -> None:
        self._set_filter("codex")

    def action_filter_gemini(self) -> None:
        self._set_filter("gemini_cli")

    def action_filter_starred(self) -> None:
        self._set_filter(None, starred=True)

    @on(Input.Changed, "#search-input")
    def on_search_changed(self, event: Input.Changed) -> None:
        self.search_query = event.value

        if self._search_timer is not None:
            try:
                self._search_timer.stop()
            except Exception:
                pass
            self._search_timer = None

        # Debounce database scans while typing.
        self._search_timer = self.set_timer(0.25, self._apply_search)

    def _apply_search(self) -> None:
        self._search_timer = None
        self.load_prompts()
        self.update_preview(None)

    @on(Tree.NodeSelected, "#prompt-tree")
    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        node = event.node
        if not node.data or node.data not in self.prompt_map:
            self.update_preview(None)
            return

        prompt_id = node.data
        cached = self.prompt_map[prompt_id]

        # Summaries intentionally omit `response`. Load a lightweight preview row on select.
        if "response" not in cached:
            full = get_prompt_preview(self.conn, prompt_id)
            if full:
                self.prompt_map[prompt_id] = full
                cached = full

        self.update_preview(cached)

    @on(Button.Pressed, "#btn-all")
    def on_all(self) -> None:
        self.action_filter_all()

    @on(Button.Pressed, "#btn-claude")
    def on_claude(self) -> None:
        self.action_filter_claude()

    @on(Button.Pressed, "#btn-cursor")
    def on_cursor(self) -> None:
        self.action_filter_cursor()

    @on(Button.Pressed, "#btn-aider")
    def on_aider(self) -> None:
        self.action_filter_aider()

    @on(Button.Pressed, "#btn-amp")
    def on_amp(self) -> None:
        self.action_filter_amp()

    @on(Button.Pressed, "#btn-codex")
    def on_codex(self) -> None:
        self.action_filter_codex()

    @on(Button.Pressed, "#btn-gemini")
    def on_gemini(self) -> None:
        self.action_filter_gemini()

    @on(Button.Pressed, "#btn-starred")
    def on_starred(self) -> None:
        self.action_filter_starred()

    @on(Button.Pressed, ".btn-copy")
    def on_preview_copy(self) -> None:
        self.action_copy_selected()

    @on(Button.Pressed, ".btn-star")
    def on_preview_star(self) -> None:
        if self.selected_prompt:
            new_status = toggle_star(self.conn, self.selected_prompt["id"])
            self.selected_prompt["starred"] = new_status
            self.notify("Starred!" if new_status else "Unstarred")
            self.load_prompts()
            self.update_stats()
            self.update_preview(self.selected_prompt)

    @on(Button.Pressed, ".btn-full")
    def on_preview_full(self) -> None:
        self.action_view_detail()

    @on(Button.Pressed, ".btn-fork")
    def on_preview_fork(self) -> None:
        self.action_fork_session()

    def action_fork_session(self) -> None:
        """Fork from current prompt - launch agent in same project."""
        if self.selected_prompt:
            self.push_screen(
                ForkConfirmScreen(self.selected_prompt),
                callback=self._on_fork_confirm
            )

    def _on_fork_confirm(self, confirmed: bool) -> None:
        """Handle fork confirmation."""
        if not confirmed or not self.selected_prompt:
            return

        source = self.selected_prompt.get("source", "")
        project = self.selected_prompt.get("project_path") or ""
        session_id = self.selected_prompt.get("session_id") or ""

        def resolve_work_dir(project_path: str) -> str:
            # Preserve Windows drive letters like "C:\\..." and "C:/...".
            if len(project_path) >= 3 and project_path[1] == ":" and project_path[0].isalpha():
                return project_path if os.path.isdir(project_path) else os.path.expanduser("~")

            if project_path.startswith("cursor:"):
                candidate = project_path.split(":", 1)[1]
                if candidate and os.path.isdir(candidate):
                    return candidate

            return project_path if os.path.isdir(project_path) else os.path.expanduser("~")

        # Determine working directory and command
        if source == "claude_code":
            # Claude Code: use --resume with --fork-session
            work_dir = resolve_work_dir(project)
            if session_id:
                cmd = ["claude", "--resume", session_id, "--fork-session"]
            else:
                cmd = ["claude"]
        elif source == "codex":
            # Codex: fork the saved session (Codex CLI supports `codex fork <SESSION_ID>`)
            work_dir = resolve_work_dir(project)
            cmd = ["codex", "fork", session_id] if session_id else ["codex"]
        elif source == "aider":
            # Aider: launch in project directory if available
            work_dir = resolve_work_dir(project)
            cmd = ["aider"]
        elif source == "cursor":
            # Cursor: open in project directory when available
            work_dir = resolve_work_dir(project)
            cmd = ["cursor", "."]
        elif source == "gemini_cli":
            # Gemini CLI: start a new session (no reliable resume command inferred)
            work_dir = os.path.expanduser("~")
            cmd = ["gemini"]
        elif source == "amp":
            # Amp: create a new thread via handoff, or start fresh
            work_dir = resolve_work_dir(project)
            cmd = ["amp", "threads", "handoff", session_id] if session_id else ["amp"]
        else:
            self.notify(f"Unknown source: {source}", severity="error")
            return

        # Verify directory exists
        if not os.path.isdir(work_dir):
            work_dir = os.path.expanduser("~")

        pretty_cmd = f"cd {shlex.quote(work_dir)} && " + " ".join(
            shlex.quote(str(part)) for part in cmd
        )

        try:
            # Launch in new terminal
            self._launch_in_terminal(cmd, work_dir)
            if source == "claude_code" and session_id:
                self.notify(f"Forking session {session_id[:8]}... in {work_dir}")
            elif source == "codex" and session_id:
                self.notify(f"Forking Codex session {session_id[:8]}... in {work_dir}")
            elif source == "amp" and session_id:
                self.notify(f"Handing off Amp thread {session_id[:8]}... in {work_dir}")
            else:
                self.notify(f"Launching {source} in {work_dir}")
        except Exception as e:
            try:
                pyperclip.copy(pretty_cmd)
                self.notify(
                    f"Launch failed: {e} (command copied to clipboard)",
                    severity="error",
                )
            except Exception:
                self.notify(f"Launch failed: {e} (run: {pretty_cmd})", severity="error")

    def _launch_in_terminal(self, cmd: list, work_dir: str) -> None:
        """Launch command in a new terminal window."""

        def shell_join(argv: list[str]) -> str:
            return " ".join(shlex.quote(str(part)) for part in argv)

        def applescript_string(text: str) -> str:
            return text.replace("\\", "\\\\").replace('"', '\\"')

        def popen_detached(argv: list[str]) -> None:
            kwargs = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
            try:
                subprocess.Popen(argv, start_new_session=True, **kwargs)
            except TypeError:
                subprocess.Popen(argv, **kwargs)

        shell_cmd = f"cd {shlex.quote(work_dir)} && {shell_join(cmd)}"

        # If we're inside tmux, prefer opening a new tmux window/tab.
        if os.environ.get("TMUX") and shutil.which("tmux"):
            popen_detached(["tmux", "new-window", "-c", work_dir, shell_cmd])
            return

        system = platform.system()
        if system == "Darwin":
            term_program = os.environ.get("TERM_PROGRAM", "")
            if term_program and term_program not in {"Apple_Terminal", "iTerm.app"}:
                direct = [
                    ("wezterm", ["wezterm", "start", "--cwd", work_dir, "--", *cmd]),
                    ("kitty", ["kitty", "--directory", work_dir, *cmd]),
                    ("alacritty", ["alacritty", "--working-directory", work_dir, "-e", *cmd]),
                ]
                for exe, term_cmd in direct:
                    if shutil.which(exe):
                        popen_detached(term_cmd)
                        return
            if term_program == "iTerm.app":
                script = f'''
                tell application "iTerm"
                    activate
                    create window with default profile
                    tell current session of current window
                        write text "{applescript_string(shell_cmd)}"
                    end tell
                end tell
                '''
            else:
                script = f'''
                tell application "Terminal"
                    activate
                    do script "{applescript_string(shell_cmd)}"
                end tell
                '''
            popen_detached(["osascript", "-e", script])
            return

        if system == "Linux":
            # Prefer modern terminals that support passing argv directly when available.
            direct = [
                ("wezterm", ["wezterm", "start", "--cwd", work_dir, "--", *cmd]),
                ("kitty", ["kitty", "--directory", work_dir, *cmd]),
                ("alacritty", ["alacritty", "--working-directory", work_dir, "-e", *cmd]),
            ]
            for exe, term_cmd in direct:
                if shutil.which(exe):
                    popen_detached(term_cmd)
                    return

            # Fallback to terminals that require a shell string.
            bash_cmd = f"{shell_cmd}; exec bash"
            candidates = [
                ("x-terminal-emulator", ["x-terminal-emulator", "-e", "bash", "-lc", bash_cmd]),
                ("gnome-terminal", ["gnome-terminal", "--", "bash", "-lc", bash_cmd]),
                ("konsole", ["konsole", "-e", "bash", "-lc", bash_cmd]),
                ("xterm", ["xterm", "-e", "bash", "-lc", bash_cmd]),
            ]
            for exe, term_cmd in candidates:
                if shutil.which(exe):
                    popen_detached(term_cmd)
                    return
            raise RuntimeError("No supported terminal found")

        # Windows or other: best-effort, run detached in a new process group.
        try:
            subprocess.Popen(cmd, cwd=work_dir, start_new_session=True)
        except TypeError:
            subprocess.Popen(cmd, cwd=work_dir)


def main():
    """Run the TUI application."""
    app = PromptManagerApp()
    app.run()


if __name__ == "__main__":
    main()
