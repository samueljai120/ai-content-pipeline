#!/usr/bin/env python3
"""
CLS Corp Content Scheduler
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Reads ContentBundle JSON files (produced by pipeline.py) and schedules
each post to the next optimal time window on its platform.
Outputs a human-readable calendar view AND a machine-readable JSON calendar.

Usage:
    python scheduler.py schedule results/abc123.json
    python scheduler.py schedule results/*.json --lookahead 14
    python scheduler.py view    calendar.json
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import click
import yaml
from pydantic import BaseModel, Field
from rich.calendar import Calendar  # noqa: F401  (available in rich extras)
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

console = Console()
log = logging.getLogger("cls.scheduler")

CONFIG_PATHS = ["config.local.yaml", "config.yaml"]

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_config(path: Optional[str] = None) -> dict:
    candidates = [path] if path else CONFIG_PATHS
    for c in candidates:
        p = Path(c)
        if p.exists():
            with p.open() as f:
                return yaml.safe_load(f)
    raise FileNotFoundError(f"Config not found. Expected one of: {', '.join(CONFIG_PATHS)}")


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class ScheduledItem(BaseModel):
    item_id: str
    run_id: str
    platform: str
    content: str
    hashtags: list[str] = Field(default_factory=list)
    scheduled_at: str           # ISO-8601 with timezone
    status: str = "pending"     # pending | published | failed | skipped


class ContentCalendar(BaseModel):
    generated_at: str
    timezone: str
    lookahead_days: int
    items: list[ScheduledItem] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Scheduling engine
# ---------------------------------------------------------------------------

class Scheduler:
    """
    Assigns posts to the next available optimal time window per platform,
    respecting minimum gaps between same-platform posts.
    """

    def __init__(self, config: dict) -> None:
        sched_cfg = config.get("scheduling", {})
        tz_name = sched_cfg.get("timezone", "America/New_York")
        try:
            self.tz = ZoneInfo(tz_name)
        except (ZoneInfoNotFoundError, KeyError):
            log.warning("Unknown timezone '%s', falling back to UTC.", tz_name)
            self.tz = timezone.utc  # type: ignore[assignment]
        self.lookahead_days: int = sched_cfg.get("lookahead_days", 7)
        self.min_gap_minutes: int = sched_cfg.get("min_gap_minutes", 60)
        self.windows: dict = sched_cfg.get("optimal_windows", self._default_windows())
        # Track the last scheduled slot per platform to enforce min gap
        self._last_slot: dict[str, datetime] = {}

    @staticmethod
    def _default_windows() -> dict:
        return {
            "twitter":   [{"day": "weekday", "hours": [8, 12, 17]},
                          {"day": "weekend", "hours": [10, 15]}],
            "linkedin":  [{"day": "weekday", "hours": [7, 12, 17]},
                          {"day": "weekend", "hours": []}],
            "instagram": [{"day": "weekday", "hours": [11, 19]},
                          {"day": "weekend", "hours": [10, 19]}],
        }

    def _candidate_slots(self, platform: str, now: datetime) -> list[datetime]:
        """Generate all candidate datetime slots within the lookahead window."""
        candidates: list[datetime] = []
        platform_windows = self.windows.get(platform, [])
        for day_offset in range(self.lookahead_days):
            candidate_day = now + timedelta(days=day_offset)
            is_weekend = candidate_day.weekday() >= 5  # Saturday=5, Sunday=6
            day_type = "weekend" if is_weekend else "weekday"
            for window in platform_windows:
                if window.get("day") != day_type:
                    continue
                for hour in window.get("hours", []):
                    slot = candidate_day.replace(
                        hour=hour, minute=0, second=0, microsecond=0
                    )
                    if slot > now:
                        candidates.append(slot)
        return sorted(candidates)

    def next_slot(self, platform: str, now: datetime) -> datetime:
        """
        Return the next available slot for the given platform, honouring
        the minimum gap constraint relative to the last scheduled item.
        """
        last = self._last_slot.get(platform)
        candidates = self._candidate_slots(platform, now)
        for slot in candidates:
            if last is None or (slot - last).total_seconds() >= self.min_gap_minutes * 60:
                return slot
        # Fallback: next hour within lookahead
        fallback = now + timedelta(hours=1)
        log.warning(
            "No optimal slot found for %s within %d days; using fallback %s",
            platform, self.lookahead_days, fallback.isoformat(),
        )
        return fallback

    def schedule_bundle(self, bundle: dict, now: datetime) -> list[ScheduledItem]:
        """Convert one ContentBundle dict into a list of ScheduledItems."""
        run_id = bundle.get("run_id", "unknown")
        hashtags = bundle.get("hashtags", [])
        items: list[ScheduledItem] = []
        import uuid

        for post in bundle.get("social_posts", []):
            platform = post.get("platform", "unknown")
            slot = self.next_slot(platform, now)
            self._last_slot[platform] = slot

            item = ScheduledItem(
                item_id=str(uuid.uuid4())[:8],
                run_id=run_id,
                platform=platform,
                content=post.get("content", ""),
                hashtags=hashtags[:10],   # attach top 10 hashtags
                scheduled_at=slot.isoformat(),
                status="pending",
            )
            items.append(item)
            log.info(
                "Scheduled %s post (run %s) for %s",
                platform, run_id, slot.strftime("%a %b %d %H:%M %Z"),
            )
        return items


# ---------------------------------------------------------------------------
# Calendar rendering
# ---------------------------------------------------------------------------

def render_calendar(calendar: ContentCalendar) -> None:
    console.print()

    # Group by date
    by_date: dict[str, list[ScheduledItem]] = {}
    for item in sorted(calendar.items, key=lambda i: i.scheduled_at):
        dt = datetime.fromisoformat(item.scheduled_at)
        key = dt.strftime("%A, %B %d %Y")
        by_date.setdefault(key, []).append(item)

    platform_colors = {
        "twitter":   "cyan",
        "linkedin":  "blue",
        "instagram": "magenta",
    }

    for date_label, items in by_date.items():
        t = Table(title=f"[bold]{date_label}[/bold]", show_lines=True)
        t.add_column("Time",     style="dim",    width=8)
        t.add_column("Platform", width=12)
        t.add_column("Content",  min_width=40)
        t.add_column("Status",   width=10)

        for item in sorted(items, key=lambda i: i.scheduled_at):
            dt = datetime.fromisoformat(item.scheduled_at)
            color = platform_colors.get(item.platform, "white")
            status_color = {
                "pending":   "yellow",
                "published": "green",
                "failed":    "red",
                "skipped":   "dim",
            }.get(item.status, "white")
            t.add_row(
                dt.strftime("%H:%M"),
                f"[{color}]{item.platform.upper()}[/{color}]",
                item.content[:120] + ("…" if len(item.content) > 120 else ""),
                f"[{status_color}]{item.status}[/{status_color}]",
            )
        console.print(t)

    console.print(
        f"\n[dim]{len(calendar.items)} posts scheduled across "
        f"{len(by_date)} days | tz: {calendar.timezone}[/dim]"
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.group()
@click.option("--config", "config_path", default=None)
@click.option("--log-level", default="INFO")
@click.pass_context
def cli(ctx: click.Context, config_path: Optional[str], log_level: str) -> None:
    """CLS Corp Content Scheduler — plan your posting calendar."""
    ctx.ensure_object(dict)
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )
    ctx.obj["config"] = load_config(config_path)


@cli.command()
@click.argument("bundle_files", nargs=-1, required=True, type=click.Path(exists=True))
@click.option(
    "--output", "-o", default="calendar.json",
    help="Path to write the calendar JSON.", show_default=True,
)
@click.option(
    "--lookahead", default=None, type=int,
    help="Override lookahead_days from config.",
)
@click.option("--from-date", default=None, help="Start scheduling from this date (ISO-8601).")
@click.pass_context
def schedule(
    ctx: click.Context,
    bundle_files: tuple[str, ...],
    output: str,
    lookahead: Optional[int],
    from_date: Optional[str],
) -> None:
    """Schedule posts from one or more ContentBundle JSON files."""
    config = ctx.obj["config"]
    if lookahead is not None:
        config.setdefault("scheduling", {})["lookahead_days"] = lookahead

    scheduler = Scheduler(config)

    now: datetime
    if from_date:
        try:
            now = datetime.fromisoformat(from_date).replace(tzinfo=scheduler.tz)
        except ValueError:
            console.print(f"[red]Invalid --from-date: {from_date}[/red]")
            sys.exit(1)
    else:
        now = datetime.now(tz=scheduler.tz)

    all_items: list[ScheduledItem] = []
    for file_path in bundle_files:
        with open(file_path) as f:
            bundle = json.load(f)
        items = scheduler.schedule_bundle(bundle, now)
        all_items.extend(items)
        log.info("Loaded %s — %d posts queued", file_path, len(items))

    tz_name = config.get("scheduling", {}).get("timezone", "America/New_York")
    calendar = ContentCalendar(
        generated_at=datetime.now(timezone.utc).isoformat(),
        timezone=tz_name,
        lookahead_days=scheduler.lookahead_days,
        items=all_items,
    )

    render_calendar(calendar)

    out_path = Path(output)
    with out_path.open("w") as f:
        json.dump(calendar.model_dump(), f, indent=2)
    console.print(f"\n[bold green]✓[/bold green] Calendar saved to [cyan]{out_path}[/cyan]")


@cli.command()
@click.argument("calendar_file", type=click.Path(exists=True))
def view(calendar_file: str) -> None:
    """Display a previously saved calendar JSON."""
    with open(calendar_file) as f:
        data = json.load(f)
    calendar = ContentCalendar(**data)
    render_calendar(calendar)


if __name__ == "__main__":
    cli()
