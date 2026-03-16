#!/usr/bin/env python3
"""
CLS Corp Content Analytics
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Fetches post-performance metrics from Twitter/X, LinkedIn, and Instagram,
enriches the calendar with real engagement data, and surfaces top performers.

Platform API calls are stubbed with realistic mock data so the pipeline
runs end-to-end without live credentials.  Swap stub functions for real
SDK calls (tweepy, linkedin-api, facebook-sdk) when deploying to production.

Usage:
    python analytics.py report  calendar.json
    python analytics.py report  calendar.json --platform twitter
    python analytics.py export  calendar.json --output report.json
    python analytics.py compare results/*.json
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import click
import yaml
from pydantic import BaseModel, Field
from rich.bar import Bar
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table

console = Console()
log = logging.getLogger("cls.analytics")

CONFIG_PATHS = ["config.local.yaml", "config.yaml"]

# ---------------------------------------------------------------------------
# Config
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

class PostMetrics(BaseModel):
    item_id: str
    platform: str
    impressions: int = 0
    reach: int = 0
    likes: int = 0
    comments: int = 0
    shares: int = 0          # retweets on Twitter
    clicks: int = 0
    saves: int = 0           # Instagram saves
    engagement_rate: float = 0.0
    fetched_at: str = ""
    is_top_performer: bool = False


class AnalyticsReport(BaseModel):
    generated_at: str
    calendar_file: str
    platform_filter: Optional[str]
    total_posts: int
    top_performer_threshold: float
    metrics: list[PostMetrics] = Field(default_factory=list)
    platform_summaries: dict = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Platform API stubs
# Production: replace with real SDK calls.
# ---------------------------------------------------------------------------

class TwitterAPIStub:
    """
    Stub for Twitter/X API v2.
    Production replacement: tweepy.Client(bearer_token=...).get_tweet(...)
    """

    def __init__(self, config: dict) -> None:
        self.config = config.get("api", {}).get("twitter", {})

    async def get_tweet_metrics(self, post_id: str) -> dict:
        """
        Fetch metrics for a single tweet.

        Real implementation:
            client = tweepy.Client(bearer_token=self.config["bearer_token"])
            tweet = client.get_tweet(
                post_id,
                tweet_fields=["public_metrics", "non_public_metrics"]
            )
            return tweet.data.public_metrics
        """
        await asyncio.sleep(0.05)  # simulate network latency
        impressions = random.randint(500, 15_000)
        likes       = random.randint(5, int(impressions * 0.08))
        retweets    = random.randint(1, max(1, likes // 4))
        replies     = random.randint(0, max(1, likes // 6))
        clicks      = random.randint(10, int(impressions * 0.05))
        engagements = likes + retweets + replies + clicks
        return {
            "impressions": impressions,
            "likes":       likes,
            "retweets":    retweets,
            "replies":     replies,
            "clicks":      clicks,
            "engagement_rate": round(engagements / impressions, 4) if impressions else 0,
        }


class LinkedInAPIStub:
    """
    Stub for LinkedIn Marketing API.
    Production replacement: requests to
        https://api.linkedin.com/v2/organizationalEntityShareStatistics
    """

    def __init__(self, config: dict) -> None:
        self.config = config.get("api", {}).get("linkedin", {})

    async def get_post_metrics(self, post_urn: str) -> dict:
        """
        Fetch organic metrics for a LinkedIn post.

        Real implementation:
            headers = {"Authorization": f"Bearer {self.config['access_token']}"}
            resp = requests.get(
                "https://api.linkedin.com/v2/organizationalEntityShareStatistics",
                params={"q": "organizationalEntity",
                        "organizationalEntity": self.config["organization_id"],
                        "shares[0]": post_urn},
                headers=headers,
            )
            return resp.json()["elements"][0]["totalShareStatistics"]
        """
        await asyncio.sleep(0.05)
        impressions = random.randint(300, 8_000)
        clicks      = random.randint(10, int(impressions * 0.06))
        likes       = random.randint(5, int(impressions * 0.05))
        comments    = random.randint(0, max(1, likes // 5))
        shares      = random.randint(0, max(1, likes // 8))
        engagements = clicks + likes + comments + shares
        return {
            "impressions":    impressions,
            "clicks":         clicks,
            "likes":          likes,
            "comments":       comments,
            "shares":         shares,
            "engagement_rate": round(engagements / impressions, 4) if impressions else 0,
        }


class InstagramAPIStub:
    """
    Stub for Instagram Graph API.
    Production replacement: facebook-sdk or direct Graph API calls to
        https://graph.facebook.com/v18.0/{media-id}/insights
    """

    def __init__(self, config: dict) -> None:
        self.config = config.get("api", {}).get("instagram", {})

    async def get_media_insights(self, media_id: str) -> dict:
        """
        Fetch insights for an Instagram media object.

        Real implementation:
            url = f"https://graph.facebook.com/v18.0/{media_id}/insights"
            params = {
                "metric": "impressions,reach,likes,comments,saved,video_views",
                "access_token": self.config["access_token"],
            }
            resp = requests.get(url, params=params)
            return {m["name"]: m["values"][0]["value"] for m in resp.json()["data"]}
        """
        await asyncio.sleep(0.05)
        impressions = random.randint(400, 20_000)
        reach       = int(impressions * random.uniform(0.6, 0.9))
        likes       = random.randint(10, int(reach * 0.12))
        comments    = random.randint(0, max(1, likes // 7))
        saves       = random.randint(1, max(1, likes // 4))
        engagements = likes + comments + saves
        return {
            "impressions":    impressions,
            "reach":          reach,
            "likes":          likes,
            "comments":       comments,
            "saves":          saves,
            "engagement_rate": round(engagements / reach, 4) if reach else 0,
        }


# ---------------------------------------------------------------------------
# Analytics engine
# ---------------------------------------------------------------------------

STUB_MAP = {
    "twitter":   TwitterAPIStub,
    "linkedin":  LinkedInAPIStub,
    "instagram": InstagramAPIStub,
}


class AnalyticsEngine:
    def __init__(self, config: dict) -> None:
        self.config = config
        self.threshold: float = (
            config.get("analytics", {}).get("top_performer_engagement_rate", 0.05)
        )
        self._stubs = {p: cls(config) for p, cls in STUB_MAP.items()}

    async def _fetch_metrics_for_item(self, item: dict) -> PostMetrics:
        platform = item.get("platform", "unknown")
        item_id  = item.get("item_id", "unknown")

        stub = self._stubs.get(platform)
        if stub is None:
            log.warning("No API stub for platform '%s', skipping.", platform)
            return PostMetrics(item_id=item_id, platform=platform)

        try:
            if platform == "twitter":
                raw = await stub.get_tweet_metrics(item_id)
            elif platform == "linkedin":
                raw = await stub.get_post_metrics(item_id)
            else:
                raw = await stub.get_media_insights(item_id)

            metrics = PostMetrics(
                item_id=item_id,
                platform=platform,
                impressions=raw.get("impressions", 0),
                reach=raw.get("reach", raw.get("impressions", 0)),
                likes=raw.get("likes", 0),
                comments=raw.get("comments", raw.get("replies", 0)),
                shares=raw.get("shares", raw.get("retweets", 0)),
                clicks=raw.get("clicks", 0),
                saves=raw.get("saves", 0),
                engagement_rate=raw.get("engagement_rate", 0.0),
                fetched_at=datetime.now(timezone.utc).isoformat(),
                is_top_performer=raw.get("engagement_rate", 0.0) >= self.threshold,
            )
            log.debug(
                "Fetched %s metrics for item %s — ER: %.2f%%",
                platform, item_id, metrics.engagement_rate * 100,
            )
            return metrics
        except Exception as exc:
            log.error("Failed to fetch metrics for %s/%s: %s", platform, item_id, exc)
            return PostMetrics(item_id=item_id, platform=platform)

    async def fetch_all(
        self,
        calendar: dict,
        platform_filter: Optional[str] = None,
    ) -> list[PostMetrics]:
        items = [
            i for i in calendar.get("items", [])
            if i.get("status") == "published"
            and (platform_filter is None or i.get("platform") == platform_filter)
        ]
        if not items:
            log.warning(
                "No published posts found%s.",
                f" for platform '{platform_filter}'" if platform_filter else "",
            )
        tasks = [self._fetch_metrics_for_item(item) for item in items]
        return await asyncio.gather(*tasks)

    @staticmethod
    def summarise_by_platform(metrics: list[PostMetrics]) -> dict:
        from collections import defaultdict
        groups: dict[str, list[PostMetrics]] = defaultdict(list)
        for m in metrics:
            groups[m.platform].append(m)

        summary = {}
        for platform, items in groups.items():
            ers = [i.engagement_rate for i in items if i.impressions > 0]
            summary[platform] = {
                "post_count":         len(items),
                "total_impressions":  sum(i.impressions for i in items),
                "total_likes":        sum(i.likes for i in items),
                "total_shares":       sum(i.shares for i in items),
                "total_clicks":       sum(i.clicks for i in items),
                "avg_engagement_rate": round(statistics.mean(ers), 4) if ers else 0.0,
                "top_performers":     sum(1 for i in items if i.is_top_performer),
            }
        return summary


# ---------------------------------------------------------------------------
# Rich output helpers
# ---------------------------------------------------------------------------

def display_report(report: AnalyticsReport) -> None:
    console.print()
    console.print(
        Panel(
            f"[bold cyan]Calendar:[/bold cyan]  {report.calendar_file}\n"
            f"[bold cyan]Generated:[/bold cyan] {report.generated_at}\n"
            f"[bold cyan]Posts:[/bold cyan]     {report.total_posts}  "
            f"[bold cyan]Top-performer ER threshold:[/bold cyan] "
            f"{report.top_performer_threshold * 100:.1f}%",
            title="[bold white]CLS Corp — Analytics Report[/bold white]",
            border_style="cyan",
        )
    )

    # Platform summaries
    if report.platform_summaries:
        summary_table = Table(title="Platform Summary", show_lines=True)
        summary_table.add_column("Platform",        style="bold")
        summary_table.add_column("Posts",           justify="right")
        summary_table.add_column("Impressions",     justify="right")
        summary_table.add_column("Likes",           justify="right")
        summary_table.add_column("Shares/RT",       justify="right")
        summary_table.add_column("Clicks",          justify="right")
        summary_table.add_column("Avg ER",          justify="right")
        summary_table.add_column("Top Performers",  justify="right")

        colors = {"twitter": "cyan", "linkedin": "blue", "instagram": "magenta"}
        for platform, s in sorted(report.platform_summaries.items()):
            c = colors.get(platform, "white")
            er = s["avg_engagement_rate"] * 100
            er_color = "green" if er >= report.top_performer_threshold * 100 else "yellow"
            summary_table.add_row(
                f"[{c}]{platform.upper()}[/{c}]",
                str(s["post_count"]),
                f"{s['total_impressions']:,}",
                f"{s['total_likes']:,}",
                f"{s['total_shares']:,}",
                f"{s['total_clicks']:,}",
                f"[{er_color}]{er:.2f}%[/{er_color}]",
                f"[green]{s['top_performers']}[/green]",
            )
        console.print(summary_table)

    # Per-post details
    if report.metrics:
        detail_table = Table(title="Post Detail", show_lines=True)
        detail_table.add_column("ID",     width=10)
        detail_table.add_column("Platform", width=12)
        detail_table.add_column("Impr.",  justify="right")
        detail_table.add_column("Likes",  justify="right")
        detail_table.add_column("Shares", justify="right")
        detail_table.add_column("Clicks", justify="right")
        detail_table.add_column("ER",     justify="right")
        detail_table.add_column("★",      justify="center", width=4)

        colors = {"twitter": "cyan", "linkedin": "blue", "instagram": "magenta"}
        for m in sorted(report.metrics, key=lambda x: -x.engagement_rate):
            c = colors.get(m.platform, "white")
            er = m.engagement_rate * 100
            er_color = "green" if m.is_top_performer else "yellow"
            star = "[bold yellow]★[/bold yellow]" if m.is_top_performer else ""
            detail_table.add_row(
                m.item_id,
                f"[{c}]{m.platform}[/{c}]",
                f"{m.impressions:,}",
                f"{m.likes:,}",
                f"{m.shares:,}",
                f"{m.clicks:,}",
                f"[{er_color}]{er:.2f}%[/{er_color}]",
                star,
            )
        console.print(detail_table)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.group()
@click.option("--config", "config_path", default=None)
@click.option("--log-level", default="INFO")
@click.pass_context
def cli(ctx: click.Context, config_path: Optional[str], log_level: str) -> None:
    """CLS Corp Analytics — measure content performance across platforms."""
    ctx.ensure_object(dict)
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )
    ctx.obj["config"] = load_config(config_path)


@cli.command()
@click.argument("calendar_file", type=click.Path(exists=True))
@click.option("--platform", default=None, type=click.Choice(["twitter", "linkedin", "instagram"]))
@click.option("--output", "-o", default=None, help="Save report to this JSON path.")
@click.pass_context
def report(
    ctx: click.Context,
    calendar_file: str,
    platform: Optional[str],
    output: Optional[str],
) -> None:
    """Fetch metrics for published posts in a calendar and display a report."""
    config = ctx.obj["config"]

    with open(calendar_file) as f:
        calendar = json.load(f)

    engine = AnalyticsEngine(config)

    # Mark all pending posts as published for demo purposes
    for item in calendar.get("items", []):
        if item.get("status") == "pending":
            item["status"] = "published"

    metrics = asyncio.run(engine.fetch_all(calendar, platform_filter=platform))
    summary = AnalyticsEngine.summarise_by_platform(metrics)

    analytics_report = AnalyticsReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        calendar_file=calendar_file,
        platform_filter=platform,
        total_posts=len(metrics),
        top_performer_threshold=engine.threshold,
        metrics=metrics,
        platform_summaries=summary,
    )

    display_report(analytics_report)

    if output:
        out_path = Path(output)
        with out_path.open("w") as f:
            json.dump(analytics_report.model_dump(), f, indent=2)
        console.print(f"\n[bold green]✓[/bold green] Report saved to [cyan]{out_path}[/cyan]")


@cli.command()
@click.argument("bundle_files", nargs=-1, required=True, type=click.Path(exists=True))
@click.pass_context
def compare(ctx: click.Context, bundle_files: tuple[str, ...]) -> None:
    """
    Compare the estimated performance potential of multiple ContentBundle files
    based on keyword density, hashtag count, and content length scores.
    """
    rows = []
    for file_path in bundle_files:
        with open(file_path) as f:
            bundle = json.load(f)
        kw_score   = min(len(bundle.get("seo_keywords", [])) / 10, 1.0)
        ht_score   = min(len(bundle.get("hashtags", [])) / 20, 1.0)
        post_lens  = [len(p.get("content", "")) for p in bundle.get("social_posts", [])]
        avg_len    = sum(post_lens) / len(post_lens) if post_lens else 0
        len_score  = min(avg_len / 280, 1.0)
        total      = round((kw_score * 0.4 + ht_score * 0.3 + len_score * 0.3) * 100, 1)
        rows.append((Path(file_path).name, bundle.get("run_id", "?"), kw_score * 100, ht_score * 100, total))

    t = Table(title="Content Bundle Comparison", show_lines=True)
    t.add_column("File")
    t.add_column("Run ID", width=10)
    t.add_column("KW Score", justify="right")
    t.add_column("HT Score", justify="right")
    t.add_column("Overall",  justify="right")

    for name, run_id, kw, ht, total in sorted(rows, key=lambda r: -r[4]):
        color = "green" if total >= 70 else ("yellow" if total >= 50 else "red")
        t.add_row(name, run_id, f"{kw:.0f}%", f"{ht:.0f}%", f"[{color}]{total}%[/{color}]")
    console.print(t)


if __name__ == "__main__":
    cli()
