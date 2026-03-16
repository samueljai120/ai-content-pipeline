#!/usr/bin/env python3
"""
CLS Corp AI Content Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Transforms a URL or topic into a full suite of platform-optimised content
using DeepSeek V3 via OpenRouter, then persists results as structured JSON.

Usage:
    python pipeline.py generate --topic "AI agents in 2025"
    python pipeline.py generate --url  https://example.com/article
    python pipeline.py generate --topic "LLMs" --output results/llm_content.json
    python pipeline.py batch   --input  topics.txt
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import click
import httpx
import yaml
from pydantic import BaseModel, Field, field_validator
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

console = Console()

def configure_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    handlers: list[logging.Handler] = [
        RichHandler(console=console, rich_tracebacks=True, markup=True)
    ]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(message)s",
        datefmt="[%X]",
        handlers=handlers,
    )

log = logging.getLogger("cls.pipeline")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CONFIG_PATHS = ["config.local.yaml", "config.yaml"]

def load_config(path: Optional[str] = None) -> dict:
    candidates = [path] if path else CONFIG_PATHS
    for candidate in candidates:
        p = Path(candidate)
        if p.exists():
            with p.open() as f:
                cfg = yaml.safe_load(f)
            log.debug("Loaded config from %s", p)
            return cfg
    raise FileNotFoundError(
        f"No config file found. Expected one of: {', '.join(CONFIG_PATHS)}"
    )

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class SocialPost(BaseModel):
    platform: str
    content: str
    char_count: int = 0
    hashtags_included: bool = False

    def model_post_init(self, __context) -> None:  # noqa: ANN001
        self.char_count = len(self.content)


class ContentBundle(BaseModel):
    """Full content bundle produced by one pipeline run."""

    run_id: str
    source: str                            # URL or topic string
    source_type: str                       # "url" | "topic"
    generated_at: str
    model: str

    blog_summary: str
    social_posts: list[SocialPost]
    seo_keywords: list[str]
    hashtags: list[str]

    # Raw token usage reported by the API
    usage: dict = Field(default_factory=dict)

    @field_validator("social_posts")
    @classmethod
    def at_least_one_post(cls, v: list[SocialPost]) -> list[SocialPost]:
        if not v:
            raise ValueError("social_posts must not be empty")
        return v


# ---------------------------------------------------------------------------
# OpenRouter / DeepSeek client
# ---------------------------------------------------------------------------

class OpenRouterClient:
    """Async HTTP client for the OpenRouter chat-completions endpoint."""

    def __init__(self, api_key: str, base_url: str, model: str, timeout: float = 120.0) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "HTTP-Referer": "https://clscorp.ai",
                "X-Title": "CLS Corp Content Pipeline",
                "Content-Type": "application/json",
            },
            timeout=timeout,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    async def chat(self, messages: list[dict], temperature: float = 0.7) -> dict:
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "response_format": {"type": "json_object"},
        }
        resp = await self._client.post("/chat/completions", json=payload)
        resp.raise_for_status()
        return resp.json()

    async def __aenter__(self) -> "OpenRouterClient":
        return self

    async def __aexit__(self, *_) -> None:
        await self.aclose()


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are an expert AI content strategist for {brand_name}, specialising in \
{industry}. Your brand voice is: {brand_voice}. \
Your primary audience: {target_audience}.

Always respond with a single, valid JSON object — no markdown fences, \
no prose outside the JSON. Follow the schema exactly.
"""

CONTENT_SCHEMA = """
{
  "blog_summary":   "<2-4 paragraph executive summary optimised for readability>",
  "twitter_post":   "<tweet ≤280 chars, punchy hook, no hashtags inline>",
  "linkedin_post":  "<professional LinkedIn post 150-300 words, storytelling arc>",
  "instagram_post": "<visual-first caption 80-150 words, emoji-friendly>",
  "seo_keywords":   ["<keyword>", "..."],   // 8-12 high-intent keywords
  "hashtags":       ["<#tag>", "..."]       // 10-20 relevant hashtags
}
"""

def build_messages(source: str, source_type: str, config: dict) -> list[dict]:
    brand = config.get("content", {})
    system = SYSTEM_PROMPT.format(
        brand_name=brand.get("brand_name", "CLS Corp"),
        industry=brand.get("industry", "AI & Automation"),
        brand_voice=brand.get("brand_voice", "authoritative yet approachable"),
        target_audience=brand.get("target_audience", "technology leaders"),
    )

    if source_type == "url":
        user_msg = (
            f"Analyse the article at this URL and generate content for all channels:\n"
            f"URL: {source}\n\n"
            f"Output schema:\n{CONTENT_SCHEMA}"
        )
    else:
        user_msg = (
            f"Generate multi-channel marketing content about this topic:\n"
            f"TOPIC: {source}\n\n"
            f"Output schema:\n{CONTENT_SCHEMA}"
        )

    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user_msg},
    ]


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------

class ContentPipeline:
    def __init__(self, config: dict) -> None:
        self.config = config
        api_cfg = config.get("api", {})
        self._client = OpenRouterClient(
            api_key=api_cfg["openrouter_api_key"],
            base_url=api_cfg.get("openrouter_base_url", "https://openrouter.ai/api/v1"),
            model=api_cfg.get("openrouter_model", "deepseek/deepseek-chat"),
        )
        constraints = config.get("content", {}).get("constraints", {})
        self._twitter_limit = constraints.get("twitter_char_limit", 280)

    async def _call_ai(self, source: str, source_type: str) -> tuple[dict, dict]:
        """Call the AI and return (parsed_content, usage)."""
        messages = build_messages(source, source_type, self.config)
        log.debug("Sending request to model %s", self._client.model)
        t0 = time.perf_counter()
        response = await self._client.chat(messages)
        elapsed = time.perf_counter() - t0
        log.debug("AI responded in %.2fs", elapsed)

        raw_content = response["choices"][0]["message"]["content"]
        usage = response.get("usage", {})

        try:
            parsed = json.loads(raw_content)
        except json.JSONDecodeError as exc:
            log.error("AI returned invalid JSON: %s", raw_content[:200])
            raise ValueError("AI response was not valid JSON") from exc

        return parsed, usage

    def _validate_and_trim(self, data: dict) -> dict:
        """Enforce character limits and clean up the AI output."""
        tw = data.get("twitter_post", "")
        if len(tw) > self._twitter_limit:
            data["twitter_post"] = tw[: self._twitter_limit - 1] + "…"
            log.warning("Twitter post trimmed to %d chars", self._twitter_limit)

        keywords = data.get("seo_keywords", [])
        if isinstance(keywords, list):
            data["seo_keywords"] = [str(k).lower().strip() for k in keywords]

        hashtags = data.get("hashtags", [])
        if isinstance(hashtags, list):
            data["hashtags"] = [
                f"#{h.lstrip('#')}" for h in hashtags if h
            ]

        return data

    async def run(self, source: str, source_type: str) -> ContentBundle:
        """Execute the full pipeline and return a ContentBundle."""
        import uuid
        run_id = str(uuid.uuid4())[:8]
        log.info("Pipeline run [bold]%s[/bold] started — source: %s", run_id, source)

        data, usage = await self._call_ai(source, source_type)
        data = self._validate_and_trim(data)

        social_posts = [
            SocialPost(platform="twitter",   content=data.get("twitter_post", "")),
            SocialPost(platform="linkedin",  content=data.get("linkedin_post", "")),
            SocialPost(platform="instagram", content=data.get("instagram_post", "")),
        ]

        bundle = ContentBundle(
            run_id=run_id,
            source=source,
            source_type=source_type,
            generated_at=datetime.now(timezone.utc).isoformat(),
            model=self._client.model,
            blog_summary=data.get("blog_summary", ""),
            social_posts=social_posts,
            seo_keywords=data.get("seo_keywords", []),
            hashtags=data.get("hashtags", []),
            usage=usage,
        )

        log.info(
            "Run [bold]%s[/bold] complete — %d tokens used",
            run_id,
            usage.get("total_tokens", 0),
        )
        return bundle

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "ContentPipeline":
        return self

    async def __aexit__(self, *_) -> None:
        await self.aclose()


# ---------------------------------------------------------------------------
# Rich output helpers
# ---------------------------------------------------------------------------

def display_bundle(bundle: ContentBundle) -> None:
    console.print()
    console.print(
        Panel(
            f"[bold cyan]Run ID:[/bold cyan] {bundle.run_id}\n"
            f"[bold cyan]Source:[/bold cyan] {bundle.source}\n"
            f"[bold cyan]Model:[/bold cyan]  {bundle.model}\n"
            f"[bold cyan]Generated:[/bold cyan] {bundle.generated_at}",
            title="[bold white]CLS Corp Content Pipeline — Results[/bold white]",
            border_style="cyan",
        )
    )

    console.rule("[bold]Blog Summary[/bold]")
    console.print(bundle.blog_summary)

    console.rule("[bold]Social Media Posts[/bold]")
    for post in bundle.social_posts:
        console.print(
            Panel(
                post.content,
                title=f"[bold]{post.platform.upper()}[/bold]  "
                      f"[dim]({post.char_count} chars)[/dim]",
                border_style="green",
            )
        )

    kw_table = Table(title="SEO Keywords", show_header=False, box=None)
    kw_table.add_column(style="bold yellow")
    for kw in bundle.seo_keywords:
        kw_table.add_row(kw)
    console.print(kw_table)

    console.print(
        Panel(
            "  ".join(bundle.hashtags),
            title="[bold]Hashtags[/bold]",
            border_style="magenta",
        )
    )

    if bundle.usage:
        console.print(
            f"\n[dim]Tokens — prompt: {bundle.usage.get('prompt_tokens', '?')}  "
            f"completion: {bundle.usage.get('completion_tokens', '?')}  "
            f"total: {bundle.usage.get('total_tokens', '?')}[/dim]"
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.group()
@click.option("--config", "config_path", default=None, help="Path to YAML config file.")
@click.option("--log-level", default=None, help="Override log level (DEBUG/INFO/WARNING/ERROR).")
@click.pass_context
def cli(ctx: click.Context, config_path: Optional[str], log_level: Optional[str]) -> None:
    """CLS Corp AI Content Pipeline — generate multi-channel content with DeepSeek."""
    ctx.ensure_object(dict)
    cfg = load_config(config_path)
    log_cfg = cfg.get("logging", {})
    configure_logging(
        level=log_level or log_cfg.get("level", "INFO"),
        log_file=log_cfg.get("file"),
    )
    ctx.obj["config"] = cfg


@cli.command()
@click.option("--topic", default=None, help="Topic or headline to generate content about.")
@click.option("--url",   default=None, help="URL of an article to summarise and repurpose.")
@click.option(
    "--output", "-o",
    default=None,
    help="Path to write the JSON output. Defaults to results/<run_id>.json.",
)
@click.pass_context
def generate(ctx: click.Context, topic: Optional[str], url: Optional[str], output: Optional[str]) -> None:
    """Generate a full content bundle from a topic or URL."""
    if not topic and not url:
        raise click.UsageError("Provide either --topic or --url.")
    if topic and url:
        raise click.UsageError("Provide either --topic or --url, not both.")

    source = url or topic
    source_type = "url" if url else "topic"

    if url:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            raise click.BadParameter(f"'{url}' does not look like a valid URL.", param_hint="--url")

    config = ctx.obj["config"]
    api_key = config.get("api", {}).get("openrouter_api_key", "")
    if not api_key or api_key.startswith("sk-or-REPLACE"):
        console.print(
            "[bold red]Error:[/bold red] Set a valid openrouter_api_key in config.yaml "
            "(or config.local.yaml)."
        )
        sys.exit(1)

    async def _run() -> ContentBundle:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            progress.add_task("Generating content with DeepSeek…", total=None)
            async with ContentPipeline(config) as pipeline:
                return await pipeline.run(source, source_type)

    bundle = asyncio.run(_run())
    display_bundle(bundle)

    # Persist output
    out_path = Path(output) if output else Path("results") / f"{bundle.run_id}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as f:
        json.dump(bundle.model_dump(), f, indent=2)
    console.print(f"\n[bold green]✓[/bold green] Saved to [cyan]{out_path}[/cyan]")


@cli.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.option("--output-dir", "-o", default="results", help="Directory for JSON outputs.")
@click.option("--concurrency", "-c", default=3, show_default=True, help="Max parallel requests.")
@click.pass_context
def batch(ctx: click.Context, input_file: str, output_dir: str, concurrency: int) -> None:
    """
    Batch-generate content for multiple topics or URLs from a text file (one per line).

    Lines starting with '#' and blank lines are ignored.
    """
    config = ctx.obj["config"]
    lines = Path(input_file).read_text().splitlines()
    sources = [l.strip() for l in lines if l.strip() and not l.startswith("#")]

    if not sources:
        console.print("[yellow]No topics found in input file.[/yellow]")
        return

    console.print(f"[bold]Batch mode:[/bold] {len(sources)} items, concurrency={concurrency}")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    semaphore = asyncio.Semaphore(concurrency)

    async def _process(pipeline: ContentPipeline, source: str, idx: int) -> None:
        src_type = "url" if source.startswith("http") else "topic"
        async with semaphore:
            try:
                bundle = await pipeline.run(source, src_type)
                out_path = out_dir / f"{idx:03d}_{bundle.run_id}.json"
                with out_path.open("w") as f:
                    json.dump(bundle.model_dump(), f, indent=2)
                console.print(f"[green]✓[/green] [{idx+1}/{len(sources)}] {source[:60]}")
            except Exception as exc:
                console.print(f"[red]✗[/red] [{idx+1}/{len(sources)}] {source[:60]} — {exc}")

    async def _run_all() -> None:
        async with ContentPipeline(config) as pipeline:
            tasks = [_process(pipeline, src, i) for i, src in enumerate(sources)]
            await asyncio.gather(*tasks)

    asyncio.run(_run_all())
    console.print(f"\n[bold green]Batch complete.[/bold green] Results in [cyan]{out_dir}/[/cyan]")


if __name__ == "__main__":
    cli()
