# CLS Corp AI Content Pipeline

> **AI-powered content automation** — from a URL or topic to a fully scheduled, cross-platform content calendar in seconds.

Built by **[CLS Corp](https://clscorp.ai)** — AI & Automation Agency

---

## Overview

The CLS Corp Content Pipeline is a production-grade Python toolkit that uses **DeepSeek V3** (via OpenRouter) to transform any topic or article URL into:

- A polished **blog post summary**
- Platform-optimised posts for **Twitter/X**, **LinkedIn**, and **Instagram**
- High-intent **SEO keywords**
- Curated **hashtag sets**

It then schedules each post to an algorithmically optimal time window and tracks real engagement metrics via platform APIs.

```
Topic / URL
    │
    ▼
┌───────────────────────────────────────────────────────┐
│  pipeline.py  ←  DeepSeek V3 (OpenRouter)            │
│  • Blog summary        • Twitter post                 │
│  • LinkedIn post       • Instagram post               │
│  • SEO keywords        • Hashtags                     │
└──────────────────────┬────────────────────────────────┘
                       │  ContentBundle JSON
                       ▼
┌───────────────────────────────────────────────────────┐
│  scheduler.py  ←  Optimal-window algorithm            │
│  • Weekday/weekend windows per platform               │
│  • Min-gap enforcement                                │
│  • Human-readable calendar + JSON output              │
└──────────────────────┬────────────────────────────────┘
                       │  ContentCalendar JSON
                       ▼
┌───────────────────────────────────────────────────────┐
│  analytics.py  ←  Twitter / LinkedIn / Instagram APIs │
│  • Impressions, ER, clicks, shares                    │
│  • Top-performer flagging                             │
│  • Bundle comparison scoring                          │
└───────────────────────────────────────────────────────┘
```

---

## Features

| Feature | Details |
|---|---|
| **AI model** | DeepSeek V3 via OpenRouter — cost-effective, GPT-4-class |
| **Async-first** | `asyncio` + `httpx` — non-blocking API calls, batch concurrency control |
| **Retry logic** | Exponential back-off via `tenacity` for resilient API calls |
| **Pydantic v2** | Strict runtime validation on every model boundary |
| **Rich CLI** | Spinner progress, colour-coded tables, branded panels |
| **JSON-schema output** | Every artefact is a typed, versioned JSON file |
| **Pluggable analytics** | Real API stubs ready to swap for live platform credentials |
| **YAML config** | All behaviour controlled via a single config file |

---

## Quick Start

### 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure

```bash
cp config.yaml config.local.yaml
# Edit config.local.yaml — add your OpenRouter API key at minimum:
#   api.openrouter_api_key: "sk-or-..."
```

Get an OpenRouter key at [openrouter.ai/keys](https://openrouter.ai/keys).

### 3. Generate content

```bash
# From a topic
python pipeline.py generate --topic "How AI agents are reshaping B2B sales in 2025"

# From a URL
python pipeline.py generate --url https://techcrunch.com/some-article

# Save to a specific file
python pipeline.py generate --topic "Generative AI ROI" --output results/gen_ai_roi.json
```

### 4. Schedule

```bash
# Schedule all posts in a bundle
python scheduler.py schedule results/abc12345.json

# Schedule multiple bundles with a 14-day lookahead
python scheduler.py schedule results/*.json --lookahead 14 --output calendar.json

# View an existing calendar
python scheduler.py view calendar.json
```

### 5. Analytics

```bash
# Full performance report
python analytics.py report calendar.json

# Filter to one platform
python analytics.py report calendar.json --platform linkedin

# Save report to JSON
python analytics.py report calendar.json --output analytics_report.json

# Compare multiple bundles
python analytics.py compare results/*.json
```

---

## Batch Processing

Generate content for hundreds of topics in one command:

```bash
# topics.txt — one topic or URL per line, # for comments
cat > topics.txt <<'EOF'
# Q2 Content Plan
AI agents in enterprise software
The ROI of marketing automation
https://example.com/article-to-repurpose
Prompt engineering best practices
EOF

python pipeline.py batch topics.txt --output-dir results/ --concurrency 5
```

---

## Configuration Reference

| Key | Default | Description |
|---|---|---|
| `api.openrouter_api_key` | — | **Required.** Your OpenRouter API key |
| `api.openrouter_model` | `deepseek/deepseek-chat` | Any OpenRouter-compatible model |
| `content.brand_voice` | `authoritative yet approachable` | Injected into every system prompt |
| `content.target_audience` | `B2B decision-makers` | Shapes copy tone and terminology |
| `scheduling.timezone` | `America/New_York` | IANA timezone for the posting calendar |
| `scheduling.lookahead_days` | `7` | How many days ahead to schedule |
| `scheduling.min_gap_minutes` | `60` | Minimum gap between same-platform posts |
| `analytics.top_performer_engagement_rate` | `0.05` | ER threshold to flag top performers |
| `logging.level` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |

---

## Project Structure

```
content-pipeline/
├── pipeline.py        # AI content generation (DeepSeek via OpenRouter)
├── scheduler.py       # Optimal-window posting calendar
├── analytics.py       # Platform metrics & reporting
├── config.yaml        # Reference configuration (no secrets)
├── config.local.yaml  # Your credentials (git-ignored)
├── requirements.txt   # Python dependencies
├── results/           # Generated ContentBundle JSON files
└── pipeline.log       # Runtime log
```

---

## Adding Live Platform Credentials

Each analytics stub is documented with the exact production replacement code.
Search for `Real implementation:` comments in `analytics.py`.

```python
# Example — replacing the Twitter stub with tweepy:
client = tweepy.Client(bearer_token=self.config["bearer_token"])
tweet  = client.get_tweet(post_id, tweet_fields=["public_metrics"])
return tweet.data.public_metrics
```

---

## Extending the Pipeline

**Custom platforms** — add a new key under `scheduling.optimal_windows` in `config.yaml` and a corresponding stub class in `analytics.py`.

**Custom models** — change `api.openrouter_model` to any model available on OpenRouter (e.g. `openai/gpt-4o`, `anthropic/claude-3.5-sonnet`).

**CI/CD integration** — the pipeline exits with code `0` on success and `1` on error, making it trivially wrappable in GitHub Actions or any scheduler.

---

## License

Copyright © 2025 CLS Corp. All rights reserved.

---

*Built with DeepSeek V3 · OpenRouter · Python 3.11+ · Rich · Pydantic v2*
