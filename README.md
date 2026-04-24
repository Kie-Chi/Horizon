<div align="center">

# 🌅 Horizon

**Enjoy the News itself. Leave others to Horizon**

[![Python](https://img.shields.io/badge/python-3.11+-blue.svg?style=flat-square&logo=python&logoColor=white)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg?style=flat-square)](LICENSE)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json&style=flat-square)](https://github.com/astral-sh/uv)
[![Daily Summary](https://github.com/Thysrael/Horizon/actions/workflows/deploy-docs.yml/badge.svg?style=flat-square)](https://thysrael.github.io/Horizon/)
[![GitHub commit activity](https://img.shields.io/github/commit-activity/m/Thysrael/Horizon?style=flat-square)](https://github.com/Thysrael/Horizon/commits/main)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)](http://makeapullrequest.com)
![Sources Welcome](https://img.shields.io/badge/📡_sources-welcome-f97316?style=flat-square)
<br>

![Claude](https://img.shields.io/badge/Claude-f0daba?style=flat-square&logo=anthropic&logoColor=black)
![GPT](https://img.shields.io/badge/GPT-412991?style=flat-square)
![Gemini](https://img.shields.io/badge/Gemini-8E75B2?style=flat-square&logo=google&logoColor=white)
![DeepSeek](https://img.shields.io/badge/DeepSeek-0A6DC2?style=flat-square)
![Doubao](https://img.shields.io/badge/Doubao-00D6C2?style=flat-square)
![MiniMax](https://img.shields.io/badge/MiniMax-FF6F00?style=flat-square)
![OpenClaw](https://img.shields.io/badge/OpenClaw-C83232?style=flat-square)

Horizon collects news from multiple customizable sources, uses AI to score and filter them, and generates a daily briefing — complete with summaries, community discussions, and background explanations in both English and Chinese.

[📖 Live Demo](https://thysrael.github.io/Horizon/) · [📋 Configuration Guide](https://thysrael.github.io/Horizon/configuration) · [简体中文](README_zh.md)

</div>

## Screenshots

<table>
<tr>
<td width="50%">
<p align="center"><strong>Daily Overview</strong></p>
<img src="docs/assets/overview_en.png" alt="Daily Overview" />
</td>
<td width="50%">
<p align="center"><strong>News Detail</strong></p>
<img src="docs/assets/one_news_en.png" alt="News Detail" />
</td>
</tr>
</table>

<details>
<summary><strong>Terminal Output</strong></summary>
<br>
<p align="center">
  <img src="docs/assets/terminal_log.png" alt="Terminal Output" width="400" />
</p>
</details>

## Features

> **🤝 Beyond a Cold AI Agent:** Designed with a "human touch". Build your personalized newsroom by deeply customizing feeds and sharing high-quality information sources via the [Horizon site](https://horizon1123.top/).

- **📡 Multi-Source Aggregation** — Collects from Hacker News, RSS feeds, Reddit, Telegram channels, and GitHub (releases & user events)
- **🤖 AI-Powered Scoring** — Uses Claude, GPT-4, Gemini, DeepSeek, Doubao, MiniMax, or any OpenAI-compatible API to rate each item 0-10, filtering out the noise
- **🌐 Bilingual Summaries** — Generates daily reports in both English and Chinese
- **🔍 Content Enrichment** — Searches the web to provide background knowledge for unfamiliar concepts
- **💬 Community Voices** — Collects and summarizes discussions from comments on HackerNews, Reddit, etc.
- **🔗 Cross-Source Deduplication** — Merges duplicate items from different platforms automatically
- **📧 Email Subscription** — Self-hosted newsletter system (SMTP/IMAP) that handles "Subscribe" requests automatically
- **🔔 Webhook Notification** — Send results to Feishu, Slack, Discord, or any webhook endpoint with template rendering and truncation
- **📝 Static Site Generation** — Deploys as a GitHub Pages site via GitHub Actions, updated on a schedule
- **⚙️ Fully Configurable** — Single JSON config file, easy to customize sources, thresholds, and AI providers
- **🧙 Setup Wizard** — Interactive CLI that recommends sources based on your interests.

## How It Works

```
              ┌──────────┐
              │ Hacker   │
┌─────────┐   │ News     │   ┌──────────┐   ┌──────────┐   ┌──────────┐
│  RSS    │──▶│ Reddit   │──▶│ AI Score │──▶│ Enrich   │──▶│ Summary  │
│ Telegram│   │ GitHub   │   │ & Filter │   │ & Search │   │ & Deploy │
└─────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘
  Fetch from      Merge &        Score          Web search     Generate
  all sources    deduplicate     0-10 each      background     Markdown &
                                & filter        knowledge      deploy site
```

1. **Fetch** — Pull latest content from all configured sources concurrently
2. **Deduplicate** — Merge items pointing to the same URL across different platforms
3. **Score** — AI rates each item 0-10 based on technical depth, novelty, and impact
4. **Filter** — Keep only items above your configured threshold (default: 6.0)
5. **Enrich** — For high-scoring items, search the web for background context and collect community discussions
6. **Summarize** — Generate a structured Markdown report with summaries, tags, and references
7. **Deploy** — Optionally publish to GitHub Pages as a daily-updated static site

## Quick Start

### 1. Install

**Option A: Local Installation**

```bash
git clone https://github.com/Thysrael/Horizon.git
cd horizon

# Install with uv (recommended)
uv sync

# Or with pip
pip install -e .
```

**Option B: Docker**

```bash
git clone https://github.com/Thysrael/Horizon.git
cd horizon

# Configure environment
cp .env.example .env
cp data/config.example.json data/config.json
# Edit .env and data/config.json with your API keys and preferences

# Run with Docker Compose
docker-compose run --rm horizon

# Or run with custom time window
docker-compose run --rm horizon --hours 48
```

### 2. Configure

**Option A: Interactive wizard (recommended)**

```bash
uv run horizon-wizard
```

The wizard asks about your interests (e.g. "LLM inference", "嵌入式", "web security") and auto-generates `data/config.json`.

**Option B: Manual configuration**

```bash
cp .env.example .env          # Add your API keys
cp data/config.example.json data/config.json  # Customize your sources
```

Here's what a config looks like:

```jsonc
{
  "ai": {
    "provider": "openai",       // or "anthropic", "gemini", "doubao", "minimax"
    "model": "gpt-4",
    "api_key_env": "OPENAI_API_KEY",
    "languages": ["en", "zh"]   // bilingual output
  },
  "sources": {
    "hackernews": { "enabled": true, "fetch_top_stories": 20, "min_score": 100 },
    "rss": [
      { "name": "Simon Willison", "url": "https://simonwillison.net/atom/everything/" }
    ],
    "reddit": {
      "subreddits": [{ "subreddit": "MachineLearning", "sort": "hot" }],
      "fetch_comments": 5
    },
    "telegram": {
      "channels": [{ "channel": "zaihuapd", "fetch_limit": 20 }]
    }
  },
  "filtering": {
    "ai_score_threshold": 6.0,
    "time_window_hours": 24
  }
}
```

For the full reference, see the [Configuration Guide](docs/configuration.md).

### 3. Run

#### Local Installation

```bash
uv run horizon           # Run with default 24h window
uv run horizon --hours 48  # Fetch from last 48 hours
```

#### With Docker

```bash
docker-compose run --rm horizon           # Run with default 24h window
docker-compose run --rm horizon --hours 48  # Fetch from last 48 hours
```

The generated report will be saved to `data/summaries/`.

### Webhook Notification

Horizon can push results to any webhook endpoint (Feishu/Lark, Slack, Discord, custom APIs, etc.) when the pipeline completes — both on success and failure.

**Configuration:**

```jsonc
{
  "webhook": {
    "enabled": true,
    "url_env": "HORIZON_WEBHOOK_URL",  // Set this env var to your webhook URL
    "request_body": {   // Use a real JSON dict (recommended) or a string
      "msg_type": "interactive",
      "card": {
        "header": {"title": {"tag": "plain_text", "content": "Horizon #{date}"}},
        "elements": [
          {"tag": "markdown", "content": "#{summary?limit=3000&split=---}"}
        ]
      }
    },
    "headers": ""       // Optional: "Authorization: Bearer xxx" per line
  }
}
```

**Template variables:**

| Variable | Description |
|----------|-------------|
| `#{date}` | Report date (e.g. `2026-04-24`) |
| `#{language}` | Language code (`en` or `zh`) |
| `#{important_items}` | Number of items that passed the score threshold |
| `#{all_items}` | Total number of fetched items |
| `#{result}` | `success` or `failed` |
| `#{timestamp}` | Unix timestamp |
| `#{summary}` | Full summary markdown |

**Parameterized syntax:** `#{key?limit=N&split=DELIM}` truncates long values by splitting on `DELIM` and keeping segments until the total character count reaches `N`. Useful for platforms with message length limits.

Example: `#{summary?limit=3000&split=---}` keeps enough `---`-separated sections to stay under 3000 characters.

When `request_body` is a dict, special characters in `#{summary}` (quotes, newlines) are safely handled via JSON serialization. When it's a string, use `#{summary}` only with safe content (no unescaped quotes).

### 4. Automate (Optional)

Horizon works great as a **GitHub Actions** cron job. See [`.github/workflows/daily-summary.yml`](.github/workflows/daily-summary.yml) for a ready-to-use workflow that generates and deploys your daily briefing to GitHub Pages automatically.

## Supported Sources

| Source | What it fetches | Comments |
|--------|----------------|----------|
| **Hacker News** | Top stories by score | Yes (top N comments) |
| **RSS / Atom** | Any RSS or Atom feed | — |
| **Reddit** | Subreddits + user posts | Yes (top N comments) |
| **Telegram** | Public channel messages | — |
| **GitHub** | User events & repo releases | — |

## MCP Integration

Horizon ships with a built-in [MCP](https://modelcontextprotocol.io/) server so AI assistants can drive the pipeline programmatically.

```bash
# Start the MCP server (stdio mode)
uv run horizon-mcp
```

Available tools include `hz_validate_config`, `hz_fetch_items`, `hz_score_items`, `hz_filter_items`, `hz_enrich_items`, `hz_generate_summary`, and `hz_run_pipeline`.

See [`src/mcp/README.md`](src/mcp/README.md) for the full tool reference and [`src/mcp/integration.md`](src/mcp/integration.md) for client setup.

## Roadmap

- [x] Multi-source aggregation (HN, RSS, Reddit, Telegram, GitHub)
- [x] AI scoring with multiple providers
- [x] Bilingual summary generation (EN/ZH)
- [x] Web search for background enrichment
- [x] Community discussion collection
- [x] GitHub Pages deployment
- [x] **Email Subscription** (SMTP/IMAP automated newsletter)
- [x] **Docker deployment support**
- [x] **MCP server integration**
- [x] Web UI dashboard
- [x] **Setup Wizard** — interactive CLI that recommends sources based on user interests
- [x] **Webhook Notification** — push results to Feishu, Slack, Discord, or any webhook endpoint
- [X] **Improved Web UI** — better digest and article detail experience
- [ ] More source types (Twitter/X, Discord, etc.)
- [ ] Custom scoring prompts per source

## Contributing

Contributions are welcome! Feel free to open issues or submit pull requests.

### Contribute News

Want to share valuable updates with the Horizon community? Please contribute news directly at **[horizon1123.top](https://horizon1123.top)**.

Great candidates: niche RSS discoveries, active subreddit trends, notable GitHub updates, or Telegram channel highlights in your area of expertise.

## Acknowledgements

- Special thanks to [LINUX.DO](https://linux.do/) for providing a promotion platform.
- Special thanks to [HelloGitHub](https://hellogithub.com/) for valuable guidance and suggestions.

## License

[MIT](LICENSE)
