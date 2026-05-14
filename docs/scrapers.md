---
layout: default
title: Source Scrapers
---

# Source Scrapers

Horizon fetches content from multiple source types. All scrapers inherit from `BaseScraper`, share an async HTTP client, and implement a `fetch(since)` method that returns a list of `ContentItem` objects. Sources are fetched concurrently via `asyncio.gather`.

## Hacker News

**File**: `src/scrapers/hackernews.py`

Uses the [Firebase HN API](https://hacker-news.firebaseio.com/v0):

- `GET /topstories.json` — fetches top story IDs
- `GET /item/{id}.json` — fetches story/comment details

Stories and their comments are fetched concurrently. For each story, the top 5 comments are included (deleted/dead comments excluded, HTML stripped, truncated at 500 chars).

**Config** (`sources.hackernews`):

```json
{
  "enabled": true,
  "fetch_top_stories": 30,
  "min_score": 100
}
```

- `fetch_top_stories` — number of top story IDs to fetch
- `min_score` — minimum HN points to include a story

**Extracted data**: title, URL (falls back to HN discussion URL), author, score, comment count, and top comment text.

## GitHub

**File**: `src/scrapers/github.py`

Uses the [GitHub REST API](https://api.github.com):

- `GET /users/{username}/events/public` — user activity events
- `GET /repos/{owner}/{repo}/releases` — repository releases

Two source types are supported:

- **`user_events`** — tracks push, create, release, public, and watch events for a user
- **`repo_releases`** — tracks new releases for a specific repository

**Config** (`sources.github`, list of entries):

```json
{
  "type": "user_events",
  "username": "torvalds",
  "enabled": true
}
```

```json
{
  "type": "repo_releases",
  "owner": "golang",
  "repo": "go",
  "enabled": true
}
```

**Authentication**: Set `GITHUB_TOKEN` in your environment for higher rate limits (5000 req/hr vs 60 without).

## RSS

**File**: `src/scrapers/rss.py`

Fetches any Atom/RSS feed using the `feedparser` library. Tries multiple date fields (`published`, `updated`, `created`) with fallback parsing.

**Config** (`sources.rss`, list of entries):

```json
{
  "name": "Simon Willison",
  "url": "https://simonwillison.net/atom/everything/",
  "enabled": true,
  "category": "ai-tools"
}
```

- `category` — optional tag for grouping (e.g., `"programming"`, `"microblog"`)

**Extracted data**: title, URL, author, content (from `summary`/`description`/`content` fields), feed name, category, and entry tags.

## Reddit

**File**: `src/scrapers/reddit.py`

Uses Reddit's public JSON API (`www.reddit.com`):

- `GET /r/{subreddit}/{sort}.json` — subreddit posts
- `GET /user/{username}/submitted.json` — user submissions
- `GET /r/{subreddit}/comments/{post_id}.json` — post comments

Subreddits and users are fetched concurrently. Comments are sorted by score, limited to the configured count, and exclude moderator-distinguished comments. Self-text is truncated at 1500 chars, comments at 500 chars.

**Config** (`sources.reddit`):

```json
{
  "enabled": true,
  "fetch_comments": 5,
  "subreddits": [
    {
      "subreddit": "MachineLearning",
      "sort": "hot",
      "fetch_limit": 25,
      "min_score": 10
    }
  ],
  "users": [
    {
      "username": "spez",
      "sort": "new",
      "fetch_limit": 10
    }
  ]
}
```

- `sort` — `hot`, `new`, `top`, or `rising` (subreddits); `hot` or `new` (users)
- `time_filter` — for `top`/`rising` sorts: `hour`, `day`, `week`, `month`, `year`, `all`
- `min_score` — minimum post score (subreddits only)

**Rate limiting**: Detects HTTP 429 responses, reads the `Retry-After` header, waits, and retries once. Uses a descriptive `User-Agent` as required by Reddit's API guidelines.

**Extracted data**: title, URL, author, score, upvote ratio, comment count, subreddit, flair, self-text, and top comments.

## CVE

**File**: `src/scrapers/cve.py`

Uses official vulnerability data sources:

- CISA KEV JSON — Known Exploited Vulnerabilities catalog (static JSON feed with ETag/Last-Modified caching)
- CVE List V5 `cvelist_v5_delta` — official CVE List delta releases from `CVEProject/cvelistV5`
- GitHub Advisory Database `ghsa` — GitHub Security Advisory REST API, sorted by last update time
- NVD API 2.0 `nvd_recent` — newly published CVEs, queried with `pubStartDate`/`pubEndDate` time range
- NVD API 2.0 `nvd_modified` — recently modified CVEs, queried with `lastModStartDate`/`lastModEndDate` time range

Horizon treats `cvelist_v5_delta` as the primary incremental discovery source. NVD providers remain useful as enrichment-style sources because the API supports server-side filtering by time range and CVSS severity.

**CVE List V5 delta**:
- Polls the repository's GitHub Releases Atom feed
- Tracks the last processed release tag/timestamp in scraper state
- Downloads the matching hourly `delta_CVEs` zip asset for each unseen release
- Applies local keyword/vendor/product/CVSS filtering after parsing the CVE JSON records

**GHSA**:
- Calls the GitHub Advisory REST API with `sort=updated&direction=desc`
- Follows pagination until the feed reaches items older than the current run window
- Preserves advisories that only have a `GHSA-*` identifier and no CVE alias
- Reuses the project's standard `GITHUB_TOKEN` when present for higher GitHub API limits

**Server-side filtering (NVD API 2.0)**:
- Time range: `pubStartDate`/`pubEndDate` (for recent) or `lastModStartDate`/`lastModEndDate` (for modified)
- CVSS severity: coarse `cvssV3Severity` filter (CRITICAL / HIGH / MEDIUM / LOW) based on `min_cvss` threshold
- Keywords, vendors, and products are still filtered locally after the server-side reduction

**Limitations**:
- Time window must not exceed 120 days per NVD API request. If the configured time window exceeds this limit, Horizon prints a warning and skips the provider. Multi-segment requests for longer windows are not yet implemented.
- Pagination: the API returns at most 2000 results per page. Horizon requests the maximum page size and follows pagination until all matching results are fetched.

**Rate limits**:
- Without API key: 5 requests per 30-second window
- With API key: 50 requests per 30-second window
- Horizon makes at most 2 NVD API requests per run (one per enabled NVD provider), so an API key is optional

All enabled providers are fetched concurrently inside one scraper. Items are deduplicated by `cve_id`, then `ghsa_id` before they leave the scraper. Priority order is:

1. `cisa_kev`
2. `cvelist_v5_delta`
3. `ghsa`
4. `nvd_recent`
5. `nvd_modified`

If the same CVE appears in both KEV and NVD, the KEV item wins and missing NVD metadata such as CVSS, CWE, and reference URLs is merged in.

**Config** (`sources.cve`):

```json
{
  "enabled": true,
  "keywords": ["linux", "openssl"],
  "vendors": [],
  "products": [],
  "providers": [
    {
      "type": "cisa_kev",
      "enabled": true,
      "keywords": [],
      "vendors": [],
      "products": []
    },
    {
      "type": "cvelist_v5_delta",
      "enabled": true,
      "min_cvss": 7.0,
      "keywords": [],
      "vendors": [],
      "products": []
    },
    {
      "type": "ghsa",
      "enabled": false,
      "min_cvss": 7.0,
      "keywords": [],
      "vendors": [],
      "products": []
    },
    {
      "type": "nvd_recent",
      "enabled": false,
      "min_cvss": 7.0,
      "keywords": [],
      "vendors": [],
      "products": []
    }
  ]
}
```

- `type` — `cisa_kev`, `cvelist_v5_delta`, `ghsa`, `nvd_recent`, or `nvd_modified`
- `min_cvss` — optional minimum CVSS threshold for every provider except `cisa_kev`
- top-level `keywords` / `vendors` / `products` — default filters shared by all CVE providers
- provider-level `keywords` / `vendors` / `products` — appended to the top-level defaults for that provider

**Extracted data**: CVE ID, CVSS, severity, CWE, affected vendors/products, KEV flags, required action, due date, published/modified timestamps, and reference URLs.

## OpenBB

**File**: `src/scrapers/openbb.py`

Uses the [OpenBB Platform](https://www.openbb.co/platform) Python SDK via `obb.news.company()` to fetch company news for one or more ticker watchlists.

The scraper imports `openbb` lazily. If the optional dependency is not installed, Horizon logs a warning and skips the source instead of failing the whole run.

**Config** (`sources.openbb`):

```json
{
  "enabled": true,
  "watchlists": [
    {
      "name": "megacaps",
      "symbols": ["AAPL", "MSFT", "NVDA"],
      "enabled": true,
      "provider": "yfinance",
      "fetch_limit": 20,
      "category": "equities"
    }
  ]
}
```

- `watchlists` — each enabled watchlist triggers one `news.company()` call per run
- `provider` — OpenBB provider name for that watchlist
- `symbols` — tickers fetched together for the same provider
- `fetch_limit` — maximum rows requested from the provider
- `category` — optional metadata tag stored on each item

Behavior:

- Wraps the synchronous OpenBB SDK in `asyncio.to_thread` so the event loop stays responsive
- Deduplicates duplicate news across watchlists by article URL
- Skips malformed rows, rows without URL/title/date, and items older than the current time window
- Keeps fetching other watchlists if one provider call fails

**Credentials**: provider-specific secrets are resolved by the OpenBB SDK from its own environment variables or settings file. Horizon does not pass those values directly.

**Extracted data**: title, URL, author, published time, article body/excerpt, watchlist name, provider, category, and symbol list.

## Twitter

**File**: `src/scrapers/twitter.py`

Uses the [Apify](https://apify.com) platform to bypass Twitter's anti-scraping measures. The actor `altimis~scweet` is called via the Apify REST API.

Flow:
1. POST to `/v2/acts/{actor_id}/runs` to trigger a run
2. Poll `/v2/actor-runs/{run_id}` until status is `SUCCEEDED` or a terminal failure
3. GET `/v2/datasets/{dataset_id}/items` to retrieve results

**Config** (`sources.twitter`):

```json
{
  "enabled": true,
  "users": ["karpathy", "ylecun"],
  "fetch_limit": 10,
  "fetch_reply_text": false,
  "max_replies_per_tweet": 3,
  "max_tweets_to_expand": 10,
  "reply_min_likes": 5,
  "actor_id": "altimis~scweet",
  "apify_token_env": "APIFY_TOKEN"
}
```

- `users` — Twitter screen names to monitor, without the `@` prefix
- `fetch_limit` — maximum tweets to fetch per run
- `fetch_reply_text` — when `true`, a second Apify run fetches reply bodies for each important tweet and appends them under `--- Top Comments ---` for AI analysis
- `max_replies_per_tweet` — maximum reply lines per tweet (sorted by engagement score)
- `max_tweets_to_expand` — cap on reply expansion runs per pipeline cycle, to control Apify credit usage
- `reply_min_likes` — minimum likes required for a reply to be included
- `actor_id` — Apify actor ID (default: `altimis~scweet`)
- `apify_token_env` — environment variable name containing the Apify API token

**Authentication**: Set `APIFY_TOKEN` in your `.env`. Get a token at [console.apify.com](https://console.apify.com/account/integrations).

**Extracted data**: tweet text, URL, author, publish time, likes, retweets, replies, views, and (optionally) reply-thread text appended under `--- Top Comments ---`.
