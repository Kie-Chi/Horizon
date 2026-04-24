"""Core data models for Horizon."""

from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any, Union
from pydantic import BaseModel, HttpUrl, Field


class SourceType(str, Enum):
    """Supported information source types."""
    GITHUB = "github"
    HACKERNEWS = "hackernews"
    RSS = "rss"
    REDDIT = "reddit"
    TELEGRAM = "telegram"


class ContentItem(BaseModel):
    """Unified content item model from any source."""

    id: str  # Format: {source}:{subtype}:{native_id}
    source_type: SourceType
    title: str
    url: HttpUrl
    content: Optional[str] = None
    author: Optional[str] = None
    published_at: datetime
    fetched_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # AI analysis results
    ai_score: Optional[float] = None  # 0-10 importance score
    ai_reason: Optional[str] = None
    ai_summary: Optional[str] = None
    ai_tags: List[str] = Field(default_factory=list)


class AIProvider(str, Enum):
    """Supported AI providers."""
    ANTHROPIC = "anthropic"
    OPENAI = "openai"
    ALI = "ali"
    GEMINI = "gemini"
    DOUBAO = "doubao"
    MINIMAX = "minimax"


class AIConfig(BaseModel):
    """AI client configuration."""

    provider: AIProvider
    model: str
    base_url: Optional[str] = None
    api_key_env: str
    temperature: float = 0.3
    max_tokens: int = 4096
    languages: List[str] = Field(default_factory=lambda: ["en"])


class GitHubSourceConfig(BaseModel):
    """GitHub source configuration."""

    type: str  # "user_events", "repo_releases", etc.
    username: Optional[str] = None
    owner: Optional[str] = None
    repo: Optional[str] = None
    enabled: bool = True


class HackerNewsConfig(BaseModel):
    """Hacker News configuration."""

    enabled: bool = True
    fetch_top_stories: int = 30
    min_score: int = 100


class RSSSourceConfig(BaseModel):
    """RSS feed source configuration."""

    name: str
    url: HttpUrl
    enabled: bool = True
    category: Optional[str] = None


class RedditSubredditConfig(BaseModel):
    """Configuration for monitoring a specific subreddit."""
    subreddit: str
    enabled: bool = True
    sort: str = "hot"           # hot, new, top, rising
    time_filter: str = "day"    # hour, day, week, month, year, all (only for top/controversial)
    fetch_limit: int = 25
    min_score: int = 10


class RedditUserConfig(BaseModel):
    """Configuration for monitoring a specific Reddit user."""
    username: str               # without u/ prefix
    enabled: bool = True
    sort: str = "new"
    fetch_limit: int = 10


class RedditConfig(BaseModel):
    """Reddit source configuration."""
    enabled: bool = True
    subreddits: List[RedditSubredditConfig] = Field(default_factory=list)
    users: List[RedditUserConfig] = Field(default_factory=list)
    fetch_comments: int = 5     # top comments per post, 0 to disable


class TelegramChannelConfig(BaseModel):
    """Configuration for monitoring a specific Telegram channel."""
    channel: str            # channel username, e.g. "zaihuapd"
    enabled: bool = True
    fetch_limit: int = 20


class TelegramConfig(BaseModel):
    """Telegram source configuration."""
    enabled: bool = True
    channels: List[TelegramChannelConfig] = Field(default_factory=list)


class SourcesConfig(BaseModel):
    """All sources configuration."""

    github: List[GitHubSourceConfig] = Field(default_factory=list)
    hackernews: HackerNewsConfig = Field(default_factory=HackerNewsConfig)
    rss: List[RSSSourceConfig] = Field(default_factory=list)
    reddit: RedditConfig = Field(default_factory=RedditConfig)
    telegram: TelegramConfig = Field(default_factory=TelegramConfig)


class WebhookConfig(BaseModel):
    """Webhook notification configuration."""

    url_env: Optional[str] = None          # Environment variable name containing the webhook URL
    request_body: Optional[Union[str, dict, list]] = None  # POST body: real JSON object or string with #{key} placeholders; if empty, will use GET
    headers: Optional[str] = None          # Custom headers, "Key: Value" per line
    enabled: bool = False


class EmailConfig(BaseModel):
    """Email configuration for updates/subscriptions."""
    imap_server: str
    imap_port: int = 993
    smtp_server: str
    smtp_port: int = 465
    email_address: str
    password_env: str = "EMAIL_PASSWORD"
    sender_name: str = "Horizon Daily"
    subscribe_keyword: str = "SUBSCRIBE"
    unsubscribe_keyword: str = "UNSUBSCRIBE"
    enabled: bool = False


class FilteringConfig(BaseModel):
    """Content filtering configuration."""

    ai_score_threshold: float = 7.0
    time_window_hours: int = 24


class Config(BaseModel):
    """Main configuration model."""

    version: str = "1.0"
    ai: AIConfig
    sources: SourcesConfig
    filtering: FilteringConfig
    email: Optional[EmailConfig] = None
    webhook: Optional[WebhookConfig] = None
