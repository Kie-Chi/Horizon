"""Webhook notification service for Horizon."""

import json
import logging
import os
import re
from typing import Optional, Union
import httpx

from ..models import WebhookConfig

logger = logging.getLogger(__name__)


# Pattern: #{key} or #{key?param1=val1&param2=val2}
_PLACEHOLDER_RE = re.compile(r"#\{(\w+)(\?\w+=[^}]+)?\}")


def _truncate(value: str, limit: int, split: str) -> str:
    """Truncate a string to at most *limit* characters by splitting on *split*.

    Segments are accumulated in order until adding the next one would
    exceed *limit* characters.  Remaining segments are dropped.

    Args:
        value: The full text to truncate
        limit: Maximum number of characters allowed
        split: Delimiter to split value into segments

    Returns:
        Truncated text
    """
    segments = value.split(split)
    kept: list[str] = []
    current_chars = 0

    for seg in segments:
        # +len(split) for the delimiter that will be re-joined
        seg_chars = len(seg) + (len(split) if kept else 0)
        if kept and current_chars + seg_chars > limit:
            break
        kept.append(seg)
        current_chars += seg_chars

    return split.join(kept)


def _render(template: Union[str, dict, list], variables: dict) -> Union[str, dict, list]:
    """Replace #{key} and #{key?params} placeholders in a template.

    Supports strings, dicts, and lists.  For dicts/lists, walks all
    string values recursively and replaces placeholders.

    Parameterized syntax: #{key?limit=N&split=DELIM}
      - limit: maximum number of output characters
      - split: delimiter to split the value into segments before
               accumulating up to *limit* characters

    Args:
        template: Template with #{key} placeholders — str, dict, or list
        variables: Dict mapping placeholder keys to replacement values

    Returns:
        Same type as template, with placeholders replaced
    """
    if isinstance(template, dict):
        return {k: _render(v, variables) for k, v in template.items()}
    if isinstance(template, list):
        return [_render(item, variables) for item in template]
    if isinstance(template, str):
        def _replace(match: re.Match) -> str:
            key = match.group(1)
            params_str = match.group(2)  # e.g. "?limit=500&split=---"

            value = variables.get(key)
            if value is None:
                return match.group(0)  # leave placeholder unchanged

            if not params_str:
                return str(value)

            # Parse params: ?limit=500&split=---
            raw_params = params_str.lstrip("?")
            params: dict[str, str] = {}
            for pair in raw_params.split("&"):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    params[k] = v

            limit = int(params.get("limit", "0")) if "limit" in params else 0
            split_delim = params.get("split", "---")

            if limit and split_delim:
                return _truncate(str(value), limit, split_delim)

            return str(value)

        return _PLACEHOLDER_RE.sub(_replace, template)
    # int, float, bool, None — return as-is
    return template


def _isjson(s: str) -> bool:
    """Return True if the string starts with a JSON open brace."""
    s = s.strip()
    return s.startswith("{") or s.startswith("[")


def _extract_headers(headers_str: Optional[str]) -> dict:
    """Parse custom headers from a multi-line "Key: Value" string.

    Args:
        headers_str: Multi-line string, each line "Key: Value"

    Returns:
        dict: Parsed headers as key-value pairs
    """
    if not headers_str:
        return {}

    headers = {}
    for line in headers_str.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(":", 1)
        if len(parts) != 2:
            logger.warning("Invalid webhook header line: %s", line)
            continue
        k, v = parts[0].strip(), parts[1].strip()
        headers[k] = v

    return headers


class WebhookNotifier:
    """Sends webhook notifications after pipeline completion or failure."""

    def __init__(self, config: WebhookConfig, console=None):
        self.config = config
        self.url = os.getenv(config.url_env or "") if config.url_env else None
        if console is None:
            try:
                from rich.console import Console
                self.console = Console()
            except ImportError:
                class DummyConsole:
                    def print(self, *args, **kwargs):
                        print(*args, **kwargs)
                self.console = DummyConsole()
        else:
            self.console = console

    async def notify(self, variables: dict) -> dict:
        """Send a webhook notification with template variable substitution.

        If request_body is empty, sends a GET request.
        If request_body is provided, sends a POST request with
        auto-detected content-type

        Args:
            variables: Dict of template variable values to replace
                       in URL, request_body, and headers.

        Returns:
            Dict with 'status_code' and 'response' keys, or None if
            skipped (disabled or URL empty).
        """
        if not self.config.enabled:
            return None

        if not self.url:
            logger.warning("Webhook enabled but URL is empty (env var %s not set), skipping notification.", self.config.url_env)
            return None

        # Replace template variables in URL
        request_url = _render(self.url, variables)

        # Determine method and content
        method = "GET"
        content_type = "application/x-www-form-urlencoded"
        body_content = None

        raw_body = self.config.request_body

        if raw_body:
            method = "POST"

            if isinstance(raw_body, (dict, list)):
                # Real JSON object/list: replace at value level, then serialize
                rendered_obj = _render(raw_body, variables)
                body_content = json.dumps(rendered_obj, ensure_ascii=False)
                content_type = "application/json"
                logger.debug("Webhook POST body (dict/list, %d chars): %s", len(body_content), body_content[:2000])
            elif isinstance(raw_body, str) and raw_body.strip():
                rendered = _render(raw_body, variables)
                body_content = rendered
                logger.debug("Webhook POST body (str, %d chars): %s", len(body_content), body_content[:2000])
                # Detect content type from the rendered string
                if _isjson(rendered):
                    try:
                        json.loads(rendered)
                        content_type = "application/json"
                    except json.JSONDecodeError:
                        pass  # keep form type

        # Build headers
        headers = _extract_headers(self.config.headers)
        headers["Content-Type"] = content_type

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                if method == "GET":
                    response = await client.get(request_url, headers=headers)
                else:
                    response = await client.post(
                        request_url,
                        content=body_content.encode("utf-8"),
                        headers=headers,
                    )

            if response.status_code == 200:
                logger.info(
                    "Webhook sent OK. URL: %s, body: %s",
                    request_url,
                    response.text[:500],
                )
                return {"status_code": response.status_code, "response": response.text[:500]}
            else:
                self.console.print(
                    f"[red]Webhook failed! status={response.status_code} "
                    f"response={response.text[:500]}[/red]"
                )
                logger.error(
                    "Webhook failed! URL: %s, status: %d, body: %s",
                    request_url,
                    response.status_code,
                    response.text[:500],
                )
                return {"status_code": response.status_code, "response": response.text[:500]}

        except Exception as e:
            self.console.print(f"[red]Webhook call failed! Exception: {e}[/red]")
            logger.error("Webhook call failed! URL: %s, exception: %s", request_url, e)
            return {"status_code": None, "response": str(e)}