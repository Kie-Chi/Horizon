"""Unit tests for webhook notification service."""

import asyncio
import json
import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from src.models import ContentItem, SourceType, WebhookConfig
from src.services.webhook import (
    WebhookNotifier,
    _format_markdown_for_webhook,
    _prepare_variables_for_body,
    _render,
    _truncate,
    _isjson,
    _extract_headers,
)
from src.ai.summarizer import DailySummarizer

_TEST_URL_ENV = "TEST_WEBHOOK_URL"
_TEST_URL = "https://example.com/webhook"


# ── Template variable replacement ──


class TestRender:
    def test_simple_replacement(self):
        template = "Hello #{name}, today is #{date}"
        variables = {"name": "Horizon", "date": "2026-04-24"}
        assert _render(template, variables) == "Hello Horizon, today is 2026-04-24"

    def test_no_matching_vars(self):
        template = "Hello #{unknown}"
        variables = {"name": "Horizon"}
        assert _render(template, variables) == "Hello #{unknown}"

    def test_empty_template(self):
        assert _render("", {"date": "2026-04-24"}) == ""

    def test_empty_vars(self):
        assert _render("Hello #{name}", {}) == "Hello #{name}"

    def test_numeric_values(self):
        template = "#{item_count} items, #{timestamp} seconds"
        variables = {"item_count": 15, "timestamp": 1745500000}
        assert _render(template, variables) == "15 items, 1745500000 seconds"

    def test_summary_with_multiline_content(self):
        template = '{"text": "#{summary}"}'
        summary = "## Title\n\nLine 1\nLine 2"
        variables = {"summary": summary}
        result = _render(template, variables)
        assert summary in result


class TestRenderDictAndList:
    def test_simple_dict(self):
        obj = {"title": "Horizon #{date}", "count": "#{item_count} items"}
        variables = {"date": "2026-04-24", "item_count": 15}
        result = _render(obj, variables)
        assert result == {"title": "Horizon 2026-04-24", "count": "15 items"}

    def test_nested_dict(self):
        obj = {
            "msg_type": "interactive",
            "card": {
                "schema": "2.0",
                "header": {"title": "Horizon #{date}"},
                "body": {
                    "elements": [{"tag": "markdown", "content": "#{summary}"}]
                },
            },
        }
        variables = {"date": "2026-04-24", "summary": "## AI News\nLine 1"}
        result = _render(obj, variables)
        assert result["card"]["header"]["title"] == "Horizon 2026-04-24"
        assert result["card"]["body"]["elements"][0]["content"] == "## AI News\nLine 1"

    def test_list(self):
        obj = ["#{date}", "#{result}", "static"]
        variables = {"date": "2026-04-24", "result": "success"}
        result = _render(obj, variables)
        assert result == ["2026-04-24", "success", "static"]

    def test_non_string_values_preserved(self):
        obj = {"count": 10, "flag": True, "extra": None, "text": "#{date}"}
        variables = {"date": "2026-04-24"}
        result = _render(obj, variables)
        assert result["count"] == 10
        assert result["flag"] is True
        assert result["extra"] is None
        assert result["text"] == "2026-04-24"

    def test_no_matching_vars(self):
        obj = {"key": "#{unknown}"}
        result = _render(obj, {"name": "test"})
        assert result == {"key": "#{unknown}"}

    def test_summary_with_quotes_safely_replaced(self):
        """Verify that quotes in summary don't break the JSON structure."""
        obj = {"content": "#{summary}"}
        summary = 'AI called "GPT-5" is great'
        result = _render(obj, {"summary": summary})
        # When serialized to JSON, the quotes should be properly escaped
        serialized = json.dumps(result)
        parsed_back = json.loads(serialized)
        assert parsed_back["content"] == summary


class TestTruncate:
    def test_short_value_not_truncated(self):
        value = "hello"
        result = _truncate(value, limit=100, split="---")
        assert result == value

    def test_truncate_by_segments(self):
        # "aaa---bbb---ccc" → segments: "aaa"(3), "bbb"(3+3=6), "ccc"(3+3=6)
        # limit=10 → keep "aaa"(3) + "bbb"(6) = 9 ≤ 10, drop "ccc"
        value = "aaa---bbb---ccc"
        result = _truncate(value, limit=10, split="---")
        assert result == "aaa---bbb"

    def test_single_segment_exceeds_limit_still_kept(self):
        # First segment alone exceeds limit, but we always keep it
        value = "abcdefghij---xyz"
        result = _truncate(value, limit=5, split="---")
        assert result == "abcdefghij"
        assert "xyz" not in result

    def test_no_split_delimiter_in_value(self):
        # Value doesn't contain the split delimiter — returned as-is
        value = "abcdefghij"
        result = _truncate(value, limit=5, split="---")
        # Without delimiter, entire value is one segment, always kept
        assert result == value

    def test_empty_value(self):
        result = _truncate("", limit=10, split="---")
        assert result == ""

    def test_exact_limit_with_join(self):
        # "aaa---bbb" → seg1=3, seg2=3+3(join)=6, total=9
        # limit=9 → exact fit, keep both
        value = "aaa---bbb"
        result = _truncate(value, limit=9, split="---")
        assert result == value

    def test_one_char_over_limit(self):
        # "aaa---bbb" → total=9 chars, limit=8 → drop "bbb"
        value = "aaa---bbb"
        result = _truncate(value, limit=8, split="---")
        assert result == "aaa"


class TestRenderParameterized:
    def test_plain_key_without_params(self):
        """#{summary} without params works as before."""
        template = "#{summary}"
        result = _render(template, {"summary": "hello world"})
        assert result == "hello world"

    def test_key_with_limit_and_split(self):
        """#{summary?limit=10&split=---} truncates by character count."""
        # "aaa---bbb---ccc" → keep "aaa---bbb" (9 chars ≤ 10), drop "ccc"
        summary = "aaa---bbb---ccc"
        template = "#{summary?limit=10&split=---}"
        result = _render(template, {"summary": summary})
        assert result == "aaa---bbb"

    def test_key_with_limit_no_truncation_needed(self):
        """When value fits within limit, no truncation occurs."""
        summary = "short text"
        template = "#{summary?limit=100&split=---}"
        result = _render(template, {"summary": summary})
        assert result == summary

    def test_missing_variable_with_params(self):
        """#{unknown?limit=5&split=---} with missing key leaves placeholder."""
        template = "#{unknown?limit=5&split=---}"
        result = _render(template, {"date": "2026-04-24"})
        assert result == "#{unknown?limit=5&split=---}"

    def test_param_in_dict_body(self):
        """#{summary?limit=10&split=---} works inside dict request_body."""
        obj = {"content": "#{summary?limit=10&split=---}", "title": "#{date}"}
        summary = "aaa---bbb---ccc"
        result = _render(obj, {"summary": summary, "date": "2026-04-24"})
        assert result["title"] == "2026-04-24"
        assert result["content"] == "aaa---bbb"

    def test_mix_of_plain_and_parameterized(self):
        """Plain #{date} and parameterized #{summary?...} in same template."""
        template = "#{date}: #{summary?limit=20&split=---}"
        summary = "aaa---bbb---ccc"
        result = _render(template, {"date": "2026-04-24", "summary": summary})
        assert result == "2026-04-24: aaa---bbb---ccc"


class TestWebhookMarkdownFormatting:
    def test_details_references_are_flattened_for_webhook(self):
        summary = """## Item

<a id="item-1"></a>
<details><summary>参考链接</summary>
<ul>
<li><a href="https://example.com/a">Example A</a></li>
<li><a href="https://example.com/b">Example B</a></li>
</ul>
</details>
"""

        result = _format_markdown_for_webhook(summary)

        assert "<details>" not in result
        assert "<summary>" not in result
        assert '<a id="item-1"></a>' not in result
        assert "**参考链接**" in result
        assert "- [Example A](https://example.com/a)" in result
        assert "- [Example B](https://example.com/b)" in result

    def test_prepare_variables_changes_summary_for_any_post_body(self):
        summary = "<details><summary>References</summary><ul><li>Plain item</li></ul></details>"
        variables = {"summary": summary, "date": "2026-04-24"}
        body = {"text": "#{summary}"}

        result = _prepare_variables_for_body(body, variables)

        assert result is not variables
        assert result["summary"] == "**References**\n\n- Plain item"
        assert variables["summary"] == summary

    def test_prepare_variables_keeps_summary_unchanged_without_body(self):
        summary = "<details><summary>References</summary><ul><li>Plain item</li></ul></details>"
        variables = {"summary": summary}

        result = _prepare_variables_for_body(None, variables)

        assert result is variables
        assert result["summary"] == summary


# ── JSON prefix detection ──


class TestIsJson:
    def test_object(self):
        assert _isjson('{"key": "value"}') is True

    def test_array(self):
        assert _isjson("[1, 2, 3]") is True

    def test_whitespace_before_brace(self):
        assert _isjson('  {"key": 1}') is True

    def test_plain_string(self):
        assert _isjson("hello world") is False

    def test_form_data(self):
        assert _isjson("key=value&foo=bar") is False

    def test_empty(self):
        assert _isjson("") is False


# ── Header parsing ──


class TestExtractHeaders:
    def test_single_header(self):
        assert _extract_headers("Content-Type: application/json") == {
            "Content-Type": "application/json"
        }

    def test_multiple_headers(self):
        result = _extract_headers("Authorization: Bearer abc\nX-Custom: value")
        assert result == {"Authorization": "Bearer abc", "X-Custom": "value"}

    def test_empty_string(self):
        assert _extract_headers("") == {}

    def test_none(self):
        assert _extract_headers(None) == {}

    def test_blank_lines(self):
        result = _extract_headers("Key: val\n\nAnother: val2")
        assert result == {"Key": "val", "Another": "val2"}

    def test_invalid_line(self):
        result = _extract_headers("NoColonHere\nValid: yes")
        assert result == {"Valid": "yes"}


# ── WebhookNotifier ──


def _run_async(coro):
    """Helper to run async coroutine in tests."""
    return asyncio.run(coro)


class TestWebhookNotifier:
    def test_disabled_skips(self):
        config = WebhookConfig(enabled=False, url_env=_TEST_URL_ENV)
        os.environ[_TEST_URL_ENV] = _TEST_URL
        notifier = WebhookNotifier(config)
        assert notifier.config.enabled is False
        del os.environ[_TEST_URL_ENV]

    def test_disabled_webhook_skips_notification(self):
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(enabled=False, url_env=_TEST_URL_ENV)
        notifier = WebhookNotifier(config)
        with patch("httpx.AsyncClient") as mock_client:
            _run_async(notifier.notify({"date": "2026-04-24"}))
            mock_client.assert_not_called()
        del os.environ[_TEST_URL_ENV]

    def test_empty_url_env_skips_notification(self):
        # url_env not set in environment
        config = WebhookConfig(enabled=True, url_env=_TEST_URL_ENV)
        notifier = WebhookNotifier(config)
        assert notifier.url is None
        with patch("httpx.AsyncClient") as mock_client:
            _run_async(notifier.notify({"date": "2026-04-24"}))
            mock_client.assert_not_called()

    def test_get_request_when_no_body(self):
        os.environ[_TEST_URL_ENV] = "https://example.com/webhook?date=#{date}"
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
        )
        notifier = WebhookNotifier(config)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            _run_async(notifier.notify({"date": "2026-04-24", "result": "success"}))
            mock_client.get.assert_called_once()
            call_url = mock_client.get.call_args[0][0]
            assert "2026-04-24" in call_url
        del os.environ[_TEST_URL_ENV]

    def test_post_request_with_json_body(self):
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
            request_body='{"msg_type": "post", "content": "Horizon #{date} #{item_count} items"}',
        )
        notifier = WebhookNotifier(config)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            _run_async(notifier.notify({"date": "2026-04-24", "item_count": 15}))
            mock_client.post.assert_called_once()

            call_kwargs = mock_client.post.call_args[1]
            assert call_kwargs["headers"]["Content-Type"] == "application/json"

            body_bytes = call_kwargs["content"]
            body_str = body_bytes.decode("utf-8")
            parsed = json.loads(body_str)
            assert parsed["content"] == "Horizon 2026-04-24 15 items"
        del os.environ[_TEST_URL_ENV]

    def test_post_request_with_json_str_body_containing_summary(self):
        """String JSON body with #{summary} that contains special characters.

        Note: when request_body is a string, #{summary} is replaced via
        simple string substitution. If #{summary} contains unescaped quotes
        or newlines, the resulting JSON string may become invalid. This test
        documents that known limitation — use dict request_body for safe
        handling of #{summary}.
        """
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
            request_body='{"msg_type": "post", "content": "#{summary}"}',
        )
        notifier = WebhookNotifier(config)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            # summary without special chars — should parse fine
            summary = "Horizon daily report: 10 items"
            _run_async(notifier.notify({"summary": summary}))
            mock_client.post.assert_called_once()

            call_kwargs = mock_client.post.call_args[1]
            body_str = call_kwargs["content"].decode("utf-8")
            parsed = json.loads(body_str)
            assert parsed["content"] == summary
        del os.environ[_TEST_URL_ENV]

    def test_post_request_with_json_str_body_summary_with_quotes_breaks_json(self):
        """String JSON body where #{summary} has quotes — JSON becomes invalid.

        This demonstrates the limitation: with string request_body, #{summary}
        containing quotes will break the JSON structure. The Content-Type falls
        back to application/x-www-form-urlencoded because json.loads fails.
        Use dict request_body instead for safe handling of #{summary}.
        """
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
            request_body='{"msg_type": "post", "content": "#{summary}"}',
        )
        notifier = WebhookNotifier(config)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            summary = 'AI called "GPT-5" is great'
            _run_async(notifier.notify({"summary": summary}))
            mock_client.post.assert_called_once()

            call_kwargs = mock_client.post.call_args[1]
            # json.loads fails on the rendered string, so content-type
            # falls back to form-urlencoded
            assert call_kwargs["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
        del os.environ[_TEST_URL_ENV]

    def test_post_request_with_form_body(self):
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
            request_body="date=#{date}&result=#{result}",
        )
        notifier = WebhookNotifier(config)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            _run_async(notifier.notify({"date": "2026-04-24", "result": "success"}))
            mock_client.post.assert_called_once()

            call_kwargs = mock_client.post.call_args[1]
            assert call_kwargs["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
        del os.environ[_TEST_URL_ENV]

    def test_custom_headers(self):
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
            request_body='{"key": "value"}',
            headers="X-Auth: token123\nX-Secret: abc",
        )
        notifier = WebhookNotifier(config)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            _run_async(notifier.notify({"date": "2026-04-24"}))
            call_kwargs = mock_client.post.call_args[1]
            assert call_kwargs["headers"]["X-Auth"] == "token123"
            assert call_kwargs["headers"]["X-Secret"] == "abc"
        del os.environ[_TEST_URL_ENV]

    def test_post_request_with_dict_body(self):
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
            request_body={
                "msg_type": "interactive",
                "card": {
                    "schema": "2.0",
                    "header": {"title": "Horizon #{date}"},
                    "body": {
                        "elements": [{"tag": "markdown", "content": "#{summary}"}]
                    },
                },
            },
        )
        notifier = WebhookNotifier(config)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            _run_async(notifier.notify({"date": "2026-04-24", "summary": "## News\nLine 1"}))
            mock_client.post.assert_called_once()

            call_kwargs = mock_client.post.call_args[1]
            assert call_kwargs["headers"]["Content-Type"] == "application/json"

            body_str = call_kwargs["content"].decode("utf-8")
            parsed = json.loads(body_str)
            assert parsed["card"]["header"]["title"] == "Horizon 2026-04-24"
            assert parsed["card"]["body"]["elements"][0]["content"] == "## News\nLine 1"
        del os.environ[_TEST_URL_ENV]

    def test_post_request_with_dict_body_and_special_chars(self):
        """Summary containing quotes should be properly serialized."""
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
            request_body={"content": "#{summary}"},
        )
        notifier = WebhookNotifier(config)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            summary = 'AI called "GPT-5" is great'
            _run_async(notifier.notify({"summary": summary}))
            mock_client.post.assert_called_once()

            body_str = mock_client.post.call_args[1]["content"].decode("utf-8")
            parsed = json.loads(body_str)
            assert parsed["content"] == summary
        del os.environ[_TEST_URL_ENV]

    def test_http_error_logged(self):
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
        )
        notifier = WebhookNotifier(config)

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(side_effect=httpx.ConnectError("Connection refused"))
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            # Should not raise — error is logged and printed
            _run_async(notifier.notify({"date": "2026-04-24"}))
        del os.environ[_TEST_URL_ENV]


# ── Config model validation ──


class TestWebhookConfigModel:
    def test_default_values(self):
        config = WebhookConfig()
        assert config.enabled is False
        assert config.url_env is None
        assert config.request_body is None
        assert config.headers is None

    def test_full_config(self):
        config = WebhookConfig(
            enabled=True,
            url_env="HORIZON_WEBHOOK_URL",
            request_body='{"msg_type":"post"}',
            headers="Authorization: Bearer xxx",
            delivery="summary_and_items",
            languages=["zh"],
        )
        assert config.enabled is True
        assert config.url_env == "HORIZON_WEBHOOK_URL"
        assert config.delivery == "summary_and_items"
        assert config.languages == ["zh"]


# ── Helper to build a ContentItem for testing ──


def _make_item(title="Test Item", url="https://example.com/test", score=8.0):
    """Create a minimal ContentItem for webhook tests."""
    return ContentItem(
        id="github:test:1",
        source_type=SourceType.GITHUB,
        title=title,
        url=url,
        content="Some content",
        author="testuser",
        published_at=datetime(2026, 4, 24, 12, 0, 0, tzinfo=timezone.utc),
        fetched_at=datetime(2026, 4, 24, 12, 0, 0, tzinfo=timezone.utc),
        ai_score=score,
        ai_summary="AI summary",
        ai_tags=["test"],
    )


# ── send_daily_summary ──


class TestSendDailySummary:
    def test_summary_delivery_calls_notify_once(self):
        """delivery='summary' sends a single notify call with message_kind='summary'."""
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
            delivery="summary",
        )
        notifier = WebhookNotifier(config)
        summarizer = DailySummarizer()
        items = [_make_item()]
        summary = "# Horizon Daily\nTest summary"

        with patch.object(notifier, "notify", new_callable=AsyncMock) as mock_notify:
            _run_async(notifier.send_daily_summary(
                summary=summary,
                important_items=items,
                all_items_count=10,
                date="2026-04-24",
                lang="en",
                summarizer=summarizer,
            ))
            mock_notify.assert_called_once()
            vars = mock_notify.call_args[0][0]
            assert vars["message_kind"] == "summary"
            assert vars["message_title"] == "Horizon 2026-04-24 Daily"
            assert vars["summary"] == summary
            assert vars["important_items"] == 1
            assert vars["all_items"] == 10
            assert vars["result"] == "success"
            assert vars["language"] == "en"
        del os.environ[_TEST_URL_ENV]

    def test_summary_delivery_zh_lang(self):
        """Chinese lang uses '日报' in message_title."""
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
            delivery="summary",
        )
        notifier = WebhookNotifier(config)
        summarizer = DailySummarizer()
        items = [_make_item()]

        with patch.object(notifier, "notify", new_callable=AsyncMock) as mock_notify:
            _run_async(notifier.send_daily_summary(
                summary="## 测试摘要",
                important_items=items,
                all_items_count=5,
                date="2026-04-24",
                lang="zh",
                summarizer=summarizer,
            ))
            vars = mock_notify.call_args[0][0]
            assert vars["message_title"] == "Horizon 2026-04-24 日报"
            assert vars["language"] == "zh"
        del os.environ[_TEST_URL_ENV]

    def test_summary_and_items_delivery_calls_notify_multiple_times(self):
        """delivery='summary_and_items' sends overview + N item notifications."""
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
            delivery="summary_and_items",
        )
        notifier = WebhookNotifier(config)
        summarizer = DailySummarizer()
        items = [_make_item(title="Item A"), _make_item(title="Item B", url="https://example.com/b")]
        summary = "# Full summary"

        with patch.object(notifier, "notify", new_callable=AsyncMock) as mock_notify:
            _run_async(notifier.send_daily_summary(
                summary=summary,
                important_items=items,
                all_items_count=20,
                date="2026-04-24",
                lang="en",
                summarizer=summarizer,
            ))
            # 1 overview + 2 items = 3 calls
            assert mock_notify.call_count == 3

            # First call: overview
            overview_vars = mock_notify.call_args_list[0][0][0]
            assert overview_vars["message_kind"] == "overview"
            assert overview_vars["message_title"] == "Horizon 2026-04-24 Overview"

            # Second call: first item
            item1_vars = mock_notify.call_args_list[1][0][0]
            assert item1_vars["message_kind"] == "item"
            assert item1_vars["item_index"] == 1
            assert item1_vars["item_count"] == 2
            assert item1_vars["item_url"] == "https://example.com/test"

            # Third call: second item
            item2_vars = mock_notify.call_args_list[2][0][0]
            assert item2_vars["message_kind"] == "item"
            assert item2_vars["item_index"] == 2
            assert item2_vars["item_url"] == "https://example.com/b"
        del os.environ[_TEST_URL_ENV]

    def test_language_filter_skips_non_matching_lang(self):
        """webhook.languages=['zh'] skips 'en' language."""
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
            delivery="summary",
            languages=["zh"],
        )
        notifier = WebhookNotifier(config)
        summarizer = DailySummarizer()
        items = [_make_item()]

        with patch.object(notifier, "notify", new_callable=AsyncMock) as mock_notify:
            _run_async(notifier.send_daily_summary(
                summary="English summary",
                important_items=items,
                all_items_count=10,
                date="2026-04-24",
                lang="en",
                summarizer=summarizer,
            ))
            mock_notify.assert_not_called()
        del os.environ[_TEST_URL_ENV]

    def test_language_filter_passes_matching_lang(self):
        """webhook.languages=['zh'] allows 'zh' language."""
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
            delivery="summary",
            languages=["zh"],
        )
        notifier = WebhookNotifier(config)
        summarizer = DailySummarizer()
        items = [_make_item()]

        with patch.object(notifier, "notify", new_callable=AsyncMock) as mock_notify:
            _run_async(notifier.send_daily_summary(
                summary="中文摘要",
                important_items=items,
                all_items_count=10,
                date="2026-04-24",
                lang="zh",
                summarizer=summarizer,
            ))
            mock_notify.assert_called_once()
        del os.environ[_TEST_URL_ENV]

    def test_no_language_filter_sends_all(self):
        """webhook.languages=None sends for all languages."""
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
            delivery="summary",
            languages=None,
        )
        notifier = WebhookNotifier(config)
        summarizer = DailySummarizer()
        items = [_make_item()]

        with patch.object(notifier, "notify", new_callable=AsyncMock) as mock_notify:
            _run_async(notifier.send_daily_summary(
                summary="English summary",
                important_items=items,
                all_items_count=10,
                date="2026-04-24",
                lang="en",
                summarizer=summarizer,
            ))
            mock_notify.assert_called_once()
        del os.environ[_TEST_URL_ENV]

    def test_timestamp_is_current_utc(self):
        """timestamp variable reflects the current UTC time."""
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
            delivery="summary",
        )
        notifier = WebhookNotifier(config)
        summarizer = DailySummarizer()
        items = [_make_item()]

        before = int(datetime.now(timezone.utc).timestamp())
        with patch.object(notifier, "notify", new_callable=AsyncMock) as mock_notify:
            _run_async(notifier.send_daily_summary(
                summary="test",
                important_items=items,
                all_items_count=5,
                date="2026-04-24",
                lang="en",
                summarizer=summarizer,
            ))
            after = int(datetime.now(timezone.utc).timestamp())
            vars = mock_notify.call_args[0][0]
            ts = int(vars["timestamp"])
            assert before <= ts <= after
        del os.environ[_TEST_URL_ENV]

    def test_summary_and_items_zh_overview_title(self):
        """summary_and_items with zh lang uses '总览' in overview title."""
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
            delivery="summary_and_items",
        )
        notifier = WebhookNotifier(config)
        summarizer = DailySummarizer()
        items = [_make_item()]

        with patch.object(notifier, "notify", new_callable=AsyncMock) as mock_notify:
            _run_async(notifier.send_daily_summary(
                summary="中文摘要",
                important_items=items,
                all_items_count=10,
                date="2026-04-24",
                lang="zh",
                summarizer=summarizer,
            ))
            overview_vars = mock_notify.call_args_list[0][0][0]
            assert overview_vars["message_title"] == "Horizon 2026-04-24 总览"
        del os.environ[_TEST_URL_ENV]


# ── send_failure_notification ──


class TestSendFailureNotification:
    def test_failure_calls_notify_with_failure_vars(self):
        """send_failure_notification sends notify with correct failure vars."""
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
        )
        notifier = WebhookNotifier(config)

        with patch.object(notifier, "notify", new_callable=AsyncMock) as mock_notify:
            _run_async(notifier.send_failure(
                date="2026-04-24",
                error_message="something went wrong",
            ))
            mock_notify.assert_called_once()
            vars = mock_notify.call_args[0][0]
            assert vars["date"] == "2026-04-24"
            assert vars["result"] == "failed"
            assert vars["language"] == ""
            assert vars["important_items"] == 0
            assert vars["all_items"] == 0
            assert vars["message_kind"] == "failure"
            assert vars["message_title"] == "Horizon generation failed"
            assert "something went wrong" in vars["summary"]
        del os.environ[_TEST_URL_ENV]

    def test_failure_timestamp_is_current_utc(self):
        """Failure notification timestamp reflects current UTC time."""
        os.environ[_TEST_URL_ENV] = _TEST_URL
        config = WebhookConfig(
            enabled=True,
            url_env=_TEST_URL_ENV,
        )
        notifier = WebhookNotifier(config)

        before = int(datetime.now(timezone.utc).timestamp())
        with patch.object(notifier, "notify", new_callable=AsyncMock) as mock_notify:
            _run_async(notifier.send_failure(
                date="2026-04-24",
                error_message="error",
            ))
            after = int(datetime.now(timezone.utc).timestamp())
            vars = mock_notify.call_args[0][0]
            ts = int(vars["timestamp"])
            assert before <= ts <= after
        del os.environ[_TEST_URL_ENV]
