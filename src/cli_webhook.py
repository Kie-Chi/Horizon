"""CLI entry point for testing webhook connectivity.

Usage:
    uv run horizon-webhook [options]

This command sends a test notification using the webhook configuration
from data/config.json, allowing you to verify connectivity and see
the rendered content before running the full pipeline.
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel

from .storage.manager import StorageManager
from .models import ContentItem, SourceType
from .services.webhook import WebhookNotifier, _render
from .ai.summarizer import DailySummarizer


console = Console()


def _make_test_items():
    """Create sample ContentItems for the test notification.

    Each item contains both English and Chinese metadata so the
    summarizer can render them in any configured language.
    """
    items = [
        ContentItem(
            id="github:test:1",
            source_type=SourceType.GITHUB,
            title="GPT-5 Released with Multimodal Capabilities",
            url="https://example.com/gpt5",
            content="OpenAI announced GPT-5 with major improvements.",
            author="openai",
            published_at=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
            fetched_at=datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc),
            ai_score=9.0,
            ai_summary="OpenAI released GPT-5 featuring multimodal capabilities and improved reasoning.",
            ai_tags=["ai", "llm", "openai"],
            metadata={
                "title_zh": "GPT-5 发布：多模态能力大幅提升",
                "detailed_summary_zh": "OpenAI 发布了 GPT-5，具备多模态能力和更强的推理能力。",
            },
        ),
        ContentItem(
            id="hackernews:test:2",
            source_type=SourceType.HACKERNEWS,
            title="New Linux Kernel 7.0 Released",
            url="https://example.com/linux7",
            content="Linux kernel 7.0 brings significant performance improvements.",
            author="torvalds",
            published_at=datetime(2026, 4, 24, 8, 0, tzinfo=timezone.utc),
            fetched_at=datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc),
            ai_score=7.5,
            ai_summary="Linux kernel 7.0 released with performance gains and new hardware support.",
            ai_tags=["linux", "kernel", "performance"],
            metadata={
                "title_zh": "Linux 内核 7.0 发布",
                "detailed_summary_zh": "Linux 内核 7.0 发布，带来显著性能提升和新硬件支持。",
            },
        ),
    ]
    return items


def _preview_variables(config, variables):
    """Show a preview of the template rendering for the given variables."""
    console.print("\n[bold]── Variable Rendering Preview ──[/bold]")

    # Show URL rendering
    url_env = config.url_env
    url_value = None
    if url_env:
        from os import getenv
        url_value = getenv(url_env)
    if url_value:
        rendered_url = _render(url_value, variables)
        console.print(f"  [cyan]URL:[/cyan] {rendered_url}")

    # Show body rendering
    body = config.request_body
    if body:
        if isinstance(body, (dict, list)):
            rendered = _render(body, variables)
            rendered_str = json.dumps(rendered, ensure_ascii=False)
            # Truncate for display
            if len(rendered_str) > 3000:
                rendered_str = rendered_str[:3000] + "\n... (truncated)"
            console.print(Panel(rendered_str, title="Request Body (JSON)", border_style="green"))
        elif isinstance(body, str):
            rendered = _render(body, variables)
            if len(rendered) > 3000:
                rendered = rendered[:3000] + "\n... (truncated)"
            console.print(Panel(rendered, title="Request Body (String)", border_style="green"))

    # Show header rendering
    headers = config.headers
    if headers:
        rendered_headers = _render(headers, variables)
        console.print(f"  [cyan]Headers:[/cyan] {rendered_headers}")


async def _run_test(webhook_config, lang: str, dry_run: bool, delivery_override: str = None):
    """Execute the webhook test.

    Args:
        webhook_config: WebhookConfig from the application config
        lang: Language code to test
        dry_run: If True, preview without sending
        delivery_override: Override delivery mode for this test
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    items = _make_test_items()
    summarizer = DailySummarizer()
    summary = await summarizer.generate_summary(items, today, len(items), language=lang)

    # Use the effective config (possibly overridden)
    effective_config = webhook_config
    if delivery_override:
        effective_config = webhook_config.model_copy(update={"delivery": delivery_override})

    notifier = WebhookNotifier(effective_config, console=console)

    if dry_run:
        # Dry run: show what would be sent without actually sending
        console.print(f"\n[bold yellow]── Dry Run (lang={lang}) ──[/bold yellow]")
        console.print(f"  [cyan]Webhook enabled:[/cyan] {effective_config.enabled}")
        console.print(f"  [cyan]URL env var:[/cyan] {effective_config.url_env}")
        console.print(f"  [cyan]Delivery mode:[/cyan] {effective_config.delivery}")

        webhook_languages = effective_config.languages
        if webhook_languages and lang not in webhook_languages:
            console.print(
                f"  [yellow]Language '{lang}' is filtered out by webhook.languages={webhook_languages}. "
                f"Notification would be skipped.[/yellow]"
            )
            return

        # Build and preview variables for each message
        base_vars = {
            "date": today,
            "language": lang,
            "important_items": len(items),
            "all_items": len(items),
            "result": "success",
            "timestamp": str(int(datetime.now(timezone.utc).timestamp())),
        }

        delivery = effective_config.delivery

        if delivery == "summary_and_items":
            console.print("\n[bold]── Message 1: Overview ──[/bold]")
            overview = summarizer.generate_webhook_overview(items, today, len(items), language=lang)
            overview_vars = {
                **base_vars,
                "message_title": f"Horizon {today} 总览" if lang == "zh" else f"Horizon {today} Overview",
                "message_kind": "overview",
                "summary": overview,
            }
            console.print(Panel(overview, title=overview_vars["message_title"], border_style="blue"))
            _preview_variables(effective_config, overview_vars)

            for i, item in enumerate(items, start=1):
                console.print(f"\n[bold]── Message {i + 1}: Item {i}/{len(items)} ──[/bold]")
                title = str(item.metadata.get(f"title_{lang}") or item.title)
                item_summary = summarizer.generate_webhook_item(item, language=lang, index=i, total=len(items))
                item_vars = {
                    **base_vars,
                    "message_title": f"{i}/{len(items)} {title}",
                    "message_kind": "item",
                    "item_index": i,
                    "item_count": len(items),
                    "item_title": title,
                    "item_url": str(item.url),
                    "item_score": item.ai_score or "",
                    "summary": item_summary,
                }
                console.print(Panel(item_summary, title=item_vars["message_title"], border_style="green"))
                _preview_variables(effective_config, item_vars)
        else:
            console.print("\n[bold]── Message: Summary ──[/bold]")
            summary_vars = {
                **base_vars,
                "message_title": f"Horizon {today} 日报" if lang == "zh" else f"Horizon {today} Daily",
                "message_kind": "summary",
                "summary": summary,
            }
            # Truncate summary for display if too long
            display_summary = summary if len(summary) <= 3000 else summary[:3000] + "\n... (truncated)"
            console.print(Panel(display_summary, title=summary_vars["message_title"], border_style="blue"))
            _preview_variables(effective_config, summary_vars)

        console.print("\n[green]Dry run complete. No notification was sent.[/green]")
    else:
        # Actually send
        console.print(f"\n[bold]── Sending Test Notification (lang={lang}) ──[/bold]")
        await notifier.send_daily_summary(
            summary=summary,
            important_items=items,
            all_items_count=len(items),
            date=today,
            lang=lang,
            summarizer=summarizer,
        )
        console.print("[green]Test notification sent.[/green]")


def main():
    """CLI entry point for horizon-webhook."""
    parser = argparse.ArgumentParser(
        description="Test webhook connectivity and preview rendered content",
    )
    parser.add_argument(
        "--lang",
        default=None,
        help="Language to test (en or zh). Defaults to the first language in config.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the rendered content without actually sending the notification.",
    )
    parser.add_argument(
        "--delivery",
        default=None,
        choices=["summary", "summary_and_items"],
        help="Override the delivery mode from config for this test.",
    )
    args = parser.parse_args()

    try:
        load_dotenv()

        storage = StorageManager(data_dir=str(Path("data")))
        try:
            config = storage.load_config()
        except FileNotFoundError:
            console.print("[bold red]Configuration file not found![/bold red]")
            console.print("Run [bold cyan]uv run horizon-wizard[/bold cyan] to set up your configuration.")
            sys.exit(1)

        if not config.webhook or not config.webhook.enabled:
            console.print("[yellow]Webhook is not enabled in config.json.[/yellow]")
            console.print("Set [cyan]webhook.enabled = true[/cyan] in data/config.json to enable it.")
            sys.exit(1)

        # Determine test language
        lang = args.lang
        if not lang:
            lang = config.ai.languages[0] if config.ai.languages else "en"

        asyncio.run(_run_test(config.webhook, lang, args.dry_run, args.delivery))

    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user[/yellow]")
        sys.exit(0)
    except Exception as e:
        console.print(f"\n[bold red]Error: {e}[/bold red]")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()