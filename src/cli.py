"""Unified CLI entry point for Horizon."""

import asyncio
import sys
from pathlib import Path
import click
from dotenv import load_dotenv
from rich.console import Console

from .orchestrator import HorizonOrchestrator
from .storage.manager import StorageManager

console = Console()


def print_banner():
    """Print the application banner."""
    banner = r"""
[bold blue]
  _    _            _
 | |  | |          (_)
 | |__| | ___  _ __ _ ___  ___  _ __
 |  __  |/ _ \| '__| |_  / / _ \| '_ \
 | |  | | (_) | |  | |/ / | (_) | | | |
 |_|  |_|\___/|_|  |_/___| \___/|_| |_|
[/bold blue]
[cyan]  AI-Driven Information Aggregation System[/cyan]
    """
    console.print(banner)


class DefaultRunGroup(click.Group):
    """When no subcommand is given, default to 'run'."""

    def parse_args(self, ctx, args):
        # Treat bare "help" as --help for the group
        if list(args) == ["help"]:
            return super().parse_args(ctx, ["--help"])

        command_names = self.list_commands(ctx)
        if any(arg in command_names for arg in args):
            return super().parse_args(ctx, args)

        # No subcommand found → prepend 'run'
        return super().parse_args(ctx, ["run"] + list(args))


@click.group(cls=DefaultRunGroup)
def cli():
    """Horizon - AI-Driven Information Aggregation System."""
    pass


@cli.command()
@click.option("--hours", type=int, default=None, help="Force fetch from last N hours")
def run(hours):
    """Run the aggregation pipeline."""
    print_banner()

    try:
        load_dotenv()

        data_dir = Path("data")
        storage = StorageManager(data_dir=str(data_dir))

        try:
            config = storage.load_config()
        except FileNotFoundError:
            console.print("[bold red]Configuration file not found![/bold red]\n")
            console.print(
                "Run [bold cyan]horizon wizard[/bold cyan] to launch the interactive setup wizard,\n"
                "or create [cyan]data/config.json[/cyan] manually based on the template:\n"
            )
            _print_config_template()
            sys.exit(1)
        except Exception as e:
            console.print(f"[bold red]Error loading configuration: {e}[/bold red]")
            sys.exit(1)

        orchestrator = HorizonOrchestrator(config, storage)
        asyncio.run(orchestrator.run(force_hours=hours))

    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user[/yellow]")
        sys.exit(0)
    except Exception as e:
        console.print(f"\n[bold red]Fatal error: {e}[/bold red]")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def _print_config_template():
    """Print configuration template."""
    template = """
{
  "version": "1.0",
  "ai": {
    "provider": "anthropic",
    "model": "claude-sonnet-4.5-20250929",
    "api_key_env": "ANTHROPIC_API_KEY",
    "temperature": 0.3,
    "max_tokens": 4096
  },
  "sources": {
    "github": [
      {
        "type": "user_events",
        "username": "torvalds",
        "enabled": true
      }
    ],
    "hackernews": {
      "enabled": true,
      "fetch_top_stories": 30,
      "min_score": 100
    },
    "rss": [
      {
        "name": "Example Blog",
        "url": "https://example.com/feed.xml",
        "enabled": true,
        "category": "software-engineering"
      }
    ]
  },
  "filtering": {
    "ai_score_threshold": 7.0,
    "time_window_hours": 24
  }
}

Also create a .env file with:
ANTHROPIC_API_KEY=your_api_key_here
GITHUB_TOKEN=your_github_token_here (optional but recommended)
"""
    console.print(template)


@cli.command()
def wizard():
    """Interactive setup wizard for configuration."""
    from .setup.wizard import main as wizard_main
    wizard_main()


@cli.command()
def mcp():
    """Start the MCP server (stdio transport)."""
    from .mcp.server import main as mcp_main
    mcp_main()


@cli.command("webhook")
@click.option("--date", default=None, help="Date string (default: today)")
@click.option("--language", default="zh", help="Language code (default: zh)")
@click.option("--result", default="success", help="Result status: success or failure")
@click.option("--summary", default="Test webhook from horizon CLI", help="Summary text to send, or @path to read from file")
def webhook_test(date, language, result, summary):
    """Send a test webhook notification using current config."""
    load_dotenv()

    # Support @path syntax for reading summary from file
    if summary.startswith("@"):
        file_path = summary[1:]
        try:
            summary = Path(file_path).read_text(encoding="utf-8")
        except FileNotFoundError:
            console.print(f"[bold red]File not found: {file_path}[/bold red]")
            sys.exit(1)

    data_dir = Path("data")
    storage = StorageManager(data_dir=str(data_dir))

    try:
        config = storage.load_config()
    except FileNotFoundError:
        console.print("[bold red]Configuration file not found![/bold red]")
        sys.exit(1)

    webhook_config = config.webhook
    if not webhook_config or not webhook_config.enabled:
        console.print("[yellow]Webhook is not enabled in configuration.[/yellow]")
        sys.exit(0)

    from datetime import datetime, timezone

    if date is None:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    from .services.webhook import WebhookNotifier

    notifier = WebhookNotifier(webhook_config, console=console)

    variables = {
        "date": date,
        "language": language,
        "important_items": 0,
        "all_items": 0,
        "result": result,
        "timestamp": str(int(datetime.now(timezone.utc).timestamp())),
        "summary": summary,
    }

    console.print(f"[cyan]Sending test webhook...[/cyan]")
    console.print(f"  URL env:   {webhook_config.url_env}")
    console.print(f"  Date:      {date}")
    console.print(f"  Language:  {language}")
    console.print(f"  Result:    {result}")

    res = asyncio.run(notifier.notify(variables))
    if res is None:
        console.print("[yellow]Webhook skipped (disabled or URL not set).[/yellow]")
    else:
        status_code = res.get("status_code")
        response_text = res.get("response", "")
        console.print(f"  Status:    {status_code}")
        console.print(f"  Response:  {response_text}")


@cli.command()
@click.option("--interval", type=int, default=None, help="Hours between runs (interval mode)")
@click.option("--schedule", type=str, default=None, help="Daily run time UTC, e.g. '08:00' (schedule mode)")
@click.option("--hours", type=int, default=None, help="Override time window hours for each run")
def daemon(interval, schedule, hours):
    """Run as a daemon with continuous scheduling."""
    load_dotenv()

    data_dir = Path("data")
    storage = StorageManager(data_dir=str(data_dir))

    try:
        config = storage.load_config()
    except FileNotFoundError:
        console.print("[bold red]Configuration file not found![/bold red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]Error loading configuration: {e}[/bold red]")
        sys.exit(1)

    from .services.daemon import DaemonRunner

    runner = DaemonRunner(config, storage)

    # Determine mode from args
    mode = None
    if schedule:
        mode = "schedule"
    elif interval:
        mode = "interval"

    try:
        asyncio.run(runner.run(
            mode=mode,
            interval_hours=interval,
            schedule_time=schedule,
            force_hours=hours,
        ))
    except KeyboardInterrupt:
        console.print("\n[yellow]Daemon interrupted by user[/yellow]")
        sys.exit(0)


@cli.group()
def service():
    """Manage systemd service for Horizon daemon."""
    pass


@service.command()
@click.option("--interval", type=int, default=24, help="Hours between runs for install (default: 24)")
@click.option("--schedule", type=str, default=None, help="Daily run time UTC for install (e.g. '08:00')")
def install(interval, schedule):
    """Install Horizon as a systemd service."""
    from .services.systemd import install_service
    install_service(interval=interval, schedule=schedule)


@service.command()
def uninstall():
    """Uninstall the Horizon systemd service."""
    from .services.systemd import uninstall_service
    uninstall_service()


@service.command()
def restart():
    """Restart the Horizon systemd service."""
    from .services.systemd import restart_service
    restart_service()


@service.command()
def status():
    """Show the status of the Horizon systemd service."""
    from .services.systemd import status_service
    status_service()