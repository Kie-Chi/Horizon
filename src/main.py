"""CLI entry point for Horizon."""

import argparse
import asyncio
import sys
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console

from .storage.manager import StorageManager
from .orchestrator import HorizonOrchestrator


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


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser with subcommands."""
    parser = argparse.ArgumentParser(
        description="Horizon - AI-Driven Information Aggregation System",
    )
    parser.add_argument("--hours", type=int, help="Force fetch from last N hours")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # daemon subcommand
    daemon_parser = subparsers.add_parser(
        "daemon",
        help="Run as a daemon (continuous loop)",
    )
    daemon_parser.add_argument(
        "--interval", type=int, default=None,
        help="Hours between runs (interval mode, default: from config or 24)",
    )
    daemon_parser.add_argument(
        "--schedule", type=str, default=None,
        help="Daily run time in UTC, e.g. '08:00' (schedule mode)",
    )
    daemon_parser.add_argument(
        "--hours", type=int, default=None,
        help="Override time window hours for each run",
    )

    # service subcommand
    service_parser = subparsers.add_parser(
        "service",
        help="Manage systemd service (install/uninstall/restart/status)",
    )
    service_parser.add_argument(
        "action",
        choices=["install", "uninstall", "restart", "status"],
        help="Service action to perform",
    )
    service_parser.add_argument(
        "--interval", type=int, default=24,
        help="Hours between runs for install (default: 24)",
    )
    service_parser.add_argument(
        "--schedule", type=str, default=None,
        help="Daily run time UTC for install (e.g. '08:00')",
    )

    return parser


def _load_config_and_storage():
    """Load config and initialize storage.

    Returns:
        Tuple of (Config, StorageManager).
    """
    load_dotenv()
    data_dir = Path("data")
    storage = StorageManager(data_dir=str(data_dir))

    try:
        config = storage.load_config()
    except FileNotFoundError:
        console.print("[bold red]Configuration file not found![/bold red]\n")
        console.print(
            "Run [bold cyan]uv run horizon-wizard[/bold cyan] to launch the interactive setup wizard,\n"
            "or create [cyan]data/config.json[/cyan] manually.\n"
        )
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]Error loading configuration: {e}[/bold red]")
        sys.exit(1)

    return config, storage


def cmd_run(hours):
    """Default run command: execute once and exit."""
    config, storage = _load_config_and_storage()
    orchestrator = HorizonOrchestrator(config, storage)
    asyncio.run(orchestrator.run(force_hours=hours))


def cmd_daemon(args):
    """Daemon command: run continuously with scheduling."""
    config, storage = _load_config_and_storage()

    from .services.daemon import DaemonRunner

    runner = DaemonRunner(config, storage)

    # Determine mode from args
    mode = None
    if args.schedule:
        mode = "schedule"
    elif args.interval:
        mode = "interval"

    asyncio.run(runner.run(
        mode=mode,
        interval_hours=args.interval,
        schedule_time=args.schedule,
        force_hours=args.hours,
    ))


def cmd_service(args):
    """Service command: manage systemd service."""
    from .services.systemd import (
        install_service,
        uninstall_service,
        restart_service,
        status_service,
    )

    actions = {
        "install": lambda: install_service(
            interval=args.interval,
            schedule=args.schedule,
        ),
        "uninstall": uninstall_service,
        "restart": restart_service,
        "status": status_service,
    }

    actions[args.action]()


def main():
    """Main CLI entry point."""
    print_banner()

    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.command == "daemon":
            cmd_daemon(args)
        elif args.command == "service":
            cmd_service(args)
        else:
            # No subcommand: default one-shot run
            cmd_run(args.hours)
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user[/yellow]")
        sys.exit(0)
    except SystemExit:
        raise
    except Exception as e:
        console.print(f"\n[bold red]Fatal error: {e}[/bold red]")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()