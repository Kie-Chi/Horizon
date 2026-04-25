"""Legacy CLI entry point for Horizon.

This module is kept for backward compatibility via the horizon-mcp and
horizon-wizard script entries in pyproject.toml.  The unified CLI is in
src.cli.
"""

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


def main():
    """Main CLI entry point."""
    print_banner()

    parser = argparse.ArgumentParser(description="Horizon - AI-Driven Information Aggregation System")
    parser.add_argument("--hours", type=int, help="Force fetch from last N hours")
    args = parser.parse_args()

    try:
        # Load environment variables from .env file
        load_dotenv()

        # Ensure we're in the project directory or use data/ in current dir
        data_dir = Path("data")

        # Initialize storage manager
        storage = StorageManager(data_dir=str(data_dir))

        # Load configuration
        try:
            config = storage.load_config()
        except FileNotFoundError:
            console.print("[bold red]Configuration file not found![/bold red]\n")
            console.print(
                "Run [bold cyan]horizon wizard[/bold cyan] to launch the interactive setup wizard,\n"
                "or create [cyan]data/config.json[/cyan] manually.\n"
            )
            sys.exit(1)
        except Exception as e:
            console.print(f"[bold red]Error loading configuration: {e}[/bold red]")
            sys.exit(1)

        # Create and run orchestrator
        orchestrator = HorizonOrchestrator(config, storage)
        asyncio.run(orchestrator.run(force_hours=args.hours))

    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user[/yellow]")
        sys.exit(0)
    except Exception as e:
        console.print(f"\n[bold red]Fatal error: {e}[/bold red]")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()