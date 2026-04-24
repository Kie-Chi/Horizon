"""Daemon runner for continuous Horizon operation."""

import asyncio
import signal
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from rich.console import Console

from ..models import Config
from ..orchestrator import HorizonOrchestrator
from ..storage.manager import StorageManager


console = Console()


class DaemonRunner:
    """Manages continuous daemon operation with scheduling."""

    def __init__(self, config: Config, storage: StorageManager):
        self.config = config
        self.storage = storage
        self.orchestrator = HorizonOrchestrator(config, storage)
        self._stop_event = asyncio.Event()
        self._loop_count = 0

    def request_stop(self):
        """Signal the daemon to stop gracefully."""
        self._stop_event.set()

    async def run(
        self,
        mode: Optional[str] = None,
        interval_hours: Optional[int] = None,
        schedule_time: Optional[str] = None,
        force_hours: Optional[int] = None,
    ):
        """Run the daemon loop.

        Args:
            mode: Override scheduling mode ("interval" or "schedule").
            interval_hours: Override interval hours.
            schedule_time: Override schedule time (HH:MM UTC).
            force_hours: Override time window for each run.
        """
        # Resolve settings from CLI args or config
        daemon_cfg = self.config.daemon
        effective_mode = mode or (daemon_cfg.mode if daemon_cfg else "interval")
        effective_interval = interval_hours or (daemon_cfg.interval_hours if daemon_cfg else self.config.filtering.time_window_hours)
        effective_schedule = schedule_time or (daemon_cfg.schedule_time if daemon_cfg else "08:00")
        effective_force_hours = force_hours or self.config.filtering.time_window_hours

        # Setup signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.request_stop)

        console.print(f"[bold cyan]Horizon daemon starting[/bold cyan]")
        console.print(f"  Mode: {effective_mode}")
        if effective_mode == "interval":
            console.print(f"  Interval: every {effective_interval} hours")
        else:
            console.print(f"  Schedule: daily at {effective_schedule} UTC")
        console.print(f"  Time window: {effective_force_hours} hours")
        console.print("")

        if effective_mode == "interval":
            await self._run_interval(effective_interval, effective_force_hours)
        elif effective_mode == "schedule":
            await self._run_schedule(effective_schedule, effective_force_hours)
        else:
            console.print(f"[red]Unknown mode: {effective_mode}. Use 'interval' or 'schedule'.[/red]")
            sys.exit(1)

    async def _run_interval(self, interval_hours: int, force_hours: int):
        """Run in interval mode: execute then sleep for interval_hours."""
        while not self._stop_event.is_set():
            self._loop_count += 1
            console.print(
                f"[bold cyan]--- Run #{self._loop_count} at "
                f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC ---[/bold cyan]"
            )

            try:
                await self.orchestrator.run(force_hours=force_hours)
            except Exception as e:
                console.print(f"[red]Run failed: {e}[/red]")

            if self._stop_event.is_set():
                break

            console.print(
                f"[dim]Next run in {interval_hours} hours...[/dim]"
            )
            # Sleep but check stop_event every 60 seconds for responsiveness
            sleep_seconds = interval_hours * 3600
            await self._interruptible_sleep(sleep_seconds)

        console.print("[yellow]Daemon stopped.[/yellow]")

    async def _run_schedule(self, schedule_time: str, force_hours: int):
        """Run in schedule mode: run daily at the specified UTC time."""
        while not self._stop_event.is_set():
            self._loop_count += 1
            now = datetime.now(timezone.utc)

            console.print(
                f"[bold cyan]--- Run #{self._loop_count} at "
                f"{now.strftime('%Y-%m-%d %H:%M:%S')} UTC ---[/bold cyan]"
            )

            try:
                await self.orchestrator.run(force_hours=force_hours)
            except Exception as e:
                console.print(f"[red]Run failed: {e}[/red]")

            if self._stop_event.is_set():
                break

            # Calculate seconds until next schedule time
            next_run = self._next_schedule_time(schedule_time)
            wait_seconds = max(1, (next_run - now).total_seconds())

            console.print(
                f"[dim]Next run at {next_run.strftime('%Y-%m-%d %H:%M:%S')} UTC "
                f"({_format_duration(wait_seconds)})...[/dim]"
            )
            await self._interruptible_sleep(wait_seconds)

        console.print("[yellow]Daemon stopped.[/yellow]")

    def _next_schedule_time(self, schedule_time: str) -> datetime:
        """Calculate the next occurrence of schedule_time from now.

        Args:
            schedule_time: Time in HH:MM format (UTC).

        Returns:
            Next datetime occurrence.
        """
        now = datetime.now(timezone.utc)
        parts = schedule_time.strip().split(":")
        hour = int(parts[0])
        minute = int(parts[1]) if len(parts) > 1 else 0

        # Today's scheduled time
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

        # If already past today's time, schedule for tomorrow
        if target <= now:
            target += timedelta(days=1)

        return target

    async def _interruptible_sleep(self, seconds: float):
        """Sleep for seconds, but wake up if stop_event is set.

        Checks every 60 seconds to allow responsive shutdown.

        Args:
            seconds: Total seconds to sleep.
        """
        check_interval = 60  # Check stop_event every minute
        remaining = seconds

        while remaining > 0 and not self._stop_event.is_set():
            chunk = min(remaining, check_interval)
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=chunk,
                )
                # stop_event was set during wait
                return
            except asyncio.TimeoutError:
                remaining -= chunk

    @staticmethod
    def _format_duration(seconds: float) -> str:
        """Format seconds into a human-readable duration string."""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        if hours > 0:
            return f"{hours}h {minutes}m"
        return f"{minutes}m"