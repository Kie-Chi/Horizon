"""Systemd service management for Horizon daemon."""

import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

from rich.console import Console

console = Console()

SERVICE_NAME = "horizon"
UNIT_FILE_PATH = Path(f"/etc/systemd/system/{SERVICE_NAME}.service")

UNIT_TEMPLATE = """\
[Unit]
Description=Horizon - AI-Driven Information Aggregation
After=network-online.target
Requires=network.target

[Service]
Type=simple
ExecStart={exec_start}
WorkingDirectory={work_dir}
Restart=on-failure
RestartSec=60
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"""


def _check_platform():
    """Check that we're running on Linux with systemd."""
    if platform.system() != "Linux":
        console.print("[red]systemd service management is only available on Linux.[/red]")
        sys.exit(1)

    # Check if systemd is present
    if not Path("/etc/systemd/system").is_dir():
        console.print("[red]systemd is not installed on this system.[/red]")
        sys.exit(1)


def _check_root():
    """Check that we have root privileges."""
    if os.geteuid() != 0:
        console.print("[red]Service management requires root privileges. Use sudo.[/red]")
        sys.exit(1)


def _detect_exec_start(
    interval: int = 24,
    schedule: Optional[str] = None,
) -> str:
    """Detect the appropriate ExecStart command for the service.

    Prefers the installed 'horizon' CLI entry point, falls back to
    'uv run horizon' if not found.

    Args:
        interval: Hours between runs (for interval mode).
        schedule: Schedule time string (for schedule mode, overrides interval).

    Returns:
        The ExecStart command string.
    """
    horizon_path = shutil.which("horizon")
    if horizon_path:
        cmd = horizon_path
    else:
        # Fall back to uv run horizon
        uv_path = shutil.which("uv")
        if uv_path:
            cmd = f"{uv_path} run horizon"
        else:
            # Fall back to python -m
            cmd = f"{sys.executable} -m src.main"

    # Build daemon args
    cmd += " daemon"
    if schedule:
        cmd += f" --schedule '{schedule}'"
    else:
        cmd += f" --interval {interval}"

    return cmd


def _detect_work_dir() -> str:
    """Detect the working directory for the service.

    Uses the current Horizon project directory if detectable,
    otherwise the current working directory.
    """
    # Try to find the project directory (where data/ and .env exist)
    cwd = os.getcwd()
    if Path(cwd, "data").is_dir():
        return cwd

    # Check common locations
    home = Path.home()
    horizon_dir = home / "Horizon"
    if horizon_dir.is_dir() and (horizon_dir / "data").is_dir():
        return str(horizon_dir)

    return cwd


def install_service(
    interval: int = 24,
    schedule: Optional[str] = None,
) -> None:
    """Install Horizon as a systemd service.

    Args:
        interval: Hours between runs (for interval mode).
        schedule: Schedule time string (for schedule mode, overrides interval).
    """
    _check_platform()
    _check_root()

    exec_start = _detect_exec_start(interval=interval, schedule=schedule)
    work_dir = _detect_work_dir()

    unit_content = UNIT_TEMPLATE.format(
        exec_start=exec_start,
        work_dir=work_dir,
    )

    console.print(f"[cyan]Generating systemd unit file...[/cyan]")
    console.print(f"  ExecStart: {exec_start}")
    console.print(f"  WorkingDirectory: {work_dir}")
    console.print(f"  Target: {UNIT_FILE_PATH}")
    console.print("")

    # Write unit file
    UNIT_FILE_PATH.write_text(unit_content)

    # Reload systemd
    console.print("[cyan]Reloading systemd daemon...[/cyan]")
    subprocess.run(["systemctl", "daemon-reload"], check=True)

    # Enable and start
    console.print("[cyan]Enabling and starting horizon service...[/cyan]")
    subprocess.run(["systemctl", "enable", "--now", SERVICE_NAME], check=True)

    console.print("[bold green]Horizon service installed and started successfully![/bold green]")
    console.print(f"  Use [cyan]horizon service status[/cyan] to check status")
    console.print(f"  Use [cyan]journalctl -u horizon -f[/cyan] to view logs")


def uninstall_service() -> None:
    """Uninstall the Horizon systemd service."""
    _check_platform()
    _check_root()

    if not UNIT_FILE_PATH.exists():
        console.print("[yellow]Horizon service is not installed.[/yellow]")
        return

    # Stop service
    console.print("[cyan]Stopping horizon service...[/cyan]")
    subprocess.run(["systemctl", "stop", SERVICE_NAME], check=False)

    # Disable service
    console.print("[cyan]Disabling horizon service...[/cyan]")
    subprocess.run(["systemctl", "disable", SERVICE_NAME], check=False)

    # Remove unit file
    console.print("[cyan]Removing unit file...[/cyan]")
    UNIT_FILE_PATH.unlink()

    # Reload systemd
    subprocess.run(["systemctl", "daemon-reload"], check=True)

    console.print("[bold green]Horizon service uninstalled successfully.[/bold green]")


def restart_service() -> None:
    """Restart the Horizon systemd service."""
    _check_platform()
    _check_root()

    if not UNIT_FILE_PATH.exists():
        console.print("[yellow]Horizon service is not installed. Run 'horizon service install' first.[/yellow]")
        sys.exit(1)

    console.print("[cyan]Restarting horizon service...[/cyan]")
    result = subprocess.run(["systemctl", "restart", SERVICE_NAME], capture_output=True, text=True)

    if result.returncode == 0:
        console.print("[bold green]Horizon service restarted successfully.[/bold green]")
    else:
        console.print(f"[red]Failed to restart service: {result.stderr}[/red]")
        sys.exit(1)


def status_service() -> None:
    """Show the status of the Horizon systemd service."""
    _check_platform()

    # status doesn't strictly require root, but systemctl may restrict info
    result = subprocess.run(
        ["systemctl", "status", SERVICE_NAME],
        capture_output=True,
        text=True,
    )

    if UNIT_FILE_PATH.exists():
        console.print(f"[cyan]Service unit file: {UNIT_FILE_PATH}[/cyan]")
        console.print(UNIT_FILE_PATH.read_text())
        console.print("")
    else:
        console.print("[yellow]No service unit file found.[/yellow]")

    console.print("[cyan]Service status:[/cyan]")
    console.print(result.stdout)
    if result.stderr:
        console.print(result.stderr)