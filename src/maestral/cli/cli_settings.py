from __future__ import annotations

from typing import TYPE_CHECKING

import click

from .common import convert_api_errors, existing_config_option, inject_client
from .core import DropboxPath
from .output import echo, ok

if TYPE_CHECKING:
    from ..main import Maestral


@click.command(
    help="""
Automatically start the sync daemon on login.

A systemd or launchd service, or a Windows user-logon entry, will start the sync daemon
for the given configuration.
""",
)
@click.option("--yes", "-Y", is_flag=True, default=False)
@click.option("--no", "-N", is_flag=True, default=False)
@existing_config_option
def autostart(yes: bool, no: bool, config_name: str) -> None:
    from ..autostart import AutoStart

    auto_start = AutoStart(config_name)

    if not auto_start.implementation:
        echo(
            "Autostart is currently not supported for your platform.\n"
            "Autostart requires systemd on Linux, launchd on macOS, or Windows."
        )
        return

    if yes or no:
        if yes:
            auto_start.enable()
            ok("Enabled start on login.")
        else:
            auto_start.disable()
            ok("Disabled start on login.")
    else:
        if auto_start.enabled:
            echo("Autostart is enabled. Use -N to disable.")
        else:
            echo("Autostart is disabled. Use -Y to enable.")


@click.group(
    name="selective-sync",
    help="Choose which Dropbox paths Maestral syncs.",
)
def selective_sync() -> None:
    pass


@selective_sync.command(
    name="list",
    help="Show the selective-sync mode and its selected paths.",
)
@inject_client(fallback=True, existing_config=True)
def selective_sync_list(m: Maestral) -> None:
    echo(f"Mode: {m.selective_sync_mode}")

    if len(m.selective_sync_paths) == 0:
        echo("No selected paths.")
    else:
        for item in sorted(m.selective_sync_paths):
            echo(item)


@selective_sync.command(
    name="set",
    help="""
Atomically replace the selective-sync mode and all selected paths.

EXCLUDE syncs everything except the named paths. INCLUDE syncs only the named paths
and keeps the parent folders needed to reach them. An empty INCLUDE selection syncs no
Dropbox content.
""",
)
@click.argument("mode", type=click.Choice(["exclude", "include"]))
@click.argument("dropbox_paths", type=DropboxPath(), nargs=-1)
@inject_client(fallback=True, existing_config=True)
@convert_api_errors
def selective_sync_set(m: Maestral, mode: str, dropbox_paths: list[str]) -> None:
    m.set_selective_sync(mode, dropbox_paths)
    ok(f"Selective sync set to {mode} mode.")


@click.command(
    name="symlinks",
    help="""
Get or set the local symbolic-link policy.

ERROR reports symbolic links that Dropbox cannot accept. IGNORE leaves local symbolic
links unmanaged. Maestral will not warn about, upload, delete remotely, or overwrite an
ignored link path.
""",
)
@click.argument(
    "policy",
    required=False,
    type=click.Choice(["error", "ignore"]),
)
@inject_client(fallback=True, existing_config=True)
def symlinks(m: Maestral, policy: str | None) -> None:
    if policy is None:
        current_policy = "ignore" if m.ignore_symlinks else "error"
        echo(f"Symbolic-link policy: {current_policy}.")
    else:
        m.ignore_symlinks = policy == "ignore"
        ok(f"Symbolic-link policy set to {policy}.")


@click.group(help="Manage desktop notifications.")
def notify() -> None:
    pass


@notify.command(
    name="level",
    help="Get or set the level for desktop notifications.",
)
@click.argument(
    "level_name",
    required=False,
    type=click.Choice(["ERROR", "SYNCISSUE", "FILECHANGE"], case_sensitive=False),
)
@inject_client(fallback=True, existing_config=True)
def notify_level(m: Maestral, level_name: str) -> None:
    from .. import notify as _notify

    if level_name:
        m.notification_level = _notify.level_name_to_number(level_name)
        ok(f"Notification level set to {level_name}.")
    else:
        level_name = _notify.level_number_to_name(m.notification_level)
        echo(f"Notification level: {level_name}.")


@notify.command(
    name="snooze",
    help="Snooze desktop notifications of file changes.",
)
@click.argument("minutes", type=click.IntRange(min=0))
@inject_client(fallback=True, existing_config=True)
def notify_snooze(m: Maestral, minutes: int) -> None:
    m.notification_snooze = minutes

    if minutes > 0:
        ok(f"Notifications snoozed for {minutes} min. Set snooze to 0 to reset.")
    else:
        ok("Notifications enabled.")


@click.group(help="View and manage bandwidth limits. Changes take effect immediately.")
def bandwidth_limit() -> None:
    pass


@bandwidth_limit.command(
    name="up", help="Get / set bandwidth limit for uploads in MB/sec (0 = unlimited)."
)
@click.argument(
    "mb_per_second",
    required=False,
    type=click.FLOAT,
)
@inject_client(fallback=True, existing_config=True)
def bandwidth_limit_up(m: Maestral, mb_per_second: float | None) -> None:
    if mb_per_second is not None:
        m.bandwidth_limit_up = mb_per_second * 10**6
        speed_str = f"{mb_per_second} MB/sec" if mb_per_second > 0 else "unlimited"
        ok(f"Upload bandwidth limit set to {speed_str}.")
    else:
        mb_per_second = m.bandwidth_limit_up / 10**6
        echo(f"{mb_per_second} MB/sec" if mb_per_second > 0 else "unlimited")


@bandwidth_limit.command(
    name="down",
    help="Get / set bandwidth limit for downloads in MB/sec (0 = unlimited).",
)
@click.argument(
    "mb_per_second",
    required=False,
    type=click.FLOAT,
)
@inject_client(fallback=True, existing_config=True)
def bandwidth_limit_down(m: Maestral, mb_per_second: float | None) -> None:
    if mb_per_second is not None:
        m.bandwidth_limit_down = mb_per_second * 10**6
        speed_fmt = f"{mb_per_second} MB/sec" if mb_per_second > 0 else "unlimited"
        ok(f"Download bandwidth limit set to {speed_fmt}.")
    else:
        mb_per_second = m.bandwidth_limit_down / 10**6
        echo(f"{mb_per_second} MB/sec" if mb_per_second > 0 else "unlimited")
