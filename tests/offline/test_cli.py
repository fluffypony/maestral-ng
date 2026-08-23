import inspect
import logging
import os
from types import SimpleNamespace
from unittest.mock import Mock

import click
from click.testing import CliRunner

import maestral.cli.cli_core as cli_core_module
import maestral.cli.cli_info as cli_info_module
import maestral.cli.cli_maintenance as cli_maintenance_module
import maestral.config as config_module
import maestral.daemon as daemon_module
from maestral.autostart import AutoStart
from maestral.cli import main
from maestral.cli.core import OrderedGroup
from maestral.config import MaestralConfig, MaestralState
from maestral.daemon import MaestralClient, Start, start_maestral_daemon_process
from maestral.logging import scoped_logger
from maestral.main import Maestral
from maestral.notify import level_name_to_number, level_number_to_name
from maestral.utils.appdirs import get_log_path

TEST_TIMEOUT = 60


def test_official_dropbox_folder_warning_can_reselect(tmp_path, monkeypatch) -> None:
    official_path = tmp_path / "Dropbox"
    official_path.mkdir()
    safe_path = tmp_path / "Maestral files"
    monkeypatch.setattr(
        cli_core_module,
        "select_path",
        Mock(side_effect=[str(official_path), str(safe_path)]),
    )
    confirm = Mock(return_value=False)
    warning = Mock()
    monkeypatch.setattr(cli_core_module, "confirm", confirm)
    monkeypatch.setattr(cli_core_module, "warn", warning)

    selected = cli_core_module.select_dbx_path_dialog("test-config", allow_merge=True)

    assert selected == str(safe_path)
    confirm.assert_called_once_with("Do you still want to use this folder?")
    assert "official Dropbox client" in warning.call_args.args[0]


def test_maestral_root_does_not_trigger_official_client_warning(tmp_path) -> None:
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    (dropbox_path / ".maestral-root").touch()

    assert not cli_core_module._looks_like_official_dropbox_folder(str(dropbox_path))


def test_help() -> None:
    """Test help output without args and with --help arg."""
    runner = CliRunner()

    result_no_arg = runner.invoke(main)
    result_help_arg = runner.invoke(main, ["--help"])

    assert result_no_arg.exit_code == 2, result_no_arg.output
    assert result_help_arg.exit_code == 0, result_no_arg.output
    assert result_no_arg.output.startswith("Usage: main [OPTIONS] COMMAND [ARGS]")

    assert result_no_arg.output == result_help_arg.output


def test_config_cleanup_keeps_unlinked_recovery_journal(
    config_name: str, monkeypatch
) -> None:
    MaestralConfig(config_name)
    state = MaestralState(config_name)
    state.set(
        "recovery",
        "local_paths",
        {
            "/local.txt": {
                "path": "/local.txt",
                "identity": [1, 2, 3],
                "phase": "tracked",
                "source": "",
            }
        },
    )
    remove_configuration = Mock()
    monkeypatch.setattr(config_module, "list_configs", lambda: [config_name])
    monkeypatch.setattr(config_module, "remove_configuration", remove_configuration)
    monkeypatch.setattr(daemon_module, "is_running", lambda _name: False)

    callback = inspect.unwrap(cli_info_module.config_files.callback)
    callback(clean=True)

    remove_configuration.assert_not_called()


def test_config_list_skips_profile_which_cannot_load(
    config_name: str, monkeypatch
) -> None:
    good_conf = MaestralConfig(config_name)
    good_state = MaestralState(config_name)
    original_conf = config_module.MaestralConfig
    original_state = config_module.MaestralState
    table = Mock()
    console = Mock()

    def load_conf(name: str):
        if name == "broken":
            raise config_module.ConfigLoadError("cannot load")
        return original_conf(name)

    def load_state(name: str):
        if name == "broken":
            raise config_module.ConfigLoadError("cannot load")
        return original_state(name)

    monkeypatch.setattr(config_module, "list_configs", lambda: ["broken", config_name])
    monkeypatch.setattr(config_module, "MaestralConfig", load_conf)
    monkeypatch.setattr(config_module, "MaestralState", load_state)
    monkeypatch.setattr(cli_info_module, "rich_table", Mock(return_value=table))
    monkeypatch.setattr(cli_info_module, "Console", Mock(return_value=console))

    callback = inspect.unwrap(cli_info_module.config_files.callback)
    callback(clean=False)

    table.add_row.assert_called_once_with(
        config_name,
        good_state.get("account", "email"),
        cli_info_module.Text(
            good_conf.config_path,
            overflow="ellipsis",
            no_wrap=True,
        ),
    )
    console.print.assert_called_once_with(table)


def test_ordered_group_sections_are_per_instance() -> None:
    first = OrderedGroup("first")
    second = OrderedGroup("second")

    first.add_command(click.Command("one"), section="Commands")

    assert list(first.sections) == ["Commands"]
    assert second.sections == {}


def test_invalid_config() -> None:
    """Test failure of commands that require an existing config file"""

    for command in [
        ("stop",),
        ("pause",),
        ("resume",),
        ("auth", "status"),
        ("auth", "unlink"),
        ("sharelink", "create"),
        ("sharelink", "list"),
        ("sharelink", "revoke"),
        ("status",),
        ("filestatus",),
        ("activity",),
        ("history",),
        ("ls",),
        ("autostart",),
        ("selective-sync", "list"),
        ("selective-sync", "set", "include"),
        ("symlinks",),
        ("notify", "level"),
        ("notify", "snooze"),
        ("move-dir",),
        ("confirm-root",),
        ("rebuild-index",),
        ("revs",),
        ("diff",),
        ("restore",),
        ("log", "level"),
        ("log", "clear"),
        ("log", "show"),
        ("config", "get", "path"),
        ("config", "set", "path"),
        ("config", "show"),
    ]:
        runner = CliRunner()
        result = runner.invoke(main, [*command, "-c", "non-existent-config"])

        assert result.exit_code == 1, command
        assert (
            result.output == "! Configuration 'non-existent-config' does not exist. "
            "Use 'maestral config-files' to list all configurations.\n"
        )


def test_start_already_running(config_name: str) -> None:
    res = start_maestral_daemon_process(config_name, timeout=TEST_TIMEOUT)

    assert res is Start.Ok

    runner = CliRunner()
    result = runner.invoke(main, ["start", "-c", config_name])

    assert result.exit_code == 0, result.output
    assert "already running" in result.output


def test_start_failure_exits_without_startup_dialog(monkeypatch) -> None:
    wait_for_startup = Mock()
    monkeypatch.setattr(daemon_module, "is_running", Mock(return_value=False))
    monkeypatch.setattr(
        daemon_module,
        "start_maestral_daemon_process",
        Mock(return_value=Start.Failed),
    )
    monkeypatch.setattr(daemon_module, "wait_for_startup", wait_for_startup)

    result = CliRunner().invoke(main, ["start", "-c", "failed-start"])

    assert result.exit_code == 1
    assert "[FAILED]" in result.output
    assert "Please check logs" in result.output
    wait_for_startup.assert_not_called()


def test_ls_prints_collected_entries_when_piped(monkeypatch) -> None:
    m = Mock()
    m.list_folder_iterator.return_value = iter(
        [
            [SimpleNamespace(name="zeta")],
            [SimpleNamespace(name="alpha")],
        ]
    )
    console = Mock()

    monkeypatch.setattr(cli_info_module, "Console", Mock(return_value=console))
    monkeypatch.setattr(
        cli_info_module,
        "sys",
        SimpleNamespace(stdout=SimpleNamespace(isatty=lambda: False)),
    )

    callback = inspect.unwrap(cli_info_module.ls.callback)
    callback(m, long=False, dropbox_path="/", include_deleted=False)

    console.print.assert_called_once_with("alpha\nzeta")


def test_ls_handles_empty_terminal_listing(monkeypatch) -> None:
    m = Mock()
    m.list_folder_iterator.return_value = iter([[]])
    console = Mock()

    monkeypatch.setattr(cli_info_module, "Console", Mock(return_value=console))
    monkeypatch.setattr(
        cli_info_module,
        "sys",
        SimpleNamespace(stdout=SimpleNamespace(isatty=lambda: True)),
    )

    callback = inspect.unwrap(cli_info_module.ls.callback)
    callback(m, long=False, dropbox_path="/", include_deleted=False)

    console.print.assert_called_once_with()


def test_confirm_root_calls_public_api() -> None:
    m = Mock(dropbox_path="/Dropbox")

    callback = inspect.unwrap(cli_maintenance_module.confirm_root.callback)
    callback(m, yes=True)

    m.confirm_dropbox_directory.assert_called_once_with()


def test_stop(config_name: str) -> None:
    res = start_maestral_daemon_process(config_name, timeout=TEST_TIMEOUT)
    assert res is Start.Ok

    runner = CliRunner()
    result = runner.invoke(main, ["stop", "-c", config_name])

    assert result.exit_code == 0, result.output


def test_filestatus(m: Maestral) -> None:
    runner = CliRunner()
    result = runner.invoke(
        main, ["filestatus", os.path.expanduser("~"), "-c", m.config_name]
    )

    assert result.exit_code == 0, result.output
    assert result.output == "unwatched\n"

    invalid_path = os.path.join(os.path.expanduser("~"), "invalid-dir")
    result = runner.invoke(main, ["filestatus", invalid_path, "-c", m.config_name])

    # the exception will be already raised by click's argument check
    assert result.exit_code == 2
    assert isinstance(result.exception, SystemExit)
    assert "invalid-dir" in result.output
    assert "does not exist" in result.output


def test_autostart(m: Maestral) -> None:
    autostart = AutoStart(m.config_name)
    autostart.disable()

    runner = CliRunner()
    result = runner.invoke(main, ["autostart", "-c", m.config_name])

    assert result.exit_code == 0, result.output
    assert "disabled" in result.output

    result = runner.invoke(main, ["autostart", "-Y", "-c", m.config_name])

    if autostart.implementation:
        if result.exit_code == 0:
            assert "Enabled" in result.output
            assert autostart.enabled
        else:
            # TODO: be more specific here
            assert result.exception is not None
    else:
        assert "not supported" in result.output
        assert not autostart.enabled

    result = runner.invoke(main, ["autostart", "-N", "-c", m.config_name])

    assert result.exit_code == 0, result.output
    assert "Disabled" in result.output
    assert not autostart.enabled


def test_selective_sync_list(m: Maestral) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["selective-sync", "list", "-c", m.config_name])

    assert result.exit_code == 0, result.output
    assert result.output == "Mode: exclude\nNo selected paths.\n"


def test_selective_sync_set_raises_not_linked_error(m: Maestral) -> None:
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["selective-sync", "set", "include", "test", "-c", m.config_name],
    )

    assert result.exit_code == 1
    assert "No Dropbox account linked" in result.output


def test_selective_sync_help_describes_both_modes() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["selective-sync", "set", "--help"])

    assert result.exit_code == 0, result.output
    assert "EXCLUDE syncs everything except" in result.output
    assert "INCLUDE syncs only" in result.output


def test_config_command_cannot_split_selective_sync_update(m: Maestral) -> None:
    result = CliRunner().invoke(
        main,
        ["config", "set", "selective_sync_mode", "include", "-c", m.config_name],
    )

    assert result.exit_code == 0, result.output
    assert "Use the selective-sync or symlink API" in result.output
    assert m.selective_sync_mode == "exclude"


def test_symlink_help_describes_unmanaged_paths() -> None:
    result = CliRunner().invoke(main, ["symlinks", "--help"])

    assert result.exit_code == 0, result.output
    assert "symbolic links unmanaged" in result.output
    assert "overwrite an ignored link path" in result.output


def test_symlink_command_sets_policy(m: Maestral) -> None:
    result = CliRunner().invoke(main, ["symlinks", "ignore", "-c", m.config_name])

    assert result.exit_code == 0, result.output
    assert result.output == "✓ Symbolic-link policy set to ignore.\n"
    assert MaestralConfig(m.config_name).get("sync", "ignore_symlinks") is True


def test_notify_level(config_name: str) -> None:
    start_maestral_daemon_process(config_name, timeout=TEST_TIMEOUT)
    m = MaestralClient(config_name)

    runner = CliRunner()
    result = runner.invoke(main, ["notify", "level", "-c", m.config_name])

    level_name = level_number_to_name(m.notification_level)

    assert result.exit_code == 0, result.output
    assert level_name in result.output

    level_name = "SYNCISSUE"
    level_number = level_name_to_number(level_name)
    result = runner.invoke(main, ["notify", "level", level_name, "-c", m.config_name])

    assert result.exit_code == 0, result.output
    assert level_name in result.output
    assert m.notification_level == level_number

    result = runner.invoke(main, ["notify", "level", "INVALID", "-c", m.config_name])

    assert result.exit_code == 2
    assert isinstance(result.exception, SystemExit)


def test_notify_snooze(config_name: str) -> None:
    start_maestral_daemon_process(config_name, timeout=TEST_TIMEOUT)
    m = MaestralClient(config_name)

    runner = CliRunner()
    result = runner.invoke(main, ["notify", "snooze", "20", "-c", m.config_name])

    assert result.exit_code == 0, result.output
    assert 0 < m.notification_snooze <= 20

    result = runner.invoke(main, ["notify", "snooze", "0", "-c", m.config_name])

    assert result.exit_code == 0, result.output
    assert m.notification_snooze == 0


def test_log_level(m: Maestral) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["log", "level", "-c", m.config_name])

    level_name = logging.getLevelName(m.log_level)

    assert result.exit_code == 0, result.output
    assert level_name in result.output

    result = runner.invoke(main, ["log", "level", "DEBUG", "-c", m.config_name])
    assert result.exit_code == 0, result.output
    assert "DEBUG" in result.output

    result = runner.invoke(main, ["notify", "level", "INVALID", "-c", m.config_name])
    assert result.exit_code == 2
    assert isinstance(result.exception, SystemExit)


def test_log_show(m: Maestral) -> None:
    # log a message
    logger = scoped_logger("maestral", m.config_name)
    logger.info("Hello from pytest!")
    runner = CliRunner()
    result = runner.invoke(main, ["log", "show", "-c", m.config_name])

    assert result.exit_code == 0, result.output
    assert "Hello from pytest!" in result.output


def test_log_clear(m: Maestral) -> None:
    # log a message
    logger = scoped_logger("maestral", m.config_name)
    logger.info("Hello from pytest!")
    runner = CliRunner()
    result = runner.invoke(main, ["log", "show", "-c", m.config_name])

    assert result.exit_code == 0, result.output
    assert "Hello from pytest!" in result.output

    # Stop connection helper to prevent spurious log messages.
    m.manager.shutdown()

    # clear the logs
    result = runner.invoke(main, ["log", "clear", "-c", m.config_name])
    assert result.exit_code == 0, result.output

    logfile = get_log_path("maestral", f"{m.config_name}.log")
    with open(logfile) as f:
        log_content = f.read()

    assert log_content == ""
