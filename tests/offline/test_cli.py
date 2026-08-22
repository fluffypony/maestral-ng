import inspect
import logging
from types import SimpleNamespace
from unittest.mock import Mock

import click
from click.testing import CliRunner

import maestral.cli.cli_info as cli_info_module
import maestral.daemon as daemon_module
from maestral.autostart import AutoStart
from maestral.cli import main
from maestral.cli.core import OrderedGroup
from maestral.daemon import MaestralClient, Start, start_maestral_daemon_process
from maestral.logging import scoped_logger
from maestral.main import Maestral
from maestral.notify import level_name_to_number, level_number_to_name
from maestral.utils.appdirs import get_log_path

TEST_TIMEOUT = 60


def test_help() -> None:
    """Test help output without args and with --help arg."""
    runner = CliRunner()

    result_no_arg = runner.invoke(main)
    result_help_arg = runner.invoke(main, ["--help"])

    assert result_no_arg.exit_code == 2, result_no_arg.output
    assert result_help_arg.exit_code == 0, result_no_arg.output
    assert result_no_arg.output.startswith("Usage: main [OPTIONS] COMMAND [ARGS]")

    assert result_no_arg.output == result_help_arg.output


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
        ("excluded", "add"),
        ("excluded", "list"),
        ("excluded", "remove"),
        ("notify", "level"),
        ("notify", "snooze"),
        ("move-dir",),
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


def test_stop(config_name: str) -> None:
    res = start_maestral_daemon_process(config_name, timeout=TEST_TIMEOUT)
    assert res is Start.Ok

    runner = CliRunner()
    result = runner.invoke(main, ["stop", "-c", config_name])

    assert result.exit_code == 0, result.output


def test_filestatus(m: Maestral) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["filestatus", "/usr", "-c", m.config_name])

    assert result.exit_code == 0, result.output
    assert result.output == "unwatched\n"

    result = runner.invoke(main, ["filestatus", "/invalid-dir", "-c", m.config_name])

    # the exception will be already raised by click's argument check
    assert result.exit_code == 2
    assert isinstance(result.exception, SystemExit)
    assert "'/invalid-dir' does not exist" in result.output


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


def test_excluded_list(m: Maestral) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["excluded", "list", "-c", m.config_name])

    assert result.exit_code == 0, result.output
    assert result.output == "No excluded files or folders.\n"


def test_excluded_add_raises_not_linked_error(m: Maestral) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["excluded", "add", "test", "-c", m.config_name])

    assert result.exit_code == 1
    assert "No Dropbox account linked" in result.output


def test_excluded_remove_raises_not_running_error(m: Maestral) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["excluded", "remove", "test", "-c", m.config_name])

    assert result.exit_code == 1
    assert "Maestral daemon is not running" in result.output


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
    m.manager._connection_helper_running = False
    m.manager.connection_helper.join()

    # clear the logs
    result = runner.invoke(main, ["log", "clear", "-c", m.config_name])
    assert result.exit_code == 0, result.output

    logfile = get_log_path("maestral", f"{m.config_name}.log")
    with open(logfile) as f:
        log_content = f.read()

    assert log_content == ""
