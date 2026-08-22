from subprocess import CompletedProcess
from unittest.mock import Mock

import pytest

import maestral.autostart as autostart_module
from maestral.autostart import (
    AutoStart,
    AutoStartSystemd,
    SupportedImplementations,
)
from maestral.exceptions import MaestralApiError


def test_systemd_autostart_uses_requested_config(tmp_path, monkeypatch):
    monkeypatch.setattr(
        autostart_module,
        "get_available_implementation",
        Mock(return_value=SupportedImplementations.systemd),
    )
    monkeypatch.setattr(
        autostart_module, "get_command_path", Mock(return_value="/usr/bin/maestral")
    )
    monkeypatch.setattr(
        autostart_module,
        "get_data_path",
        lambda *parts: str(tmp_path / parts[-1]),
    )

    autostart = AutoStart("work")
    backend = autostart._impl

    assert isinstance(backend, AutoStartSystemd)
    assert backend.service_name == "maestral-daemon@work.service"
    assert backend.destination == str(tmp_path / "maestral-daemon@.service")
    assert backend.service_config["Service"]["ExecStart"] == (
        "/usr/bin/maestral start --foreground --config-name %i"
    )
    assert backend.service_config["Service"]["ExecStop"] == (
        "/usr/bin/maestral stop --config-name %i"
    )


@pytest.mark.parametrize("method", ["enable", "disable"])
def test_systemd_error_includes_stderr(method, monkeypatch):
    backend = object.__new__(AutoStartSystemd)
    backend.service_name = "maestral-daemon@work.service"
    run = Mock(return_value=CompletedProcess([], 1, stderr="systemd failed\n"))
    monkeypatch.setattr(autostart_module.subprocess, "run", run)

    with pytest.raises(MaestralApiError, match="systemd failed"):
        getattr(backend, method)()

    assert run.call_args.kwargs == {"capture_output": True, "text": True}
