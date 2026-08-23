import asyncio
import os
import signal
import sqlite3
import stat
import subprocess
import sys
import threading
import time
import uuid
from unittest.mock import Mock

import pytest

import maestral.daemon as daemon_module
import maestral.logging as logging_module
from maestral.config import validate_config_name
from maestral.daemon import (
    CommunicationError,
    Lock,
    MaestralClient,
    Start,
    Stop,
    sockpath_for_config,
    start_maestral_daemon_process,
    stop_maestral_daemon_process,
)
from maestral.exceptions import NotLinkedError
from maestral.main import Maestral
from maestral.rpc import JsonRpcConnection

# locking tests


def test_locking_from_same_thread(tmp_path):
    path = f"{tmp_path}/test-lock-{uuid.uuid4()}"

    # initialise lock
    lock = Lock.singleton(path)
    assert not lock.locked()

    # acquire lock
    res = lock.acquire()
    assert res
    assert lock.locked()

    # try to reacquire
    res = lock.acquire()
    assert not res
    assert lock.locked()

    # check pid of locking process
    assert lock.locking_pid() == os.getpid()

    # release lock
    lock.release()
    assert not lock.locked()

    # try to re-release lock
    with pytest.raises(RuntimeError):
        lock.release()


def test_locking_threaded(tmp_path):
    path = f"{tmp_path}/test-lock-{uuid.uuid4()}"

    # initialise lock
    lock = Lock.singleton(path)
    assert not lock.locked()

    # acquire lock from thread

    def acquire_in_thread():
        lock_thread = Lock.singleton(path)
        lock_thread.acquire()

    t = threading.Thread(
        target=acquire_in_thread,
        daemon=True,
    )
    t.start()
    t.join()

    # check that lock is acquired
    assert lock.locked()

    # try to re-acquire
    res = lock.acquire()
    assert not res
    assert lock.locked()

    # check pid of locking process
    assert lock.locking_pid() == os.getpid()

    # release lock
    lock.release()
    assert not lock.locked()

    # try to re-release lock
    with pytest.raises(RuntimeError):
        lock.release()


def test_locking_multiprocess(tmp_path):
    path = f"{tmp_path}/test-lock-{uuid.uuid4()}"

    # initialise lock
    lock = Lock.singleton(path)
    assert not lock.locked()

    # try to release lock, will fail because it is not acquired
    with pytest.raises(RuntimeError):
        lock.release()

    # acquire lock from different process

    cmd = (
        "import time; from maestral.daemon import Lock; "
        f"l = Lock.singleton({path!r}); l.acquire(); "
        "time.sleep(60);"
    )

    p = subprocess.Popen([sys.executable, "-c", cmd])

    deadline = time.monotonic() + 10
    while not lock.locked() and time.monotonic() < deadline:
        assert p.poll() is None
        time.sleep(0.05)

    # check that lock is acquired
    assert lock.locked()

    # try to re-acquire
    res = lock.acquire()
    assert not res
    assert lock.locked()

    # try to release lock, will fail because it is owned by a different process
    with pytest.raises(RuntimeError):
        lock.release()

    # A Windows virtual-environment launcher can start the base interpreter in a
    # second process. The PID sidecar identifies that interpreter, not the launcher.
    locking_pid = lock.locking_pid()
    if daemon_module._is_windows():
        assert locking_pid is not None and locking_pid > 0
    else:
        assert locking_pid == p.pid

    # Release lock by terminating the process.
    if locking_pid is not None and locking_pid != p.pid:
        os.kill(locking_pid, signal.SIGTERM)
    p.terminate()
    p.wait()
    assert not lock.locked()


def test_locked_uses_read_only_probe(tmp_path, monkeypatch):
    if daemon_module._is_windows():
        pytest.skip("Windows must probe the file lock to read its state")

    lock = Lock.singleton(str(tmp_path / f"test-lock-{uuid.uuid4()}"))
    acquire = Mock(side_effect=AssertionError("locked() must not acquire the lock"))

    monkeypatch.setattr(lock, "acquire", acquire)
    monkeypatch.setattr(lock, "locking_pid", Mock(return_value=123))

    assert lock.locked()
    acquire.assert_not_called()


def test_wait_for_startup_fails_when_child_exits(monkeypatch):
    connection = Mock()
    connection.request.side_effect = CommunicationError("not ready")
    process = Mock(returncode=7)
    process.poll.return_value = 7

    monkeypatch.setattr(
        daemon_module, "JsonRpcConnection", Mock(return_value=connection)
    )
    monkeypatch.setattr(
        daemon_module,
        "endpoint_for_config",
        Mock(return_value=Mock()),
    )

    with pytest.raises(ChildProcessError, match="status 7"):
        daemon_module.wait_for_startup("test-config", timeout=30, process=process)

    process.poll.assert_called_once_with()
    connection.close.assert_called_once_with()


def test_start_process_passes_config_as_argv_and_reaps(monkeypatch):
    process = Mock()
    popen = Mock(return_value=process)
    wait_for_startup = Mock()
    reaper = Mock()
    thread = Mock(return_value=reaper)

    monkeypatch.setattr(daemon_module, "is_running", Mock(return_value=False))
    monkeypatch.setattr(daemon_module.subprocess, "Popen", popen)
    monkeypatch.setattr(daemon_module, "wait_for_startup", wait_for_startup)
    monkeypatch.setattr(daemon_module.threading, "Thread", thread)

    result = daemon_module.start_maestral_daemon_process("work-config", timeout=4)

    assert result is Start.Ok
    command = popen.call_args.args[0]
    assert command[-1] == "work-config"
    assert "work-config" not in command[-2]
    wait_for_startup.assert_called_once_with("work-config", 4, process)
    assert thread.call_args.kwargs["target"] == process.wait
    assert thread.call_args.kwargs["daemon"] is True
    reaper.start.assert_called_once_with()


@pytest.mark.parametrize(
    "config_name",
    [
        "",
        ".",
        "..",
        "...",
        "two words",
        "quoted'name",
        'quoted"name',
        "../escape",
        "name/slash",
    ],
)
def test_config_name_rejects_unsupported_characters(config_name):
    with pytest.raises(ValueError):
        validate_config_name(config_name)


@pytest.mark.parametrize("config_name", ["maestral", "work-2", "work_test", "a.b"])
def test_config_name_accepts_safe_characters(config_name):
    assert validate_config_name(config_name) == config_name


def test_daemon_logs_exception_without_args(monkeypatch):
    logger = Mock()
    lock = Mock()
    lock.acquire.return_value = True

    monkeypatch.setattr(logging_module, "setup_logging", Mock())
    monkeypatch.setattr(logging_module, "scoped_logger", Mock(return_value=logger))
    monkeypatch.setattr(daemon_module, "maestral_lock", Mock(return_value=lock))
    monkeypatch.setattr(daemon_module, "SystemdNotifier", Mock(return_value=Mock()))
    monkeypatch.setattr(asyncio, "new_event_loop", Mock(side_effect=RuntimeError()))

    daemon_module.start_maestral_daemon("test-config")

    logger.error.assert_called_once_with("", exc_info=True)
    lock.release.assert_called_once_with()


# daemon lifecycle tests


def test_start_enum_has_uninitialized_state() -> None:
    assert Start.Uninitialized.value == 3


def test_lifecycle(config_name: str) -> None:
    # start daemon process
    res_start = start_maestral_daemon_process(config_name, timeout=20)

    assert res_start is Start.Ok

    # retry start daemon process
    res_start = start_maestral_daemon_process(config_name, timeout=20)
    assert res_start is Start.AlreadyRunning

    # stop daemon
    res_stop = stop_maestral_daemon_process(config_name)
    assert res_stop is Stop.Ok

    # retry stop daemon
    res_stop = stop_maestral_daemon_process(config_name)
    assert res_stop is Stop.NotRunning


# client tests


def test_connection(config_name: str) -> None:
    # start daemon process
    res_start = start_maestral_daemon_process(config_name, timeout=20)
    assert res_start is Start.Ok

    # create client
    with MaestralClient(config_name) as m:
        assert m.config_name == config_name
        assert not m._is_fallback
        assert isinstance(m._m, JsonRpcConnection)
        if not daemon_module._is_windows():
            mode = os.stat(sockpath_for_config(config_name)).st_mode
            assert stat.S_IMODE(mode) == 0o600

    # stop daemon
    res_stop = stop_maestral_daemon_process(config_name)
    assert res_stop is Stop.Ok


def test_shutdown_closes_existing_clients(config_name: str) -> None:
    assert start_maestral_daemon_process(config_name, timeout=20) is Start.Ok
    client = MaestralClient(config_name)

    try:
        assert stop_maestral_daemon_process(config_name, timeout=5) is Stop.Ok
    finally:
        client._disconnect()


def test_fallback(config_name: str) -> None:
    # create client w/o fallback
    with pytest.raises(CommunicationError):
        MaestralClient(config_name)

    # create client w/ fallback
    with MaestralClient(config_name, fallback=True) as m:
        assert m.config_name == config_name
        assert m._is_fallback
        assert isinstance(m._m, Maestral)
        fallback = m._m

    assert not fallback.manager.connection_helper.is_alive()
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        fallback.sync._connection.execute("SELECT 1")


def test_remote_exceptions(config_name: str) -> None:
    # start daemon process
    start_maestral_daemon_process(config_name, timeout=20)

    # create a client and call a remote method which raises an error
    with MaestralClient(config_name) as m:
        with pytest.raises(NotLinkedError):
            m.get_account_info()

    # stop daemon
    stop_maestral_daemon_process(config_name)
