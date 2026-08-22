import ntpath
import os
import platform
import signal
import sys
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from watchdog.events import FileDeletedEvent, FileSystemEvent, FileSystemEventHandler

import maestral.autostart as autostart_module
import maestral.client as client_module
import maestral.daemon as daemon_module
import maestral.sync as sync_module
import maestral.utils.path as path_module
from maestral.autostart import (
    AutoStart,
    AutoStartWindows,
    SupportedImplementations,
)
from maestral.config import validate_config_name
from maestral.daemon import Start, Stop
from maestral.database.query import PathTreeQuery
from maestral.database.types import SqlPath
from maestral.errorhandling import os_to_maestral_error
from maestral.exceptions import MaestralApiError, PathError, SymlinkError
from maestral.fsevents import Observer
from maestral.models import ItemType, SyncEvent
from maestral.sync import SyncEngine
from maestral.utils.appdirs import (
    get_autostart_path,
    get_cache_path,
    get_conf_path,
    get_data_path,
    get_log_path,
    get_runtime_path,
)
from maestral.utils.path import get_local_change_time, get_local_change_time_ns

windows_only = pytest.mark.skipif(
    platform.system() != "Windows", reason="requires Windows"
)


def test_windows_app_dirs(monkeypatch, tmp_path):
    roaming = tmp_path / "Roaming"
    local = tmp_path / "Local"

    monkeypatch.setattr(platform, "system", lambda: "Windows")
    monkeypatch.setenv("APPDATA", str(roaming))
    monkeypatch.setenv("LOCALAPPDATA", str(local))

    assert get_conf_path(create=False) == str(roaming)
    assert get_data_path(create=False) == str(local)
    assert get_cache_path(create=False) == str(local)
    assert get_log_path(create=False) == str(local)
    assert get_runtime_path(create=False) == str(local)
    assert get_autostart_path(create=False) == str(
        roaming / "Microsoft/Windows/Start Menu/Programs/Startup"
    )

    endpoint = get_runtime_path("maestral", "work.endpoint")
    assert endpoint == str(local / "maestral/work.endpoint")
    assert Path(endpoint).parent.is_dir()


@pytest.mark.parametrize(
    "config_name", ["CON", "con.txt", "NUL", "COM1", "lpt9.log", "work."]
)
def test_windows_reserved_config_names(config_name, monkeypatch):
    monkeypatch.setattr(platform, "system", lambda: "Windows")

    with pytest.raises(ValueError, match="reserved on Windows"):
        validate_config_name(config_name)


def test_windows_uses_mtime_for_local_changes(monkeypatch):
    stat_result = SimpleNamespace(
        st_mtime=10.0,
        st_ctime=20.0,
        st_mtime_ns=10,
        st_ctime_ns=20,
    )

    monkeypatch.setattr(path_module, "IS_WINDOWS", True)

    assert get_local_change_time(stat_result) == 10.0
    assert get_local_change_time_ns(stat_result) == 10


def test_unix_keeps_ctime_for_local_changes(monkeypatch):
    stat_result = SimpleNamespace(
        st_mtime=10.0,
        st_ctime=20.0,
        st_mtime_ns=10,
        st_ctime_ns=20,
    )

    monkeypatch.setattr(path_module, "IS_WINDOWS", False)

    assert get_local_change_time(stat_result) == 20.0
    assert get_local_change_time_ns(stat_result) == 20


def test_upload_change_check_uses_windows_mtime(monkeypatch):
    old_stat = SimpleNamespace(st_mtime_ns=10, st_ctime_ns=20)
    new_stat = SimpleNamespace(st_mtime_ns=11, st_ctime_ns=20)

    monkeypatch.setattr(path_module, "IS_WINDOWS", True)

    assert client_module.file_was_modified(new_stat, old_stat)


def test_windows_local_and_dropbox_path_conversion(monkeypatch):
    engine = object.__new__(SyncEngine)
    engine._dropbox_path = r"C:\Users\Alice\Dropbox"
    engine._is_fs_case_sensitive = False

    monkeypatch.setattr(sync_module, "osp", ntpath)
    monkeypatch.setattr(path_module, "osp", ntpath)

    assert engine.to_dbx_path(r"c:\users\alice\dropbox\Folder\File.txt") == (
        "/Folder/File.txt"
    )
    assert engine.to_dbx_path(r"C:\Users\Alice\Dropbox") == "/"
    assert engine.to_local_path_from_cased("/Folder/File.txt") == (
        r"C:\Users\Alice\Dropbox\Folder\File.txt"
    )

    with pytest.raises(ValueError, match="Invalid Dropbox path"):
        engine.to_local_path_from_cased(r"/Folder\..\outside.txt")


def test_dropbox_case_conversion_always_uses_posix_paths():
    engine = object.__new__(SyncEngine)
    engine._case_conversion_cache = Mock()
    engine._correct_case_helper = Mock(return_value="/Parent")

    assert engine.correct_case("/parent/Child.txt") == "/Parent/Child.txt"
    engine._correct_case_helper.assert_called_once_with("/parent", "/parent")


def test_path_tree_query_always_uses_dropbox_separator():
    column = SimpleNamespace(type=SqlPath(), name="dbx_path")

    query = PathTreeQuery(column, "/Folder")

    assert query.dir_blob == b"/Folder/"


def test_deleted_folder_uses_index_type_when_observer_reports_file():
    sync_engine = Mock()
    sync_engine.client.account_info.account_id = "dbid"
    sync_engine.get_local_hash.return_value = None
    sync_engine.to_dbx_path.return_value = "/Folder"
    sync_engine.get_index_entry.return_value = SimpleNamespace(
        is_directory=True, item_type=ItemType.Folder
    )

    event = SyncEvent.from_file_system_event(
        FileDeletedEvent(r"C:\Dropbox\Folder"), sync_engine
    )

    assert event.item_type is ItemType.Folder


@windows_only
def test_windows_invalid_filename_is_a_path_error(tmp_path):
    local_path = tmp_path / "invalid?.txt"

    with pytest.raises(OSError) as exc_info:
        local_path.touch()

    error = os_to_maestral_error(
        exc_info.value,
        dbx_path="/invalid?.txt",
        local_path=str(local_path),
    )

    assert isinstance(error, PathError)
    assert error.dbx_path == "/invalid?.txt"
    assert error.local_path == str(local_path)


@windows_only
def test_windows_symlink_permission_has_specific_error(tmp_path):
    local_path = tmp_path / "link"

    try:
        local_path.symlink_to("target")
    except OSError as exc:
        if getattr(exc, "winerror", None) != 1314:
            raise
        error = os_to_maestral_error(exc, local_path=str(local_path))
    else:
        local_path.unlink()
        pytest.skip("Windows permits unprivileged symlink creation")

    assert isinstance(error, SymlinkError)
    assert error.local_path == str(local_path)


class _RecordingEventHandler(FileSystemEventHandler):
    def __init__(self) -> None:
        self.events: list[FileSystemEvent] = []
        self.changed = threading.Event()

    def on_any_event(self, event: FileSystemEvent) -> None:
        self.events.append(event)
        self.changed.set()

    def wait_for(self, predicate, timeout: float = 5) -> FileSystemEvent:
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            for event in self.events:
                if predicate(event):
                    return event
            self.changed.wait(0.1)
            self.changed.clear()

        pytest.fail(f"No matching event in {self.events!r}")


@windows_only
def test_windows_observer_case_rename_and_directory_delete(tmp_path):
    handler = _RecordingEventHandler()
    observer = Observer(timeout=0.1)
    observer.schedule(handler, str(tmp_path), recursive=True)
    observer.start()

    original = tmp_path / "CaseName"
    renamed = tmp_path / "casename"

    try:
        original.mkdir()
        handler.wait_for(
            lambda event: event.event_type == "created"
            and os.path.normcase(event.src_path) == os.path.normcase(str(original))
        )
        handler.events.clear()

        original.rename(renamed)
        moved = handler.wait_for(
            lambda event: event.event_type == "moved"
            and getattr(event, "dest_path", "") == str(renamed)
        )
        assert moved.src_path == str(original)
        handler.events.clear()

        renamed.rmdir()
        handler.wait_for(
            lambda event: event.event_type == "deleted"
            and os.path.normcase(event.src_path) == os.path.normcase(str(renamed))
        )
    finally:
        observer.stop()
        observer.join()


class _RegistryKey:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None


class _FakeRegistry:
    HKEY_CURRENT_USER = object()
    KEY_QUERY_VALUE = 1
    KEY_SET_VALUE = 2
    REG_SZ = 1

    def __init__(self):
        self.values = {}

    def CreateKeyEx(self, *_):
        return _RegistryKey()

    def OpenKey(self, *_):
        return _RegistryKey()

    def SetValueEx(self, _key, name, _reserved, value_type, value):
        self.values[name] = (value, value_type)

    def QueryValueEx(self, _key, name):
        try:
            return self.values[name]
        except KeyError as exc:
            raise FileNotFoundError(name) from exc

    def DeleteValue(self, _key, name):
        try:
            del self.values[name]
        except KeyError as exc:
            raise FileNotFoundError(name) from exc


def test_windows_registry_autostart_enable_disable(monkeypatch):
    registry = _FakeRegistry()
    monkeypatch.setattr(autostart_module, "winreg", registry)
    backend = AutoStartWindows(
        "Maestral.work", [r"C:\Program Files\Maestral\maestral.exe", "start"]
    )

    assert not backend.enabled
    backend.enable()
    assert backend.enabled
    backend.disable()
    assert not backend.enabled


def test_windows_registry_autostart_rejects_long_command(monkeypatch):
    monkeypatch.setattr(autostart_module, "winreg", _FakeRegistry())
    backend = AutoStartWindows("Maestral.work", ["C:\\" + "x" * 270])

    with pytest.raises(MaestralApiError, match="260 characters"):
        backend.enable()


@windows_only
def test_windows_registry_autostart_roundtrip():
    value_name = f"Maestral.pytest.{uuid.uuid4()}"
    backend = AutoStartWindows(value_name, [sys.executable, "-c", "pass"])

    try:
        assert not backend.enabled
        backend.enable()
        assert backend.enabled
    finally:
        backend.disable()

    assert not backend.enabled


def test_windows_autostart_starts_detached_daemon(monkeypatch):
    monkeypatch.setattr(
        autostart_module,
        "get_available_implementation",
        Mock(return_value=SupportedImplementations.windows_registry),
    )
    monkeypatch.setattr(
        autostart_module,
        "get_command_path",
        Mock(return_value=r"C:\Python\Scripts\maestral.exe"),
    )

    autostart = AutoStart("work")

    assert isinstance(autostart._impl, AutoStartWindows)
    assert "--foreground" not in autostart._impl.command
    assert "--config-name work" in autostart._impl.command


def test_windows_daemon_process_is_detached(monkeypatch):
    process = Mock()
    popen = Mock(return_value=process)

    monkeypatch.setattr(daemon_module, "is_running", Mock(return_value=False))
    monkeypatch.setattr(daemon_module, "_is_windows", Mock(return_value=True))
    monkeypatch.setattr(daemon_module.subprocess, "Popen", popen)
    monkeypatch.setattr(daemon_module, "wait_for_startup", Mock())
    monkeypatch.setattr(daemon_module.threading, "Thread", Mock(return_value=Mock()))

    result = daemon_module.start_maestral_daemon_process("work")

    assert result is Start.Ok
    assert popen.call_args.kwargs["creationflags"] == getattr(
        daemon_module.subprocess, "DETACHED_PROCESS", 0x00000008
    )
    assert popen.call_args.kwargs["stdin"] is daemon_module.subprocess.DEVNULL
    assert popen.call_args.kwargs["stdout"] is daemon_module.subprocess.DEVNULL
    assert popen.call_args.kwargs["stderr"] is daemon_module.subprocess.DEVNULL


def test_stop_requests_graceful_rpc_shutdown(monkeypatch):
    connection = Mock()
    running = Mock(side_effect=[True, False])

    monkeypatch.setattr(daemon_module, "is_running", running)
    monkeypatch.setattr(daemon_module, "get_maestral_pid", Mock(return_value=42))
    monkeypatch.setattr(daemon_module, "endpoint_for_config", Mock(return_value=Mock()))
    monkeypatch.setattr(
        daemon_module, "JsonRpcConnection", Mock(return_value=connection)
    )
    send_signal = Mock()
    monkeypatch.setattr(daemon_module, "_send_signal", send_signal)

    result = daemon_module.stop_maestral_daemon_process("work", timeout=1)

    assert result is Stop.Ok
    connection.request.assert_called_once_with("shutdown_daemon")
    connection.close.assert_called_once_with()
    send_signal.assert_not_called()


def test_windows_stop_uses_terminate_process_fallback(monkeypatch):
    connection = Mock()
    connection.request.side_effect = daemon_module.CommunicationError("offline")

    monkeypatch.setattr(daemon_module, "is_running", Mock(return_value=True))
    monkeypatch.setattr(daemon_module, "get_maestral_pid", Mock(return_value=42))
    monkeypatch.setattr(daemon_module, "_is_windows", Mock(return_value=True))
    monkeypatch.setattr(daemon_module, "endpoint_for_config", Mock(return_value=Mock()))
    monkeypatch.setattr(
        daemon_module, "JsonRpcConnection", Mock(return_value=connection)
    )
    send_signal = Mock()
    monkeypatch.setattr(daemon_module, "_send_signal", send_signal)

    result = daemon_module.stop_maestral_daemon_process("work", timeout=0)

    assert result is Stop.Killed
    send_signal.assert_called_once_with(42, signal.SIGTERM)


def test_windows_pid_sidecar_is_removed_before_unlock(monkeypatch, tmp_path):
    lock = daemon_module.Lock(str(tmp_path / "work.lock"))
    monkeypatch.setattr(daemon_module, "fcntl", None)

    assert lock.acquire()
    pid_path = Path(f"{lock.path}.pid")
    assert pid_path.read_text() == str(os.getpid())

    original_release = lock._external_lock.release

    def checked_release():
        assert not pid_path.exists()
        original_release()

    monkeypatch.setattr(lock._external_lock, "release", checked_release)

    lock.release()
    assert not pid_path.exists()
