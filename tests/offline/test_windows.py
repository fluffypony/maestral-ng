import ctypes
import ntpath
import os
import platform
import signal
import stat
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from watchdog.events import (
    DirCreatedEvent,
    FileCreatedEvent,
    FileDeletedEvent,
    FileSystemEvent,
    FileSystemEventHandler,
)

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
from maestral.client import DropboxClient
from maestral.config import MaestralConfig, validate_config_name
from maestral.core import DeletedMetadata, FileMetadata, ListFolderResult
from maestral.daemon import Start, Stop
from maestral.database.query import PathTreeQuery
from maestral.database.types import SqlPath
from maestral.errorhandling import os_to_maestral_error
from maestral.exceptions import MaestralApiError, PathError, SymlinkError
from maestral.fsevents import Observer
from maestral.keyring import CredentialStorage
from maestral.models import (
    ChangeType,
    IndexEntry,
    ItemType,
    SyncDirection,
    SyncEvent,
    SyncStatus,
)
from maestral.sync import Conflict, SyncEngine
from maestral.utils.appdirs import (
    get_autostart_path,
    get_cache_path,
    get_conf_path,
    get_data_path,
    get_log_path,
    get_runtime_path,
)
from maestral.utils.hashing import DropboxContentHasher
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


@pytest.mark.parametrize(
    ("remote_change", "expected_conflict"),
    [
        ("update", Conflict.Conflict),
        ("deletion", Conflict.LocalNewerOrIdentical),
    ],
)
def test_windows_older_mtime_with_changed_content_conflicts(
    config_name,
    tmp_path,
    monkeypatch,
    remote_change,
    expected_conflict,
):
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    local_path = dropbox_path / "changed.txt"
    local_path.write_bytes(b"local change")
    os.utime(local_path, (1, 1))
    indexed_hasher = DropboxContentHasher()
    indexed_hasher.update(b"indexed content")
    conf = MaestralConfig(config_name)
    conf.set("sync", "path", str(dropbox_path))
    engine = SyncEngine(DropboxClient(config_name, CredentialStorage(config_name)))
    engine.create_root_marker()
    with engine._database_access():
        engine._index_table.update(
            IndexEntry(
                dbx_path_lower="/changed.txt",
                dbx_path_cased="/changed.txt",
                dbx_id="id:changed",
                item_type=ItemType.File,
                last_sync=10,
                rev="indexed-rev",
                content_hash=indexed_hasher.hexdigest(),
                symlink_target=None,
            )
        )
    event = SyncEvent(
        direction=SyncDirection.Down,
        item_type=ItemType.File,
        sync_time=0,
        dbx_path="/changed.txt",
        dbx_path_lower="/changed.txt",
        dbx_id="id:changed",
        local_path=str(local_path),
        rev="remote-rev" if remote_change == "update" else None,
        content_hash="remote-hash" if remote_change == "update" else None,
        symlink_target=None,
        change_type=(
            ChangeType.Modified if remote_change == "update" else ChangeType.Removed
        ),
        change_time=0,
        change_dbid=None,
        status=SyncStatus.Queued,
        size=6,
        completed=0,
    )
    local_snapshot = engine._snapshot_local_tree(str(local_path))
    monkeypatch.setattr(path_module, "IS_WINDOWS", True)
    monkeypatch.setattr(sync_module.sys, "platform", "win32")

    try:
        assert (
            engine._check_download_conflict(event, local_snapshot) is expected_conflict
        )
    finally:
        engine._connection.close()


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


@pytest.mark.parametrize(
    "dbx_path",
    [
        "/Folder/name:stream",
        "/Folder/name.",
        "/Folder/name ",
        "/Folder/CON",
        "/Folder/con.txt",
        "/Folder/LPT9.log",
        "/Folder/COM¹.txt",
        "/Folder/bad<name",
        "/Folder/bad\x01name",
    ],
)
def test_windows_path_conversion_and_selection_reject_aliases(dbx_path, monkeypatch):
    engine = object.__new__(SyncEngine)
    engine._dropbox_path = r"C:\Users\Alice\Dropbox"
    engine._is_fs_case_sensitive = False
    monkeypatch.setattr(sync_module, "osp", ntpath)

    with pytest.raises(ValueError, match="Invalid Dropbox path"):
        engine.to_local_path_from_cased(dbx_path)
    with pytest.raises(ValueError, match="Invalid Dropbox path"):
        engine.clean_selective_sync_paths([dbx_path])


def test_windows_path_conversion_accepts_nonreserved_prefixes(monkeypatch):
    engine = object.__new__(SyncEngine)
    engine._dropbox_path = r"C:\Users\Alice\Dropbox"
    engine._is_fs_case_sensitive = False
    monkeypatch.setattr(sync_module, "osp", ntpath)

    assert engine.to_local_path_from_cased("/Folder/COM10.txt") == (
        r"C:\Users\Alice\Dropbox\Folder\COM10.txt"
    )


def test_windows_invalid_remote_name_does_not_stop_download_cycle(
    config_name, tmp_path, monkeypatch
):
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    conf = MaestralConfig(config_name)
    conf.set("sync", "path", str(dropbox_path))
    engine = SyncEngine(DropboxClient(config_name, CredentialStorage(config_name)))
    engine.create_root_marker()
    metadata = FileMetadata(
        name="name:stream",
        path_lower="/name:stream",
        path_display="/name:stream",
        id="id:invalid",
        client_modified=datetime.now(tz=timezone.utc),
        server_modified=datetime.now(tz=timezone.utc),
        rev="remote-rev",
        size=6,
        symlink_target=None,
        shared=True,
        modified_by="id:user",
        is_downloadable=True,
        content_hash="remote-hash",
    )
    engine.client.list_folder_iterator = Mock(
        return_value=iter(
            [
                ListFolderResult(
                    entries=[metadata],
                    has_more=False,
                    cursor="after-invalid",
                )
            ]
        )
    )
    monkeypatch.setattr(sync_module, "osp", ntpath)
    engine.ensure_dropbox_folder_present = Mock()  # type: ignore[method-assign]

    try:
        engine.download_sync_cycle()

        assert engine.remote_cursor == "after-invalid"
        assert [error.dbx_path_lower for error in engine.download_errors] == [
            "/name:stream"
        ]

        engine.client.get_metadata = Mock(return_value=metadata)

        assert not engine.get_remote_item("/name:stream")
        assert [error.dbx_path_lower for error in engine.download_errors] == [
            "/name:stream"
        ]
    finally:
        engine._connection.close()


def test_windows_invalid_deleted_name_removes_legacy_index(
    config_name, tmp_path, monkeypatch
):
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    conf = MaestralConfig(config_name)
    conf.set("sync", "path", str(dropbox_path))
    engine = SyncEngine(DropboxClient(config_name, CredentialStorage(config_name)))
    invalid_path = "/name:stream"
    with engine._database_access():
        engine._index_table.update(
            IndexEntry(
                dbx_path_lower=invalid_path,
                dbx_path_cased=invalid_path,
                dbx_id="id:invalid",
                item_type=ItemType.File,
                last_sync=0,
                rev="legacy-rev",
                content_hash="legacy-hash",
                symlink_target=None,
            )
        )
    monkeypatch.setattr(sync_module, "osp", ntpath)

    try:
        metadata = DeletedMetadata(
            name="name:stream",
            path_lower=invalid_path,
            path_display=invalid_path,
        )

        assert engine._sync_event_from_remote_metadata(metadata) == (None, True)
        assert engine.get_index_entry(invalid_path) is None
    finally:
        engine._connection.close()


def test_windows_inactive_scan_removes_invalid_legacy_index(
    config_name, tmp_path, monkeypatch
):
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    conf = MaestralConfig(config_name)
    conf.set("sync", "path", str(dropbox_path))
    engine = SyncEngine(DropboxClient(config_name, CredentialStorage(config_name)))
    engine.create_root_marker()
    invalid_path = "/name:stream"
    with engine._database_access():
        engine._index_table.update(
            IndexEntry(
                dbx_path_lower=invalid_path,
                dbx_path_cased=invalid_path,
                dbx_id="id:invalid",
                item_type=ItemType.File,
                last_sync=0,
                rev="legacy-rev",
                content_hash="legacy-hash",
                symlink_target=None,
            )
        )
    monkeypatch.setattr(sync_module, "osp", ntpath)
    engine.ensure_dropbox_folder_present = Mock()  # type: ignore[method-assign]

    try:
        changes, _ = engine._get_local_changes_while_inactive()

        assert changes == []
        assert engine.get_index_entry(invalid_path) is None
    finally:
        engine._connection.close()


def test_windows_selective_exclusion_unindexes_invalid_legacy_path(m, monkeypatch):
    invalid_path = "/name:stream"
    monkeypatch.setattr(m, "_check_linked", Mock())
    monkeypatch.setattr(m, "_check_dropbox_dir", Mock())
    with m.sync._database_access():
        m.sync._index_table.update(
            IndexEntry(
                dbx_path_lower=invalid_path,
                dbx_path_cased=invalid_path,
                dbx_id="id:invalid",
                item_type=ItemType.File,
                last_sync=0,
                rev="legacy-rev",
                content_hash="legacy-hash",
                symlink_target=None,
            )
        )
    monkeypatch.setattr(sync_module, "osp", ntpath)

    m.set_selective_sync("exclude", [invalid_path])

    assert m.sync.get_index_entry(invalid_path) is None


def test_windows_mount_point_stat_is_treated_as_link():
    junction_stat = SimpleNamespace(
        st_mode=stat.S_IFDIR,
        st_reparse_tag=0xA0000003,
    )

    assert path_module.is_fs_link(junction_stat)


@windows_only
def test_rooted_mkdir_rejects_preopened_writable_ancestor(tmp_path):
    root = tmp_path / "root"
    ancestor = root / "ancestor"
    target = ancestor / "child"
    ancestor.mkdir(parents=True)
    root_stat = os.lstat(root)
    kernel32 = path_module._windows_kernel32()
    writer_handle = kernel32.CreateFileW(
        str(ancestor),
        path_module._GENERIC_WRITE,
        path_module._FILE_SHARE_READ | path_module._FILE_SHARE_WRITE | 0x4,
        None,
        path_module._OPEN_EXISTING,
        path_module._FILE_FLAG_BACKUP_SEMANTICS
        | path_module._FILE_FLAG_OPEN_REPARSE_POINT,
        None,
    )
    invalid_handle = ctypes.c_void_p(-1).value
    if writer_handle is None or writer_handle == invalid_handle:
        pytest.skip("a writable directory handle is not available")

    try:
        with pytest.raises(OSError) as exc_info:
            path_module.mkdir(
                str(target),
                root_path=str(root),
                expected_root_identity=(
                    root_stat.st_dev,
                    root_stat.st_ino,
                    root_stat.st_mode,
                ),
            )
    finally:
        path_module._close_windows_handle(writer_handle)

    assert getattr(exc_info.value, "winerror", None) == 32
    assert not target.exists()


def test_walk_does_not_descend_into_windows_mount_point(monkeypatch):
    root = r"C:\Dropbox"
    junction = SimpleNamespace(path=root + r"\junction")
    calls = []

    def listdir(path):
        calls.append(path)
        return [junction]

    monkeypatch.setattr(
        path_module.os,
        "lstat",
        lambda _path: SimpleNamespace(
            st_mode=stat.S_IFDIR,
            st_reparse_tag=0xA0000003,
        ),
    )

    assert list(path_module.walk(root, listdir)) == [
        (junction.path, path_module.os.lstat(junction.path))
    ]
    assert calls == [root]


def test_equivalent_path_lookup_does_not_descend_windows_mount_point(
    tmp_path, monkeypatch
):
    root = tmp_path / "Dropbox"
    junction = root / "junction"
    child = junction / "child.txt"
    child.parent.mkdir(parents=True)
    child.write_text("external")
    real_lstat = os.lstat

    def junction_lstat(path):
        if Path(path) == junction:
            return SimpleNamespace(
                st_mode=stat.S_IFDIR,
                st_reparse_tag=0xA0000003,
            )
        return real_lstat(path)

    monkeypatch.setattr(path_module.os, "lstat", junction_lstat)

    assert path_module.get_existing_equivalent_paths(str(junction), root=str(root)) == [
        str(junction)
    ]
    assert path_module.get_existing_equivalent_paths(str(child), root=str(root)) == []


@pytest.mark.parametrize("child_event", [False, True], ids=["junction", "child"])
def test_upload_preflight_rejects_windows_mount_point(
    child_event, config_name, tmp_path, monkeypatch
):
    dropbox_path = tmp_path / "Dropbox"
    junction_path = dropbox_path / "junction"
    junction_path.mkdir(parents=True)
    child_path = junction_path / "child.txt"
    child_path.write_text("external data")
    conf = MaestralConfig(config_name)
    conf.set("sync", "path", str(dropbox_path))
    engine = SyncEngine(DropboxClient(config_name, CredentialStorage(config_name)))
    engine.create_root_marker()
    engine.ignore_symlinks = False
    engine.client.make_dir = Mock()
    engine.client.upload = Mock()
    engine.get_local_hash = Mock(  # type: ignore[method-assign]
        side_effect=AssertionError("content hashing reached the junction")
    )
    real_lstat = os.lstat

    def junction_lstat(path):
        if Path(path) == junction_path:
            return SimpleNamespace(
                st_mode=stat.S_IFDIR,
                st_reparse_tag=0xA0000003,
            )
        return real_lstat(path)

    monkeypatch.setattr(sync_module.os, "lstat", junction_lstat)
    fs_event = (
        FileCreatedEvent(str(child_path))
        if child_event
        else DirCreatedEvent(str(junction_path))
    )
    rooted_snapshot = Mock(side_effect=OSError("junction access refused"))
    monkeypatch.setattr(sync_module, "rooted_item_snapshot", rooted_snapshot)
    event = engine._sync_events_from_fs_events([fs_event])[0]

    try:
        result = engine._create_remote_entry(event)

        assert result.status is SyncStatus.Failed
        assert event.content_hash is None
        rooted_snapshot.assert_called()
        engine.get_local_hash.assert_not_called()
        engine.client.make_dir.assert_not_called()
        engine.client.upload.assert_not_called()
        assert [error.dbx_path_lower for error in engine.upload_errors] == [
            event.dbx_path_lower
        ]
    finally:
        engine._connection.close()


@windows_only
def test_remote_update_does_not_traverse_windows_junction(config_name, tmp_path):
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    target_path = tmp_path / "external"
    target_path.mkdir()
    target_file = target_path / "child.txt"
    target_file.write_text("keep me")
    junction_path = dropbox_path / "junction"
    subprocess.run(
        ["cmd.exe", "/d", "/c", "mklink", "/J", junction_path, target_path],
        check=True,
        capture_output=True,
        text=True,
    )

    conf = MaestralConfig(config_name)
    conf.set("sync", "path", str(dropbox_path))
    engine = SyncEngine(DropboxClient(config_name, CredentialStorage(config_name)))
    engine.create_root_marker()
    engine.ignore_symlinks = True
    engine.client.download = Mock()
    event = SyncEvent(
        direction=SyncDirection.Down,
        item_type=ItemType.File,
        sync_time=0,
        dbx_path="/junction/child.txt",
        dbx_path_lower="/junction/child.txt",
        dbx_id="id:child",
        local_path=str(junction_path / "child.txt"),
        rev="remote-rev",
        content_hash="remote-hash",
        symlink_target=None,
        change_type=ChangeType.Modified,
        change_time=0,
        change_dbid=None,
        status=SyncStatus.Queued,
        size=6,
        completed=0,
    )

    try:
        result = engine._create_local_entry(event)

        assert result.status is SyncStatus.Skipped
        assert target_file.read_text() == "keep me"
        assert engine._stored_ignored_symlink_for_path("/junction") == "/junction"
        engine.client.download.assert_not_called()
    finally:
        engine._connection.close()
        os.rmdir(junction_path)


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


@windows_only
def test_windows_symlink_target_omits_win32_namespace_prefix(tmp_path):
    target = tmp_path / "target"
    target.write_text("target")
    link = tmp_path / "link"

    try:
        link.symlink_to(target)
    except OSError as exc:
        if getattr(exc, "winerror", None) == 1314:
            pytest.skip("Windows did not grant symlink creation permission")
        raise

    assert path_module.get_symlink_target(str(link)) == str(target)


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
