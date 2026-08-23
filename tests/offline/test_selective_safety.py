import errno
import os
import shutil
import stat
from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from pathlib import Path
from threading import Event, Thread
from typing import BinaryIO
from unittest.mock import Mock, call

import pytest
from watchdog.events import (
    DirCreatedEvent,
    DirDeletedEvent,
    DirMovedEvent,
    FileCreatedEvent,
    FileDeletedEvent,
    FileModifiedEvent,
)

import maestral.manager as manager_module
import maestral.sync as sync_module
from maestral.client import DropboxClient
from maestral.config import MaestralConfig
from maestral.constants import (
    CASE_CHANGE_TEMP_PREFIX,
    MIGNORE_FILE,
    PATH_ROOT_RECOVERY_PREFIX,
    ROOT_MARKER_FILE,
)
from maestral.core import (
    AccountType,
    FileMetadata,
    FolderMetadata,
    FullAccount,
    TeamRootInfo,
    UserRootInfo,
)
from maestral.exceptions import (
    CacheDirError,
    DropboxConnectionError,
    DropboxServerError,
    FileConflictError,
    FolderConflictError,
    NoDropboxDirError,
    SymlinkError,
)
from maestral.keyring import CredentialStorage
from maestral.main import Maestral
from maestral.models import ChangeType, IndexEntry, ItemType, SyncEvent, SyncStatus
from maestral.sync import Conflict, SyncDirection, SyncEngine
from maestral.utils.hashing import DropboxContentHasher
from maestral.utils.path import get_symlink_target


@pytest.fixture
def sync_engine(config_name: str, tmp_path: Path) -> Iterator[SyncEngine]:
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    conf = MaestralConfig(config_name)
    conf.set("sync", "path", str(dropbox_path))
    client = DropboxClient(config_name, CredentialStorage(config_name))
    sync = SyncEngine(client)
    sync.create_root_marker()

    yield sync

    sync._connection.close()


def add_index_entry(
    sync: SyncEngine,
    dbx_path: str,
    item_type: ItemType,
    *,
    symlink_target: str | None = None,
    content_hash: str | None = None,
) -> None:
    is_folder = item_type is ItemType.Folder
    entry = IndexEntry(
        dbx_path_lower=dbx_path.lower(),
        dbx_path_cased=dbx_path,
        dbx_id=f"id:{dbx_path}",
        item_type=item_type,
        last_sync=datetime.now(tz=timezone.utc).timestamp() + 60,
        rev="folder" if is_folder else "rev",
        content_hash="folder" if is_folder else content_hash or "hash",
        symlink_target=symlink_target,
    )
    with sync._database_access():
        sync._index_table.update(entry)


def make_deleted_event(
    sync: SyncEngine, dbx_path: str, item_type: ItemType
) -> SyncEvent:
    return SyncEvent(
        direction=SyncDirection.Down,
        item_type=item_type,
        sync_time=0,
        dbx_path=dbx_path,
        dbx_path_lower=dbx_path.lower(),
        local_path=sync.to_local_path_from_cased(dbx_path),
        content_hash=None,
        symlink_target=None,
        change_type=ChangeType.Removed,
        change_time=None,
        change_dbid=None,
        status=SyncStatus.Queued,
        size=0,
        completed=0,
    )


def make_file_metadata(dbx_path: str, rev: str = "remote-rev") -> FileMetadata:
    name = dbx_path.rsplit("/", maxsplit=1)[-1]
    hasher = DropboxContentHasher()
    hasher.update(b"remote")
    return FileMetadata(
        name=name,
        path_lower=dbx_path.lower(),
        path_display=dbx_path,
        id=f"id:{dbx_path}",
        client_modified=datetime.now(tz=timezone.utc),
        server_modified=datetime.now(tz=timezone.utc),
        rev=rev,
        size=6,
        symlink_target=None,
        shared=True,
        modified_by="id:user",
        is_downloadable=True,
        content_hash=hasher.hexdigest(),
    )


def make_folder_metadata(dbx_path: str) -> FolderMetadata:
    name = dbx_path.rsplit("/", maxsplit=1)[-1]
    return FolderMetadata(
        name=name,
        path_lower=dbx_path.lower(),
        path_display=dbx_path,
        id=f"id:{dbx_path}",
        shared=False,
    )


def make_local_folder_event(sync: SyncEngine, dbx_path: str) -> SyncEvent:
    return SyncEvent(
        direction=SyncDirection.Up,
        item_type=ItemType.Folder,
        sync_time=0,
        dbx_path=dbx_path,
        dbx_path_lower=dbx_path.lower(),
        local_path=sync.to_local_path_from_cased(dbx_path),
        content_hash="folder",
        symlink_target=None,
        change_type=ChangeType.Added,
        change_time=0,
        change_dbid=None,
        status=SyncStatus.Queued,
        size=0,
        completed=0,
    )


def create_symlink(link: Path, target: Path) -> None:
    try:
        link.symlink_to(target, target_is_directory=target.is_dir())
    except OSError as exc:
        if getattr(exc, "winerror", None) == 1314:
            pytest.skip("Windows did not grant symlink creation permission")
        raise


def stop_connection_helper(m: Maestral) -> None:
    m.manager.shutdown()


def test_root_marker_symlink_is_rejected(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    marker_path = Path(sync_engine.dropbox_path) / ROOT_MARKER_FILE
    marker_path.unlink()
    external_file = tmp_path / "external-marker"
    external_file.write_text("not a marker")
    create_symlink(marker_path, external_file)

    with pytest.raises(NoDropboxDirError, match="not confirmed"):
        sync_engine.ensure_dropbox_folder_present()
    with pytest.raises(FileExistsError):
        sync_engine.create_root_marker()


def test_linked_dropbox_root_is_fatal_before_remote_index_update(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    event = SyncEvent.from_metadata(make_file_metadata("/remote.txt"), sync_engine)
    real_root = tmp_path / "real-root"
    Path(sync_engine.dropbox_path).rename(real_root)
    create_symlink(Path(sync_engine.dropbox_path), real_root)
    sync_engine.ignore_symlinks = True

    with pytest.raises(NoDropboxDirError, match="Dropbox folder missing"):
        sync_engine._create_local_entry(event)

    assert sync_engine.get_index_entry("/remote.txt") is None


def test_cache_symlink_is_rejected_before_download(
    sync_engine: SyncEngine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    external_cache = tmp_path / "external-cache"
    external_cache.mkdir()
    cache_path = Path(sync_engine.file_cache_path)
    create_symlink(cache_path, external_cache)
    rooted_temp_file = Mock(side_effect=AssertionError("temporary file created"))
    monkeypatch.setattr("maestral.sync.create_rooted_tempfile", rooted_temp_file)
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.RemoteNewer
    )
    sync_engine.client.download = Mock()  # type: ignore[method-assign]
    event = SyncEvent.from_metadata(make_file_metadata("/remote.txt"), sync_engine)

    with pytest.raises(CacheDirError, match="Cannot use cache directory"):
        sync_engine._create_local_entry(event)

    rooted_temp_file.assert_not_called()
    sync_engine.client.download.assert_not_called()
    assert list(external_cache.iterdir()) == []


def test_mignore_symlink_cannot_control_local_scan(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    external_rules = tmp_path / "external-mignore"
    external_rules.write_text("*.txt\n")
    mignore_path = Path(sync_engine.dropbox_path) / MIGNORE_FILE
    create_symlink(mignore_path, external_rules)
    local_file = Path(sync_engine.dropbox_path) / "upload.txt"
    local_file.write_text("upload me")
    sync_engine.ignore_symlinks = True

    sync_engine.load_mignore_file()
    scanned_names = {
        entry.name
        for entry in sync_engine._scandir_with_ignore(sync_engine.dropbox_path)
    }

    assert not sync_engine._is_mignore_path("/upload.txt")
    assert scanned_names == {"upload.txt"}


@pytest.mark.parametrize(
    "dbx_path",
    [
        "",
        ".",
        "..",
        "relative/path",
        "/.",
        "/..",
        "/folder/./file",
        "/folder/../file",
        "//folder",
        "/folder//file",
        "/folder/",
        "/folder\\file",
        "/folder\0file",
    ],
)
def test_selective_sync_rejects_noncanonical_paths(
    sync_engine: SyncEngine, dbx_path: str
) -> None:
    with pytest.raises(ValueError, match="Invalid Dropbox path"):
        sync_engine.clean_selective_sync_paths([dbx_path])


def test_selective_sync_config_update_holds_config_lock(
    sync_engine: SyncEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    mode_written = Event()
    release_update = Event()
    reader_done = Event()
    observed: list[tuple[str, list[str]]] = []
    original_set = sync_engine._conf.set

    def pausing_set(
        section: str, option: str, value: object, save: bool = True
    ) -> None:
        original_set(section, option, value, save)
        if option == "selective_sync_mode" and value == "include":
            mode_written.set()
            assert release_update.wait(timeout=2)

    monkeypatch.setattr(sync_engine._conf, "set", pausing_set)
    setter = Thread(
        target=sync_engine.set_selective_sync,
        args=("include", ["/Projects"]),
    )
    setter.start()
    assert mode_written.wait(timeout=2)

    def read_selection() -> None:
        observed.append(
            (
                sync_engine._conf.get("sync", "selective_sync_mode"),
                sync_engine._conf.get("sync", "selective_sync_paths"),
            )
        )
        reader_done.set()

    reader = Thread(target=read_selection)
    reader.start()
    assert not reader_done.wait(timeout=0.1)
    release_update.set()
    setter.join(timeout=2)
    reader.join(timeout=2)

    assert not setter.is_alive()
    assert not reader.is_alive()
    assert observed == [("include", ["/projects"])]


def test_include_selection_change_preserves_untracked_sibling(
    m: Maestral, tmp_path: Path
) -> None:
    stop_connection_helper(m)
    dropbox_path = tmp_path / "Dropbox"
    selected_path = dropbox_path / "projects" / "selected"
    untracked_path = dropbox_path / "projects" / "local-only.txt"
    selected_path.mkdir(parents=True)
    untracked_path.write_text("keep me")
    m.sync.dropbox_path = str(dropbox_path)
    m.sync.create_root_marker()
    m.sync.set_selective_sync("include", ["/projects/selected"])
    add_index_entry(m.sync, "/projects", ItemType.Folder)
    add_index_entry(m.sync, "/projects/selected", ItemType.Folder)
    m.sync._local_cursor = 1
    m.sync.remote_cursor = "cursor"
    m._check_linked = Mock()  # type: ignore[method-assign]
    m._check_dropbox_dir = Mock()  # type: ignore[method-assign]

    m.sync.remove_local_after_selective_sync = Mock(  # type: ignore[method-assign]
        wraps=m.sync.remove_local_after_selective_sync
    )

    m.set_selective_sync("include", ["/projects/other"])

    assert not selected_path.exists()
    assert untracked_path.read_text() == "keep me"
    assert m.sync.get_index_entry("/projects") is not None
    assert m.sync.get_index_entry("/projects/selected") is None
    m.sync.remove_local_after_selective_sync.assert_called_once_with(
        "/projects/selected"
    )


def test_selection_exclusion_unindexes_before_failed_evacuation(
    m: Maestral,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop_connection_helper(m)
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    local_path = dropbox_path / "folder"
    local_path.mkdir()
    (local_path / "managed.txt").write_text("managed")
    m.sync.dropbox_path = str(dropbox_path)
    m.sync.create_root_marker()
    add_index_entry(m.sync, "/folder", ItemType.Folder)
    add_index_entry(m.sync, "/folder/managed.txt", ItemType.File)
    m._check_linked = Mock()  # type: ignore[method-assign]
    m._check_dropbox_dir = Mock()  # type: ignore[method-assign]

    def fail_evacuation(_event: SyncEvent) -> tuple[str, str]:
        assert m.sync.get_index_entry("/folder") is None
        assert m.sync.get_index_entry("/folder/managed.txt") is None
        raise OSError("evacuation failed")

    monkeypatch.setattr(m.sync, "_evacuate_local_item", fail_evacuation)

    with pytest.raises(OSError, match="evacuation failed"):
        m.set_selective_sync("exclude", ["/folder"])

    assert local_path.is_dir()
    assert m.sync.get_index_entry("/folder") is None
    assert "/folder" in m.get_state("sync", "pending_downloads")
    assert "/folder" in m.manager.download_queue


def test_selection_removal_uses_indexed_path_casing(
    m: Maestral, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stop_connection_helper(m)
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    m.sync.dropbox_path = str(dropbox_path)
    if not m.sync.is_fs_case_sensitive:
        pytest.skip("The test needs a case-sensitive file system")

    indexed_path = dropbox_path / "Foo"
    indexed_path.write_text("managed")
    untracked_path = dropbox_path / "foo"
    untracked_path.write_text("keep me")
    m.sync.create_root_marker()
    add_index_entry(m.sync, "/Foo", ItemType.File)
    m.sync._local_cursor = 1
    m.sync.remote_cursor = "cursor"
    m._check_linked = Mock()  # type: ignore[method-assign]
    m._check_dropbox_dir = Mock()  # type: ignore[method-assign]

    m.sync.remove_local_after_selective_sync = Mock(  # type: ignore[method-assign]
        wraps=m.sync.remove_local_after_selective_sync
    )
    monkeypatch.setattr(
        "maestral.main.to_existing_unnormalized_path",
        lambda _path, root: str(untracked_path),
        raising=False,
    )

    m.set_selective_sync("exclude", ["/foo"])

    assert not indexed_path.exists()
    assert untracked_path.read_text() == "keep me"
    m.sync.remove_local_after_selective_sync.assert_called_once_with("/Foo")


def test_selective_sync_reads_old_pair_under_sync_lock(
    m: Maestral, monkeypatch: pytest.MonkeyPatch
) -> None:
    stop_connection_helper(m)
    m._check_linked = Mock()  # type: ignore[method-assign]
    m._check_dropbox_dir = Mock()  # type: ignore[method-assign]
    mode_read = Event()
    release_read = Event()
    errors: list[BaseException] = []
    original_getter = SyncEngine.selective_sync_mode.fget
    assert original_getter is not None

    def pausing_mode_getter(sync: SyncEngine) -> str:
        if not mode_read.is_set():
            mode_read.set()
            assert release_read.wait(timeout=2)
        return original_getter(sync)

    monkeypatch.setattr(
        SyncEngine,
        "selective_sync_mode",
        property(pausing_mode_getter),
    )

    def update_selection() -> None:
        try:
            m.set_selective_sync("include", ["/selected"])
        except BaseException as exc:
            errors.append(exc)

    setter = Thread(target=update_selection)
    setter.start()
    assert mode_read.wait(timeout=2)
    lock_was_available = m.sync.sync_lock.acquire(blocking=False)
    if lock_was_available:
        m.sync.sync_lock.release()
    release_read.set()
    setter.join(timeout=2)

    assert not setter.is_alive()
    assert not errors
    assert not lock_was_available


def test_selection_change_does_not_follow_ignored_symlink_ancestor(
    m: Maestral, tmp_path: Path
) -> None:
    stop_connection_helper(m)
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    external_path = tmp_path / "external"
    external_path.mkdir()
    external_child = external_path / "child.txt"
    external_child.write_text("keep me")
    link_path = dropbox_path / "link"
    create_symlink(link_path, external_path)
    m.sync.dropbox_path = str(dropbox_path)
    m.sync.create_root_marker()
    m.ignore_symlinks = True
    m.sync.set_selective_sync("include", ["/link/child.txt"])
    add_index_entry(m.sync, "/link", ItemType.Folder)
    add_index_entry(m.sync, "/link/child.txt", ItemType.File)
    m.sync._local_cursor = 1
    m.sync.remote_cursor = "cursor"
    m._check_linked = Mock()  # type: ignore[method-assign]
    m._check_dropbox_dir = Mock()  # type: ignore[method-assign]

    m.set_selective_sync("include", [])

    assert link_path.is_symlink()
    assert external_child.read_text() == "keep me"
    assert m.sync.get_index_entry("/link/child.txt") is None


def test_selection_removal_keeps_ignored_link_but_removes_managed_sibling(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    external_path = tmp_path / "external"
    external_path.mkdir()
    external_child = external_path / "outside.txt"
    external_child.write_text("keep me")
    folder_path = Path(sync_engine.dropbox_path) / "folder"
    folder_path.mkdir()
    managed_file = folder_path / "managed.txt"
    managed_contents = b"remove me"
    managed_file.write_bytes(managed_contents)
    link_path = folder_path / "link"
    create_symlink(link_path, external_path)
    add_index_entry(sync_engine, "/folder", ItemType.Folder)
    hasher = DropboxContentHasher()
    hasher.update(managed_contents)
    add_index_entry(
        sync_engine,
        "/folder/managed.txt",
        ItemType.File,
        content_hash=hasher.hexdigest(),
    )
    sync_engine.ignore_symlinks = True

    status = sync_engine.remove_local_after_selective_sync("/folder")

    assert status is SyncStatus.Done
    assert folder_path.is_dir()
    assert link_path.is_symlink()
    assert external_child.read_text() == "keep me"
    assert not managed_file.exists()
    assert sync_engine.get_index_entry("/folder") is None
    assert sync_engine.get_index_entry("/folder/managed.txt") is None


def test_selection_change_does_not_follow_in_root_symlink_ancestor(
    m: Maestral, tmp_path: Path
) -> None:
    stop_connection_helper(m)
    dropbox_path = tmp_path / "Dropbox"
    target_path = dropbox_path / "target"
    target_path.mkdir(parents=True)
    target_child = target_path / "child.txt"
    target_child.write_text("keep me")
    link_path = dropbox_path / "link"
    create_symlink(link_path, target_path)
    m.sync.dropbox_path = str(dropbox_path)
    m.sync.create_root_marker()
    m.sync.set_selective_sync("include", ["/link/child.txt"])
    add_index_entry(m.sync, "/link", ItemType.Folder)
    add_index_entry(m.sync, "/link/child.txt", ItemType.File)
    m.sync._local_cursor = 1
    m.sync.remote_cursor = "cursor"
    m._check_linked = Mock()  # type: ignore[method-assign]
    m._check_dropbox_dir = Mock()  # type: ignore[method-assign]

    m.set_selective_sync("include", [])

    assert link_path.is_symlink()
    assert target_child.read_text() == "keep me"
    assert m.sync.get_index_entry("/link/child.txt") is None


@pytest.mark.parametrize("remote_change", ["delete", "update"])
def test_remote_change_does_not_follow_error_policy_symlink_ancestor(
    sync_engine: SyncEngine,
    tmp_path: Path,
    remote_change: str,
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    target_child = target_path / "child.txt"
    target_child.write_text("keep me")
    link_path = Path(sync_engine.dropbox_path) / "link"
    create_symlink(link_path, target_path)
    add_index_entry(sync_engine, "/link", ItemType.Folder)
    add_index_entry(sync_engine, "/link/child.txt", ItemType.File)
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.RemoteNewer
    )
    sync_engine.client.download = Mock()  # type: ignore[method-assign]

    if remote_change == "delete":
        event = make_deleted_event(sync_engine, "/link/child.txt", ItemType.File)
    else:
        metadata = FileMetadata(
            name="child.txt",
            path_lower="/link/child.txt",
            path_display="/link/child.txt",
            id="id:child",
            client_modified=datetime.now(tz=timezone.utc),
            server_modified=datetime.now(tz=timezone.utc),
            rev="remote-rev",
            size=7,
            symlink_target=None,
            shared=True,
            modified_by="id:user",
            is_downloadable=True,
            content_hash="remote-hash",
        )
        event = SyncEvent.from_metadata(metadata, sync_engine)

    result = sync_engine._create_local_entry(event)

    assert result.status is SyncStatus.Failed
    assert target_child.read_text() == "keep me"
    sync_engine.client.download.assert_not_called()
    assert [error.dbx_path_lower for error in sync_engine.download_errors] == [
        "/link/child.txt"
    ]


def test_remote_required_parent_deletion_preserves_untracked_sibling(
    sync_engine: SyncEngine,
) -> None:
    projects_path = Path(sync_engine.dropbox_path) / "projects"
    selected_path = projects_path / "selected.txt"
    untracked_path = projects_path / "local-only.txt"
    projects_path.mkdir()
    selected_path.write_text("managed")
    untracked_path.write_text("keep me")
    sync_engine.set_selective_sync("include", ["/projects/selected.txt"])
    add_index_entry(sync_engine, "/projects", ItemType.Folder)
    add_index_entry(sync_engine, "/projects/selected.txt", ItemType.File)
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.RemoteNewer
    )
    event = make_deleted_event(sync_engine, "/projects", ItemType.Folder)

    status = sync_engine._on_remote_deleted(event)

    assert status is SyncStatus.Done
    assert not selected_path.exists()
    assert untracked_path.read_text() == "keep me"
    assert projects_path.is_dir()


def test_remote_file_cannot_replace_required_include_parent(
    sync_engine: SyncEngine,
) -> None:
    projects_path = Path(sync_engine.dropbox_path) / "projects"
    selected_path = projects_path / "selected.txt"
    projects_path.mkdir()
    selected_path.write_text("keep me")
    sync_engine.set_selective_sync("include", ["/projects/selected.txt"])
    add_index_entry(sync_engine, "/projects", ItemType.Folder)
    add_index_entry(sync_engine, "/projects/selected.txt", ItemType.File)
    sync_engine.client.download = Mock()  # type: ignore[method-assign]
    event = SyncEvent.from_metadata(make_file_metadata("/projects"), sync_engine)

    results = sync_engine.apply_remote_changes([event])

    assert results == []
    assert projects_path.is_dir()
    assert selected_path.read_text() == "keep me"
    assert sync_engine.get_index_entry("/projects").is_directory  # type: ignore[union-attr]
    sync_engine.client.download.assert_not_called()


def test_targeted_required_parent_file_is_successful_noop(
    sync_engine: SyncEngine,
) -> None:
    sync_engine.set_selective_sync("include", ["/projects/selected.txt"])
    metadata = make_file_metadata("/projects")
    sync_engine.client.get_metadata = Mock(return_value=metadata)  # type: ignore[method-assign]
    sync_engine.client.download = Mock()  # type: ignore[method-assign]

    assert sync_engine.get_remote_item("/projects")

    assert sync_engine.get_index_entry("/projects") is None
    sync_engine.client.download.assert_not_called()


@pytest.mark.parametrize("parent_indexed", [False, True])
def test_targeted_required_parent_file_removes_stale_folder(
    sync_engine: SyncEngine,
    parent_indexed: bool,
) -> None:
    projects_path = Path(sync_engine.dropbox_path) / "projects"
    selected_path = projects_path / "selected.txt"
    projects_path.mkdir()
    selected_path.write_text("managed")
    sync_engine.set_selective_sync("include", ["/projects/selected.txt"])
    if parent_indexed:
        add_index_entry(sync_engine, "/projects", ItemType.Folder)
    add_index_entry(sync_engine, "/projects/selected.txt", ItemType.File)
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.RemoteNewer
    )
    metadata = make_file_metadata("/projects")
    sync_engine.client.get_metadata = Mock(return_value=metadata)  # type: ignore[method-assign]
    sync_engine.client.download = Mock()  # type: ignore[method-assign]

    assert sync_engine.get_remote_item("/projects")

    assert not projects_path.exists()
    assert sync_engine.get_index_entry("/projects") is None
    assert sync_engine.get_index_entry("/projects/selected.txt") is None
    sync_engine.client.download.assert_not_called()


def test_upload_conflict_preserves_distinct_destination(
    sync_engine: SyncEngine,
) -> None:
    source = Path(sync_engine.dropbox_path) / "source.txt"
    source.write_text("source")
    destination = Path(sync_engine.dropbox_path) / "server-name.txt"
    destination.write_text("keep me")
    event = SyncEvent.from_metadata(make_file_metadata("/source.txt"), sync_engine)
    metadata = make_file_metadata("/server-name.txt")

    with pytest.raises(FileConflictError, match="different local item"):
        sync_engine._handle_upload_conflict(metadata, event)

    assert source.read_text() == "source"
    assert destination.read_text() == "keep me"


def test_remote_required_parent_deletion_rescans_surviving_local_change(
    sync_engine: SyncEngine,
) -> None:
    projects_path = Path(sync_engine.dropbox_path) / "projects"
    selected_path = projects_path / "selected.txt"
    projects_path.mkdir()
    selected_path.write_text("local edit")
    sync_engine.set_selective_sync("include", ["/projects/selected.txt"])
    add_index_entry(sync_engine, "/projects", ItemType.Folder)
    add_index_entry(sync_engine, "/projects/selected.txt", ItemType.File)
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.LocalNewerOrIdentical
    )
    sync_engine.fs_events.queue_event = Mock()  # type: ignore[method-assign]
    event = make_deleted_event(sync_engine, "/projects", ItemType.Folder)

    status = sync_engine._on_remote_deleted(event)

    assert status is SyncStatus.Skipped
    assert sync_engine.get_index_entry("/projects") is None
    assert sync_engine.get_index_entry("/projects/selected.txt") is not None
    created_parent = DirCreatedEvent(str(projects_path))
    assert sync_engine._filter_local_events([created_parent]) == [created_parent]
    assert (
        sync_engine._filter_local_events([FileCreatedEvent(str(projects_path))]) == []
    )
    sync_engine._create_remote_entry = Mock()  # type: ignore[method-assign]
    file_parent_event = SyncEvent.from_file_system_event(
        FileCreatedEvent(str(projects_path)), sync_engine
    )
    assert sync_engine.apply_local_changes([file_parent_event]) == []
    sync_engine._create_remote_entry.assert_not_called()
    sync_engine.fs_events.queue_event.assert_any_call(created_parent)


def test_remote_required_parent_deletion_preserves_upload_error(
    sync_engine: SyncEngine,
) -> None:
    projects_path = Path(sync_engine.dropbox_path) / "projects"
    selected_path = projects_path / "selected.txt"
    projects_path.mkdir()
    selected_path.write_text("never uploaded")
    sync_engine.set_selective_sync("include", ["/projects/selected.txt"])
    add_index_entry(sync_engine, "/projects", ItemType.Folder)
    add_index_entry(sync_engine, "/projects/selected.txt", ItemType.File)
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.Conflict
    )
    sync_engine.rescan = Mock()  # type: ignore[method-assign]
    event = make_deleted_event(sync_engine, "/projects", ItemType.Folder)

    status = sync_engine._on_remote_deleted(event)

    assert status is SyncStatus.Conflict
    assert selected_path.read_text() == "never uploaded"
    assert sync_engine.get_index_entry("/projects/selected.txt") is not None
    assert sync_engine.rescan.call_args_list == [
        call(str(selected_path)),
        call(str(projects_path)),
    ]


def test_remote_required_parent_rescans_unindexed_selected_child(
    sync_engine: SyncEngine,
) -> None:
    projects_path = Path(sync_engine.dropbox_path) / "projects"
    selected_path = projects_path / "selected.txt"
    projects_path.mkdir()
    selected_path.write_text("new local file")
    sync_engine.set_selective_sync("include", ["/projects/selected.txt"])
    add_index_entry(sync_engine, "/projects", ItemType.Folder)
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.LocalNewerOrIdentical
    )
    sync_engine.fs_events.queue_event = Mock()  # type: ignore[method-assign]
    event = make_deleted_event(sync_engine, "/projects", ItemType.Folder)

    status = sync_engine._on_remote_deleted(event)

    assert status is SyncStatus.Skipped
    assert sync_engine.get_index_entry("/projects") is None
    assert selected_path.read_text() == "new local file"
    queued_events = [
        call.args[0] for call in sync_engine.fs_events.queue_event.call_args_list
    ]
    assert DirCreatedEvent(str(projects_path)) in queued_events
    assert FileModifiedEvent(str(selected_path)) in queued_events


def test_remote_required_parent_recovery_preserves_local_casing(
    sync_engine: SyncEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    projects_path = Path(sync_engine.dropbox_path) / "Projects"
    sub_path = projects_path / "Sub"
    selected_path = sub_path / "Selected.txt"
    sub_path.mkdir(parents=True)
    selected_path.write_text("local edit")
    sync_engine.set_selective_sync("include", ["/Projects/Sub/Selected.txt"])
    add_index_entry(sync_engine, "/Projects", ItemType.Folder)
    add_index_entry(sync_engine, "/Projects/Sub", ItemType.Folder)
    add_index_entry(sync_engine, "/Projects/Sub/Selected.txt", ItemType.File)
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.LocalNewerOrIdentical
    )
    sync_engine.rescan = Mock()  # type: ignore[method-assign]
    rmdir = Mock(side_effect=OSError(errno.ENOTEMPTY, "not empty"))
    monkeypatch.setattr("maestral.sync.rooted_rmdir", rmdir)
    event = make_deleted_event(sync_engine, "/Projects", ItemType.Folder)

    status = sync_engine._on_remote_deleted(event)

    assert status is SyncStatus.Skipped
    assert [call.args[0] for call in rmdir.call_args_list] == [
        str(sub_path),
        str(projects_path),
    ]
    sync_engine.rescan.assert_called_once_with(str(projects_path))


def test_remote_required_parent_keeps_failed_child_retry(
    sync_engine: SyncEngine,
) -> None:
    projects_path = Path(sync_engine.dropbox_path) / "projects"
    selected_path = projects_path / "selected.txt"
    projects_path.mkdir()
    selected_path.write_text("managed")
    sync_engine.set_selective_sync("include", ["/projects/selected.txt"])
    add_index_entry(sync_engine, "/projects", ItemType.Folder)
    add_index_entry(sync_engine, "/projects/selected.txt", ItemType.File)
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.RemoteNewer
    )
    sync_engine._evacuate_local_item = (  # type: ignore[method-assign]
        Mock(
            side_effect=FileConflictError(
                "Deletion blocked",
                "Try again",
                dbx_path="/projects/selected.txt",
                local_path=str(selected_path),
            )
        )
    )
    sync_engine.download_callback = Mock()
    sync_engine.rescan = Mock()  # type: ignore[method-assign]
    event = make_deleted_event(sync_engine, "/projects", ItemType.Folder)

    result = sync_engine._create_local_entry(event)

    assert result.status is SyncStatus.Failed
    assert [error.dbx_path_lower for error in sync_engine.download_errors] == [
        "/projects/selected.txt"
    ]
    assert sync_engine.get_index_entry("/projects") is not None
    assert sync_engine.get_index_entry("/projects/selected.txt") is not None
    sync_engine.download_callback.assert_called_once_with("/projects")
    sync_engine.rescan.assert_not_called()


@pytest.mark.parametrize("ignore_symlinks", [False, True])
def test_remote_required_parent_cleanup_never_removes_link(
    sync_engine: SyncEngine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ignore_symlinks: bool,
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    target_child = target_path / "child.txt"
    target_child.write_text("keep me")
    parent_path = Path(sync_engine.dropbox_path) / "parent"
    parent_path.mkdir()
    link_path = parent_path / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = ignore_symlinks
    sync_engine.set_selective_sync("include", ["/parent/link/child.txt"])
    add_index_entry(sync_engine, "/parent", ItemType.Folder)
    add_index_entry(sync_engine, "/parent/link", ItemType.Folder)
    add_index_entry(sync_engine, "/parent/link/child.txt", ItemType.File)
    original_rmdir = os.rmdir
    sync_engine.rescan = Mock()  # type: ignore[method-assign]

    def reject_link_rmdir(path: str | bytes, *args: object, **kwargs: object) -> None:
        assert Path(path) != link_path
        original_rmdir(path, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr("maestral.sync.os.rmdir", reject_link_rmdir)
    event = make_deleted_event(sync_engine, "/parent", ItemType.Folder)

    result = sync_engine._create_local_entry(event)

    expected_status = SyncStatus.Skipped if ignore_symlinks else SyncStatus.Failed
    assert result.status is expected_status
    assert link_path.is_symlink()
    assert target_child.read_text() == "keep me"
    sync_engine.rescan.assert_not_called()


def test_remote_required_parent_cleanup_never_traverses_link(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_child = target_path / "sub" / "child.txt"
    target_child.parent.mkdir(parents=True)
    target_child.write_text("keep me")
    link_path = Path(sync_engine.dropbox_path) / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    sync_engine.set_selective_sync("include", ["/link/sub/child.txt"])
    add_index_entry(sync_engine, "/link", ItemType.Folder)
    add_index_entry(sync_engine, "/link/sub", ItemType.Folder)
    add_index_entry(sync_engine, "/link/sub/child.txt", ItemType.File)
    event = make_deleted_event(sync_engine, "/link", ItemType.Folder)

    result = sync_engine._create_local_entry(event)

    assert result.status is SyncStatus.Skipped
    assert link_path.is_symlink()
    assert target_child.read_text() == "keep me"


def test_local_ancestor_deletion_queues_ignored_symlink_restore(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    parent_path = Path(sync_engine.dropbox_path) / "parent"
    parent_path.mkdir()
    link_path = parent_path / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    sync_engine.download_callback = Mock()
    add_index_entry(sync_engine, "/parent", ItemType.Folder)
    add_index_entry(
        sync_engine,
        "/parent/link",
        ItemType.File,
        symlink_target=str(target_path),
    )
    shutil.rmtree(parent_path)

    events = sync_engine._filter_local_events([DirDeletedEvent(str(parent_path))])

    assert events == []
    sync_engine.download_callback.assert_called_once_with("/parent")
    assert sync_engine.get_index_entry("/parent") is None
    assert sync_engine.get_index_entry("/parent/link") is None


def test_remote_restore_keeps_link_state_when_queue_write_fails(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    link_path = Path(sync_engine.dropbox_path) / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    add_index_entry(sync_engine, "/link", ItemType.Folder)
    sync_engine.download_callback = Mock(side_effect=OSError("queue write failed"))

    with pytest.raises(OSError, match="queue write failed"):
        sync_engine._queue_remote_restore("/link")

    assert sync_engine._stored_ignored_symlink_for_path("/link") == "/link"
    assert sync_engine.get_index_entry("/link") is not None


def test_remote_restore_keeps_link_state_when_index_write_fails(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    link_path = Path(sync_engine.dropbox_path) / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    add_index_entry(sync_engine, "/link", ItemType.Folder)
    sync_engine.download_callback = Mock()
    sync_engine.remove_node_from_index = Mock(  # type: ignore[method-assign]
        side_effect=OSError("index write failed")
    )

    with pytest.raises(OSError, match="index write failed"):
        sync_engine._queue_remote_restore("/link")

    sync_engine.download_callback.assert_called_once_with("/link")
    assert sync_engine._stored_ignored_symlink_for_path("/link") == "/link"


def test_missing_overlay_keeps_link_state_when_index_write_fails(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    link_path = Path(sync_engine.dropbox_path) / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    add_index_entry(sync_engine, "/link", ItemType.Folder)
    link_path.unlink()
    sync_engine.remove_node_from_index = Mock(  # type: ignore[method-assign]
        side_effect=OSError("index write failed")
    )

    with pytest.raises(OSError, match="index write failed"):
        sync_engine._ignored_symlink_for_local_path(str(link_path))

    assert sync_engine._stored_ignored_symlink_for_path("/link") == "/link"


def test_inactive_scan_checks_stored_link_before_descendant_lookup(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    target_child = target_path / "child.txt"
    target_child.write_text("external")
    link_path = Path(sync_engine.dropbox_path) / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    add_index_entry(sync_engine, "/link/child.txt", ItemType.File)
    sync_engine._exists_with_given_casing = Mock(  # type: ignore[method-assign]
        side_effect=AssertionError("descendant path lookup")
    )

    events, _cursor = sync_engine._get_local_changes_while_inactive()

    assert events == []
    sync_engine._exists_with_given_casing.assert_not_called()


def test_remote_ancestor_deletion_preserves_ignored_symlink(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    parent_path = Path(sync_engine.dropbox_path) / "parent"
    parent_path.mkdir()
    managed_path = parent_path / "managed.txt"
    managed_contents = b"remove me"
    managed_path.write_bytes(managed_contents)
    link_path = parent_path / "link"
    create_symlink(link_path, target_path)
    hasher = DropboxContentHasher()
    hasher.update(managed_contents)
    add_index_entry(sync_engine, "/parent", ItemType.Folder)
    add_index_entry(
        sync_engine,
        "/parent/managed.txt",
        ItemType.File,
        content_hash=hasher.hexdigest(),
    )
    sync_engine.ignore_symlinks = True
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.RemoteNewer
    )
    event = make_deleted_event(sync_engine, "/parent", ItemType.Folder)

    status = sync_engine._on_remote_deleted(event)

    assert status is SyncStatus.Done
    assert link_path.is_symlink()
    assert get_symlink_target(str(link_path)) == str(target_path)
    assert not managed_path.exists()
    assert parent_path.is_dir()


def test_remote_deletion_proves_managed_tree_beside_ignored_link(
    sync_engine: SyncEngine,
    tmp_path: Path,
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    parent_path = Path(sync_engine.dropbox_path) / "parent"
    parent_path.mkdir()
    managed_path = parent_path / "managed.txt"
    managed_contents = b"remove me"
    managed_path.write_bytes(managed_contents)
    link_path = parent_path / "link"
    create_symlink(link_path, target_path)
    hasher = DropboxContentHasher()
    hasher.update(managed_contents)
    add_index_entry(sync_engine, "/parent", ItemType.Folder)
    add_index_entry(
        sync_engine,
        "/parent/managed.txt",
        ItemType.File,
        content_hash=hasher.hexdigest(),
    )
    sync_engine.ignore_symlinks = True
    event = make_deleted_event(sync_engine, "/parent", ItemType.Folder)

    status = sync_engine._on_remote_deleted(event)

    assert status is SyncStatus.Done
    assert link_path.is_symlink()
    assert not managed_path.exists()
    assert sync_engine.get_index_entry("/parent") is None
    assert sync_engine.get_index_entry("/parent/managed.txt") is None


def test_remote_deletion_commits_index_after_partial_mixed_tree_change(
    sync_engine: SyncEngine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    parent_path = Path(sync_engine.dropbox_path) / "parent"
    parent_path.mkdir()
    first_path = parent_path / "a.txt"
    second_path = parent_path / "b.txt"
    first_contents = b"first"
    second_contents = b"second"
    first_path.write_bytes(first_contents)
    second_path.write_bytes(second_contents)
    link_path = parent_path / "ignored-link"
    create_symlink(link_path, target_path)
    add_index_entry(sync_engine, "/parent", ItemType.Folder)
    for dbx_path, contents in (
        ("/parent/a.txt", first_contents),
        ("/parent/b.txt", second_contents),
    ):
        hasher = DropboxContentHasher()
        hasher.update(contents)
        add_index_entry(
            sync_engine,
            dbx_path,
            ItemType.File,
            content_hash=hasher.hexdigest(),
        )
    sync_engine.ignore_symlinks = True
    original_evacuate = sync_engine._evacuate_local_item

    def fail_second_child(event: SyncEvent) -> tuple[str, str]:
        if event.local_path == str(second_path):
            raise PermissionError("blocked child")
        return original_evacuate(event)

    monkeypatch.setattr(sync_engine, "_evacuate_local_item", fail_second_child)
    event = make_deleted_event(sync_engine, "/parent", ItemType.Folder)

    first_result = sync_engine._create_local_entry(event)
    first_status = first_result.status
    second_result = sync_engine._create_local_entry(event)

    assert first_status is SyncStatus.Failed
    assert second_result.status is SyncStatus.Skipped
    assert not first_path.exists()
    assert second_path.read_bytes() == second_contents
    assert link_path.is_symlink()
    assert sync_engine.get_index_entry("/parent") is None
    assert sync_engine.get_index_entry("/parent/a.txt") is None
    assert sync_engine.get_index_entry("/parent/b.txt") is None
    recovery_paths = sync_engine._validated_recovered_local_paths()
    assert set(recovery_paths) == {"/parent"}


def test_exclusion_preserves_modified_and_unindexed_mixed_tree_files(
    sync_engine: SyncEngine,
    tmp_path: Path,
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    parent_path = Path(sync_engine.dropbox_path) / "parent"
    parent_path.mkdir()
    modified_path = parent_path / "modified.txt"
    unindexed_path = parent_path / "unindexed.txt"
    modified_path.write_bytes(b"local change")
    unindexed_path.write_bytes(b"local only")
    link_path = parent_path / "ignored-link"
    create_symlink(link_path, target_path)
    old_hasher = DropboxContentHasher()
    old_hasher.update(b"old remote data")
    add_index_entry(sync_engine, "/parent", ItemType.Folder)
    add_index_entry(
        sync_engine,
        "/parent/modified.txt",
        ItemType.File,
        content_hash=old_hasher.hexdigest(),
    )
    sync_engine.ignore_symlinks = True

    status = sync_engine.remove_local_after_selective_sync("/parent")

    assert status is SyncStatus.Conflict
    assert link_path.is_symlink()
    visible_contents = {
        path.read_bytes()
        for path in parent_path.iterdir()
        if path.is_file() and not path.is_symlink()
    }
    assert visible_contents == {b"local change", b"local only"}
    assert sync_engine.get_index_entry("/parent") is None
    assert sync_engine.get_index_entry("/parent/modified.txt") is None
    assert len(sync_engine._validated_recovered_local_paths()) == 2


def test_remote_deletion_preserves_selectively_excluded_local_child(
    sync_engine: SyncEngine,
) -> None:
    parent_path = Path(sync_engine.dropbox_path) / "parent"
    parent_path.mkdir()
    managed_path = parent_path / "managed.txt"
    excluded_path = parent_path / "local-only.txt"
    managed_contents = b"managed"
    excluded_contents = b"local only"
    managed_path.write_bytes(managed_contents)
    excluded_path.write_bytes(excluded_contents)
    hasher = DropboxContentHasher()
    hasher.update(managed_contents)
    add_index_entry(sync_engine, "/parent", ItemType.Folder)
    add_index_entry(
        sync_engine,
        "/parent/managed.txt",
        ItemType.File,
        content_hash=hasher.hexdigest(),
    )
    sync_engine.set_selective_sync("exclude", ["/parent/local-only.txt"])
    event = make_deleted_event(sync_engine, "/parent", ItemType.Folder)

    status = sync_engine._on_remote_deleted(event)

    assert status is SyncStatus.Conflict
    assert not parent_path.exists()
    recovered_folders = [
        path
        for path in Path(sync_engine.dropbox_path).iterdir()
        if path.is_dir() and path.name.startswith("parent (")
    ]
    assert len(recovered_folders) == 1
    assert (recovered_folders[0] / "local-only.txt").read_bytes() == excluded_contents
    assert sync_engine.get_index_entry("/parent") is None
    assert sync_engine.get_index_entry("/parent/managed.txt") is None


def test_existing_empty_remote_folder_clears_local_recovery_marker(
    sync_engine: SyncEngine,
) -> None:
    folder_path = Path(sync_engine.dropbox_path) / "folder"
    folder_path.mkdir()
    folder_stat = os.lstat(folder_path)
    sync_engine._record_recovered_local_path(
        str(folder_path),
        (folder_stat.st_dev, folder_stat.st_ino, folder_stat.st_mode),
    )
    metadata = make_folder_metadata("/folder")
    sync_engine.client.make_dir = Mock(  # type: ignore[method-assign]
        side_effect=FolderConflictError("Folder exists", "Folder exists")
    )
    sync_engine.client.get_metadata = Mock(  # type: ignore[method-assign]
        return_value=metadata
    )
    event = make_local_folder_event(sync_engine, "/folder")

    status = sync_engine._on_local_folder_created(event)

    assert status is SyncStatus.Skipped
    assert sync_engine._validated_recovered_local_paths() == {}


def test_tracked_recovery_replacement_bypasses_include_filter(
    sync_engine: SyncEngine,
) -> None:
    sync_engine.set_selective_sync("include", ["/selected.txt"])
    local_path = Path(sync_engine.dropbox_path) / "recovered.txt"
    old_path = Path(sync_engine.dropbox_path) / "old-recovered.txt"
    local_path.write_text("old")
    old_stat = os.lstat(local_path)
    sync_engine._record_recovered_local_path(
        str(local_path),
        (old_stat.st_dev, old_stat.st_ino, old_stat.st_mode),
    )
    local_path.rename(old_path)
    local_path.write_text("replacement")
    event = FileModifiedEvent(str(local_path))

    assert sync_engine._filter_local_events([event]) == [event]


def test_required_parent_move_transfers_recovered_descendant(
    sync_engine: SyncEngine,
) -> None:
    sync_engine.set_selective_sync("include", ["/projects/selected.txt"])
    projects_path = Path(sync_engine.dropbox_path) / "projects"
    recovered_path = projects_path / "recovery" / "local.txt"
    recovered_path.parent.mkdir(parents=True)
    recovered_path.write_text("local only")
    recovered_stat = os.lstat(recovered_path)
    sync_engine._record_recovered_local_path(
        str(recovered_path),
        (recovered_stat.st_dev, recovered_stat.st_ino, recovered_stat.st_mode),
    )
    renamed_path = Path(sync_engine.dropbox_path) / "renamed"
    projects_path.rename(renamed_path)
    moved_recovered_path = renamed_path / "recovery" / "local.txt"
    sync_engine.rescan = Mock()  # type: ignore[method-assign]
    event = DirMovedEvent(str(projects_path), str(renamed_path))

    assert sync_engine._filter_local_events([event]) == []

    paths = sync_engine._validated_recovered_local_paths()
    assert set(paths) == {"/renamed/recovery/local.txt"}
    assert paths["/renamed/recovery/local.txt"]["path"] == (
        "/renamed/recovery/local.txt"
    )
    assert paths["/renamed/recovery/local.txt"]["identity"] == [
        recovered_stat.st_dev,
        recovered_stat.st_ino,
        recovered_stat.st_mode,
    ]
    sync_engine.rescan.assert_called_once_with(str(moved_recovered_path))


def test_stale_move_event_keeps_live_source_recovery_marker(
    sync_engine: SyncEngine,
) -> None:
    source_path = Path(sync_engine.dropbox_path) / "source"
    recovered_path = source_path / "local.txt"
    recovered_path.parent.mkdir()
    recovered_path.write_text("local only")
    recovered_stat = os.lstat(recovered_path)
    sync_engine._record_recovered_local_path(
        str(recovered_path),
        (recovered_stat.st_dev, recovered_stat.st_ino, recovered_stat.st_mode),
    )
    destination_path = Path(sync_engine.dropbox_path) / "destination"
    unrelated_path = destination_path / "local.txt"
    destination_path.mkdir()
    unrelated_path.write_text("unrelated")
    event = DirMovedEvent(str(source_path), str(destination_path))

    assert sync_engine._filter_local_events([event]) == [event]

    paths = sync_engine._validated_recovered_local_paths()
    assert set(paths) == {"/source/local.txt"}
    assert paths["/source/local.txt"]["identity"] == [
        recovered_stat.st_dev,
        recovered_stat.st_ino,
        recovered_stat.st_mode,
    ]


def test_ignored_link_move_transfers_recovered_sibling(
    sync_engine: SyncEngine,
    tmp_path: Path,
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    source_path = Path(sync_engine.dropbox_path) / "source"
    recovered_path = source_path / "local.txt"
    recovered_path.parent.mkdir()
    recovered_path.write_text("local only")
    link_path = source_path / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    sync_engine._remember_ignored_symlink("/source/link")
    recovered_stat = os.lstat(recovered_path)
    sync_engine._record_recovered_local_path(
        str(recovered_path),
        (recovered_stat.st_dev, recovered_stat.st_ino, recovered_stat.st_mode),
    )
    destination_path = Path(sync_engine.dropbox_path) / "destination"
    source_path.rename(destination_path)
    moved_recovered_path = destination_path / "local.txt"
    sync_engine.rescan = Mock()  # type: ignore[method-assign]
    event = DirMovedEvent(str(source_path), str(destination_path))

    assert sync_engine._filter_local_events([event]) == []

    paths = sync_engine._validated_recovered_local_paths()
    assert set(paths) == {"/destination/local.txt"}
    sync_engine.rescan.assert_any_call(str(moved_recovered_path))


def test_recovered_directory_rescan_includes_excluded_children(
    sync_engine: SyncEngine,
) -> None:
    sync_engine.set_selective_sync("include", ["/selected.txt"])
    recovery_path = Path(sync_engine.dropbox_path) / "recovery"
    child_folder = recovery_path / "child"
    child_path = child_folder / "local.txt"
    child_folder.mkdir(parents=True)
    child_path.write_text("local only")
    recovery_stat = os.lstat(recovery_path)
    sync_engine._record_recovered_local_path(
        str(recovery_path),
        (recovery_stat.st_dev, recovery_stat.st_ino, recovery_stat.st_mode),
    )
    sync_engine.fs_events.queue_event = Mock()  # type: ignore[method-assign]

    sync_engine.rescan(str(recovery_path))

    assert sync_engine.fs_events.queue_event.call_args_list == [
        call(DirCreatedEvent(str(recovery_path))),
        call(DirCreatedEvent(str(child_folder))),
        call(FileModifiedEvent(str(child_path))),
    ]


def test_recovery_completion_rebinds_replacement_after_clear(
    sync_engine: SyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sync_engine.set_selective_sync("include", ["/selected.txt"])
    local_path = Path(sync_engine.dropbox_path) / "recovered.txt"
    old_path = Path(sync_engine.dropbox_path) / "uploaded.txt"
    local_path.write_text("uploaded")
    local_stat = os.lstat(local_path)
    sync_engine._record_recovered_local_path(
        str(local_path),
        (local_stat.st_dev, local_stat.st_ino, local_stat.st_mode),
    )
    add_index_entry(sync_engine, "/recovered.txt", ItemType.File)
    monkeypatch.setattr(sync_engine, "_snapshot_matches_index", lambda *_: True)
    sync_engine.rescan = Mock()  # type: ignore[method-assign]
    original_forget = sync_engine._forget_recovered_local_path

    def clear_then_replace(*args: object, **kwargs: object) -> bool:
        cleared = original_forget(*args, **kwargs)
        local_path.rename(old_path)
        local_path.write_text("replacement")
        return cleared

    monkeypatch.setattr(
        sync_engine,
        "_forget_recovered_local_path",
        clear_then_replace,
    )

    sync_engine._maybe_finish_recovered_local_path("/recovered.txt")

    replacement_stat = os.lstat(local_path)
    entry = sync_engine._validated_recovered_local_paths()["/recovered.txt"]
    assert entry["identity"] == [
        replacement_stat.st_dev,
        replacement_stat.st_ino,
        replacement_stat.st_mode,
    ]
    sync_engine.rescan.assert_called_once_with(str(local_path))


def test_remote_ancestor_deletion_recovers_writes_from_open_managed_file(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    parent_path = Path(sync_engine.dropbox_path) / "parent"
    parent_path.mkdir()
    managed_path = parent_path / "managed.txt"
    managed_contents = b"before"
    managed_path.write_bytes(managed_contents)
    link_path = parent_path / "link"
    create_symlink(link_path, target_path)
    hasher = DropboxContentHasher()
    hasher.update(managed_contents)
    add_index_entry(sync_engine, "/parent", ItemType.Folder)
    add_index_entry(
        sync_engine,
        "/parent/managed.txt",
        ItemType.File,
        content_hash=hasher.hexdigest(),
    )
    sync_engine.ignore_symlinks = True
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.RemoteNewer
    )
    event = make_deleted_event(sync_engine, "/parent", ItemType.Folder)

    with managed_path.open("a") as open_writer:
        assert sync_engine._on_remote_deleted(event) is SyncStatus.Done
        open_writer.write(" after deletion")
        open_writer.flush()
        os.fsync(open_writer.fileno())

    sync_engine._recover_pending_local_evacuations()

    recovered_paths = sync_engine._validated_recovered_local_paths()
    assert len(recovered_paths) == 1
    recovered_entry = next(iter(recovered_paths.values()))
    recovered_path = Path(sync_engine.to_local_path_from_cased(recovered_entry["path"]))
    assert recovered_path.read_text() == "before after deletion"
    assert link_path.is_symlink()
    assert not managed_path.exists()


def test_local_evacuation_recovers_after_crash_at_discard_rename(
    sync_engine: SyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sync_engine.ensure_cache_dir_present()
    token = "rename-crash"
    backup_path = Path(sync_engine.file_cache_path) / f"evac-{token}"
    discard_path = Path(sync_engine.file_cache_path) / f"discard-{token}"
    backup_path.write_text("unchanged")
    snapshot = sync_engine._snapshot_local_tree(str(backup_path))
    identity = snapshot[str(backup_path)]
    sync_engine._state.set(
        "recovery",
        "local_evacuations",
        {
            token: {
                "backup_name": backup_path.name,
                "dbx_path": "/item.txt",
                "change_dbid": "",
                "identity": list(identity[:3]),
                "phase": "retained",
                "retained_boot_id": "boot:previous",
                "snapshot_digest": sync_engine._snapshot_digest(
                    snapshot, str(backup_path)
                ),
                "recovered_source": "",
                "visible_path": "",
            }
        },
    )
    monkeypatch.setattr(sync_engine, "_system_boot_id", lambda: "boot:current")

    def crash_after_rename(path: str, **kwargs: object) -> None:
        quarantine_path = kwargs["quarantine_path"]
        assert isinstance(quarantine_path, str)
        assert path == str(backup_path)
        assert quarantine_path == str(discard_path)
        os.rename(path, quarantine_path)
        raise OSError(errno.EIO, "simulated crash after discard rename", path)

    with monkeypatch.context() as crash_patch:
        crash_patch.setattr("maestral.sync.delete", crash_after_rename)
        with pytest.raises(OSError, match="simulated crash"):
            sync_engine._recover_pending_local_evacuations()

    journal = sync_engine._validated_local_evacuations()
    assert journal[token]["phase"] == "discarding"
    assert not backup_path.exists()
    assert discard_path.read_text() == "unchanged"

    sync_engine._recover_pending_local_evacuations()

    assert sync_engine._validated_local_evacuations() == {}
    assert not discard_path.exists()


def test_partial_journalled_discard_is_preserved_as_visible_recovery(
    sync_engine: SyncEngine,
) -> None:
    sync_engine.ensure_cache_dir_present()
    token = "partial-discard"
    backup_path = Path(sync_engine.file_cache_path) / f"evac-{token}"
    discard_path = Path(sync_engine.file_cache_path) / f"discard-{token}"
    backup_path.mkdir()
    (backup_path / "kept.txt").write_text("keep me")
    (backup_path / "removed.txt").write_text("removed")
    snapshot = sync_engine._snapshot_local_tree(str(backup_path))
    identity = snapshot[str(backup_path)]
    snapshot_digest = sync_engine._snapshot_digest(snapshot, str(backup_path))
    backup_path.rename(discard_path)
    (discard_path / "removed.txt").unlink()
    sync_engine._state.set(
        "recovery",
        "local_evacuations",
        {
            token: {
                "backup_name": backup_path.name,
                "dbx_path": "/folder",
                "change_dbid": "",
                "identity": list(identity[:3]),
                "phase": "discarding",
                "retained_boot_id": "boot:previous",
                "snapshot_digest": snapshot_digest,
                "recovered_source": "",
                "visible_path": "",
            }
        },
    )

    sync_engine._recover_pending_local_evacuations()

    recovered_paths = sync_engine._validated_recovered_local_paths()
    assert len(recovered_paths) == 1
    recovered_entry = next(iter(recovered_paths.values()))
    recovered_path = Path(sync_engine.to_local_path_from_cased(recovered_entry["path"]))
    assert recovered_path.is_dir()
    assert (recovered_path / "kept.txt").read_text() == "keep me"
    assert not (recovered_path / "removed.txt").exists()
    assert not backup_path.exists()
    assert not discard_path.exists()
    assert sync_engine._validated_local_evacuations() == {}


def test_remote_ancestor_deletion_fails_before_error_policy_symlink(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    parent_path = Path(sync_engine.dropbox_path) / "parent"
    parent_path.mkdir()
    managed_path = parent_path / "managed.txt"
    managed_path.write_text("keep me")
    link_path = parent_path / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = False
    event = make_deleted_event(sync_engine, "/parent", ItemType.Folder)

    result = sync_engine._create_local_entry(event)

    assert result.status is SyncStatus.Failed
    assert parent_path.is_dir()
    assert managed_path.read_text() == "keep me"
    assert link_path.is_symlink()


def test_normalization_conflict_does_not_move_ignored_symlink(
    sync_engine: SyncEngine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    folder_path = Path(sync_engine.dropbox_path) / "folder"
    folder_path.mkdir()
    link_path = folder_path / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    event = make_local_folder_event(sync_engine, "/folder")
    monkeypatch.setattr(
        "maestral.sync.get_existing_equivalent_paths",
        lambda _path, root: [str(folder_path), str(Path(root) / "Folder")],
    )

    with pytest.raises(SymlinkError, match="normalization conflict copy"):
        sync_engine._handle_normalization_conflict(event)

    assert folder_path.is_dir()
    assert link_path.is_symlink()


def test_selective_conflict_does_not_move_ignored_symlink(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    folder_path = Path(sync_engine.dropbox_path) / "folder"
    folder_path.mkdir()
    link_path = folder_path / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    sync_engine.set_selective_sync("exclude", ["/folder"])
    event = make_local_folder_event(sync_engine, "/folder")

    with pytest.raises(SymlinkError, match="selective sync conflict copy"):
        sync_engine._handle_selective_sync_conflict(event)

    assert folder_path.is_dir()
    assert link_path.is_symlink()


def test_upload_conflict_does_not_move_ignored_symlink(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    folder_path = Path(sync_engine.dropbox_path) / "folder"
    folder_path.mkdir()
    link_path = folder_path / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    event = make_local_folder_event(sync_engine, "/folder")
    metadata = Mock(
        name="folder (1)",
        path_lower="/folder (1)",
        path_display="/folder (1)",
    )
    sync_engine.to_local_path = Mock(  # type: ignore[method-assign]
        return_value=str(Path(sync_engine.dropbox_path) / "folder (1)")
    )

    with pytest.raises(SymlinkError, match="upload conflict rename"):
        sync_engine._handle_upload_conflict(metadata, event)

    assert folder_path.is_dir()
    assert link_path.is_symlink()


def test_remote_folder_conflict_does_not_move_ignored_symlink(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    folder_path = Path(sync_engine.dropbox_path) / "folder"
    folder_path.mkdir()
    link_path = folder_path / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    event = make_local_folder_event(sync_engine, "/folder")
    event.direction = SyncDirection.Down
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.Conflict
    )

    result = sync_engine._create_local_entry(event)

    assert result.status is SyncStatus.Failed
    assert folder_path.is_dir()
    assert link_path.is_symlink()


def test_remote_folder_creation_rechecks_linked_parent(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    parent_path = Path(sync_engine.dropbox_path) / "parent"
    parent_path.mkdir()
    external_path = tmp_path / "external"
    external_path.mkdir()
    event = make_local_folder_event(sync_engine, "/parent/remote")
    event.direction = SyncDirection.Down
    sync_engine.ignore_symlinks = True
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.RemoteNewer
    )

    def replace_parent_with_link(_event: SyncEvent) -> None:
        parent_path.rmdir()
        create_symlink(parent_path, external_path)

    sync_engine._ensure_parent = Mock(  # type: ignore[method-assign]
        side_effect=replace_parent_with_link
    )

    result = sync_engine._create_local_entry(event)

    assert result.status is SyncStatus.Failed
    assert parent_path.is_symlink()
    assert not (external_path / "remote").exists()
    assert sync_engine.get_index_entry("/parent/remote") is None


def test_remote_folder_creation_rejects_file_created_during_mkdir(
    sync_engine: SyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    folder_path = Path(sync_engine.dropbox_path) / "folder"
    event = make_local_folder_event(sync_engine, "/folder")
    event.direction = SyncDirection.Down
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.RemoteNewer
    )

    def create_file_then_fail(path: str, *args: object, **kwargs: object) -> None:
        Path(path).write_text("local race winner")
        raise FileExistsError(path)

    monkeypatch.setattr("maestral.sync.rooted_mkdir", create_file_then_fail)

    result = sync_engine._create_local_entry(event)

    assert result.status is SyncStatus.Failed
    assert folder_path.read_text() == "local race winner"
    assert sync_engine.get_index_entry("/folder") is None


def test_remote_deletion_stops_after_child_scan_error(
    sync_engine: SyncEngine,
    tmp_path: Path,
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    parent_path = Path(sync_engine.dropbox_path) / "parent"
    blocked_path = parent_path / "blocked"
    blocked_path.mkdir(parents=True)
    link_path = blocked_path / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    sync_engine._snapshot_local_tree = Mock(  # type: ignore[method-assign]
        side_effect=PermissionError("blocked test directory")
    )

    result = sync_engine._delete_local_path_preserving_ignored_symlinks(
        str(parent_path)
    )

    assert isinstance(result.error, PermissionError)
    assert not result.preserved
    assert parent_path.is_dir()
    assert link_path.is_symlink()


def test_error_symlink_policy_rescans_only_the_link(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    external_child = target_path / "external.txt"
    external_child.write_text("do not upload")
    link_path = Path(sync_engine.dropbox_path) / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    sync_engine.fs_events.queue_event = Mock()  # type: ignore[method-assign]

    sync_engine.ignore_symlinks = False

    sync_engine.fs_events.queue_event.assert_called_once()
    queued_event = sync_engine.fs_events.queue_event.call_args.args[0]
    assert isinstance(queued_event, FileModifiedEvent)
    assert queued_event.src_path == str(link_path)
    assert queued_event.src_path != str(external_child)


def test_newly_included_local_file_is_rescanned(sync_engine: SyncEngine) -> None:
    local_file = Path(sync_engine.dropbox_path) / "LocalOnly.txt"
    local_file.write_text("upload me")
    sync_engine.fs_events.queue_event = Mock()  # type: ignore[method-assign]

    sync_engine.rescan_dbx_path("/localonly.txt")

    sync_engine.fs_events.queue_event.assert_called_once()
    event = sync_engine.fs_events.queue_event.call_args.args[0]
    assert isinstance(event, FileModifiedEvent)
    assert event.src_path == str(local_file)


def test_newly_included_local_folder_descendants_are_rescanned(
    sync_engine: SyncEngine,
) -> None:
    local_folder = Path(sync_engine.dropbox_path) / "Folder"
    local_folder.mkdir()
    local_child = local_folder / "LocalOnly.txt"
    local_child.write_text("upload me")
    sync_engine.set_selective_sync("include", ["/folder"])
    sync_engine.fs_events.queue_event = Mock()  # type: ignore[method-assign]

    sync_engine.rescan_dbx_path("/folder")

    events = [call.args[0] for call in sync_engine.fs_events.queue_event.call_args_list]
    assert any(
        isinstance(event, DirCreatedEvent) and event.src_path == str(local_folder)
        for event in events
    )
    assert any(
        isinstance(event, FileModifiedEvent) and event.src_path == str(local_child)
        for event in events
    )


def test_newly_included_root_rescans_children_without_root_creation(
    sync_engine: SyncEngine,
) -> None:
    local_file = Path(sync_engine.dropbox_path) / "LocalOnly.txt"
    local_file.write_text("upload me")
    local_folder = Path(sync_engine.dropbox_path) / "Folder"
    local_folder.mkdir()
    local_child = local_folder / "Child.txt"
    local_child.write_text("upload me too")
    sync_engine.fs_events.queue_event = Mock()  # type: ignore[method-assign]

    sync_engine.rescan_dbx_path("/")

    events = [call.args[0] for call in sync_engine.fs_events.queue_event.call_args_list]
    assert all(event.src_path != sync_engine.dropbox_path for event in events)
    assert any(event.src_path == str(local_file) for event in events)
    assert any(event.src_path == str(local_folder) for event in events)
    assert any(event.src_path == str(local_child) for event in events)


def test_newly_included_root_does_not_queue_indexed_local_file(
    sync_engine: SyncEngine,
) -> None:
    local_file = Path(sync_engine.dropbox_path) / "RemoteDeleted.txt"
    local_file.write_text("stale local copy")
    add_index_entry(sync_engine, "/RemoteDeleted.txt", ItemType.File)
    sync_engine.fs_events.queue_event = Mock()  # type: ignore[method-assign]

    sync_engine.rescan_dbx_path("/")

    sync_engine.fs_events.queue_event.assert_not_called()


def test_get_remote_item_does_not_replace_ignored_symlink(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target.txt"
    target_path.write_text("local target")
    link_path = Path(sync_engine.dropbox_path) / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    metadata = make_file_metadata("/link")
    sync_engine.client.get_metadata = Mock(  # type: ignore[method-assign]
        return_value=metadata
    )
    sync_engine.client.download = Mock()  # type: ignore[method-assign]

    assert sync_engine.get_remote_item("/link")

    assert link_path.is_symlink()
    assert target_path.read_text() == "local target"
    assert sync_engine.get_index_entry("/link").rev == "remote-rev"  # type: ignore[union-attr]
    sync_engine.client.download.assert_not_called()


@pytest.mark.parametrize("ignore_symlinks", [False, True])
def test_remote_folder_replaces_proof_matched_managed_symlink(
    sync_engine: SyncEngine,
    tmp_path: Path,
    ignore_symlinks: bool,
) -> None:
    target_path = tmp_path / "target.txt"
    target_path.write_text("external")
    link_path = Path(sync_engine.dropbox_path) / "item"
    create_symlink(link_path, target_path)
    add_index_entry(
        sync_engine,
        "/item",
        ItemType.File,
        symlink_target=str(target_path),
    )
    sync_engine.ignore_symlinks = ignore_symlinks
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.RemoteNewer
    )
    event = SyncEvent.from_metadata(make_folder_metadata("/item"), sync_engine)

    result = sync_engine._create_local_entry(event)

    assert result.status is SyncStatus.Done
    assert link_path.is_dir()
    assert not link_path.is_symlink()
    assert target_path.read_text() == "external"
    assert sync_engine.get_index_entry("/item").is_directory  # type: ignore[union-attr]


def test_remote_batch_replaces_managed_symlink_under_ignore_policy(
    sync_engine: SyncEngine,
    tmp_path: Path,
) -> None:
    target_path = tmp_path / "target.txt"
    target_path.write_text("external")
    link_path = Path(sync_engine.dropbox_path) / "item"
    create_symlink(link_path, target_path)
    add_index_entry(
        sync_engine,
        "/item",
        ItemType.File,
        symlink_target=str(target_path),
    )
    sync_engine.ignore_symlinks = True
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.RemoteNewer
    )
    event = SyncEvent.from_metadata(make_folder_metadata("/item"), sync_engine)

    results = sync_engine.apply_remote_changes([event])

    assert results == [event]
    assert event.status is SyncStatus.Done
    assert link_path.is_dir()
    assert not link_path.is_symlink()
    assert target_path.read_text() == "external"
    assert sync_engine.get_index_entry("/item").is_directory  # type: ignore[union-attr]


@pytest.mark.parametrize("ignore_symlinks", [False, True])
def test_remote_deletion_removes_proof_matched_managed_symlink(
    sync_engine: SyncEngine,
    tmp_path: Path,
    ignore_symlinks: bool,
) -> None:
    target_path = tmp_path / "target.txt"
    target_path.write_text("external")
    link_path = Path(sync_engine.dropbox_path) / "item"
    create_symlink(link_path, target_path)
    add_index_entry(
        sync_engine,
        "/item",
        ItemType.File,
        symlink_target=str(target_path),
    )
    sync_engine.ignore_symlinks = ignore_symlinks
    event = make_deleted_event(sync_engine, "/item", ItemType.File)

    result = sync_engine._create_local_entry(event)

    assert result.status is SyncStatus.Done
    assert not link_path.exists()
    assert not link_path.is_symlink()
    assert target_path.read_text() == "external"
    assert sync_engine.get_index_entry("/item") is None


def test_windows_directory_reparse_snapshot_is_a_managed_symlink(
    sync_engine: SyncEngine,
    tmp_path: Path,
) -> None:
    target_path = tmp_path / "target"
    link_path = Path(sync_engine.dropbox_path) / "item"
    add_index_entry(
        sync_engine,
        "/item",
        ItemType.File,
        symlink_target=str(target_path),
    )
    snapshot = {
        str(link_path): (
            1,
            2,
            stat.S_IFDIR | 0o755,
            0,
            0,
            0,
            f"symlink:{target_path}",
        )
    }

    assert sync_engine._snapshot_matches_index(snapshot, str(link_path))

    event = SyncEvent.from_metadata(make_folder_metadata("/item"), sync_engine)
    assert (
        sync_engine._check_download_conflict(event, snapshot) is not Conflict.Identical
    )


def test_windows_directory_reparse_snapshot_queues_file_events(
    sync_engine: SyncEngine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target_path = tmp_path / "target"
    link_path = Path(sync_engine.dropbox_path) / "junction"
    identity = (
        1,
        2,
        stat.S_IFDIR | 0o755,
        0,
        0,
        0,
        f"symlink:{target_path}",
    )
    monkeypatch.setattr(
        sync_module,
        "rooted_item_snapshot",
        lambda *_args, **_kwargs: identity,
    )
    sync_engine.fs_events.queue_event = Mock()  # type: ignore[method-assign]

    event = sync_engine._sync_event_from_fs_event(DirCreatedEvent(str(link_path)))

    assert event.item_type is ItemType.File
    assert event.content_hash is None
    assert event.symlink_target == str(target_path)

    sync_engine.rescan(str(link_path))
    sync_engine._rescan_unindexed(str(link_path))

    assert all(
        isinstance(call.args[0], FileModifiedEvent)
        for call in sync_engine.fs_events.queue_event.call_args_list
    )


def test_remote_file_replaces_folder_with_managed_symlink_descendant(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target.txt"
    target_path.write_text("external")
    folder_path = Path(sync_engine.dropbox_path) / "folder"
    folder_path.mkdir()
    link_path = folder_path / "link"
    create_symlink(link_path, target_path)
    add_index_entry(sync_engine, "/folder", ItemType.Folder)
    add_index_entry(
        sync_engine,
        "/folder/link",
        ItemType.File,
        symlink_target=str(target_path),
    )
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.RemoteNewer
    )
    metadata = make_file_metadata("/folder")

    def download_file(
        _rev: str, download_stream: BinaryIO, **_kwargs: object
    ) -> FileMetadata:
        download_stream.write(b"remote")
        return metadata

    sync_engine.client.download = Mock(  # type: ignore[method-assign]
        side_effect=download_file
    )
    event = SyncEvent.from_metadata(metadata, sync_engine)

    result = sync_engine._create_local_entry(event)

    assert result.status is SyncStatus.Done
    assert folder_path.is_file()
    assert folder_path.read_bytes() == b"remote"
    assert target_path.read_text() == "external"
    assert sync_engine.get_index_entry("/folder").is_file  # type: ignore[union-attr]
    assert sync_engine.get_index_entry("/folder/link") is None


@pytest.mark.parametrize(
    "metadata_factory",
    [make_file_metadata, make_folder_metadata],
    ids=["remote-file", "remote-folder"],
)
def test_remote_type_change_does_not_replace_unmanaged_symlink(
    sync_engine: SyncEngine,
    tmp_path: Path,
    metadata_factory: Callable[[str], FileMetadata | FolderMetadata],
) -> None:
    target_path = tmp_path / "target.txt"
    target_path.write_text("external")
    link_path = Path(sync_engine.dropbox_path) / "item"
    create_symlink(link_path, target_path)
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.RemoteNewer
    )
    sync_engine.client.download = Mock()  # type: ignore[method-assign]
    metadata = metadata_factory("/item")
    event = SyncEvent.from_metadata(metadata, sync_engine)

    result = sync_engine._create_local_entry(event)

    assert result.status is SyncStatus.Failed
    assert link_path.is_symlink()
    assert target_path.read_text() == "external"
    sync_engine.client.download.assert_not_called()


def test_remote_folder_to_file_preserves_ignored_symlink(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    folder_path = Path(sync_engine.dropbox_path) / "folder"
    folder_path.mkdir()
    managed_path = folder_path / "managed.txt"
    managed_contents = b"remove me"
    managed_path.write_bytes(managed_contents)
    link_path = folder_path / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    add_index_entry(sync_engine, "/folder", ItemType.Folder)
    hasher = DropboxContentHasher()
    hasher.update(managed_contents)
    add_index_entry(
        sync_engine,
        "/folder/managed.txt",
        ItemType.File,
        content_hash=hasher.hexdigest(),
    )
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.RemoteNewer
    )
    sync_engine.client.download = Mock()  # type: ignore[method-assign]
    deleted_event = make_deleted_event(sync_engine, "/folder", ItemType.Folder)
    file_event = SyncEvent.from_metadata(make_file_metadata("/folder"), sync_engine)

    results = sync_engine.apply_remote_changes([deleted_event, file_event])

    assert results == [deleted_event, file_event]
    assert deleted_event.status is SyncStatus.Done
    assert file_event.status is SyncStatus.Failed
    assert folder_path.is_dir()
    assert link_path.is_symlink()
    assert not managed_path.exists()
    assert sync_engine.get_index_entry("/folder") is None
    sync_engine.client.download.assert_not_called()


def test_targeted_folder_to_file_preserves_unseen_ignored_symlink(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    folder_path = Path(sync_engine.dropbox_path) / "folder"
    folder_path.mkdir()
    managed_path = folder_path / "managed.txt"
    managed_path.write_text("keep me")
    link_path = folder_path / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    sync_engine.client.get_metadata = Mock(  # type: ignore[method-assign]
        return_value=make_file_metadata("/folder")
    )
    sync_engine.client.download = Mock()  # type: ignore[method-assign]

    assert not sync_engine.get_remote_item("/folder")

    assert folder_path.is_dir()
    assert managed_path.read_text() == "keep me"
    assert link_path.is_symlink()
    assert sync_engine._stored_ignored_symlink_for_path("/folder/link") == (
        "/folder/link"
    )
    assert sync_engine.get_index_entry("/folder") is None
    sync_engine.client.download.assert_not_called()


def test_targeted_folder_to_file_removes_descendant_index_rows(
    sync_engine: SyncEngine,
) -> None:
    folder_path = Path(sync_engine.dropbox_path) / "folder"
    folder_path.mkdir()
    child_path = folder_path / "child.txt"
    child_path.write_text("old child")
    add_index_entry(sync_engine, "/folder", ItemType.Folder)
    add_index_entry(sync_engine, "/folder/child.txt", ItemType.File)
    metadata = make_file_metadata("/folder")
    sync_engine.client.get_metadata = Mock(  # type: ignore[method-assign]
        return_value=metadata
    )
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.RemoteNewer
    )

    def download_file(
        _rev: str, download_stream: BinaryIO, **_kwargs: object
    ) -> FileMetadata:
        download_stream.write(b"remote")
        return metadata

    sync_engine.client.download = Mock(  # type: ignore[method-assign]
        side_effect=download_file
    )

    assert sync_engine.get_remote_item("/folder")

    assert folder_path.is_file()
    assert folder_path.read_text() == "remote"
    assert sync_engine.get_index_entry("/folder").is_file  # type: ignore[union-attr]
    assert sync_engine.get_index_entry("/folder/child.txt") is None


def test_local_folder_to_identical_file_removes_descendant_index_rows(
    sync_engine: SyncEngine,
) -> None:
    local_path = Path(sync_engine.dropbox_path) / "folder"
    local_path.write_bytes(b"remote")
    add_index_entry(sync_engine, "/folder", ItemType.Folder)
    add_index_entry(sync_engine, "/folder/child.txt", ItemType.File)
    metadata = make_file_metadata("/folder")
    sync_engine.client.get_metadata = Mock(  # type: ignore[method-assign]
        return_value=metadata
    )
    sync_engine.client.upload = Mock()  # type: ignore[method-assign]
    sync_engine._wait_for_creation = Mock()  # type: ignore[method-assign]
    event = SyncEvent(
        direction=SyncDirection.Up,
        item_type=ItemType.File,
        sync_time=0,
        dbx_path="/folder",
        dbx_path_lower="/folder",
        local_path=str(local_path),
        content_hash=None,
        symlink_target=None,
        change_type=ChangeType.Added,
        change_time=0,
        change_dbid=None,
        status=SyncStatus.Queued,
        size=0,
        completed=0,
    )

    assert sync_engine._on_local_file_modified(event) is SyncStatus.Skipped

    assert sync_engine.get_index_entry("/folder").is_file  # type: ignore[union-attr]
    assert sync_engine.get_index_entry("/folder/child.txt") is None
    sync_engine.client.upload.assert_not_called()


def test_inactive_scan_does_not_check_descendant_through_error_policy_link(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    target_child = target_path / "child.txt"
    target_child.write_text("external")
    link_path = Path(sync_engine.dropbox_path) / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = False
    add_index_entry(sync_engine, "/link/child.txt", ItemType.File)
    sync_engine._exists_with_given_casing = Mock(  # type: ignore[method-assign]
        side_effect=AssertionError("existence lookup traversed the link")
    )

    changes, _ = sync_engine._get_local_changes_while_inactive()

    assert FileDeletedEvent(str(link_path / "child.txt")) not in changes
    sync_engine._exists_with_given_casing.assert_not_called()
    assert target_child.read_text() == "external"


def test_inactive_scan_queues_old_replacement_for_ignored_link(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target.txt"
    target_path.write_text("external")
    local_path = Path(sync_engine.dropbox_path) / "overlay.txt"
    create_symlink(local_path, target_path)
    sync_engine.ignore_symlinks = True
    sync_engine._remember_ignored_symlink("/overlay.txt")
    add_index_entry(sync_engine, "/overlay.txt", ItemType.File)
    local_path.unlink()
    local_path.write_text("local replacement")
    os.utime(local_path, (1, 1))
    sync_engine.fs_events.queue_event = Mock()  # type: ignore[method-assign]

    sync_engine._get_local_changes_while_inactive()

    assert sync_engine.get_index_entry("/overlay.txt") is None
    assert sync_engine._stored_ignored_symlink_for_path("/overlay.txt") is None
    sync_engine.fs_events.queue_event.assert_called_once_with(
        FileModifiedEvent(str(local_path))
    )


def test_folder_to_file_rechecks_for_symlink_created_during_download(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    folder_path = Path(sync_engine.dropbox_path) / "folder"
    folder_path.mkdir()
    link_path = folder_path / "link"
    sync_engine.ignore_symlinks = True
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.RemoteNewer
    )
    downloaded_paths: list[Path] = []

    def download_with_link(
        _rev: str, download_stream: BinaryIO, **_kwargs: object
    ) -> FileMetadata:
        download_stream.write(b"remote")
        downloaded_paths.extend(Path(sync_engine.file_cache_path).iterdir())
        create_symlink(link_path, target_path)
        return make_file_metadata("/folder")

    sync_engine.client.download = Mock(  # type: ignore[method-assign]
        side_effect=download_with_link
    )
    event = SyncEvent.from_metadata(make_file_metadata("/folder"), sync_engine)

    result = sync_engine._create_local_entry(event)

    assert result.status is SyncStatus.Failed
    assert folder_path.is_dir()
    assert link_path.is_symlink()
    assert downloaded_paths and not downloaded_paths[0].exists()


@pytest.mark.parametrize("ignore_symlinks", [False, True])
def test_case_change_does_not_move_directory_with_symlink(
    sync_engine: SyncEngine,
    tmp_path: Path,
    ignore_symlinks: bool,
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    old_folder_path = Path(sync_engine.dropbox_path) / "Folder"
    old_folder_path.mkdir()
    link_path = old_folder_path / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = ignore_symlinks
    add_index_entry(sync_engine, "/Folder", ItemType.Folder)
    sync_engine.client.download = Mock()  # type: ignore[method-assign]
    event = SyncEvent.from_metadata(make_file_metadata("/folder"), sync_engine)

    result = sync_engine._create_local_entry(event)

    assert result.status is SyncStatus.Failed
    assert old_folder_path.is_dir()
    assert link_path.is_symlink()
    assert sync_engine.get_index_entry("/folder").dbx_path_cased == "/Folder"  # type: ignore[union-attr]
    sync_engine.client.download.assert_not_called()


def test_case_change_moves_managed_symlink_with_ignore_policy(
    sync_engine: SyncEngine,
    tmp_path: Path,
) -> None:
    target_path = tmp_path / "target"
    target_path.write_text("external")
    old_path = Path(sync_engine.dropbox_path) / "Item"
    new_path = Path(sync_engine.dropbox_path) / "item"
    create_symlink(old_path, target_path)
    add_index_entry(
        sync_engine,
        "/Item",
        ItemType.File,
        symlink_target=str(target_path),
    )
    sync_engine.ignore_symlinks = True
    event = make_deleted_event(sync_engine, "/item", ItemType.File)

    sync_engine._apply_case_change(event)

    assert new_path.is_symlink()
    assert get_symlink_target(str(new_path)) == str(target_path)
    assert "item" in {path.name for path in Path(sync_engine.dropbox_path).iterdir()}
    assert "Item" not in {
        path.name for path in Path(sync_engine.dropbox_path).iterdir()
    }
    assert sync_engine.get_index_entry("/item").dbx_path_cased == "/item"  # type: ignore[union-attr]


def test_case_change_preserves_distinct_destination_on_case_sensitive_fs(
    sync_engine: SyncEngine,
) -> None:
    if not sync_engine.is_fs_case_sensitive:
        pytest.skip("The test needs a case-sensitive file system")

    old_path = Path(sync_engine.dropbox_path) / "Foo"
    old_path.write_text("managed")
    destination = Path(sync_engine.dropbox_path) / "foo"
    destination.write_text("local only")
    add_index_entry(sync_engine, "/Foo", ItemType.File)
    event = SyncEvent.from_metadata(make_file_metadata("/foo"), sync_engine)

    result = sync_engine._create_local_entry(event)

    assert result.status is SyncStatus.Failed
    assert old_path.read_text() == "managed"
    assert destination.read_text() == "local only"


def test_folder_case_change_updates_descendant_index_rows(
    sync_engine: SyncEngine,
) -> None:
    old_folder_path = Path(sync_engine.dropbox_path) / "Folder"
    old_folder_path.mkdir()
    child_path = old_folder_path / "Child.txt"
    child_path.write_text("child")
    add_index_entry(sync_engine, "/Folder", ItemType.Folder)
    add_index_entry(sync_engine, "/Folder/Child.txt", ItemType.File)
    event = SyncEvent.from_metadata(make_folder_metadata("/folder"), sync_engine)

    sync_engine._apply_case_change(event)

    names = {entry.name for entry in Path(sync_engine.dropbox_path).iterdir()}
    assert "folder" in names
    assert "Folder" not in names
    assert sync_engine.get_index_entry("/folder").dbx_path_cased == "/folder"  # type: ignore[union-attr]
    assert (
        sync_engine.get_index_entry("/folder/child.txt").dbx_path_cased  # type: ignore[union-attr]
        == "/folder/Child.txt"
    )


def test_linux_casefold_case_change_uses_durable_stage(
    sync_engine: SyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_path = Path(sync_engine.dropbox_path) / "Folder"
    new_path = Path(sync_engine.dropbox_path) / "folder"
    old_path.mkdir()
    add_index_entry(sync_engine, "/Folder", ItemType.Folder)
    event = SyncEvent.from_metadata(make_folder_metadata("/folder"), sync_engine)
    real_lexists = sync_module.osp.lexists
    real_samefile = sync_module.osp.samefile
    real_move = sync_module.move
    moves: list[tuple[str, str, bool]] = []

    def casefold_lexists(path: str) -> bool:
        if path == str(new_path) and old_path.exists():
            return True
        return real_lexists(path)

    def casefold_samefile(first: str, second: str) -> bool:
        if {first, second} == {str(old_path), str(new_path)} and old_path.exists():
            return True
        return real_samefile(first, second)

    def recording_move(source: str, destination: str, **kwargs) -> object:
        moves.append((source, destination, kwargs["replace"]))
        return real_move(source, destination, **kwargs)

    monkeypatch.setattr(sync_module.osp, "lexists", casefold_lexists)
    monkeypatch.setattr(sync_module.osp, "samefile", casefold_samefile)
    monkeypatch.setattr(sync_module, "move", recording_move)

    sync_engine._apply_case_change(event)

    assert new_path.is_dir()
    names = {path.name for path in Path(sync_engine.dropbox_path).iterdir()}
    assert "folder" in names
    assert "Folder" not in names
    assert len(moves) == 2
    assert moves[0][0] == str(old_path)
    assert Path(moves[0][1]).name.startswith(CASE_CHANGE_TEMP_PREFIX)
    assert moves[1][0] == moves[0][1]
    assert moves[1][1] == str(new_path)
    assert all(not replace for _, _, replace in moves)
    assert sync_engine._validated_case_changes() == {}


@pytest.mark.parametrize("actual_name", ["Folder", "folder"])
def test_planned_case_change_recovery_requires_new_exact_casing(
    sync_engine: SyncEngine,
    monkeypatch: pytest.MonkeyPatch,
    actual_name: str,
) -> None:
    actual_path = Path(sync_engine.dropbox_path) / actual_name
    actual_path.write_text("managed")
    actual_identity = sync_engine._snapshot_local_tree(str(actual_path))[
        str(actual_path)
    ]
    add_index_entry(sync_engine, "/Folder", ItemType.File)
    sync_engine._state.set(
        "recovery",
        "case_changes",
        {
            "/folder": {
                "old_path": "/Folder",
                "new_path": "/folder",
                "stage_name": f"{CASE_CHANGE_TEMP_PREFIX}{'a' * 32}",
                "identity": list(actual_identity[:3]),
                "phase": "planned",
            }
        },
    )

    def has_exact_case(path: str, *_args: object, **_kwargs: object) -> bool:
        return Path(path).name == actual_name

    monkeypatch.setattr("maestral.sync.rooted_name_has_exact_case", has_exact_case)
    monkeypatch.setattr(
        "maestral.sync.rooted_item_snapshot",
        lambda *_args, **_kwargs: actual_identity,
    )

    sync_engine._recover_pending_case_changes()

    entry = sync_engine.get_index_entry("/folder")
    assert entry is not None
    assert entry.dbx_path_cased == "/folder"
    assert sync_engine._validated_case_changes() == {}


@pytest.mark.parametrize("phase", ["planned", "staged"])
def test_case_change_recovery_completes_private_stage(
    sync_engine: SyncEngine,
    phase: str,
) -> None:
    old_path = Path(sync_engine.dropbox_path) / "Folder"
    new_path = Path(sync_engine.dropbox_path) / "folder"
    stage_name = f"{CASE_CHANGE_TEMP_PREFIX}{'b' * 32}"
    stage_path = Path(sync_engine.dropbox_path) / stage_name
    old_path.write_text("managed")
    identity = list(sync_engine._snapshot_local_tree(str(old_path))[str(old_path)][:3])
    add_index_entry(sync_engine, "/Folder", ItemType.File)
    old_path.rename(stage_path)
    sync_engine._state.set(
        "recovery",
        "case_changes",
        {
            "/folder": {
                "old_path": "/Folder",
                "new_path": "/folder",
                "stage_name": stage_name,
                "identity": identity,
                "phase": phase,
            }
        },
    )

    sync_engine._recover_pending_case_changes()

    assert new_path.read_text() == "managed"
    assert not stage_path.exists()
    assert sync_engine.get_index_entry("/folder").dbx_path_cased == "/folder"  # type: ignore[union-attr]
    assert sync_engine._validated_case_changes() == {}


def test_deleted_new_team_folder_preserves_ignored_symlink(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    folder_path = Path(sync_engine.dropbox_path) / "team-folder"
    folder_path.mkdir()
    managed_path = folder_path / "managed.txt"
    managed_path.write_text("remove me")
    link_path = folder_path / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    sync_engine.client._is_team_space = True
    sync_engine.client.share_dir = Mock(return_value=None)  # type: ignore[method-assign]
    sync_engine._wait_for_creation = Mock()  # type: ignore[method-assign]
    event = SyncEvent(
        direction=SyncDirection.Up,
        item_type=ItemType.Folder,
        sync_time=0,
        dbx_path="/team-folder",
        dbx_path_lower="/team-folder",
        local_path=str(folder_path),
        content_hash="folder",
        symlink_target=None,
        change_type=ChangeType.Added,
        change_time=0,
        change_dbid=None,
        status=SyncStatus.Queued,
        size=0,
        completed=0,
    )

    result = sync_engine._create_remote_entry(event)

    assert result.status is SyncStatus.Conflict
    assert folder_path.is_dir()
    assert link_path.is_symlink()
    assert managed_path.read_text() == "remove me"
    assert sync_engine.upload_errors == []


@pytest.mark.parametrize("error_type", [DropboxConnectionError, DropboxServerError])
@pytest.mark.parametrize("worker", ["active", "startup"])
def test_added_download_workers_requeue_transient_errors(
    m: Maestral,
    error_type: type[DropboxConnectionError | DropboxServerError],
    worker: str,
) -> None:
    stop_connection_helper(m)
    manager = m.manager
    dbx_path = "/newly-included"
    manager.running.set()
    manager.startup_completed.set()
    manager.download_queue.put(dbx_path)
    manager.sync.get_remote_item = Mock(  # type: ignore[method-assign]
        side_effect=error_type("Dropbox unavailable", "Try again later")
    )
    manager.stop = Mock(  # type: ignore[method-assign]
        side_effect=manager.running.clear
    )

    if worker == "active":
        manager.download_worker_added_item(
            manager.running,
            manager.startup_completed,
            manager.autostart,
        )
    else:
        manager.sync.ensure_dropbox_folder_present = Mock()  # type: ignore[method-assign]
        manager.sync.load_mignore_file = Mock()  # type: ignore[method-assign]
        manager.sync.client.get_space_usage = Mock()  # type: ignore[method-assign]
        manager.check_and_update_path_root = Mock()  # type: ignore[method-assign]
        manager.startup_worker(
            manager.running,
            manager.startup_completed,
            manager.autostart,
        )

    manager.stop.assert_not_called()
    assert not manager.running.is_set()
    assert manager.autostart.is_set()
    assert manager.download_queue.qsize() == 1
    assert manager.download_queue.get() == dbx_path
    manager.download_queue.task_done(dbx_path)


def test_added_download_workers_requeue_failed_downloads(
    m: Maestral,
) -> None:
    stop_connection_helper(m)
    manager = m.manager
    dbx_path = "/newly-included"
    manager.running.set()
    manager.startup_completed.set()
    manager.download_queue.put(dbx_path)
    manager.download_retry_interval = 0
    manager.download_queue.requeue = Mock(  # type: ignore[method-assign]
        wraps=manager.download_queue.requeue
    )

    def fail_once_then_stop(path: str) -> bool:
        if manager.sync.get_remote_item.call_count == 1:
            return False
        manager.running.clear()
        return True

    manager.sync.get_remote_item = Mock(  # type: ignore[method-assign]
        side_effect=fail_once_then_stop
    )

    manager.download_worker_added_item(
        manager.running,
        manager.startup_completed,
        manager.autostart,
    )

    assert manager.sync.get_remote_item.call_count == 2
    manager.download_queue.requeue.assert_called_once_with(dbx_path)
    assert manager.download_queue.qsize() == 0


def test_startup_worker_requeues_failed_download(m: Maestral) -> None:
    stop_connection_helper(m)
    manager = m.manager
    dbx_path = "/newly-included"
    manager.running.set()
    manager.startup_completed.set()
    manager.download_queue.put(dbx_path)
    manager.sync.get_remote_item = Mock(  # type: ignore[method-assign]
        return_value=False
    )
    manager.sync.ensure_dropbox_folder_present = Mock()  # type: ignore[method-assign]
    manager.sync.load_mignore_file = Mock()  # type: ignore[method-assign]
    manager.sync.client.get_space_usage = Mock()  # type: ignore[method-assign]
    manager.check_and_update_path_root = Mock()  # type: ignore[method-assign]
    manager.sync.download_sync_cycle = Mock()  # type: ignore[method-assign]
    manager.sync.upload_local_changes_while_inactive = (  # type: ignore[method-assign]
        Mock()
    )

    manager.startup_worker(
        manager.running,
        manager.startup_completed,
        manager.autostart,
    )

    manager.sync.get_remote_item.assert_called_once_with(dbx_path)
    assert manager.download_queue.qsize() == 1
    assert manager.download_queue.get() == dbx_path
    manager.download_queue.task_done(dbx_path)


def configure_root_migration(
    m: Maestral,
    tmp_path: Path,
    transition: str,
) -> tuple[Path, UserRootInfo | TeamRootInfo]:
    stop_connection_helper(m)
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    m.sync.dropbox_path = str(dropbox_path)
    m.sync.create_root_marker()
    m.sync.set_selective_sync("include", ["/"])

    if transition == "user_to_team":
        (dropbox_path / "Personal.txt").write_text("personal")
        old_root_type = "user"
        old_home_path = ""
        root_info: UserRootInfo | TeamRootInfo = TeamRootInfo(
            root_namespace_id="new-team",
            home_namespace_id="user",
            home_path="/Home",
        )
    elif transition == "team_to_user":
        home_path = dropbox_path / "Old Home"
        home_path.mkdir()
        (home_path / "Personal.txt").write_text("personal")
        (home_path / "Second.txt").write_text("second")
        (dropbox_path / "Team Folder").mkdir()
        old_root_type = "team"
        old_home_path = "/Old Home"
        root_info = UserRootInfo(
            root_namespace_id="new-user",
            home_namespace_id="user",
        )
        add_index_entry(m.sync, "/Old Home", ItemType.Folder)
        add_index_entry(m.sync, "/Old Home/Personal.txt", ItemType.File)
        add_index_entry(m.sync, "/Old Home/Second.txt", ItemType.File)
        add_index_entry(m.sync, "/Team Folder", ItemType.Folder)
    else:
        home_path = dropbox_path / "Old Home"
        home_path.mkdir()
        (home_path / "Personal.txt").write_text("personal")
        (dropbox_path / "Team Folder").mkdir()
        old_root_type = "team"
        old_home_path = "/Old Home"
        root_info = TeamRootInfo(
            root_namespace_id="new-team",
            home_namespace_id="user",
            home_path="/New Home",
        )
        add_index_entry(m.sync, "/Old Home", ItemType.Folder)
        add_index_entry(m.sync, "/Old Home/Personal.txt", ItemType.File)
        add_index_entry(m.sync, "/Team Folder", ItemType.Folder)

    m.set_state("account", "path_root_type", old_root_type)
    m.set_state("account", "path_root_nsid", "old-root")
    m.set_state("account", "home_path", old_home_path)
    m.client._cached_account_info = FullAccount(
        account_id="id:user",
        display_name="User",
        email="user@example.com",
        profile_photo_url=None,
        email_verified=True,
        disabled=False,
        country=None,
        locale="en",
        team=None,
        team_member_id=None,
        account_type=AccountType.Business,
        root_info=root_info,
    )

    def save_root_state(info: UserRootInfo | TeamRootInfo) -> None:
        root_type = "team" if isinstance(info, TeamRootInfo) else "user"
        home = info.home_path if isinstance(info, TeamRootInfo) else ""
        with m.manager._state._lock:
            m.manager._state.set(
                "account", "path_root_nsid", info.root_namespace_id, save=False
            )
            m.manager._state.set("account", "path_root_type", root_type, save=False)
            m.manager._state.set("account", "home_path", home, save=False)
            m.manager._state.save()

    m.client.update_path_root = Mock(  # type: ignore[method-assign]
        side_effect=save_root_state
    )
    return dropbox_path, root_info


def assert_root_migration_completed(
    m: Maestral,
    dropbox_path: Path,
    transition: str,
    root_info: UserRootInfo | TeamRootInfo,
) -> None:
    if transition == "user_to_team":
        personal_path = dropbox_path / "Home" / "Personal.txt"
    elif transition == "team_to_user":
        personal_path = dropbox_path / "Personal.txt"
        assert (dropbox_path / "Second.txt").read_text() == "second"
        assert not (dropbox_path / "Team Folder").exists()
    else:
        personal_path = dropbox_path / "New Home" / "Personal.txt"
        assert not (dropbox_path / "Team Folder").exists()

    assert personal_path.read_text() == "personal"
    assert m.get_state("account", "path_root_nsid") == root_info.root_namespace_id
    assert m.get_state("account", "path_root_migration") == {}
    assert not m.manager.download_queue.has_pending()


@pytest.mark.parametrize("transition", ["user_to_team", "team_to_user", "team_to_team"])
@pytest.mark.parametrize(
    "crash_point",
    [
        "prepared",
        "partial_staging",
        "final_layout",
        "selection_saved",
        "root_updated",
        "files_cleaned",
        "sync_reset",
    ],
)
def test_path_root_migration_resumes_each_durable_phase(
    m: Maestral,
    tmp_path: Path,
    transition: str,
    crash_point: str,
) -> None:
    dropbox_path, root_info = configure_root_migration(m, tmp_path, transition)
    manager = m.manager
    migration = manager._new_path_root_migration(root_info)
    manager.download_queue.put("/old-root/item")

    if crash_point == "partial_staging":
        stage_path = Path(migration["staging_path"])
        stage_path.mkdir(mode=0o700)
        manager._ensure_path_root_migration_proof(migration, create=True)
        first_name = migration["top_level_names"][0]
        source_path = dropbox_path / first_name
        destination_path = stage_path / first_name
        manager._path_root_move_proof(
            migration,
            str(source_path),
            str(destination_path),
            f"stage:{first_name}",
        )
        os.rename(source_path, destination_path)
    elif crash_point != "prepared":
        manager._apply_path_root_file_moves(migration)

        if crash_point in {
            "selection_saved",
            "root_updated",
            "files_cleaned",
            "sync_reset",
        }:
            manager._set_path_root_migration_phase(migration, "FILES_MOVED")
            m.sync.set_selective_sync(
                migration["new_selection_mode"],
                migration["new_selection_paths"],
            )

        if crash_point in {"root_updated", "files_cleaned", "sync_reset"}:
            manager._set_path_root_migration_phase(migration, "SELECTION_SAVED")
            m.client.update_path_root(root_info)

        if crash_point in {"files_cleaned", "sync_reset"}:
            manager._set_path_root_migration_phase(migration, "ROOT_UPDATED")
            manager._cleanup_path_root_migration_files(migration)

        if crash_point == "sync_reset":
            manager.reset_sync_state()
            manager._set_path_root_migration_phase(migration, "SYNC_RESET")

    manager._update_path_root()

    assert_root_migration_completed(m, dropbox_path, transition, root_info)


def test_path_root_migration_keeps_proof_after_prepared_postcommit_interrupt(
    m: Maestral,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dropbox_path, root_info = configure_root_migration(m, tmp_path, "user_to_team")
    manager = m.manager
    original_save = manager._state.save
    interrupted = False

    def save_then_interrupt() -> None:
        nonlocal interrupted
        original_save()
        migration = manager._state.get("account", "path_root_migration")
        if (
            not interrupted
            and isinstance(migration, dict)
            and migration.get("phase") == "PREPARED"
        ):
            interrupted = True
            raise KeyboardInterrupt("after prepared commit")

    monkeypatch.setattr(manager._state, "save", save_then_interrupt)

    with pytest.raises(KeyboardInterrupt, match="after prepared commit"):
        manager._new_path_root_migration(root_info)

    migration = manager._state.get("account", "path_root_migration")
    assert migration["phase"] == "PREPARED"
    assert Path(manager._path_root_migration_proof_path(migration)).is_file()

    monkeypatch.setattr(manager._state, "save", original_save)
    manager._update_path_root()

    assert interrupted
    assert_root_migration_completed(m, dropbox_path, "user_to_team", root_info)


@pytest.mark.parametrize(
    "phase",
    ["PREPARED", "FILES_MOVED", "SELECTION_SAVED"],
)
def test_path_root_migration_restarts_for_changed_uncommitted_target(
    m: Maestral,
    tmp_path: Path,
    phase: str,
) -> None:
    dropbox_path, first_root = configure_root_migration(m, tmp_path, "user_to_team")
    m.sync.set_selective_sync("include", ["/Personal.txt"])
    manager = m.manager
    migration = manager._new_path_root_migration(first_root)

    if phase in {"FILES_MOVED", "SELECTION_SAVED"}:
        manager._apply_path_root_file_moves(migration)
        manager._set_path_root_migration_phase(migration, "FILES_MOVED")
    if phase == "SELECTION_SAVED":
        m.sync.set_selective_sync(
            migration["new_selection_mode"],
            migration["new_selection_paths"],
        )
        manager._set_path_root_migration_phase(migration, "SELECTION_SAVED")

    second_root = TeamRootInfo(
        root_namespace_id="second-team",
        home_namespace_id="user",
        home_path="/Other Home",
    )
    m.client._cached_account_info.root_info = second_root  # type: ignore[union-attr]

    manager._update_path_root()

    assert (dropbox_path / "Other Home" / "Personal.txt").read_text() == "personal"
    assert not (dropbox_path / "Home").exists()
    assert m.sync.selective_sync_paths == {"/other home/personal.txt"}
    assert m.get_state("account", "path_root_nsid") == "second-team"
    assert m.get_state("account", "path_root_migration") == {}


@pytest.mark.parametrize("phase", ["ROOT_UPDATED", "SYNC_RESET"])
def test_path_root_migration_finishes_committed_target_before_new_target(
    m: Maestral,
    tmp_path: Path,
    phase: str,
) -> None:
    dropbox_path, first_root = configure_root_migration(m, tmp_path, "user_to_team")
    m.sync.set_selective_sync("include", ["/Personal.txt"])
    manager = m.manager
    migration = manager._new_path_root_migration(first_root)
    manager._apply_path_root_file_moves(migration)
    manager._set_path_root_migration_phase(migration, "FILES_MOVED")
    m.sync.set_selective_sync(
        migration["new_selection_mode"],
        migration["new_selection_paths"],
    )
    manager._set_path_root_migration_phase(migration, "SELECTION_SAVED")
    m.client.update_path_root(first_root)
    manager._set_path_root_migration_phase(migration, "ROOT_UPDATED")
    if phase == "SYNC_RESET":
        manager._cleanup_path_root_migration_files(migration)
        manager.reset_sync_state()
        manager._set_path_root_migration_phase(migration, "SYNC_RESET")

    second_root = TeamRootInfo(
        root_namespace_id="second-team",
        home_namespace_id="user",
        home_path="/Other Home",
    )
    m.client._cached_account_info.root_info = second_root  # type: ignore[union-attr]

    manager._update_path_root()

    assert (dropbox_path / "Other Home" / "Personal.txt").read_text() == "personal"
    assert not (dropbox_path / "Home").exists()
    assert m.sync.selective_sync_paths == {"/other home/personal.txt"}
    assert m.get_state("account", "path_root_nsid") == "second-team"
    assert m.get_state("account", "path_root_migration") == {}


def test_user_to_team_move_failure_rolls_back_staged_data(
    m: Maestral,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dropbox_path, _ = configure_root_migration(m, tmp_path, "user_to_team")
    (dropbox_path / "Second.txt").write_text("second")
    original_move = manager_module.move
    staged_moves = 0

    def fail_second_stage(
        source: str,
        destination: str,
        *args: object,
        **kwargs: object,
    ) -> object:
        nonlocal staged_moves
        if ".maestral-migration-" in str(destination):
            staged_moves += 1
            if staged_moves == 2:
                raise PermissionError("injected move failure")
        return original_move(source, destination, *args, **kwargs)

    monkeypatch.setattr(manager_module, "move", fail_second_stage)

    with pytest.raises(PermissionError, match="injected move failure"):
        m.manager._update_path_root()

    assert (dropbox_path / "Personal.txt").read_text() == "personal"
    assert (dropbox_path / "Second.txt").read_text() == "second"
    assert m.get_state("account", "path_root_migration") == {}


def test_team_to_user_move_failure_rolls_back_personal_data(
    m: Maestral,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dropbox_path, _ = configure_root_migration(m, tmp_path, "team_to_user")
    original_move = manager_module.move
    final_moves = 0

    def fail_second_personal_move(
        source: str,
        destination: str,
        *args: object,
        **kwargs: object,
    ) -> object:
        nonlocal final_moves
        if ".maestral-migration-" in str(source) and Path(destination).parent == (
            dropbox_path
        ):
            final_moves += 1
            if final_moves == 2:
                raise PermissionError("injected personal move failure")
        return original_move(source, destination, *args, **kwargs)

    monkeypatch.setattr(manager_module, "move", fail_second_personal_move)

    with pytest.raises(PermissionError, match="injected personal move failure"):
        m.manager._update_path_root()

    assert (dropbox_path / "Old Home" / "Personal.txt").read_text() == "personal"
    assert (dropbox_path / "Old Home" / "Second.txt").read_text() == "second"
    assert (dropbox_path / "Team Folder").is_dir()
    assert m.get_state("account", "path_root_migration") == {}


def test_path_root_migration_preserves_unsynced_team_files(
    m: Maestral,
    tmp_path: Path,
) -> None:
    dropbox_path, root_info = configure_root_migration(m, tmp_path, "team_to_user")
    unsynced_file = dropbox_path / "Team Folder" / "local-only.txt"
    unsynced_file.write_text("keep me")

    m.manager._update_path_root()

    recovery_paths = list(dropbox_path.glob(f"{PATH_ROOT_RECOVERY_PREFIX}*"))
    assert len(recovery_paths) == 1
    recovered_file = recovery_paths[0] / "Team Folder" / "local-only.txt"
    assert recovered_file.read_text() == "keep me"
    assert m.sync.is_excluded(str(recovery_paths[0]))
    assert_root_migration_completed(
        m,
        dropbox_path,
        "team_to_user",
        root_info,
    )


@pytest.mark.parametrize("transition", ["team_to_user", "team_to_team"])
def test_path_root_migration_never_deletes_old_team_trees(
    m: Maestral,
    tmp_path: Path,
    transition: str,
) -> None:
    dropbox_path, root_info = configure_root_migration(m, tmp_path, transition)
    old_team_file = dropbox_path / "Team Folder" / "remote-copy.txt"
    old_team_file.write_text("keep recoverable")
    add_index_entry(m.sync, "/Team Folder/remote-copy.txt", ItemType.File)

    m.manager._update_path_root()

    recovery_paths = list(dropbox_path.glob(f"{PATH_ROOT_RECOVERY_PREFIX}*"))
    assert len(recovery_paths) == 1
    recovered_file = recovery_paths[0] / "Team Folder" / "remote-copy.txt"
    assert recovered_file.read_text() == "keep recoverable"
    assert_root_migration_completed(m, dropbox_path, transition, root_info)


@pytest.mark.parametrize("mode", ["exclude", "include"])
@pytest.mark.parametrize(
    ("current_root_type", "current_home_path", "new_root_info"),
    [
        (
            "user",
            "",
            TeamRootInfo(
                root_namespace_id="team-1",
                home_namespace_id="user-1",
                home_path="/home",
            ),
        ),
        (
            "team",
            "/home",
            UserRootInfo(
                root_namespace_id="user-1",
                home_namespace_id="user-1",
            ),
        ),
        (
            "team",
            "/home",
            TeamRootInfo(
                root_namespace_id="team-2",
                home_namespace_id="user-1",
                home_path="/home",
            ),
        ),
    ],
    ids=["user-to-team", "team-to-user", "team-to-team"],
)
def test_root_selection_survives_path_root_migration(
    m: Maestral,
    tmp_path: Path,
    mode: str,
    current_root_type: str,
    current_home_path: str,
    new_root_info: UserRootInfo | TeamRootInfo,
) -> None:
    stop_connection_helper(m)
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    if current_root_type == "team":
        (dropbox_path / "home").mkdir()
    m.sync.dropbox_path = str(dropbox_path)
    m.sync.create_root_marker()
    m.sync.set_selective_sync(mode, ["/"])
    m.set_state("account", "path_root_type", current_root_type)
    m.set_state("account", "path_root_nsid", "old-root")
    m.set_state("account", "home_path", current_home_path)
    m.client._cached_account_info = FullAccount(
        account_id="id:user",
        display_name="User",
        email="user@example.com",
        profile_photo_url=None,
        email_verified=True,
        disabled=False,
        country=None,
        locale="en",
        team=None,
        team_member_id=None,
        account_type=AccountType.Business,
        root_info=new_root_info,
    )
    m.client.update_path_root = Mock()  # type: ignore[method-assign]
    m.manager.download_queue.put("/old-namespace/item")

    m.manager._update_path_root()

    assert m.sync.selective_sync_mode == mode
    assert m.sync.selective_sync_paths == {"/"}
    assert not m.manager.download_queue.has_pending()
    assert m.get_state("sync", "pending_downloads") == []


@pytest.mark.parametrize("mode", ["exclude", "include"])
def test_team_switch_moves_changed_home_and_selection_prefix(
    m: Maestral,
    tmp_path: Path,
    mode: str,
) -> None:
    stop_connection_helper(m)
    dropbox_path = tmp_path / "Dropbox"
    old_home_path = dropbox_path / "Old Home"
    old_home_path.mkdir(parents=True)
    personal_file = old_home_path / "Keep.txt"
    personal_file.write_text("keep me")
    team_path = dropbox_path / "Team Folder"
    team_path.mkdir()
    m.sync.dropbox_path = str(dropbox_path)
    m.sync.create_root_marker()
    m.sync.set_selective_sync(
        mode,
        ["/Old Home/Keep.txt", "/Team Folder/Other.txt"],
    )
    m.set_state("account", "path_root_type", "team")
    m.set_state("account", "path_root_nsid", "old-team")
    m.set_state("account", "home_path", "/Old Home")
    m.client._cached_account_info = FullAccount(
        account_id="id:user",
        display_name="User",
        email="user@example.com",
        profile_photo_url=None,
        email_verified=True,
        disabled=False,
        country=None,
        locale="en",
        team=None,
        team_member_id=None,
        account_type=AccountType.Business,
        root_info=TeamRootInfo(
            root_namespace_id="new-team",
            home_namespace_id="user-1",
            home_path="/New Home",
        ),
    )
    m.client.update_path_root = Mock()  # type: ignore[method-assign]

    m.manager._update_path_root()

    new_home_path = dropbox_path / "New Home"
    assert not old_home_path.exists()
    assert (new_home_path / "Keep.txt").read_text() == "keep me"
    assert not team_path.exists()
    assert m.sync.selective_sync_mode == mode
    assert m.sync.selective_sync_paths == {"/new home/keep.txt"}
    m.client.update_path_root.assert_called_once()


@pytest.mark.parametrize(
    ("current_root_type", "current_home_path", "new_root_info"),
    [
        (
            "user",
            "",
            TeamRootInfo(
                root_namespace_id="team-1",
                home_namespace_id="user-1",
                home_path="/home",
            ),
        ),
        (
            "team",
            "/home",
            UserRootInfo(
                root_namespace_id="user-1",
                home_namespace_id="user-1",
            ),
        ),
        (
            "team",
            "/home",
            TeamRootInfo(
                root_namespace_id="team-2",
                home_namespace_id="user-1",
                home_path="/home",
            ),
        ),
    ],
    ids=["user-to-team", "team-to-user", "team-to-team"],
)
def test_path_root_migration_preflights_ignored_symlinks(
    m: Maestral,
    tmp_path: Path,
    current_root_type: str,
    current_home_path: str,
    new_root_info: UserRootInfo | TeamRootInfo,
) -> None:
    stop_connection_helper(m)
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    first_path = dropbox_path / "first"
    first_path.mkdir()
    second_path = dropbox_path / "second"
    second_path.mkdir()
    if current_root_type == "team":
        (dropbox_path / "home").mkdir()
    target_path = tmp_path / "target"
    target_path.mkdir()
    link_path = second_path / "link"
    create_symlink(link_path, target_path)
    m.sync.dropbox_path = str(dropbox_path)
    m.sync.create_root_marker()
    m.sync.ignore_symlinks = True
    m.set_state("account", "path_root_type", current_root_type)
    m.set_state("account", "path_root_nsid", "old-root")
    m.set_state("account", "home_path", current_home_path)
    m.client._cached_account_info = FullAccount(
        account_id="id:user",
        display_name="User",
        email="user@example.com",
        profile_photo_url=None,
        email_verified=True,
        disabled=False,
        country=None,
        locale="en",
        team=None,
        team_member_id=None,
        account_type=AccountType.Business,
        root_info=new_root_info,
    )
    m.client.update_path_root = Mock()  # type: ignore[method-assign]

    with pytest.raises(SymlinkError, match="Cannot migrate folder structure"):
        m.manager._update_path_root()

    assert first_path.is_dir()
    assert second_path.is_dir()
    assert link_path.is_symlink()
    assert m.get_state("account", "path_root_nsid") == "old-root"
    m.client.update_path_root.assert_not_called()


@pytest.mark.parametrize(
    ("section", "option"),
    [
        ("sync", "ignore_symlinks"),
        ("sync", "IGNORE_SYMLINKS"),
        ("sync", "Selective_Sync_Paths"),
        ("sync", "SELECTIVE_SYNC_MODE"),
        ("sync", "EXCLUDED_ITEMS"),
        ("main", "Excluded_Items"),
    ],
)
def test_direct_config_write_cannot_bypass_selective_apis(
    m: Maestral, section: str, option: str
) -> None:
    stop_connection_helper(m)

    with pytest.raises(ValueError, match="symlink API"):
        m.set_conf(section, option, True)

    assert m.ignore_symlinks is False
    assert m.get_conf("sync", "ignore_symlinks") is False


@pytest.mark.parametrize("option", ["path", "PATH", "root_marker_id", "ROOT_MARKER_ID"])
def test_direct_config_write_cannot_bypass_root_api(m: Maestral, option: str) -> None:
    stop_connection_helper(m)
    old_path = m.get_conf("sync", "path")
    old_marker = m.get_conf("sync", "root_marker_id")

    with pytest.raises(ValueError, match="Dropbox root API"):
        m.set_conf("SYNC", option, "replacement")

    assert m.get_conf("sync", "path") == old_path
    assert m.get_conf("sync", "root_marker_id") == old_marker
