import os
import shutil
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest
from watchdog.events import DirDeletedEvent

from maestral.client import DropboxClient
from maestral.config import MaestralConfig
from maestral.core import (
    AccountType,
    FileMetadata,
    FullAccount,
    TeamRootInfo,
    UserRootInfo,
)
from maestral.exceptions import DropboxConnectionError, DropboxServerError
from maestral.keyring import CredentialStorage
from maestral.main import Maestral
from maestral.models import ChangeType, IndexEntry, ItemType, SyncEvent, SyncStatus
from maestral.sync import Conflict, SyncDirection, SyncEngine


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
) -> None:
    is_folder = item_type is ItemType.Folder
    entry = IndexEntry(
        dbx_path_lower=dbx_path.lower(),
        dbx_path_cased=dbx_path,
        dbx_id=f"id:{dbx_path}",
        item_type=item_type,
        last_sync=datetime.now(tz=timezone.utc).timestamp() + 60,
        rev="folder" if is_folder else "rev",
        content_hash="folder" if is_folder else "hash",
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


def create_symlink(link: Path, target: Path) -> None:
    try:
        link.symlink_to(target, target_is_directory=target.is_dir())
    except OSError as exc:
        if getattr(exc, "winerror", None) == 1314:
            pytest.skip("Windows did not grant symlink creation permission")
        raise


def stop_connection_helper(m: Maestral) -> None:
    m.manager._connection_helper_running = False


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


def test_include_selection_change_preserves_untracked_sibling(
    m: Maestral, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
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
    monkeypatch.setattr(
        "maestral.main.to_existing_unnormalized_path", lambda path, root: path
    )

    def remove_selected(path: str) -> tuple[None, bool]:
        shutil.rmtree(path)
        return None, False

    m.sync._delete_local_path_preserving_ignored_symlinks = Mock(  # type: ignore[method-assign]
        side_effect=remove_selected
    )

    m.set_selective_sync("include", ["/projects/other"])

    assert not selected_path.exists()
    assert untracked_path.read_text() == "keep me"
    assert m.sync.get_index_entry("/projects") is not None
    assert m.sync.get_index_entry("/projects/selected") is None
    m.sync._delete_local_path_preserving_ignored_symlinks.assert_called_once_with(
        str(selected_path)
    )


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


def test_remote_ancestor_deletion_preserves_ignored_symlink(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target"
    target_path.mkdir()
    parent_path = Path(sync_engine.dropbox_path) / "parent"
    parent_path.mkdir()
    managed_path = parent_path / "managed.txt"
    managed_path.write_text("remove me")
    link_path = parent_path / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    sync_engine._check_download_conflict = Mock(  # type: ignore[method-assign]
        return_value=Conflict.RemoteNewer
    )
    event = make_deleted_event(sync_engine, "/parent", ItemType.Folder)

    status = sync_engine._on_remote_deleted(event)

    assert status is SyncStatus.Done
    assert link_path.is_symlink()
    assert os.readlink(link_path) == str(target_path)
    assert not managed_path.exists()
    assert parent_path.is_dir()


def test_get_remote_item_does_not_replace_ignored_symlink(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target_path = tmp_path / "target.txt"
    target_path.write_text("local target")
    link_path = Path(sync_engine.dropbox_path) / "link"
    create_symlink(link_path, target_path)
    sync_engine.ignore_symlinks = True
    metadata = FileMetadata(
        name="link",
        path_lower="/link",
        path_display="/link",
        id="id:link",
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
    sync_engine.client.get_metadata = Mock(  # type: ignore[method-assign]
        return_value=metadata
    )
    sync_engine.client.download = Mock()  # type: ignore[method-assign]

    assert sync_engine.get_remote_item("/link")

    assert link_path.is_symlink()
    assert target_path.read_text() == "local target"
    assert sync_engine.get_index_entry("/link").rev == "remote-rev"  # type: ignore[union-attr]
    sync_engine.client.download.assert_not_called()


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

    manager.stop.assert_called_once_with()
    assert manager.autostart.is_set()
    assert manager.download_queue.qsize() == 1
    assert manager.download_queue.get() == dbx_path
    manager.download_queue.task_done(dbx_path)


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

    m.manager._update_path_root()

    assert m.sync.selective_sync_mode == mode
    assert m.sync.selective_sync_paths == {"/"}


def test_direct_config_write_cannot_change_ignore_symlinks(m: Maestral) -> None:
    stop_connection_helper(m)

    with pytest.raises(ValueError, match="symlink API"):
        m.set_conf("sync", "ignore_symlinks", True)

    assert m.ignore_symlinks is False
    assert m.get_conf("sync", "ignore_symlinks") is False
