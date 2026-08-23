import errno
import os
import platform
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

import maestral.utils.path as path_module
from maestral.client import DropboxClient
from maestral.config import MaestralConfig
from maestral.core import FileMetadata
from maestral.keyring import CredentialStorage
from maestral.models import ChangeType, ItemType, SyncEvent, SyncStatus
from maestral.sync import SyncDirection, SyncEngine
from maestral.utils.path import rooted_walk


@pytest.mark.skipif(platform.system() == "Windows", reason="requires POSIX dir_fd")
def test_rooted_walk_rejects_child_directory_replaced_after_recursion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    tree = root / "tree"
    child = tree / "child"
    saved_child = tree / "saved-child"
    child.mkdir(parents=True)
    (child / "original.txt").write_text("original")

    real_open = os.open
    real_fstat = os.fstat
    real_stat = os.stat
    tree_fd = -1
    child_fd = -1
    tree_opened_stat: os.stat_result | None = None
    child_fstat_calls = 0
    raced = False

    def tracking_open(
        name: str,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal tree_fd, child_fd
        if dir_fd is None:
            fd = real_open(name, flags, mode)
        else:
            fd = real_open(name, flags, mode, dir_fd=dir_fd)
        if name == tree.name and dir_fd is not None:
            tree_fd = fd
        elif name == child.name and dir_fd == tree_fd:
            child_fd = fd
        return fd

    def racing_fstat(fd: int) -> os.stat_result:
        nonlocal tree_opened_stat, child_fstat_calls, raced
        stat_result = real_fstat(fd)
        if fd == tree_fd:
            if tree_opened_stat is None:
                tree_opened_stat = stat_result
            elif raced:
                return tree_opened_stat
        elif fd == child_fd:
            child_fstat_calls += 1
            if child_fstat_calls == 2:
                child.rename(saved_child)
                child.mkdir()
                (child / "replacement.txt").write_text("replacement")
                raced = True
        return stat_result

    def stable_tree_stat(
        name: str,
        *,
        dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> os.stat_result:
        if raced and name == tree.name and dir_fd is not None:
            assert tree_opened_stat is not None
            return tree_opened_stat
        return real_stat(name, dir_fd=dir_fd, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(path_module.os, "open", tracking_open)
    monkeypatch.setattr(path_module.os, "fstat", racing_fstat)
    monkeypatch.setattr(path_module.os, "stat", stable_tree_stat)

    with pytest.raises(OSError) as exc_info:
        tuple(rooted_walk(str(tree), str(root)))

    assert raced
    assert exc_info.value.errno == errno.ESTALE
    assert (saved_child / "original.txt").read_text() == "original"
    assert (child / "replacement.txt").read_text() == "replacement"


@pytest.mark.skipif(platform.system() == "Windows", reason="requires POSIX dir_fd")
def test_rooted_walk_rejects_non_directory_child_replaced_during_scan(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    tree = root / "tree"
    child = tree / "child.txt"
    saved_child = tree / "saved-child.txt"
    tree.mkdir(parents=True)
    child.write_text("original")

    real_open = os.open
    real_fstat = os.fstat
    real_stat = os.stat
    tree_fd = -1
    tree_opened_stat: os.stat_result | None = None
    raced = False

    def tracking_open(
        name: str,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal tree_fd
        if dir_fd is None:
            fd = real_open(name, flags, mode)
        else:
            fd = real_open(name, flags, mode, dir_fd=dir_fd)
        if name == tree.name and dir_fd is not None:
            tree_fd = fd
        return fd

    def stable_tree_fstat(fd: int) -> os.stat_result:
        nonlocal tree_opened_stat
        stat_result = real_fstat(fd)
        if fd == tree_fd:
            if tree_opened_stat is None:
                tree_opened_stat = stat_result
            elif raced:
                return tree_opened_stat
        return stat_result

    def racing_child_stat(
        name: str,
        *,
        dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> os.stat_result:
        nonlocal raced
        if raced and name == tree.name and dir_fd is not None:
            assert tree_opened_stat is not None
            return tree_opened_stat

        stat_result = real_stat(
            name,
            dir_fd=dir_fd,
            follow_symlinks=follow_symlinks,
        )
        if name == child.name and dir_fd == tree_fd and not raced:
            child.rename(saved_child)
            child.write_text("replacement has different metadata")
            raced = True
        return stat_result

    monkeypatch.setattr(path_module.os, "open", tracking_open)
    monkeypatch.setattr(path_module.os, "fstat", stable_tree_fstat)
    monkeypatch.setattr(path_module.os, "stat", racing_child_stat)

    with pytest.raises(OSError) as exc_info:
        tuple(rooted_walk(str(tree), str(root)))

    assert raced
    assert exc_info.value.errno == errno.ESTALE
    assert saved_child.read_text() == "original"
    assert child.read_text() == "replacement has different metadata"


@pytest.mark.skipif(platform.system() != "Windows", reason="requires Windows handles")
def test_rooted_walk_rejects_file_changed_before_final_child_check(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    tree = root / "tree"
    child = tree / "child.txt"
    tree.mkdir(parents=True)
    child.write_text("original")

    real_lstat = os.lstat
    raced = False

    def racing_lstat(path: str) -> os.stat_result:
        nonlocal raced
        stat_result = real_lstat(path)
        if os.path.normcase(path) == os.path.normcase(str(child)) and not raced:
            child.write_text("replacement has different metadata")
            raced = True
        return stat_result

    monkeypatch.setattr(path_module.os, "lstat", racing_lstat)

    with pytest.raises(OSError) as exc_info:
        tuple(rooted_walk(str(tree), str(root)))

    assert raced
    assert exc_info.value.errno == errno.ESTALE
    assert child.read_text() == "replacement has different metadata"


@pytest.fixture
def deletion_sync_engine(
    config_name: str,
    tmp_path: Path,
) -> Iterator[SyncEngine]:
    conf = MaestralConfig(config_name)
    conf.set("sync", "path", str(tmp_path))
    client = DropboxClient(config_name, CredentialStorage(config_name))
    sync = SyncEngine(client)
    sync.create_root_marker()
    yield sync
    sync._connection.close()


def test_local_file_deletion_without_index_revision_queues_durable_restore(
    deletion_sync_engine: SyncEngine,
    tmp_path: Path,
) -> None:
    now = datetime.now(tz=timezone.utc)
    metadata = FileMetadata(
        name="file.txt",
        path_lower="/file.txt",
        path_display="/file.txt",
        id="id:file",
        client_modified=now,
        server_modified=now,
        rev="remote-rev",
        size=1,
        symlink_target=None,
        shared=False,
        modified_by="id:user",
        is_downloadable=True,
        content_hash="remote-hash",
    )
    deletion_sync_engine.client.get_metadata = Mock(return_value=metadata)
    deletion_sync_engine.client.remove = Mock()
    event = SyncEvent(
        direction=SyncDirection.Up,
        item_type=ItemType.File,
        sync_time=0,
        dbx_path="/file.txt",
        dbx_path_lower="/file.txt",
        local_path=str(tmp_path / "file.txt"),
        content_hash=None,
        symlink_target=None,
        change_type=ChangeType.Removed,
        change_time=0,
        change_dbid=None,
        status=SyncStatus.Queued,
        size=0,
        completed=0,
    )

    assert deletion_sync_engine.get_index_entry("/file.txt") is None
    assert deletion_sync_engine._on_local_deleted(event) is SyncStatus.Conflict

    deletion_sync_engine.client.remove.assert_not_called()
    assert deletion_sync_engine._validated_download_intents() == {
        "/file.txt": "restore"
    }
