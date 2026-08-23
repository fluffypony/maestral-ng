import os
import platform
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO
from unittest.mock import Mock

import pytest

import maestral.sync as sync_module
from maestral.client import DropboxClient
from maestral.config import MaestralConfig
from maestral.core import FileMetadata, FolderMetadata, ListFolderResult
from maestral.keyring import CredentialStorage
from maestral.models import ChangeType, IndexEntry, ItemType, SyncEvent, SyncStatus
from maestral.sync import SyncDirection, SyncEngine
from maestral.utils.hashing import DropboxContentHasher


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


def dropbox_hash(data: bytes) -> str:
    hasher = DropboxContentHasher()
    hasher.update(data)
    return hasher.hexdigest()


def make_file_metadata(dbx_path: str, data: bytes, rev: str) -> FileMetadata:
    now = datetime.now(tz=timezone.utc)
    return FileMetadata(
        name=dbx_path.rsplit("/", maxsplit=1)[-1],
        path_lower=dbx_path.lower(),
        path_display=dbx_path,
        id=f"id:{dbx_path}",
        client_modified=now,
        server_modified=now,
        rev=rev,
        size=len(data),
        symlink_target=None,
        shared=False,
        modified_by="id:user",
        is_downloadable=True,
        content_hash=dropbox_hash(data),
    )


def add_index_entry(
    sync: SyncEngine,
    dbx_path: str,
    item_type: ItemType,
    *,
    content_hash: str,
    last_sync: float = 1,
) -> None:
    with sync._database_access():
        sync._index_table.update(
            IndexEntry(
                dbx_path_lower=dbx_path.lower(),
                dbx_path_cased=dbx_path,
                provider_id=f"id:{dbx_path}",
                item_type=item_type,
                last_sync=last_sync,
                rev="folder" if item_type is ItemType.Folder else "source-rev",
                content_hash=content_hash,
                symlink_target=None,
            )
        )


def make_move_event(
    sync: SyncEngine,
    source: str,
    destination: str,
    item_type: ItemType,
    content_hash: str,
) -> SyncEvent:
    local_source = sync.to_local_path_from_cased(source)
    local_destination = sync.to_local_path_from_cased(destination)
    return SyncEvent(
        direction=SyncDirection.Up,
        item_type=item_type,
        sync_time=0,
        dbx_path=destination,
        dbx_path_lower=destination.lower(),
        local_path=local_destination,
        dbx_path_from=source,
        dbx_path_from_lower=source.lower(),
        local_path_from=local_source,
        content_hash=content_hash,
        symlink_target=None,
        change_type=ChangeType.Moved,
        change_time=0,
        change_dbid=None,
        status=SyncStatus.Queued,
        size=0,
        completed=0,
    )


def test_file_move_rejects_changed_remote_source(sync_engine: SyncEngine) -> None:
    local_data = b"stable local file"
    local_hash = dropbox_hash(local_data)
    destination = Path(sync_engine.dropbox_path) / "destination.txt"
    destination.write_bytes(local_data)
    add_index_entry(
        sync_engine,
        "/source.txt",
        ItemType.File,
        content_hash=local_hash,
    )
    event = make_move_event(
        sync_engine,
        "/source.txt",
        "/destination.txt",
        ItemType.File,
        local_hash,
    )
    returned_path = "/destination (1).txt"
    moved_metadata = make_file_metadata(
        returned_path,
        b"remote source changed before move",
        "moved-rev",
    )
    sync_engine.client.move = Mock(return_value=moved_metadata)
    sync_engine.rescan = Mock()  # type: ignore[method-assign]

    status = sync_engine._on_local_moved(event)

    assert status is SyncStatus.Conflict
    assert destination.read_bytes() == local_data
    assert not (Path(sync_engine.dropbox_path) / "destination (1).txt").exists()
    assert sync_engine.get_index_entry("/source.txt") is None
    assert sync_engine.get_index_entry(returned_path.lower()) is None
    assert sync_engine._validated_download_intents() == {
        returned_path.lower(): "restore"
    }
    recovered = sync_engine._validated_recovered_local_paths()
    assert recovered["/destination.txt"]["path"] == "/destination.txt"
    destination_stat = os.lstat(destination)
    assert recovered["/destination.txt"]["identity"] == [
        destination_stat.st_dev,
        destination_stat.st_ino,
        destination_stat.st_mode,
    ]
    sync_engine.rescan.assert_called_once_with(str(destination))


def test_folder_move_rejects_changed_remote_child(sync_engine: SyncEngine) -> None:
    local_data = b"stable local child"
    local_hash = dropbox_hash(local_data)
    destination = Path(sync_engine.dropbox_path) / "destination"
    destination.mkdir()
    child = destination / "child.txt"
    child.write_bytes(local_data)
    add_index_entry(
        sync_engine,
        "/source",
        ItemType.Folder,
        content_hash="folder",
    )
    add_index_entry(
        sync_engine,
        "/source/child.txt",
        ItemType.File,
        content_hash=local_hash,
    )
    event = make_move_event(
        sync_engine,
        "/source",
        "/destination",
        ItemType.Folder,
        "folder",
    )
    moved_folder = FolderMetadata(
        name="destination",
        path_lower="/destination",
        path_display="/destination",
        id="id:moved-folder",
        shared=False,
    )
    moved_child = make_file_metadata(
        "/destination/child.txt",
        b"remote child changed before move",
        "moved-child-rev",
    )
    sync_engine.client.move = Mock(return_value=moved_folder)
    sync_engine.client.list_folder = Mock(
        return_value=ListFolderResult(
            entries=[moved_child],
            has_more=False,
            cursor="after-move",
        )
    )
    sync_engine.rescan = Mock()  # type: ignore[method-assign]

    status = sync_engine._on_local_moved(event)

    assert status is SyncStatus.Conflict
    assert child.read_bytes() == local_data
    assert sync_engine.get_index_entry("/source") is None
    assert sync_engine.get_index_entry("/source/child.txt") is None
    assert sync_engine.get_index_entry("/destination") is None
    assert sync_engine.get_index_entry("/destination/child.txt") is None
    assert sync_engine._validated_download_intents() == {"/destination": "restore"}
    recovered = sync_engine._validated_recovered_local_paths()
    assert recovered["/destination"]["path"] == "/destination"
    destination_stat = os.lstat(destination)
    assert recovered["/destination"]["identity"] == [
        destination_stat.st_dev,
        destination_stat.st_ino,
        destination_stat.st_mode,
    ]
    sync_engine.rescan.assert_called_once_with(str(destination))


@pytest.mark.skipif(
    platform.system() == "Windows",
    reason="Windows rooted readers prevent path replacement",
)
def test_upload_rescans_path_replaced_after_open_with_old_mtime(
    sync_engine: SyncEngine,
    tmp_path: Path,
) -> None:
    original_data = b"upload this inode"
    replacement_data = b"replacement at the same pathname"
    local_path = Path(sync_engine.dropbox_path) / "upload.txt"
    local_path.write_bytes(original_data)
    replacement = tmp_path / "replacement.txt"
    replacement.write_bytes(replacement_data)

    last_sync = time.time() - 60
    replacement_mtime = last_sync - 60
    os.utime(replacement, (replacement_mtime, replacement_mtime))
    add_index_entry(
        sync_engine,
        "/upload.txt",
        ItemType.File,
        content_hash=dropbox_hash(b"previous remote data"),
        last_sync=last_sync,
    )

    remote_before = make_file_metadata(
        "/upload.txt",
        b"previous remote data",
        "previous-rev",
    )
    remote_after = make_file_metadata(
        "/upload.txt",
        original_data,
        "uploaded-rev",
    )
    sync_engine.client.get_metadata = Mock(return_value=remote_before)

    def replace_path_during_upload(
        upload_file: BinaryIO,
        *_args: object,
        **_kwargs: object,
    ) -> FileMetadata:
        os.replace(replacement, local_path)
        assert local_path.stat().st_mtime < last_sync
        assert upload_file.read() == original_data
        return remote_after

    sync_engine.client.upload = Mock(side_effect=replace_path_during_upload)
    sync_engine._wait_for_creation = Mock()  # type: ignore[method-assign]
    sync_engine.rescan = Mock()  # type: ignore[method-assign]
    event = SyncEvent(
        direction=SyncDirection.Up,
        item_type=ItemType.File,
        sync_time=0,
        dbx_path="/upload.txt",
        dbx_path_lower="/upload.txt",
        local_path=str(local_path),
        content_hash=None,
        symlink_target=None,
        change_type=ChangeType.Modified,
        change_time=0,
        change_dbid=None,
        status=SyncStatus.Queued,
        size=0,
        completed=0,
    )

    status = sync_engine._on_local_file_modified(event)

    assert status is SyncStatus.Conflict
    assert local_path.read_bytes() == replacement_data
    assert local_path.stat().st_mtime < last_sync
    assert sync_engine.get_index_entry("/upload.txt").content_hash == dropbox_hash(
        b"previous remote data"
    )
    recovered = sync_engine._validated_recovered_local_paths()["/upload.txt"]
    replacement_stat = os.lstat(local_path)
    assert recovered["identity"] == [
        replacement_stat.st_dev,
        replacement_stat.st_ino,
        replacement_stat.st_mode,
    ]
    sync_engine.rescan.assert_called_once_with(str(local_path))


@pytest.mark.skipif(
    platform.system() == "Windows",
    reason="Windows rooted readers prevent path replacement",
)
def test_identical_upload_shortcut_does_not_index_replacement(
    sync_engine: SyncEngine,
    tmp_path: Path,
) -> None:
    original_data = b"already remote"
    replacement_data = b"replacement"
    local_path = Path(sync_engine.dropbox_path) / "upload.txt"
    replacement_path = tmp_path / "replacement.txt"
    local_path.write_bytes(original_data)
    replacement_path.write_bytes(replacement_data)
    matching_remote = make_file_metadata(
        "/upload.txt",
        original_data,
        "matching-rev",
    )

    def replace_during_metadata_lookup(_path: str) -> FileMetadata:
        os.replace(replacement_path, local_path)
        return matching_remote

    sync_engine.client.get_metadata = Mock(side_effect=replace_during_metadata_lookup)
    sync_engine.client.upload = Mock()
    sync_engine._wait_for_creation = Mock()  # type: ignore[method-assign]
    sync_engine.rescan = Mock()  # type: ignore[method-assign]
    event = SyncEvent(
        direction=SyncDirection.Up,
        item_type=ItemType.File,
        sync_time=0,
        dbx_path="/upload.txt",
        dbx_path_lower="/upload.txt",
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

    status = sync_engine._on_local_file_modified(event)

    assert status is SyncStatus.Skipped
    assert local_path.read_bytes() == replacement_data
    assert sync_engine.get_index_entry("/upload.txt") is None
    sync_engine.client.upload.assert_not_called()
    recovered = sync_engine._validated_recovered_local_paths()["/upload.txt"]
    replacement_stat = os.lstat(local_path)
    assert recovered["identity"] == [
        replacement_stat.st_dev,
        replacement_stat.st_ino,
        replacement_stat.st_mode,
    ]
    sync_engine.rescan.assert_called_once_with(str(local_path))


def test_download_does_not_index_replacement_after_install_proof(
    sync_engine: SyncEngine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote_data = b"remote"
    replacement_data = b"local replacement"
    local_path = Path(sync_engine.dropbox_path) / "download.txt"
    replacement_path = tmp_path / "replacement.txt"
    replacement_path.write_bytes(replacement_data)
    metadata = make_file_metadata("/download.txt", remote_data, "remote-rev")
    sync_engine.client.get_account_info = Mock(return_value=Mock(account_id="id:user"))

    def download(
        _rev: str,
        download_file: BinaryIO,
        **_kwargs: object,
    ) -> FileMetadata:
        download_file.write(remote_data)
        return metadata

    sync_engine.client.download = Mock(side_effect=download)
    original_record = sync_engine._record_recovered_local_path
    record_count = 0

    def replace_after_marker(
        path: str,
        identity: tuple[int, ...],
        **kwargs: object,
    ) -> str:
        nonlocal record_count
        result = original_record(path, identity, **kwargs)
        record_count += 1
        if record_count == 2:
            os.replace(replacement_path, local_path)
        return result

    monkeypatch.setattr(
        sync_engine,
        "_record_recovered_local_path",
        replace_after_marker,
    )
    sync_engine.rescan = Mock()  # type: ignore[method-assign]
    event = SyncEvent.from_metadata(metadata, sync_engine)

    status = sync_engine._on_remote_file(event)

    assert status is SyncStatus.Conflict
    assert local_path.read_bytes() == replacement_data
    assert sync_engine.get_index_entry("/download.txt") is None
    recovered = sync_engine._validated_recovered_local_paths()["/download.txt"]
    replacement_stat = os.lstat(local_path)
    assert recovered["identity"] == [
        replacement_stat.st_dev,
        replacement_stat.st_ino,
        replacement_stat.st_mode,
    ]
    sync_engine.rescan.assert_called_once_with(str(local_path))


def test_download_snapshot_error_keeps_durable_rescan_marker(
    sync_engine: SyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote_data = b"remote"
    local_path = Path(sync_engine.dropbox_path) / "download.txt"
    metadata = make_file_metadata("/download.txt", remote_data, "remote-rev")
    sync_engine.client.get_account_info = Mock(return_value=Mock(account_id="id:user"))

    def download(
        _rev: str,
        download_file: BinaryIO,
        **_kwargs: object,
    ) -> FileMetadata:
        download_file.write(remote_data)
        return metadata

    sync_engine.client.download = Mock(side_effect=download)
    original_snapshot = sync_module.rooted_item_snapshot

    def fail_installed_snapshot(path: str, *args: object, **kwargs: object):
        if path == str(local_path) and local_path.exists():
            raise PermissionError("cannot prove installed file")
        return original_snapshot(path, *args, **kwargs)

    monkeypatch.setattr(
        sync_module,
        "rooted_item_snapshot",
        fail_installed_snapshot,
    )
    event = SyncEvent.from_metadata(metadata, sync_engine)

    result = sync_engine._create_local_entry(event)

    assert result.status is SyncStatus.Failed
    assert local_path.read_bytes() == remote_data
    recovered = sync_engine._validated_recovered_local_paths()["/download.txt"]
    local_stat = os.lstat(local_path)
    assert recovered["identity"] == [
        local_stat.st_dev,
        local_stat.st_ino,
        local_stat.st_mode,
    ]
    queued_event = sync_engine.fs_events.local_file_event_queue.get_nowait()
    assert queued_event.src_path == str(local_path)


def test_remote_folder_does_not_publish_file_replacement(
    sync_engine: SyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local_path = Path(sync_engine.dropbox_path) / "folder"
    metadata = FolderMetadata(
        name="folder",
        path_lower="/folder",
        path_display="/folder",
        id="id:/folder",
        shared=False,
    )
    original_record = sync_engine._record_recovered_local_path
    replaced = False

    def replace_folder_after_marker(
        path: str,
        identity: tuple[int, ...],
        **kwargs: object,
    ) -> str:
        nonlocal replaced
        result = original_record(path, identity, **kwargs)
        if not replaced and path == str(local_path):
            replaced = True
            local_path.rmdir()
            local_path.write_bytes(b"local file")
        return result

    monkeypatch.setattr(
        sync_engine,
        "_record_recovered_local_path",
        replace_folder_after_marker,
    )
    sync_engine.rescan = Mock()  # type: ignore[method-assign]
    event = SyncEvent.from_metadata(metadata, sync_engine)

    status = sync_engine._on_remote_folder(event)

    assert status is SyncStatus.Conflict
    assert local_path.read_bytes() == b"local file"
    assert sync_engine.get_index_entry("/folder") is None
    recovered = sync_engine._validated_recovered_local_paths()["/folder"]
    local_stat = os.lstat(local_path)
    assert recovered["identity"] == [
        local_stat.st_dev,
        local_stat.st_ino,
        local_stat.st_mode,
    ]
    sync_engine.rescan.assert_called_once_with(str(local_path))
