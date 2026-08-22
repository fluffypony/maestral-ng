import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest

import maestral.sync as sync_module
from maestral.client import DropboxClient
from maestral.config import MaestralConfig
from maestral.core import ListFolderResult
from maestral.exceptions import InsufficientPermissionsError
from maestral.keyring import CredentialStorage
from maestral.models import ChangeType, ItemType, SyncEvent, SyncStatus
from maestral.sync import Conflict, SyncDirection, SyncEngine


def make_sync_engine(config_name: str, dropbox_path: Path) -> SyncEngine:
    conf = MaestralConfig(config_name)
    conf.set("sync", "path", str(dropbox_path))
    client = DropboxClient(config_name, CredentialStorage(config_name))
    return SyncEngine(client)


@pytest.fixture
def sync_engine(config_name: str, tmp_path: Path):
    sync = make_sync_engine(config_name, tmp_path)
    yield sync
    sync._connection.close()


def make_event(
    local_path: str,
    dbx_path: str,
    *,
    direction: SyncDirection = SyncDirection.Down,
    item_type: ItemType = ItemType.File,
    change_type: ChangeType = ChangeType.Added,
    status: SyncStatus = SyncStatus.Queued,
    local_path_from: str | None = None,
    dbx_path_from: str | None = None,
    dbx_path_from_lower: str | None = None,
    symlink_target: str | None = None,
) -> SyncEvent:
    return SyncEvent(
        direction=direction,
        item_type=item_type,
        sync_time=0,
        dbx_path=dbx_path,
        dbx_path_lower=dbx_path.lower(),
        local_path=local_path,
        dbx_path_from=dbx_path_from,
        dbx_path_from_lower=dbx_path_from_lower,
        local_path_from=local_path_from,
        content_hash="content-hash",
        symlink_target=symlink_target,
        change_type=change_type,
        change_time=0,
        change_dbid=None,
        status=status,
        size=0,
        completed=0,
    )


def prepare_local_move(sync: SyncEngine) -> None:
    sync._handle_selective_sync_conflict = Mock(return_value=False)
    sync._handle_normalization_conflict = Mock(return_value=False)
    sync.remove_node_from_index = Mock()
    sync._handle_upload_conflict = Mock(return_value=False)
    sync._update_index_recursive = Mock()


def test_case_only_move_does_not_remove_remote_destination(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    prepare_local_move(sync_engine)
    sync_engine.get_index_entry = Mock(
        return_value=SimpleNamespace(
            is_file=True, dbx_path_lower="/readme.txt", rev="old-rev"
        )
    )
    sync_engine.client.remove = Mock()
    sync_engine.client.move = Mock(return_value=Mock())
    event = make_event(
        str(tmp_path / "README.txt"),
        "/README.txt",
        direction=SyncDirection.Up,
        change_type=ChangeType.Moved,
        local_path_from=str(tmp_path / "readme.txt"),
        dbx_path_from="/readme.txt",
        dbx_path_from_lower="/readme.txt",
    )

    assert sync_engine._on_local_moved(event) is SyncStatus.Done
    sync_engine.client.remove.assert_not_called()
    sync_engine.client.move.assert_called_once_with(
        "/readme.txt", "/README.txt", autorename=True
    )


def test_move_to_different_path_still_removes_remote_destination(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    prepare_local_move(sync_engine)
    destination = SimpleNamespace(
        is_file=True, dbx_path_lower="/destination.txt", rev="old-rev"
    )
    sync_engine.get_index_entry = Mock(return_value=destination)
    sync_engine.client.remove = Mock()
    sync_engine.client.move = Mock(return_value=Mock())
    event = make_event(
        str(tmp_path / "destination.txt"),
        "/destination.txt",
        direction=SyncDirection.Up,
        change_type=ChangeType.Moved,
        local_path_from=str(tmp_path / "source.txt"),
        dbx_path_from="/source.txt",
        dbx_path_from_lower="/source.txt",
    )

    assert sync_engine._on_local_moved(event) is SyncStatus.Done
    sync_engine.client.remove.assert_called_once_with(
        "/destination.txt", parent_rev="old-rev"
    )


def test_unicode_normalization_only_move_is_skipped(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    prepare_local_move(sync_engine)
    sync_engine.get_index_entry = Mock()
    sync_engine.client.move = Mock()
    source = str(tmp_path / "cafe\N{COMBINING ACUTE ACCENT}.txt")
    destination = str(tmp_path / "caf\N{LATIN SMALL LETTER E WITH ACUTE}.txt")
    event = make_event(
        destination,
        "/caf\N{LATIN SMALL LETTER E WITH ACUTE}.txt",
        direction=SyncDirection.Up,
        change_type=ChangeType.Moved,
        local_path_from=source,
        dbx_path_from="/caf\N{LATIN SMALL LETTER E WITH ACUTE}.txt",
        dbx_path_from_lower="/caf\N{LATIN SMALL LETTER E WITH ACUTE}.txt",
    )

    assert sync_engine._on_local_moved(event) is SyncStatus.Skipped
    sync_engine.get_index_entry.assert_not_called()
    sync_engine.client.move.assert_not_called()


def test_downloaded_symlink_replaces_existing_file(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    destination = tmp_path / "link"
    destination.write_text("old contents")
    event = make_event(
        str(destination),
        "/link",
        symlink_target="target",
    )
    sync_engine._apply_case_change = Mock()
    sync_engine._check_download_conflict = Mock(return_value=Conflict.RemoteNewer)
    sync_engine._ensure_parent = Mock()
    sync_engine.get_index_entry = Mock(return_value=None)
    sync_engine.update_index_from_sync_event = Mock()
    sync_engine._save_local_hash = Mock()
    sync_engine.client.download = Mock()

    assert sync_engine._on_remote_file(event) is SyncStatus.Done
    assert destination.is_symlink()
    assert os.readlink(destination) == "target"
    sync_engine.client.download.assert_not_called()


def test_remote_folder_fails_when_conflicting_file_cannot_be_deleted(
    sync_engine: SyncEngine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "folder"
    destination.write_text("local file")
    event = make_event(
        str(destination),
        "/folder",
        item_type=ItemType.Folder,
    )
    sync_engine._apply_case_change = Mock()
    sync_engine._check_download_conflict = Mock(return_value=Conflict.RemoteNewer)
    sync_engine._ensure_parent = Mock()
    sync_engine.update_index_from_sync_event = Mock()
    permission_error = PermissionError("cannot delete")
    monkeypatch.setattr(sync_module, "delete", Mock(return_value=permission_error))

    with pytest.raises(InsufficientPermissionsError):
        sync_engine._on_remote_folder(event)

    assert destination.is_file()
    sync_engine.update_index_from_sync_event.assert_not_called()


def test_remote_folder_accumulates_page_failures(
    sync_engine: SyncEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    failed_event = make_event("/failed.txt", "/failed.txt", status=SyncStatus.Failed)
    done_event = make_event("/done.txt", "/done.txt", status=SyncStatus.Done)
    pages = [
        ListFolderResult(
            entries=[SimpleNamespace(path_lower="/failed.txt", event=failed_event)],
            has_more=True,
            cursor="first",
        ),
        ListFolderResult(
            entries=[SimpleNamespace(path_lower="/done.txt", event=done_event)],
            has_more=False,
            cursor="second",
        ),
    ]
    sync_engine.client.list_folder_iterator = Mock(return_value=iter(pages))
    sync_engine.apply_remote_changes = Mock(side_effect=lambda events: events)
    monkeypatch.setattr(
        SyncEvent,
        "from_metadata",
        staticmethod(lambda metadata, engine: metadata.event),
    )

    assert not sync_engine._get_remote_folder("/folder")


def test_remote_folder_empty_listing_succeeds(sync_engine: SyncEngine) -> None:
    sync_engine.client.list_folder_iterator = Mock(return_value=iter(()))

    assert sync_engine._get_remote_folder("/folder")


def test_parallel_download_setting_is_used(config_name: str, tmp_path: Path) -> None:
    conf = MaestralConfig(config_name)
    conf.set("sync", "path", str(tmp_path))
    conf.set("app", "max_parallel_downloads", 2)
    conf.set("app", "max_parallel_uploads", 5)
    sync = SyncEngine(DropboxClient(config_name, CredentialStorage(config_name)))

    try:
        assert sync._parallel_down_semaphore._value == 2
        assert sync._parallel_up_semaphore._value == 5
    finally:
        sync._connection.close()


def test_conflict_notifications_use_their_own_event(
    sync_engine: SyncEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    notifier = Mock()
    sync_engine.desktop_notifier = notifier
    sync_engine.client._cached_account_info = SimpleNamespace(account_id="self")
    first = make_event(
        "/local/first.txt",
        "/first.txt",
        change_type=ChangeType.Modified,
        status=SyncStatus.Conflict,
    )
    second = make_event(
        "/local/second.txt",
        "/second.txt",
        change_type=ChangeType.Modified,
        status=SyncStatus.Conflict,
    )
    launch = Mock()
    monkeypatch.setattr(sync_module.click, "launch", launch)

    sync_engine.notify_user([first, second])

    conflict_calls = notifier.notify.call_args_list[1:]
    assert [notification.args[1] for notification in conflict_calls] == [
        "Conflicting copy for first.txt",
        "Conflicting copy for second.txt",
    ]

    for notification in conflict_calls:
        notification.kwargs["on_click"]()

    assert launch.call_args_list == [
        call(first.local_path, locate=True),
        call(second.local_path, locate=True),
    ]


def test_unicode_conflict_uses_unicode_suffix(
    sync_engine: SyncEngine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    decomposed = str(tmp_path / "cafe\N{COMBINING ACUTE ACCENT}.txt")
    composed = str(tmp_path / "caf\N{LATIN SMALL LETTER E WITH ACUTE}.txt")
    event = make_event(
        decomposed,
        "/caf\N{LATIN SMALL LETTER E WITH ACUTE}.txt",
        direction=SyncDirection.Up,
    )
    generated_name = Mock(return_value=str(tmp_path / "renamed.txt"))
    monkeypatch.setattr(
        sync_module,
        "get_existing_equivalent_paths",
        Mock(return_value=[decomposed, composed]),
    )
    monkeypatch.setattr(sync_module, "generate_cc_name", generated_name)
    sync_engine.rescan = Mock()

    assert sync_engine._handle_normalization_conflict(event)
    generated_name.assert_called_once_with(decomposed, suffix="unicode conflict")
