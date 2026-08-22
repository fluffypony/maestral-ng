import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from watchdog.events import FileCreatedEvent, FileDeletedEvent

import maestral.config.main as config_main_module
import maestral.sync as sync_module
from maestral.client import DropboxClient
from maestral.config import MaestralConfig
from maestral.core import ListFolderResult
from maestral.exceptions import InsufficientPermissionsError
from maestral.keyring import CredentialStorage
from maestral.models import ChangeType, ItemType, SyncEvent, SyncStatus
from maestral.sync import Conflict, SyncDirection, SyncEngine
from maestral.utils.appdirs import get_conf_path


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


def test_legacy_exclusions_migrate_to_selective_sync(config_name: str) -> None:
    config_main_module._config_instances.pop(config_name, None)
    config_path = Path(get_conf_path("maestral", f"{config_name}.ini"))
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "[main]\nversion = 20.0\n\n[sync]\nexcluded_items = ['/Photos']\n"
    )

    conf = MaestralConfig(config_name)

    assert conf.get("sync", "selective_sync_mode") == "exclude"
    assert conf.get("sync", "selective_sync_paths") == ["/Photos"]
    assert not conf.has_option("sync", "excluded_items")
    assert "excluded_items" not in config_path.read_text()


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


def test_inactive_scan_ignores_unicode_normalization_differences(
    sync_engine: SyncEngine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    decomposed_path = str(tmp_path / "cafe\N{COMBINING ACUTE ACCENT}.txt")
    composed_path = str(tmp_path / "caf\N{LATIN SMALL LETTER E WITH ACUTE}.txt")
    index_entry = SimpleNamespace(
        dbx_path_cased="/caf\N{LATIN SMALL LETTER E WITH ACUTE}.txt"
    )
    sync_engine.get_index_entry = Mock(return_value=index_entry)

    assert sync_engine.get_index_entry_for_local_path(decomposed_path) is index_entry

    sync_engine._is_fs_case_sensitive = False
    monkeypatch.setattr(sync_module, "exists", Mock(return_value=True))
    monkeypatch.setattr(
        sync_module,
        "to_existing_unnormalized_path",
        Mock(return_value=decomposed_path),
    )

    assert sync_engine._exists_with_given_casing(composed_path)


def test_include_mode_keeps_deep_file_parents_and_excludes_siblings(
    sync_engine: SyncEngine,
) -> None:
    sync_engine.set_selective_sync("include", ["/Projects/App/config.toml"])

    assert sync_engine.selective_sync_status("/") == "partially included"
    assert sync_engine.selective_sync_status("/Projects") == "partially included"
    assert sync_engine.selective_sync_status("/Projects/App") == "partially included"
    assert sync_engine.selective_sync_status("/Projects/App/config.toml") == "included"
    assert sync_engine.selective_sync_status("/Projects/App/notes.txt") == "excluded"
    assert sync_engine.selective_sync_status("/Projects/Other") == "excluded"


def test_selective_sync_mode_and_paths_roll_back_together_on_save_failure(
    sync_engine: SyncEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    sync_engine.set_selective_sync("exclude", ["/Archive"])
    config_before = Path(sync_engine._conf.config_path).read_bytes()
    monkeypatch.setattr(
        sync_engine._conf,
        "save",
        Mock(side_effect=OSError("cannot save")),
    )

    with pytest.raises(OSError, match="cannot save"):
        sync_engine.set_selective_sync("include", ["/Projects"])

    assert sync_engine.selective_sync_mode == "exclude"
    assert sync_engine.selective_sync_paths == {"/archive"}
    assert sync_engine._conf.get("sync", "selective_sync_mode") == "exclude"
    assert sync_engine._conf.get("sync", "selective_sync_paths") == ["/archive"]
    assert Path(sync_engine._conf.config_path).read_bytes() == config_before


def test_ignored_symlink_hides_its_subtree_from_local_events(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)
    sync_engine.ignore_symlinks = True
    sync_engine._sync_event_from_fs_event = Mock()

    events = sync_engine._sync_events_from_fs_events(
        [
            FileCreatedEvent(str(link)),
            FileCreatedEvent(str(link / "child.txt")),
        ]
    )

    assert events == []
    assert sync_engine._ignored_symlink_paths == {"/link"}
    sync_engine._sync_event_from_fs_event.assert_not_called()


def test_remote_update_under_ignored_symlink_does_not_touch_target(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target = tmp_path / "target"
    target.mkdir()
    target_file = target / "child.txt"
    target_file.write_text("local target")
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)
    sync_engine.ignore_symlinks = True
    sync_engine.client.download = Mock()
    event = make_event(str(link / "child.txt"), "/link/child.txt")
    event.rev = "remote-rev"
    event.dbx_id = "id:child"

    result = sync_engine.apply_remote_changes([event])

    assert result == [event]
    assert event.status is SyncStatus.Skipped
    assert target_file.read_text() == "local target"
    assert sync_engine.get_index_entry("/link/child.txt").rev == "remote-rev"  # type: ignore[union-attr]
    sync_engine.client.download.assert_not_called()


def test_removing_ignored_symlink_restores_remote_without_uploading_deletion(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    target = tmp_path / "target"
    target.write_text("target")
    link = tmp_path / "link"
    link.symlink_to(target)
    sync_engine.ignore_symlinks = True
    sync_engine.download_callback = Mock()
    sync_engine.client.remove = Mock()
    remote_event = make_event(str(link), "/link")
    remote_event.rev = "remote-rev"
    remote_event.dbx_id = "id:link"
    sync_engine.apply_remote_changes([remote_event])
    assert link.is_symlink()
    assert sync_engine.get_index_entry("/link") is not None

    link.unlink()
    events = sync_engine._filter_local_events([FileDeletedEvent(str(link))])

    assert events == []
    assert sync_engine.get_index_entry("/link") is None
    sync_engine.download_callback.assert_called_once_with("/link")  # type: ignore[union-attr]
    sync_engine.client.remove.assert_not_called()


def test_downloaded_symlink_replaces_existing_file(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    probe = tmp_path / "symlink-probe"
    try:
        probe.symlink_to("target")
    except OSError as exc:
        if getattr(exc, "winerror", None) == 1314:
            pytest.skip("Windows did not grant symlink creation permission")
        raise
    else:
        probe.unlink()

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
