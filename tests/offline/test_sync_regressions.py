import ntpath
import os
from datetime import datetime, timezone
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from watchdog.events import FileCreatedEvent, FileDeletedEvent

import maestral.config.main as config_main_module
import maestral.sync as sync_module
from maestral.client import DropboxClient
from maestral.config import MaestralConfig
from maestral.core import FileMetadata, FolderMetadata, ListFolderResult
from maestral.keyring import CredentialStorage
from maestral.models import ChangeType, IndexEntry, ItemType, SyncEvent, SyncStatus
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
    sync.create_root_marker()
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


def test_legacy_windows_invalid_exclusion_loads(
    config_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_main_module._config_instances.pop(config_name, None)
    config_path = Path(get_conf_path("maestral", f"{config_name}.ini"))
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "[main]\nversion = 20.0\n\n[sync]\n" "excluded_items = ['/name:stream']\n"
    )
    monkeypatch.setattr(sync_module, "osp", ntpath)
    conf = MaestralConfig(config_name)
    sync = SyncEngine(DropboxClient(config_name, CredentialStorage(config_name)))

    try:
        assert conf.get("sync", "selective_sync_mode") == "exclude"
        assert sync.selective_sync_paths == {"/name:stream"}
    finally:
        sync._connection.close()


def prepare_local_move(sync: SyncEngine) -> None:
    sync._handle_selective_sync_conflict = Mock(return_value=False)
    sync._handle_normalization_conflict = Mock(return_value=False)


def add_move_file_index_entry(
    sync: SyncEngine,
    dbx_path: str,
    content_hash: str,
    *,
    dbx_id: str = "id:source",
    rev: str = "source-rev",
) -> None:
    with sync._database_access():
        sync._index_table.update(
            IndexEntry(
                dbx_path_lower=dbx_path.lower(),
                dbx_path_cased=dbx_path,
                provider_id=dbx_id,
                item_type=ItemType.File,
                last_sync=1,
                rev=rev,
                content_hash=content_hash,
                symlink_target=None,
            )
        )


def make_index_entry(
    dbx_path: str,
    provider_id: str,
    *,
    item_type: ItemType = ItemType.File,
) -> IndexEntry:
    is_folder = item_type is ItemType.Folder
    return IndexEntry(
        provider_id=provider_id,
        dbx_path_lower=dbx_path.lower(),
        dbx_path_cased=dbx_path,
        item_type=item_type,
        last_sync=1,
        rev="folder" if is_folder else "rev",
        content_hash="folder" if is_folder else "hash",
        symlink_target=None,
    )


def store_index_entries(sync: SyncEngine, *entries: IndexEntry) -> None:
    with sync._database_access():
        for entry in entries:
            sync._index_table.update(entry)


def make_moved_file_metadata(
    dbx_path: str,
    content_hash: str,
    *,
    dbx_id: str = "id:source",
    rev: str = "source-rev",
) -> FileMetadata:
    now = datetime.now(tz=timezone.utc)
    return FileMetadata(
        name=dbx_path.rsplit("/", maxsplit=1)[-1],
        path_lower=dbx_path.lower(),
        path_display=dbx_path,
        id=dbx_id,
        client_modified=now,
        server_modified=now,
        rev=rev,
        size=0,
        symlink_target=None,
        shared=False,
        modified_by="id:user",
        is_downloadable=True,
        content_hash=content_hash,
    )


def test_index_identity_move_replaces_old_identity_and_destination(
    sync_engine: SyncEngine,
) -> None:
    old_entry = make_index_entry("/old.txt", "id:stable")
    destination_entry = make_index_entry("/new.txt", "id:destination")
    store_index_entries(sync_engine, old_entry, destination_entry)
    with sync_engine._database_access():
        assert sync_engine._index_table.get("id:stable") is old_entry
    moved_metadata = make_moved_file_metadata(
        "/new.txt", "hash", dbx_id="id:stable", rev="rev"
    )

    sync_engine.update_index_from_dbx_metadata(moved_metadata)

    with sync_engine._database_access():
        moved_entry = sync_engine._index_table.get("id:stable")
        assert moved_entry is not None
        assert moved_entry is not old_entry
        assert moved_entry.dbx_path_lower == "/new.txt"
        assert sync_engine._index_table.get("id:destination") is None

    assert sync_engine.get_index_entry("/old.txt") is None
    assert sync_engine.get_index_entry("/new.txt").provider_id == "id:stable"  # type: ignore[union-attr]
    assert sync_engine.index_count() == 1


def test_index_path_replacement_removes_stale_identity_cache(
    sync_engine: SyncEngine,
) -> None:
    old_entry = make_index_entry("/same.txt", "id:old")
    store_index_entries(sync_engine, old_entry)
    with sync_engine._database_access():
        assert sync_engine._index_table.get("id:old") is old_entry
    new_metadata = make_moved_file_metadata(
        "/same.txt", "hash", dbx_id="id:new", rev="rev"
    )

    sync_engine.update_index_from_dbx_metadata(new_metadata)

    with sync_engine._database_access():
        assert sync_engine._index_table.get("id:old") is None
        new_entry = sync_engine._index_table.get("id:new")
        assert new_entry is not None
        assert new_entry.dbx_path_lower == "/same.txt"

    assert sync_engine.get_index_entry("/same.txt").provider_id == "id:new"  # type: ignore[union-attr]
    assert sync_engine.index_count() == 1


@pytest.mark.parametrize("replacement_type", [ItemType.File, ItemType.Folder])
def test_index_path_replacement_removes_old_subtree(
    sync_engine: SyncEngine, replacement_type: ItemType
) -> None:
    root = make_index_entry("/node", "id:old-root", item_type=ItemType.Folder)
    child = make_index_entry("/node/child.txt", "id:old-child")
    unrelated = make_index_entry("/other.txt", "id:other")
    store_index_entries(sync_engine, root, child, unrelated)

    replacement = make_index_entry("/node", "id:new-root", item_type=replacement_type)
    with sync_engine._database_access():
        sync_engine._replace_index_entry(replacement)

    assert sync_engine.get_index_entry("/node").provider_id == "id:new-root"  # type: ignore[union-attr]
    assert sync_engine.get_index_entry("/node/child.txt") is None
    assert sync_engine.get_index_entry("/other.txt").provider_id == "id:other"  # type: ignore[union-attr]


def test_index_folder_refresh_preserves_existing_subtree(
    sync_engine: SyncEngine,
) -> None:
    root = make_index_entry("/folder", "id:folder", item_type=ItemType.Folder)
    child = make_index_entry("/folder/child.txt", "id:child")
    store_index_entries(sync_engine, root, child)

    refreshed_root = make_index_entry("/folder", "id:folder", item_type=ItemType.Folder)
    refreshed_root.last_sync = 2
    with sync_engine._database_access():
        sync_engine._replace_index_entry(refreshed_root)

    assert sync_engine.get_index_entry("/folder").last_sync == 2  # type: ignore[union-attr]
    assert (
        sync_engine.get_index_entry("/folder/child.txt").provider_id  # type: ignore[union-attr]
        == "id:child"
    )


def test_remove_exact_index_path_preserves_descendants(
    sync_engine: SyncEngine,
) -> None:
    root = make_index_entry("/folder", "id:folder", item_type=ItemType.Folder)
    child = make_index_entry("/folder/child.txt", "id:child")
    store_index_entries(sync_engine, root, child)

    sync_engine.remove_index_entry("/folder")

    assert sync_engine.get_index_entry("/folder") is None
    assert (
        sync_engine.get_index_entry("/folder/child.txt").provider_id  # type: ignore[union-attr]
        == "id:child"
    )


def test_remove_index_subtree_uses_paths_with_unrelated_provider_ids(
    sync_engine: SyncEngine,
) -> None:
    root = make_index_entry("/folder", "id:folder", item_type=ItemType.Folder)
    child = make_index_entry("/folder/child.txt", "unrelated:id")
    sibling = make_index_entry("/sibling.txt", "id:sibling")
    store_index_entries(sync_engine, root, child, sibling)

    sync_engine.remove_node_from_index("/folder")

    assert sync_engine.get_index_entry("/folder") is None
    assert sync_engine.get_index_entry("/folder/child.txt") is None
    assert sync_engine.get_index_entry("/sibling.txt").provider_id == "id:sibling"  # type: ignore[union-attr]


def test_case_only_move_does_not_remove_remote_destination(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    prepare_local_move(sync_engine)
    destination_path = tmp_path / "README.txt"
    destination_path.write_text("local move")
    content_hash = sync_engine.get_local_hash(str(destination_path))
    assert content_hash is not None
    add_move_file_index_entry(
        sync_engine,
        "/readme.txt",
        content_hash,
    )
    sync_engine.client.remove = Mock()
    sync_engine.client.move = Mock(
        return_value=make_moved_file_metadata("/README.txt", content_hash)
    )
    event = make_event(
        str(destination_path),
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
        "/readme.txt",
        "/README.txt",
        autorename=True,
        expected_provider_id="id:source",
    )


def test_move_to_different_path_still_removes_remote_destination(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    prepare_local_move(sync_engine)
    destination_path = tmp_path / "destination.txt"
    destination_path.write_text("local move")
    content_hash = sync_engine.get_local_hash(str(destination_path))
    assert content_hash is not None
    add_move_file_index_entry(sync_engine, "/source.txt", content_hash)
    add_move_file_index_entry(
        sync_engine,
        "/destination.txt",
        "old-destination-hash",
        dbx_id="id:destination",
        rev="old-rev",
    )
    sync_engine.client.remove = Mock()
    sync_engine.client.move = Mock(
        return_value=make_moved_file_metadata("/destination.txt", content_hash)
    )
    event = make_event(
        str(destination_path),
        "/destination.txt",
        direction=SyncDirection.Up,
        change_type=ChangeType.Moved,
        local_path_from=str(tmp_path / "source.txt"),
        dbx_path_from="/source.txt",
        dbx_path_from_lower="/source.txt",
    )

    assert sync_engine._on_local_moved(event) is SyncStatus.Done
    sync_engine.client.remove.assert_called_once_with(
        "/destination.txt",
        parent_rev="old-rev",
        expected_provider_id="id:destination",
    )


def test_local_move_records_destination_before_remote_replacement_failure(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    prepare_local_move(sync_engine)
    destination_path = tmp_path / "destination.txt"
    destination_path.write_text("local move")
    content_hash = sync_engine.get_local_hash(str(destination_path))
    assert content_hash is not None
    add_move_file_index_entry(sync_engine, "/source.txt", content_hash)
    add_move_file_index_entry(
        sync_engine,
        "/destination.txt",
        "old-destination-hash",
        dbx_id="id:destination",
        rev="old-rev",
    )

    def fail_remote_replacement(*_args: object, **_kwargs: object) -> None:
        recovered = sync_engine._validated_recovered_local_paths()
        assert recovered["/destination.txt"]["path"] == "/destination.txt"
        raise RuntimeError("remote replacement failed")

    sync_engine.client.remove = Mock(side_effect=fail_remote_replacement)
    sync_engine.client.move = Mock()
    event = make_event(
        str(destination_path),
        "/destination.txt",
        direction=SyncDirection.Up,
        change_type=ChangeType.Moved,
        local_path_from=str(tmp_path / "source.txt"),
        dbx_path_from="/source.txt",
        dbx_path_from_lower="/source.txt",
    )

    with pytest.raises(RuntimeError, match="remote replacement failed"):
        sync_engine._on_local_moved(event)

    recovered = sync_engine._validated_recovered_local_paths()
    assert recovered["/destination.txt"]["path"] == "/destination.txt"
    sync_engine.client.move.assert_not_called()


def test_successful_local_move_clears_recovery_after_matching_index_proof(
    sync_engine: SyncEngine, tmp_path: Path
) -> None:
    prepare_local_move(sync_engine)
    destination_path = tmp_path / "destination.txt"
    destination_path.write_text("local move")
    content_hash = sync_engine.get_local_hash(str(destination_path))
    assert content_hash is not None
    add_move_file_index_entry(sync_engine, "/source.txt", content_hash)
    sync_engine.client.move = Mock(
        return_value=make_moved_file_metadata("/destination.txt", content_hash)
    )
    event = make_event(
        str(destination_path),
        "/destination.txt",
        direction=SyncDirection.Up,
        change_type=ChangeType.Moved,
        local_path_from=str(tmp_path / "source.txt"),
        dbx_path_from="/source.txt",
        dbx_path_from_lower="/source.txt",
    )

    assert sync_engine._on_local_moved(event) is SyncStatus.Done
    recovered = sync_engine._validated_recovered_local_paths()
    assert recovered["/destination.txt"]["path"] == "/destination.txt"

    sync_engine._maybe_finish_recovered_local_path("/destination.txt")
    assert sync_engine._validated_recovered_local_paths() == {}


def test_local_folder_deletion_always_queues_durable_restore(
    sync_engine: SyncEngine,
    tmp_path: Path,
) -> None:
    root_metadata = FolderMetadata(
        name="folder",
        path_lower="/folder",
        path_display="/folder",
        id="id:folder",
        shared=False,
    )
    with sync_engine._database_access():
        sync_engine._index_table.update(
            IndexEntry(
                dbx_path_lower="/folder",
                dbx_path_cased="/folder",
                provider_id="id:folder",
                item_type=ItemType.Folder,
                last_sync=1,
                rev="folder",
                content_hash="folder",
                symlink_target=None,
            )
        )
        sync_engine._index_table.update(
            IndexEntry(
                dbx_path_lower="/folder/child.txt",
                dbx_path_cased="/folder/child.txt",
                provider_id="id:child",
                item_type=ItemType.File,
                last_sync=1,
                rev="indexed-rev",
                content_hash="indexed-hash",
                symlink_target=None,
            )
        )
    sync_engine.client.get_metadata = Mock(return_value=root_metadata)
    sync_engine.client.list_folder = Mock()
    sync_engine.client.remove = Mock()
    event = make_event(
        str(tmp_path / "folder"),
        "/folder",
        direction=SyncDirection.Up,
        item_type=ItemType.Folder,
        change_type=ChangeType.Removed,
    )

    assert sync_engine._on_local_deleted(event) is SyncStatus.Conflict

    sync_engine.client.remove.assert_not_called()
    sync_engine.client.list_folder.assert_not_called()
    assert sync_engine._validated_download_intents() == {"/folder": "restore"}
    assert sync_engine.get_index_entry("/folder") is None
    assert sync_engine.get_index_entry("/folder/child.txt") is None


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


def test_selective_sync_postcommit_base_exception_keeps_parser_and_cache(
    sync_engine: SyncEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_save = sync_engine._conf.save

    def save_then_interrupt() -> int:
        original_save()
        raise KeyboardInterrupt("after config commit")

    monkeypatch.setattr(sync_engine._conf, "save", save_then_interrupt)

    with pytest.raises(KeyboardInterrupt, match="after config commit"):
        sync_engine.set_selective_sync("include", ["/Projects"])

    assert sync_engine._conf.get("sync", "selective_sync_mode") == "include"
    assert sync_engine._conf.get("sync", "selective_sync_paths") == ["/projects"]
    assert sync_engine.selective_sync_mode == "include"
    assert sync_engine.selective_sync_paths == {"/projects"}


@pytest.mark.parametrize("setting", ["dropbox_path", "ignore_symlinks"])
def test_single_config_setter_does_not_reuse_concurrent_commit(
    sync_engine: SyncEngine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    setting: str,
) -> None:
    old_path = sync_engine.dropbox_path
    new_path = str(tmp_path / "new-dropbox")
    target_option = "path" if setting == "dropbox_path" else "ignore_symlinks"
    target_failed = Event()
    other_started = Event()
    other_saved = Event()
    errors: list[BaseException] = []
    original_set = sync_engine._conf.set
    original_save = sync_engine._conf.save
    fail_next_save = True

    def fail_first_save() -> int:
        nonlocal fail_next_save
        if fail_next_save:
            fail_next_save = False
            raise OSError("cannot save target")
        return original_save()

    def pause_failed_target(
        section: str,
        option: str,
        value: object,
        save: bool = True,
    ) -> None:
        try:
            original_set(section, option, value, save)
        except OSError:
            if option == target_option:
                target_failed.set()
                other_started.wait(5)
                other_saved.wait(0.2)
            raise

    monkeypatch.setattr(sync_engine._conf, "save", fail_first_save)
    monkeypatch.setattr(sync_engine._conf, "set", pause_failed_target)

    def set_target() -> None:
        try:
            if setting == "dropbox_path":
                sync_engine.dropbox_path = new_path
            else:
                sync_engine.ignore_symlinks = True
        except BaseException as exc:
            errors.append(exc)

    def save_unrelated_value() -> None:
        other_started.set()
        original_set("main", "log_level", 10)
        other_saved.set()

    target_thread = Thread(target=set_target)
    target_thread.start()
    assert target_failed.wait(5)
    other_thread = Thread(target=save_unrelated_value)
    other_thread.start()
    target_thread.join(5)
    other_thread.join(5)

    assert not target_thread.is_alive()
    assert not other_thread.is_alive()
    assert other_saved.is_set()
    assert len(errors) == 1
    assert isinstance(errors[0], OSError)
    assert sync_engine._conf.get("main", "log_level") == 10
    if setting == "dropbox_path":
        assert sync_engine.dropbox_path == old_path
        assert sync_engine._conf.get("sync", "path") == old_path
    else:
        assert sync_engine.ignore_symlinks is False
        assert sync_engine._conf.get("sync", "ignore_symlinks") is False


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


def test_remote_folder_does_not_index_when_file_evacuation_fails(
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
    expected_snapshot = sync_engine._snapshot_local_tree(str(destination))
    sync_engine._stable_download_conflict = Mock(
        return_value=(Conflict.RemoteNewer, expected_snapshot)
    )
    sync_engine._ensure_parent = Mock()
    sync_engine.update_index_from_sync_event = Mock()
    monkeypatch.setattr(
        sync_module,
        "move",
        Mock(side_effect=PermissionError("cannot evacuate")),
    )

    with pytest.raises(PermissionError, match="cannot evacuate"):
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
    Path(decomposed).write_text("local")
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
