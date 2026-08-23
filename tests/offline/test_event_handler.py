import os
from pathlib import Path

from watchdog.events import (
    DirCreatedEvent,
    DirModifiedEvent,
    DirMovedEvent,
    FileDeletedEvent,
    FileModifiedEvent,
    FileMovedEvent,
)

from maestral.constants import MOVE_TEMP_PREFIX, REMOVE_TEMP_PREFIX
from maestral.models import ChangeType, ItemType
from maestral.sync import FSEventHandler, SyncDirection, SyncEngine
from maestral.utils.path import get_local_change_time, move


def ipath(i: int) -> str:
    """Returns path names '/test 1', '/test 2', ..."""
    return f"/test {i}"


def test_receiving_events(sync: SyncEngine) -> None:
    new_dir = Path(sync.dropbox_path) / "parent"
    new_dir.mkdir()

    sync.wait_for_local_changes()
    sync_events, _ = sync.list_local_changes()

    assert len(sync_events) == 1

    change_time = get_local_change_time(os.stat(new_dir))

    event = sync_events[0]
    assert event.direction == SyncDirection.Up
    assert event.item_type == ItemType.Folder
    assert event.change_type == ChangeType.Added
    assert event.change_time == change_time
    assert event.local_path == str(new_dir)


def test_always_ignored_events(sync: SyncEngine) -> None:
    sync.fs_events.on_any_event(DirModifiedEvent("/test"))
    sync.fs_events.on_any_event(DirMovedEvent("/test", "/test"))
    sync.fs_events.on_any_event(FileMovedEvent("/test", "/test"))

    assert sync.fs_events.local_file_event_queue.empty()


def test_sync_root_creation_event_is_ignored(sync: SyncEngine) -> None:
    event = DirCreatedEvent(sync.dropbox_path)

    assert sync._filter_local_events([event]) == []


def test_rooted_mutation_temporary_events_are_ignored() -> None:
    handler = FSEventHandler()
    handler.enable()

    handler.on_any_event(FileDeletedEvent(f"/{MOVE_TEMP_PREFIX}backup"))
    handler.on_any_event(DirCreatedEvent(f"/{REMOVE_TEMP_PREFIX}quarantine"))

    assert handler.local_file_event_queue.empty()


def test_fs_ignore_tree_creation(sync: SyncEngine) -> None:
    new_dir = Path(sync.dropbox_path) / "parent"

    with sync.fs_events.ignore(DirCreatedEvent(str(new_dir))):
        new_dir.mkdir()
        for i in range(10):
            file = new_dir / f"test_{i}"
            file.touch()

    sync.wait_for_local_changes(timeout=1)
    sync_events, _ = sync.list_local_changes()
    assert len(sync_events) == 0


def test_recursive_ignore_accepts_child_event_with_different_type() -> None:
    handler = FSEventHandler()

    with handler.ignore(DirCreatedEvent("/parent")):
        assert handler._is_ignored(FileModifiedEvent("/parent/file.txt"))
        assert not handler._is_ignored(FileModifiedEvent("/other/file.txt"))


def test_fs_ignore_tree_move(sync: SyncEngine) -> None:
    new_dir = Path(sync.dropbox_path) / "parent"

    new_dir.mkdir()
    for i in range(10):
        file = new_dir / f"test_{i}"
        file.touch()

    sync.wait_for_local_changes()
    sync.list_local_changes()

    new_dir_1 = Path(sync.dropbox_path) / "parent2"

    with sync.fs_events.ignore(DirMovedEvent(str(new_dir), str(new_dir_1))):
        move(str(new_dir), str(new_dir_1))

    sync.wait_for_local_changes(timeout=1)
    sync_events, _ = sync.list_local_changes()
    assert len(sync_events) == 0


def test_catching_non_ignored_events(sync: SyncEngine) -> None:
    new_dir = Path(sync.dropbox_path) / "parent"

    with sync.fs_events.ignore(DirCreatedEvent(str(new_dir)), recursive=False):
        new_dir.mkdir()
        for i in range(10):
            # may trigger FileCreatedEvent and FileModifiedVent
            file = new_dir / f"test_{i}"
            file.touch()

    sync.wait_for_local_changes()
    sync_events, _ = sync.list_local_changes()
    assert all(not event.is_directory for event in sync_events)
