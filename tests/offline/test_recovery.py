from __future__ import annotations

from unittest.mock import Mock

import pytest

import maestral.manager as manager_module
from maestral.exceptions import (
    DropboxConnectionError,
    DropboxServerError,
    NoDropboxDirError,
)
from maestral.main import Maestral


def test_interrupted_remote_index_resumes_from_saved_cursor(
    m: Maestral, tmp_path
) -> None:
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    m.sync.dropbox_path = str(dropbox_path)
    m.sync.create_root_marker()
    first_page = [Mock(name="first-page-event")]

    def interrupted_index(_cursor: str):
        yield first_page, "cursor-1"
        raise DropboxServerError("Dropbox unavailable", "Try again later")

    m.sync.list_remote_changes_iterator = Mock(  # type: ignore[method-assign]
        side_effect=interrupted_index
    )
    m.sync.apply_remote_changes = Mock(return_value=[])  # type: ignore[method-assign]

    with pytest.raises(DropboxServerError):
        m.sync.download_sync_cycle()

    assert m.sync.remote_cursor == "cursor-1"
    assert m.get_state("sync", "indexing_counter") == 1
    assert not m.get_state("sync", "did_finish_indexing")

    second_page = [Mock(name="second-page-event")]
    m.sync.list_remote_changes_iterator = Mock(  # type: ignore[method-assign]
        return_value=iter([(second_page, "cursor-2")])
    )

    m.sync.download_sync_cycle()

    m.sync.list_remote_changes_iterator.assert_called_once_with("cursor-1")
    assert m.sync.remote_cursor == "cursor-2"
    assert m.get_state("sync", "indexing_counter") == 0
    assert m.get_state("sync", "did_finish_indexing")


@pytest.mark.parametrize("error_type", [DropboxConnectionError, DropboxServerError])
def test_transient_dropbox_error_schedules_sync_restart(
    m: Maestral, error_type: type[DropboxConnectionError | DropboxServerError]
) -> None:
    m.manager.running.set()
    m.manager.autostart.clear()

    with m.manager._handle_sync_thread_errors(m.manager.running, m.manager.autostart):
        raise error_type("Dropbox unavailable", "Try again later")

    assert not m.manager.running.is_set()
    assert m.manager.autostart.is_set()


def test_connection_monitor_restarts_after_server_error(
    m: Maestral, monkeypatch: pytest.MonkeyPatch
) -> None:
    m.manager.running.clear()
    m.manager.autostart.set()
    m.manager.connected = True
    m.manager._connection_helper_running = True
    m.manager.start = Mock()  # type: ignore[method-assign]

    def connected_once(*_args, **_kwargs) -> bool:
        m.manager._connection_helper_running = False
        return True

    monkeypatch.setattr(manager_module, "check_connection", connected_once)
    monkeypatch.setattr(manager_module.time, "sleep", Mock())

    m.manager.connection_monitor()

    m.manager.start.assert_called_once_with()


def test_external_drive_can_retry_same_configured_root(
    m: Maestral, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    dropbox_path = tmp_path / "External Dropbox"
    m.sync.dropbox_path = str(dropbox_path)
    m.sync.remote_cursor = "saved-cursor"
    monkeypatch.setattr(manager_module, "check_connection", Mock(return_value=False))

    with pytest.raises(NoDropboxDirError):
        m.manager.start()

    dropbox_path.mkdir()
    m.sync.create_root_marker()
    m.manager.start()

    assert m.sync.remote_cursor == "saved-cursor"
    assert m.manager.autostart.is_set()
