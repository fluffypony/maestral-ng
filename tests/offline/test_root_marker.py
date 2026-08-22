from __future__ import annotations

from unittest.mock import Mock

import pytest

from maestral.constants import ROOT_MARKER_FILE
from maestral.exceptions import NoDropboxDirError
from maestral.main import Maestral


def bypass_link_check(m: Maestral, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(m, "_check_linked", Mock())


def test_new_dropbox_directory_has_root_marker(
    m: Maestral, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    bypass_link_check(m, monkeypatch)
    dropbox_path = tmp_path / "Dropbox"

    m.create_dropbox_directory(str(dropbox_path))

    marker_path = dropbox_path / ROOT_MARKER_FILE
    assert marker_path.is_file()
    assert m.sync.is_excluded(str(marker_path))


def test_existing_dropbox_directory_requires_confirmation(
    m: Maestral, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    bypass_link_check(m, monkeypatch)
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    (dropbox_path / "existing.txt").write_text("existing")
    m.sync.dropbox_path = str(dropbox_path)
    m.manager.start = Mock()  # type: ignore[method-assign]

    with pytest.raises(NoDropboxDirError, match="Dropbox folder not confirmed"):
        m.start_sync()

    m.manager.start.assert_not_called()

    m.confirm_dropbox_directory()
    m.confirm_dropbox_directory()
    m.start_sync()

    assert (dropbox_path / ROOT_MARKER_FILE).is_file()
    m.manager.start.assert_called_once_with()


def test_empty_mount_point_is_rejected_before_sync(
    m: Maestral, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    bypass_link_check(m, monkeypatch)
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    m.sync.dropbox_path = str(dropbox_path)
    m.manager.start = Mock()  # type: ignore[method-assign]

    with pytest.raises(NoDropboxDirError, match="Dropbox folder not confirmed"):
        m.start_sync()

    assert list(dropbox_path.iterdir()) == []
    m.manager.start.assert_not_called()


def test_manager_restart_rechecks_root_marker(
    m: Maestral, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    bypass_link_check(m, monkeypatch)
    dropbox_path = tmp_path / "Dropbox"
    m.create_dropbox_directory(str(dropbox_path))
    (dropbox_path / ROOT_MARKER_FILE).unlink()

    with pytest.raises(NoDropboxDirError, match="Dropbox folder not confirmed"):
        m.manager.start()


def test_rebuild_index_checks_root_marker_before_reset(
    m: Maestral, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    bypass_link_check(m, monkeypatch)
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    m.sync.dropbox_path = str(dropbox_path)
    m.manager.rebuild_index = Mock()  # type: ignore[method-assign]

    with pytest.raises(NoDropboxDirError, match="Dropbox folder not confirmed"):
        m.rebuild_index()

    m.manager.rebuild_index.assert_not_called()


def test_sync_entry_points_check_root_marker_before_work(m: Maestral, tmp_path) -> None:
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    m.sync.dropbox_path = str(dropbox_path)
    m.sync._get_local_changes_while_inactive = Mock()  # type: ignore[method-assign]
    m.sync.list_local_changes = Mock()  # type: ignore[method-assign]
    m.sync.list_remote_changes_iterator = Mock()  # type: ignore[method-assign]
    m.sync.client.get_metadata = Mock()  # type: ignore[method-assign]

    with pytest.raises(NoDropboxDirError):
        m.sync.upload_local_changes_while_inactive()
    with pytest.raises(NoDropboxDirError):
        m.sync.upload_sync_cycle()
    with pytest.raises(NoDropboxDirError):
        m.sync.download_sync_cycle()
    with pytest.raises(NoDropboxDirError):
        m.sync.get_remote_item("/file")

    m.sync._get_local_changes_while_inactive.assert_not_called()
    m.sync.list_local_changes.assert_not_called()
    m.sync.list_remote_changes_iterator.assert_not_called()
    m.sync.client.get_metadata.assert_not_called()


def test_move_dropbox_directory_preserves_root_marker(
    m: Maestral, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    bypass_link_check(m, monkeypatch)
    old_path = tmp_path / "Dropbox"
    new_path = tmp_path / "Moved Dropbox"
    m.create_dropbox_directory(str(old_path))

    m.move_dropbox_directory(str(new_path))

    assert not old_path.exists()
    assert (new_path / ROOT_MARKER_FILE).is_file()
    assert m.sync.dropbox_path == str(new_path)
