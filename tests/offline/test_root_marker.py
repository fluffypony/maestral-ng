from __future__ import annotations

from unittest.mock import Mock

import pytest

import maestral.main as main_module
import maestral.utils.path as path_module
from maestral.constants import ROOT_MARKER_FILE
from maestral.exceptions import MaestralApiError, NoDropboxDirError, SymlinkError
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
    marker_id = m.get_conf("sync", "root_marker_id")
    assert marker_path.read_text() == f"maestral-root-v1:{marker_id}\n"
    assert m.sync.is_excluded(str(marker_path))


def test_create_dropbox_directory_suppresses_offline_autostart(
    m: Maestral, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    bypass_link_check(m, monkeypatch)
    dropbox_path = tmp_path / "Dropbox"
    m.manager.autostart.set()
    original_create_marker = m.sync.create_root_marker

    def create_marker_with_suppressed_start() -> None:
        assert not m.manager.autostart.is_set()
        original_create_marker()

    monkeypatch.setattr(
        m.sync,
        "create_root_marker",
        create_marker_with_suppressed_start,
    )

    m.create_dropbox_directory(str(dropbox_path))

    assert m.manager.autostart.is_set()
    assert (dropbox_path / ROOT_MARKER_FILE).is_file()


def test_create_dropbox_directory_keeps_autostart_off_for_pending_reset(
    m: Maestral, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    bypass_link_check(m, monkeypatch)
    m.manager.autostart.set()
    monkeypatch.setattr(
        m.sync,
        "_complete_pending_sync_reset",
        Mock(side_effect=OSError("injected reset failure")),
    )

    with pytest.raises(OSError, match="injected reset failure"):
        m.create_dropbox_directory(str(tmp_path / "Dropbox"))

    assert not m.manager.autostart.is_set()
    assert m.get_state("recovery", "sync_reset")["phase"] == "pending"


def test_create_dropbox_directory_rejects_raced_parent_link(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    bypass_link_check(m, monkeypatch)
    selected_parent = tmp_path / "selected"
    saved_parent = tmp_path / "saved-selected"
    outside = tmp_path / "outside"
    selected_parent.mkdir()
    outside.mkdir()
    requested_path = selected_parent / "nested" / "Dropbox"
    old_config_path = m.get_conf("sync", "path")
    reset_sync_state = Mock()
    m.manager.reset_sync_state = reset_sync_state  # type: ignore[method-assign]
    real_mkdir = path_module.mkdir
    raced = False

    def racing_mkdir(path: str, mode: int = 0o777, **kwargs) -> None:
        nonlocal raced
        if not raced:
            raced = True
            selected_parent.rename(saved_parent)
            try:
                selected_parent.symlink_to(outside, target_is_directory=True)
            except OSError:
                pytest.skip("directory links are not available")
        real_mkdir(path, mode, **kwargs)

    monkeypatch.setattr(path_module, "mkdir", racing_mkdir)

    with pytest.raises(OSError):
        m.create_dropbox_directory(str(requested_path))

    assert raced
    assert list(outside.iterdir()) == []
    assert m.get_conf("sync", "path") == old_config_path
    reset_sync_state.assert_not_called()


def test_root_marker_from_another_profile_is_rejected(
    m: Maestral, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    bypass_link_check(m, monkeypatch)
    dropbox_path = tmp_path / "Dropbox"
    m.create_dropbox_directory(str(dropbox_path))
    marker_path = dropbox_path / ROOT_MARKER_FILE
    original_marker = marker_path.read_bytes()
    m._conf.set("sync", "root_marker_id", "0" * 32)

    with pytest.raises(NoDropboxDirError, match="Dropbox folder not confirmed"):
        m.sync.ensure_dropbox_folder_present()

    with pytest.raises(MaestralApiError, match="Could not confirm Dropbox folder"):
        m.confirm_dropbox_directory()

    assert marker_path.read_bytes() == original_marker


def test_empty_legacy_root_marker_is_rejected(
    m: Maestral, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    bypass_link_check(m, monkeypatch)
    dropbox_path = tmp_path / "Dropbox"
    m.create_dropbox_directory(str(dropbox_path))
    marker_path = dropbox_path / ROOT_MARKER_FILE
    marker_path.write_bytes(b"")

    with pytest.raises(NoDropboxDirError, match="Dropbox folder not confirmed"):
        m.sync.ensure_dropbox_folder_present()


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


def test_move_dropbox_directory_rejects_descendant_link(
    m: Maestral, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    bypass_link_check(m, monkeypatch)
    old_path = tmp_path / "Dropbox"
    new_path = tmp_path / "Moved Dropbox"
    m.create_dropbox_directory(str(old_path))
    original_snapshot = main_module.rooted_tree_snapshot

    def snapshot_with_link(path: str, root_path: str, **kwargs):
        snapshot = original_snapshot(path, root_path, **kwargs)
        if path == str(old_path):
            root_identity = snapshot[str(old_path)]
            snapshot[str(old_path / "junction")] = (
                root_identity[0],
                root_identity[1] + 1,
                root_identity[2],
                0,
                root_identity[4],
                root_identity[5],
                "symlink:/outside",
            )
        return snapshot

    monkeypatch.setattr(main_module, "rooted_tree_snapshot", snapshot_with_link)
    move = Mock()
    monkeypatch.setattr("maestral.main.move", move)

    with pytest.raises(SymlinkError, match="Cannot move Dropbox folder"):
        m.move_dropbox_directory(str(new_path))

    move.assert_not_called()
    assert old_path.is_dir()
    assert not new_path.exists()


def test_dropbox_root_change_rejects_pending_path_root_migration(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    bypass_link_check(m, monkeypatch)
    old_path = tmp_path / "Dropbox"
    moved_path = tmp_path / "Moved Dropbox"
    created_path = tmp_path / "Replacement Dropbox"
    m.create_dropbox_directory(str(old_path))
    m._state.set("account", "path_root_migration", {"phase": "staged"})

    with pytest.raises(MaestralApiError, match="path-root migration"):
        m.move_dropbox_directory(str(moved_path))
    with pytest.raises(MaestralApiError, match="path-root migration"):
        m.create_dropbox_directory(str(created_path))

    assert old_path.is_dir()
    assert not moved_path.exists()
    assert not created_path.exists()
