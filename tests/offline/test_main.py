import stat
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests

import maestral.main
from maestral.constants import GITHUB_RELEASES_API
from maestral.exceptions import (
    MaestralApiError,
    NotLinkedError,
    VirtualFileBusyError,
    VirtualFilesUnsupportedError,
)
from maestral.main import Maestral
from maestral.virtual_files import VirtualFileRootBinding, VirtualFileRootIdentity

from .virtual_files_fakes import FakeVirtualFileBackend


def unlink_reset_journal(
    *,
    sync_mode: str = "mirror",
    root_path: str = "",
    source_root_path: str = "",
    source_root_identity: list[object] | None = None,
    root_marker_id: str = "",
    native_registration_committed: bool = False,
) -> dict[str, object]:
    return {
        "kind": "unlink",
        "phase": "virtual",
        "provider": "dropbox",
        "account_id": "old-account",
        "keyring": "automatic",
        "credentials_deleted": True,
        "vault_password_deleted": True,
        "virtual_files_reset": False,
        "sync_mode": sync_mode,
        "root_path": root_path,
        "source_root_path": source_root_path,
        "source_root_identity": source_root_identity,
        "root_marker_id": root_marker_id,
        "native_registration_committed": native_registration_committed,
        "root_marker_removed": False,
    }


def test_root_creation_reservation_blocks_a_concurrent_start(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    m.manager._connection_helper_stop.set()
    m.manager.connection_helper.join(timeout=2)
    root_update_started = Event()
    finish_root_update = Event()
    errors: list[BaseException] = []
    monkeypatch.setattr(m, "_check_linked", Mock())
    monkeypatch.setattr(m.manager, "reset_sync_state", Mock())
    monkeypatch.setattr(m.sync, "create_root_marker", Mock())

    def set_dropbox_path(*args: object, **kwargs: object) -> None:
        root_update_started.set()
        finish_root_update.wait(5)

    monkeypatch.setattr(m.sync, "set_dropbox_path", set_dropbox_path)

    def create_root() -> None:
        try:
            m.create_dropbox_directory(str(tmp_path / "new-dropbox"))
        except BaseException as exc:
            errors.append(exc)

    thread = Thread(target=create_root)
    thread.start()
    assert root_update_started.wait(5)

    with pytest.raises(MaestralApiError, match="internal operation"):
        m.manager.start()

    finish_root_update.set()
    thread.join(5)

    assert not thread.is_alive()
    assert errors == []


def test_root_creation_releases_its_reservation_before_replay_logging(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(m, "_check_linked", Mock())

    def fail_reset() -> None:
        m._state.set("recovery", "sync_reset", {"kind": "sync"})
        raise OSError("injected reset failure")

    monkeypatch.setattr(m.manager, "reset_sync_state", fail_reset)
    monkeypatch.setattr(
        m.sync,
        "_complete_pending_sync_reset",
        Mock(side_effect=RuntimeError("injected replay failure")),
    )
    monkeypatch.setattr(
        m._logger,
        "debug",
        Mock(side_effect=KeyboardInterrupt("injected logging failure")),
    )

    try:
        with pytest.raises(KeyboardInterrupt, match="injected logging failure"):
            m.create_dropbox_directory(str(tmp_path / "new-dropbox"))

        assert m.manager._active_internal_operation is None
    finally:
        m._state.set("recovery", "sync_reset", {})


def test_mirror_only_operations_reject_virtual_mode(
    m: Maestral, monkeypatch: pytest.MonkeyPatch
) -> None:
    m._sync_mode = "virtual"
    rebuild = Mock()
    selective = Mock()
    monkeypatch.setattr(m.manager, "rebuild_index", rebuild)
    monkeypatch.setattr(m.sync, "clean_selective_sync_paths", selective)

    with pytest.raises(MaestralApiError, match="only available for a mirror root"):
        m.rebuild_index()
    with pytest.raises(MaestralApiError, match="only available for a mirror root"):
        m.set_selective_sync("exclude", ["/Folder"])

    rebuild.assert_not_called()
    selective.assert_not_called()


def test_stopped_virtual_root_validates_native_registration(
    m: Maestral, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    marker_id = "a" * 32
    root = tmp_path / "visible-root"
    root.mkdir()
    m._sync_mode = "virtual"
    m._conf.set("sync", "mode", "virtual")
    m._conf.set("sync", "root_marker_id", marker_id)
    m.sync._publish_dropbox_path(str(root))
    validate_root = Mock()
    monkeypatch.setattr(m.virtual_files, "validate_root", validate_root)

    m._check_dropbox_dir()

    validate_root.assert_called_once_with(str(root), marker_id)


def test_stopped_virtual_root_propagates_inactive_registration(
    m: Maestral, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    marker_id = "b" * 32
    root = tmp_path / "visible-root"
    root.mkdir()
    m._sync_mode = "virtual"
    m._conf.set("sync", "mode", "virtual")
    m._conf.set("sync", "root_marker_id", marker_id)
    m.sync._publish_dropbox_path(str(root))
    monkeypatch.setattr(
        m.virtual_files,
        "validate_root",
        Mock(
            side_effect=VirtualFileBusyError(
                "Native virtual root is inactive",
                "The File Provider domain is unavailable.",
            )
        ),
    )

    with pytest.raises(VirtualFileBusyError, match="domain is unavailable"):
        m._check_dropbox_dir()


def test_shutdown_retries_an_engine_before_shared_resource_close(
    m: Maestral, monkeypatch: pytest.MonkeyPatch
) -> None:
    virtual_close = m.virtual_files.close
    manager_shutdown = m.manager.shutdown
    client_close = m.client.close
    calls: list[str] = []
    virtual_attempts = 0

    def close_virtual_files() -> None:
        nonlocal virtual_attempts
        virtual_attempts += 1
        calls.append("virtual")
        if virtual_attempts == 1:
            raise OSError("native stop failed")
        virtual_close()

    def close_manager() -> None:
        calls.append("manager")
        manager_shutdown()

    def close_client() -> None:
        calls.append("client")
        client_close()

    monkeypatch.setattr(m.virtual_files, "close", close_virtual_files)
    monkeypatch.setattr(m.manager, "shutdown", close_manager)
    monkeypatch.setattr(m.client, "close", close_client)

    with pytest.raises(OSError, match="native stop failed"):
        m.shutdown_daemon()

    assert calls == ["virtual"]

    m.shutdown_daemon()
    m.shutdown_daemon()

    assert calls == ["virtual", "virtual", "manager", "client"]


def test_check_for_updates(m: Maestral) -> None:
    # get current releases from GitHub

    resp = requests.get(GITHUB_RELEASES_API)

    try:
        resp.raise_for_status()
    except Exception:
        # rate limit etc, connection error, etc
        return

    data = resp.json()

    previous_release = data[1]["tag_name"].lstrip("v")
    latest_stable_release = data[0]["tag_name"].lstrip("v")

    # check that no update is offered from current (newest) version

    maestral.main.__version__ = latest_stable_release

    update_res = m.check_for_updates()

    assert update_res.latest_release == latest_stable_release
    assert not update_res.update_available
    assert update_res.release_notes == ""

    # check that update is offered from previous release

    maestral.main.__version__ = previous_release

    update_res = m.check_for_updates()

    assert update_res.latest_release == latest_stable_release
    assert update_res.update_available
    assert update_res.release_notes != ""


def test_not_linked_error(m: Maestral) -> None:
    with pytest.raises(NotLinkedError):
        m.get_metadata("/test")


def test_unlink_keeps_valid_default_state_with_existing_database(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(m, "_check_linked", Mock())
    monkeypatch.setattr(m.client, "unlink", Mock())
    monkeypatch.setattr(m.cred_storage, "delete_creds", Mock())
    m._conf.set("auth", "account_id", "linked-account")
    m._conf.set("sync", "path", "/old/dropbox")
    m._state.set("account", "path_root_nsid", "old-root")

    m.unlink()

    assert Path(m._conf.config_path).is_file()
    assert Path(m._state.config_path).is_file()
    assert Path(m.sync._db_path).is_file()
    assert m._conf.get("auth", "account_id") == ""
    assert m._conf.get("sync", "path") == ""
    assert m._state.get("account", "path_root_nsid") == ""
    backup_path = Path(m._conf.backup_path_for_version(None))
    assert "linked-account" not in backup_path.read_text()


@pytest.mark.parametrize(
    ("sync_mode", "registered"),
    [("mirror", True), ("virtual", False)],
)
def test_unlink_without_a_live_virtual_registration_clears_core_state(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
    sync_mode: str,
    registered: bool,
) -> None:
    journal = unlink_reset_journal(
        sync_mode=sync_mode,
        native_registration_committed=registered,
    )
    m._state.set("recovery", "sync_reset", journal)
    clear = Mock()
    detach = Mock()
    monkeypatch.setattr(m.virtual_files, "clear", clear)
    monkeypatch.setattr(m.virtual_files, "detach", detach)
    monkeypatch.setattr(m.manager, "reload_download_queue", Mock())

    m._complete_pending_unlink_reset()

    clear.assert_called_once_with()
    detach.assert_not_called()
    completed = m._state.get("recovery", "sync_reset")
    assert completed["phase"] == "queue"
    assert completed["virtual_files_reset"] is True
    assert completed["root_marker_removed"] is True


def test_registered_virtual_unlink_detaches_before_marker_removal(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    marker_id = "a" * 32
    source = tmp_path / "source"
    visible = tmp_path / "visible"
    source.mkdir()
    visible.mkdir()
    marker = source / ".maestral-root"
    marker.write_text(f"maestral-root-v1:{marker_id}\n")
    marker.chmod(0o600)
    source_stat = source.lstat()
    journal = unlink_reset_journal(
        sync_mode="virtual",
        root_path=str(visible),
        source_root_path=str(source),
        source_root_identity=[
            str(source_stat.st_dev),
            str(source_stat.st_ino),
            source_stat.st_mode,
        ],
        root_marker_id=marker_id,
        native_registration_committed=True,
    )
    m._state.set("recovery", "sync_reset", journal)

    def detach(root_path: str, saved_marker_id: str) -> str:
        assert marker.is_file()
        assert root_path == str(visible)
        assert saved_marker_id == marker_id
        return str(source)

    monkeypatch.setattr(m.virtual_files, "detach", Mock(side_effect=detach))
    monkeypatch.setattr(m.virtual_files, "clear", Mock())
    monkeypatch.setattr(m.manager, "reload_download_queue", Mock())

    m._complete_pending_unlink_reset()

    assert not marker.exists()
    m.virtual_files.clear.assert_not_called()
    completed = m._state.get("recovery", "sync_reset")
    assert completed["phase"] == "queue"
    assert completed["root_marker_removed"] is True


def test_failed_virtual_detach_keeps_the_unlink_reset_replayable(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    marker_id = "b" * 32
    source = tmp_path / "source"
    visible = tmp_path / "visible"
    source.mkdir()
    visible.mkdir()
    marker = source / ".maestral-root"
    marker.write_text(f"maestral-root-v1:{marker_id}\n")
    source_stat = source.lstat()
    journal = unlink_reset_journal(
        sync_mode="virtual",
        root_path=str(visible),
        source_root_path=str(source),
        source_root_identity=[
            str(source_stat.st_dev),
            str(source_stat.st_ino),
            source_stat.st_mode,
        ],
        root_marker_id=marker_id,
        native_registration_committed=True,
    )
    m._state.set("recovery", "sync_reset", journal)
    detach = Mock(side_effect=OSError("native detach failed"))
    monkeypatch.setattr(m.virtual_files, "detach", detach)
    monkeypatch.setattr(m.manager, "reload_download_queue", Mock())

    with pytest.raises(OSError, match="native detach failed"):
        m._complete_pending_unlink_reset()

    assert marker.is_file()
    assert m._state.get("recovery", "sync_reset") == journal


def test_unlink_retries_after_native_detach_before_marker_removal(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    marker_id = "c" * 32
    source = tmp_path / "source"
    visible = tmp_path / "visible"
    source.mkdir()
    visible.mkdir()
    marker = source / ".maestral-root"
    marker.write_text(f"maestral-root-v1:{marker_id}\n")
    marker.chmod(0o600)
    source_stat = source.lstat()
    journal = unlink_reset_journal(
        sync_mode="virtual",
        root_path=str(visible),
        source_root_path=str(source),
        source_root_identity=[
            str(source_stat.st_dev),
            str(source_stat.st_ino),
            source_stat.st_mode,
        ],
        root_marker_id=marker_id,
        native_registration_committed=True,
    )
    m._state.set("recovery", "sync_reset", journal)
    detach = Mock(return_value=str(source))
    original_remove = m._remove_pending_unlink_root_marker
    remove_attempts = 0

    def remove(saved_journal: dict[str, object]) -> None:
        nonlocal remove_attempts
        remove_attempts += 1
        if remove_attempts == 1:
            raise OSError("crash after native detach")
        original_remove(saved_journal)  # type: ignore[arg-type]

    monkeypatch.setattr(m.virtual_files, "detach", detach)
    monkeypatch.setattr(m, "_remove_pending_unlink_root_marker", remove)
    monkeypatch.setattr(m.manager, "reload_download_queue", Mock())

    with pytest.raises(OSError, match="crash after native detach"):
        m._complete_pending_unlink_reset()
    assert marker.is_file()
    assert m._state.get("recovery", "sync_reset") == journal

    m._complete_pending_unlink_reset()

    assert detach.call_count == 2
    assert not marker.exists()
    assert m._state.get("recovery", "sync_reset")["phase"] == "queue"


def test_unlink_config_reset_keeps_the_saved_sync_mode(m: Maestral) -> None:
    m.sync._begin_sync_reset(
        "unlink",
        provider="dropbox",
        account_id="old-account",
        sync_mode="virtual",
    )

    m.sync._complete_pending_sync_reset()

    assert m._conf.get("sync", "mode") == "virtual"
    assert m.sync._validated_sync_reset()["phase"] == "virtual"
    m._state.set("recovery", "sync_reset", {})


def test_old_unlink_journal_migrates_root_context_once(m: Maestral) -> None:
    m._conf.set("sync", "mode", "virtual")
    old_journal = {
        "kind": "unlink",
        "phase": "virtual",
        "provider": "dropbox",
        "account_id": "old-account",
        "keyring": "automatic",
        "credentials_deleted": True,
        "vault_password_deleted": True,
        "virtual_files_reset": False,
        "root_path": "/old/root",
        "root_marker_id": "d" * 32,
    }
    m._state.set("recovery", "sync_reset", old_journal)

    migrated = m.sync._validated_sync_reset()

    assert migrated["sync_mode"] == "virtual"
    assert migrated["source_root_path"] == "/old/root"
    assert migrated["source_root_identity"] is None
    assert migrated["native_registration_committed"] is False
    assert migrated["root_marker_removed"] is False
    assert m._state.get("recovery", "sync_reset") == migrated
    m._state.set("recovery", "sync_reset", {})


@pytest.mark.parametrize(
    "saved_bindings",
    [
        {},
        {"e" * 32: {"state": "accepted"}},
    ],
)
def test_unlink_rejects_a_missing_backend_before_credential_deletion(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
    saved_bindings: dict[str, object],
) -> None:
    marker_id = "e" * 32
    m._sync_mode = "virtual"
    m._conf.set("sync", "mode", "virtual")
    m._conf.set("sync", "path", "/old/virtual-root")
    m._conf.set("sync", "root_marker_id", marker_id)
    m._state.set("virtual_files", "root_bindings", saved_bindings)
    unlink = Mock()
    delete_creds = Mock()
    monkeypatch.setattr(m, "_check_linked", Mock())
    monkeypatch.setattr(m.client, "unlink", unlink)
    monkeypatch.setattr(m.cred_storage, "delete_creds", delete_creds)

    with pytest.raises(VirtualFilesUnsupportedError, match="native virtual-file"):
        m.unlink()

    unlink.assert_not_called()
    delete_creds.assert_not_called()
    assert m._state.get("recovery", "sync_reset") == {}


def test_unlink_repairs_a_pending_native_registration_before_reset(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    marker_id = "f" * 32
    source = tmp_path / "source"
    visible = tmp_path / "visible"
    source.mkdir()
    visible.mkdir()
    source_stat = source.lstat()
    visible_stat = visible.lstat()
    binding = VirtualFileRootBinding(
        source_root_path=str(source),
        root_path=str(visible),
        cache_path=str(tmp_path / "native-cache"),
        source_root_identity=VirtualFileRootIdentity(
            str(source_stat.st_dev), str(source_stat.st_ino), source_stat.st_mode
        ),
        root_identity=VirtualFileRootIdentity(
            str(visible_stat.st_dev), str(visible_stat.st_ino), visible_stat.st_mode
        ),
    )
    backend = FakeVirtualFileBackend()
    backend.root_binding = binding
    m.virtual_files.replace_backend(backend)
    m._sync_mode = "virtual"
    m._conf.set("sync", "mode", "virtual")
    m._conf.set("sync", "root_marker_id", marker_id)
    m.sync._publish_dropbox_path(
        str(source), (source_stat.st_dev, source_stat.st_ino, source_stat.st_mode)
    )
    events: list[str] = []

    def repair() -> VirtualFileRootBinding:
        events.append("repair")
        backend._registration_committed = True
        return binding

    monkeypatch.setattr(m, "_start_virtual_files", repair)

    context = m._unlink_root_context()

    assert events == ["repair"]
    assert context == {
        "sync_mode": "virtual",
        "root_path": str(visible),
        "source_root_path": str(source),
        "source_root_identity": [
            str(source_stat.st_dev),
            str(source_stat.st_ino),
            source_stat.st_mode,
        ],
        "root_marker_id": marker_id,
        "native_registration_committed": True,
    }


def test_unlink_uses_an_already_detached_binding_without_restart(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    marker_id = "1" * 32
    source = tmp_path / "source"
    visible = tmp_path / "visible"
    source.mkdir()
    visible.mkdir()
    source_stat = source.lstat()
    visible_stat = visible.lstat()
    binding = VirtualFileRootBinding(
        source_root_path=str(source),
        root_path=str(visible),
        cache_path=str(tmp_path / "native-cache"),
        source_root_identity=VirtualFileRootIdentity(
            str(source_stat.st_dev), str(source_stat.st_ino), source_stat.st_mode
        ),
        root_identity=VirtualFileRootIdentity(
            str(visible_stat.st_dev), str(visible_stat.st_ino), visible_stat.st_mode
        ),
    )
    backend = FakeVirtualFileBackend()
    backend.root_binding = binding
    backend._binding_detached = True
    m.virtual_files.replace_backend(backend)
    m._sync_mode = "virtual"
    m._conf.set("sync", "mode", "virtual")
    m._conf.set("sync", "root_marker_id", marker_id)
    m.sync._publish_dropbox_path(str(visible))
    repair = Mock()
    monkeypatch.setattr(m, "_start_virtual_files", repair)

    context = m._unlink_root_context()

    repair.assert_not_called()
    assert context["root_path"] == str(visible)
    assert context["source_root_path"] == str(source)
    assert context["native_registration_committed"] is False


def test_unlink_reservation_does_not_wait_for_a_concurrent_stop(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    m.manager._connection_helper_stop.set()
    m.manager.connection_helper.join(timeout=2)
    begin_entered = Event()
    allow_begin = Event()
    stop_started = Event()
    unlink_errors: list[BaseException] = []
    stop_errors: list[BaseException] = []
    monkeypatch.setattr(m, "_check_linked", Mock())
    monkeypatch.setattr(m.client, "unlink", Mock())
    monkeypatch.setattr(m.cred_storage, "delete_creds", Mock())
    m._conf.set("auth", "account_id", "linked-account")
    m._conf.set("sync", "path", "/old/dropbox")
    original_begin = m.manager.begin_unlink_reset

    def delayed_begin(*args: object, **kwargs: object) -> None:
        begin_entered.set()
        allow_begin.wait(5)
        original_begin(*args, **kwargs)

    monkeypatch.setattr(m.manager, "begin_unlink_reset", delayed_begin)

    def unlink() -> None:
        try:
            m.unlink()
        except BaseException as exc:
            unlink_errors.append(exc)

    unlink_thread = Thread(target=unlink)
    unlink_thread.start()
    assert begin_entered.wait(5)

    original_request_cancel = m.sync.request_cancel

    def request_cancel() -> None:
        stop_started.set()
        original_request_cancel()

    monkeypatch.setattr(m.sync, "request_cancel", request_cancel)

    def stop() -> None:
        try:
            m.manager.stop()
        except BaseException as exc:
            stop_errors.append(exc)

    stop_thread = Thread(target=stop)
    stop_thread.start()
    assert stop_started.wait(5)
    allow_begin.set()
    unlink_thread.join(5)
    stop_thread.join(5)

    assert not unlink_thread.is_alive()
    assert not stop_thread.is_alive()
    assert unlink_errors == []
    assert stop_errors == []


def test_interrupted_unlink_keeps_a_reopenable_state_file(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(m, "_check_linked", Mock())
    monkeypatch.setattr(m.client, "unlink", Mock())
    monkeypatch.setattr(m.cred_storage, "delete_creds", Mock())
    m._conf.set("auth", "account_id", "linked-account")
    original_reset = m._state.reset_to_defaults

    def reset_then_interrupt(*args, **kwargs) -> None:
        original_reset(*args, **kwargs)
        raise KeyboardInterrupt("after state reset")

    monkeypatch.setattr(m._state, "reset_to_defaults", reset_then_interrupt)

    with pytest.raises(KeyboardInterrupt, match="after state reset"):
        m.unlink()

    assert Path(m._state.config_path).is_file()
    assert Path(m.sync._db_path).is_file()
    assert m._conf.get("auth", "account_id") == "linked-account"


@pytest.mark.parametrize(
    ("section", "name"),
    [
        ("recovery", "local_evacuations"),
        ("recovery", "local_paths"),
        ("recovery", "case_changes"),
        ("recovery", "root_move"),
        ("account", "path_root_migration"),
    ],
)
def test_unlink_refuses_root_bound_recovery_before_credential_deletion(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
    section: str,
    name: str,
) -> None:
    monkeypatch.setattr(m, "_check_linked", Mock())
    unlink = Mock()
    delete_creds = Mock()
    monkeypatch.setattr(m.client, "unlink", unlink)
    monkeypatch.setattr(m.cred_storage, "delete_creds", delete_creds)
    m._state.set(section, name, {"pending": True})

    with pytest.raises(MaestralApiError, match="pending local recovery"):
        m.unlink()

    unlink.assert_not_called()
    delete_creds.assert_not_called()
    assert m._state.get("recovery", "sync_reset") == {}


def test_link_refuses_old_root_recovery_after_interrupted_unlink(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal = {
        "kind": "unlink",
        "phase": "queue",
        "provider": "dropbox",
        "account_id": "old-account",
        "keyring": "automatic",
        "credentials_deleted": False,
        "vault_password_deleted": True,
        "virtual_files_reset": True,
        "sync_mode": "mirror",
        "root_path": "/old/dropbox",
        "source_root_path": "/old/dropbox",
        "source_root_identity": None,
        "root_marker_id": "a" * 32,
        "native_registration_committed": False,
        "root_marker_removed": False,
    }
    m._state.set("recovery", "local_paths", {"/old.txt": {}})
    m._state.set("recovery", "sync_reset", journal)
    link = Mock()
    delete_creds = Mock()
    monkeypatch.setattr(m.client, "link", link)
    monkeypatch.setattr(m.cred_storage, "delete_creds", delete_creds)

    with pytest.raises(MaestralApiError, match="old account recovery"):
        m.link(access_token="new-token")

    link.assert_not_called()
    delete_creds.assert_not_called()
    assert m._state.get("recovery", "sync_reset") == {
        **journal,
        "sync_mode": "mirror",
        "source_root_path": "/old/dropbox",
        "source_root_identity": None,
        "native_registration_committed": False,
        "root_marker_removed": False,
    }


def test_reset_marker_remains_until_root_bound_recovery_is_clear(m: Maestral) -> None:
    journal = {
        "kind": "unlink",
        "phase": "queue",
        "provider": "dropbox",
        "account_id": "old-account",
        "keyring": "automatic",
        "credentials_deleted": True,
        "vault_password_deleted": True,
        "virtual_files_reset": True,
        "root_path": "/old/dropbox",
        "root_marker_id": "a" * 32,
    }
    m._state.set("recovery", "local_paths", {"/old.txt": {}})
    m._state.set("recovery", "sync_reset", journal)

    m.sync._finish_sync_reset_queue()

    assert m._state.get("recovery", "sync_reset") == {
        **journal,
        "sync_mode": "mirror",
        "source_root_path": "/old/dropbox",
        "source_root_identity": None,
        "native_registration_committed": False,
        "root_marker_removed": False,
    }
    with pytest.raises(MaestralApiError, match="pending reset"):
        m.manager.start()


def test_profile_cleanup_rejects_linked_cache(
    m: Maestral, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    cache_path = tmp_path / "cache"
    cache_path.mkdir()
    profile_pic = cache_path / f"{m.config_name}_profile_pic.jpeg"
    profile_pic.write_text("external")
    monkeypatch.setattr(
        "maestral.main.get_cache_path",
        Mock(return_value=str(cache_path)),
    )
    monkeypatch.setattr(
        "maestral.main.os.lstat",
        Mock(
            return_value=SimpleNamespace(
                st_mode=stat.S_IFDIR,
                st_reparse_tag=0xA0000003,
            )
        ),
    )
    scandir = Mock(side_effect=AssertionError("linked cache was scanned"))
    monkeypatch.setattr("maestral.main.os.scandir", scandir)

    m._delete_old_profile_pics()

    scandir.assert_not_called()
    assert profile_pic.read_text() == "external"


def test_profile_write_replaces_link_without_following_it(
    m: Maestral, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    cache_path = tmp_path / "cache"
    cache_path.mkdir()
    external_file = tmp_path / "external.jpeg"
    external_file.write_bytes(b"keep me")
    profile_path = cache_path / f"{m.config_name}_profile_pic.jpeg"
    try:
        profile_path.symlink_to(external_file)
    except OSError as exc:
        if getattr(exc, "winerror", None) == 1314:
            pytest.skip("Windows did not grant symlink creation permission")
        raise

    monkeypatch.setattr(m, "_check_linked", Mock())
    monkeypatch.setattr(
        m.client,
        "get_account_info",
        Mock(
            return_value=SimpleNamespace(profile_photo_url="https://example.test/pic")
        ),
    )
    monkeypatch.setattr(
        "maestral.main.get_cache_path",
        Mock(return_value=str(cache_path)),
    )
    monkeypatch.setattr(
        "maestral.main.requests.get",
        Mock(return_value=SimpleNamespace(content=b"new picture")),
    )

    result = m.get_profile_pic()

    assert Path(result) == profile_path
    assert external_file.read_bytes() == b"keep me"
    assert not profile_path.is_symlink()
    assert profile_path.read_bytes() == b"new picture"
