import errno
import os
import stat
import time
from pathlib import Path
from threading import Event, Thread
from unittest import mock

import pytest
from keyrings.alt.file import PlaintextKeyring

import maestral.manager as manager_module
from maestral.constants import (
    FILE_CACHE,
    MIGNORE_FILE,
    OLD_REV_FILE,
    ROOT_MARKER_FILE,
)
from maestral.core import AccountType, FullAccount, TeamRootInfo, UserRootInfo
from maestral.exceptions import CancelledError, MaestralApiError, NoDropboxDirError
from maestral.main import Maestral
from maestral.utils.appdirs import get_home_dir
from maestral.utils.path import delete, generate_cc_name


def fake_linked(m: Maestral, account_info: FullAccount) -> None:
    m.client.get_account_info = mock.Mock(return_value=account_info)  # type: ignore
    m.cred_storage.set_keyring_backend(PlaintextKeyring())
    m.cred_storage.save_creds("account_id", "1234", allow_plaintext=True)


def reserve_unlink_reset(m: Maestral) -> None:
    stop_state = m.manager.pause_for_internal_operation()
    try:
        m.manager.begin_unlink_reset(
            stop_state,
            provider="dropbox",
            account_id="old-account",
            keyring="automatic",
            sync_mode="mirror",
            root_path="/old/dropbox",
            source_root_path="/old/dropbox",
            source_root_identity=None,
            root_marker_id="a" * 32,
            native_registration_committed=False,
        )
    finally:
        m.manager._finish_internal_operation(stop_state)


def verify_folder_structure(root: str, structure: dict) -> None:
    for name, children in structure.items():
        path = os.path.join(root, name)
        assert os.path.exists(path)

        verify_folder_structure(path, children)


def create_folder_structure(root: str, structure: dict) -> None:
    for name, children in structure.items():
        path = os.path.join(root, name)
        os.makedirs(path)

        create_folder_structure(path, children)


account_info = FullAccount(
    account_id="",
    display_name="",
    email="",
    profile_photo_url="",
    email_verified=False,
    disabled=False,
    country=None,
    locale="",
    team=None,
    team_member_id=None,
    account_type=AccountType.Business,
    root_info=UserRootInfo("", ""),
)


def test_migrate_path_root_user_to_team(m: Maestral) -> None:
    new_namespace_id = "2"
    home_path = "/John Doe"

    # patch client and sync engine

    account_info.root_info = TeamRootInfo(
        root_namespace_id=new_namespace_id,
        home_namespace_id="1",
        home_path=home_path,
    )

    fake_linked(m, account_info)

    home = get_home_dir()
    local_dropbox_dir = generate_cc_name(home + "/Dropbox", suffix="test runner")
    os.makedirs(local_dropbox_dir)

    try:
        m.sync.dropbox_path = local_dropbox_dir
        m.sync.create_root_marker()

        m.set_state("account", "path_root_type", "user")
        m.set_state("account", "path_root_nsid", "1")
        m.set_state("account", "home_path", "")

        # define folder structures before and after migration

        dir_layout_old = {
            "Documents": {},
            "Photos": {
                "March 2019": {},
            },
            "John Doe": {},
            "Personal": {},
        }

        dir_layout_new = {
            "John Doe": {
                "Documents": {},
                "Photos": {
                    "March 2019": {},
                },
                "John Doe": {},
                "Personal": {},
            }
        }

        # create folder structure before migration

        create_folder_structure(local_dropbox_dir, dir_layout_old)

        # migrate folder structure and verify migration

        m.manager.check_and_update_path_root()

        verify_folder_structure(local_dropbox_dir, dir_layout_new)
        assert os.path.isfile(os.path.join(local_dropbox_dir, ROOT_MARKER_FILE))

        assert m.get_state("account", "path_root_type") == "team"
        assert m.get_state("account", "path_root_nsid") == new_namespace_id
        assert m.get_state("account", "home_path") == home_path

    finally:
        delete(local_dropbox_dir)


def test_migrate_path_root_team_to_user(m: Maestral) -> None:
    new_namespace_id = "1"

    # patch client and sync engine

    account_info.root_info = UserRootInfo(
        root_namespace_id=new_namespace_id,
        home_namespace_id=new_namespace_id,
    )

    fake_linked(m, account_info)

    home = get_home_dir()
    local_dropbox_dir = generate_cc_name(home + "/Dropbox", suffix="test runner")
    os.makedirs(local_dropbox_dir)

    try:
        m.sync.dropbox_path = local_dropbox_dir
        m.sync.create_root_marker()

        m.set_state("account", "path_root_type", "team")
        m.set_state("account", "path_root_nsid", "2")
        m.set_state("account", "home_path", "/JOHN DOE")

        # define folder structures before and after migration

        dir_layout_old = {
            "John Doe": {
                "Documents": {},
                "Photos": {
                    "March 2019": {},
                },
                "John Doe": {},
                "Personal": {},
            },
            "Team folder 1": {},
            "Team folder 2": {
                "Subfolder": {},
            },
        }

        dir_layout_new = {
            "Documents": {},
            "Photos": {
                "March 2019": {},
            },
            "John Doe": {},
            "Personal": {},
        }

        # create folder structure before migration

        create_folder_structure(local_dropbox_dir, dir_layout_old)
        for file_name in (MIGNORE_FILE, OLD_REV_FILE):
            open(os.path.join(local_dropbox_dir, file_name), "w").close()
        os.mkdir(os.path.join(local_dropbox_dir, FILE_CACHE))

        # migrate folder structure and verify migration

        m.manager.check_and_update_path_root()

        verify_folder_structure(local_dropbox_dir, dir_layout_new)
        assert os.path.isfile(os.path.join(local_dropbox_dir, MIGNORE_FILE))
        assert os.path.isfile(os.path.join(local_dropbox_dir, OLD_REV_FILE))
        assert os.path.isdir(os.path.join(local_dropbox_dir, FILE_CACHE))
        assert os.path.isfile(os.path.join(local_dropbox_dir, ROOT_MARKER_FILE))

        assert m.get_state("account", "path_root_type") == "user"
        assert m.get_state("account", "path_root_nsid") == new_namespace_id
        assert m.get_state("account", "home_path") == ""

    finally:
        delete(local_dropbox_dir)


def test_migrate_path_root_team_to_team(m: Maestral) -> None:
    new_namespace_id = "3"

    # patch client and sync engine

    account_info.root_info = TeamRootInfo(
        root_namespace_id=new_namespace_id,
        home_namespace_id="1",
        home_path="/John Doe",
    )

    fake_linked(m, account_info)

    home = get_home_dir()
    local_dropbox_dir = generate_cc_name(home + "/Dropbox", suffix="test runner")
    os.makedirs(local_dropbox_dir)

    try:
        m.sync.dropbox_path = local_dropbox_dir
        m.sync.create_root_marker()

        m.set_state("account", "path_root_type", "team")
        m.set_state("account", "path_root_nsid", "2")
        m.set_state("account", "home_path", "/John Doe")

        # define folder structures before and after migration

        dir_layout_old = {
            "John Doe": {
                "Documents": {},
                "Photos": {
                    "March 2019": {},
                },
                "John Doe": {},
                "Personal": {},
            },
            "Team folder 1": {},
            "Team folder 2": {
                "Subfolder": {},
            },
        }

        dir_layout_new = {
            "John Doe": {
                "Documents": {},
                "Photos": {
                    "March 2019": {},
                },
                "John Doe": {},
                "Personal": {},
            },
        }

        # create folder structure before migration

        create_folder_structure(local_dropbox_dir, dir_layout_old)

        # migrate folder structure and verify migration

        m.manager.check_and_update_path_root()

        verify_folder_structure(local_dropbox_dir, dir_layout_new)
        assert os.path.isfile(os.path.join(local_dropbox_dir, ROOT_MARKER_FILE))

        assert m.get_state("account", "path_root_type") == "team"
        assert m.get_state("account", "path_root_nsid") == new_namespace_id
        assert m.get_state("account", "home_path") == "/John Doe"

    finally:
        delete(local_dropbox_dir)


def test_migrate_path_root_error(m: Maestral) -> None:
    new_namespace_id = "2"
    home_path = "/John Doe"

    # patch client and sync engine

    account_info.root_info = TeamRootInfo(
        root_namespace_id=new_namespace_id,
        home_namespace_id="1",
        home_path=home_path,
    )

    fake_linked(m, account_info)

    home = get_home_dir()
    local_dropbox_dir = generate_cc_name(home + "/Dropbox", suffix="test runner")

    m.sync.dropbox_path = local_dropbox_dir

    m.set_state("account", "path_root_type", "user")
    m.set_state("account", "path_root_nsid", "1")
    m.set_state("account", "home_path", "")

    # attempt to migrate folder structure without Dropbox dir

    with pytest.raises(NoDropboxDirError):
        m.manager.check_and_update_path_root()


def test_cancelled_selective_sync_download_is_requeued(m: Maestral) -> None:
    dbx_path = "/newly-included"
    running = Event()
    startup_completed = Event()
    autostart = Event()
    running.set()
    startup_completed.set()

    m.manager.download_queue.put(dbx_path)
    m.sync.get_remote_item = mock.Mock(  # type: ignore
        side_effect=CancelledError("Sync cancelled")
    )

    m.manager.download_worker_added_item(running, startup_completed, autostart)

    assert not running.is_set()
    assert m.manager.download_queue.qsize() == 1
    assert m.manager.download_queue.get() == dbx_path
    m.manager.download_queue.task_done(dbx_path)


def test_persistent_queue_put_recovers_from_save_failure(
    m: Maestral, monkeypatch: pytest.MonkeyPatch
) -> None:
    queue = m.manager.download_queue
    dbx_path = "/save-failed-put"
    original_save = queue._persistent._conf.save
    monkeypatch.setattr(
        queue._persistent._conf,
        "save",
        mock.Mock(side_effect=OSError("injected save failure")),
    )

    with pytest.raises(OSError, match="injected save failure"):
        queue.put(dbx_path)

    assert dbx_path not in queue
    assert queue.qsize() == 0

    monkeypatch.setattr(queue._persistent._conf, "save", original_save)
    queue.put(dbx_path)
    assert queue.get() == dbx_path
    queue.task_done(dbx_path)


def test_persistent_queue_completion_requeues_after_save_failure(
    m: Maestral, monkeypatch: pytest.MonkeyPatch
) -> None:
    queue = m.manager.download_queue
    dbx_path = "/save-failed-completion"
    queue.put(dbx_path)
    assert queue.get() == dbx_path
    original_save = queue._persistent._conf.save
    monkeypatch.setattr(
        queue._persistent._conf,
        "save",
        mock.Mock(side_effect=OSError("injected save failure")),
    )

    with pytest.raises(OSError, match="injected save failure"):
        queue.task_done(dbx_path)

    assert dbx_path in queue
    assert queue.qsize() == 1

    monkeypatch.setattr(queue._persistent._conf, "save", original_save)
    assert queue.get() == dbx_path
    queue.task_done(dbx_path)


def test_sync_reset_preserves_targeted_downloads_only(m: Maestral) -> None:
    targeted_path = "/targeted"
    ordinary_path = "/ordinary"
    m.sync.queue_targeted_download(targeted_path, "restore")
    m.manager.download_queue.put(ordinary_path)

    m.manager.reset_sync_state()

    assert m.sync._validated_download_intents() == {targeted_path: "restore"}
    assert m.get_state("sync", "pending_downloads") == [targeted_path]
    assert targeted_path in m.manager.download_queue
    assert ordinary_path not in m.manager.download_queue
    assert m.get_state("recovery", "sync_reset") == {}


def test_sync_reset_replays_after_precommit_state_failure(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    m.manager.autostart.set()
    m.sync._begin_sync_reset("sync")
    clear_database = mock.Mock(wraps=m.sync._clear_sync_database)
    monkeypatch.setattr(m.sync, "_clear_sync_database", clear_database)
    original_save = m.manager._state.save
    monkeypatch.setattr(
        m.manager._state,
        "save",
        mock.Mock(side_effect=OSError("before reset state commit")),
    )

    with pytest.raises(OSError, match="before reset state commit"):
        m.manager.reset_sync_state()

    assert clear_database.call_count == 1
    assert m.get_state("recovery", "sync_reset")["phase"] == "pending"
    assert not m.manager.autostart.is_set()

    monkeypatch.setattr(m.manager._state, "save", original_save)
    m.manager.reset_sync_state()

    assert clear_database.call_count == 2
    assert m.get_state("recovery", "sync_reset") == {}


def test_sync_reset_reloads_queue_after_postcommit_state_interrupt(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dbx_path = "/restore"
    m.sync.queue_targeted_download(dbx_path, "restore")
    m.sync._begin_sync_reset("sync")
    original_save = m.manager._state.save
    interrupted = False

    def save_then_interrupt() -> None:
        nonlocal interrupted
        original_save()
        journal = m.get_state("recovery", "sync_reset")
        if not interrupted and journal.get("phase") == "queue":
            interrupted = True
            raise KeyboardInterrupt("after reset state commit")

    monkeypatch.setattr(m.manager._state, "save", save_then_interrupt)

    with pytest.raises(KeyboardInterrupt, match="after reset state commit"):
        m.manager.reset_sync_state()

    assert m.get_state("recovery", "sync_reset")["phase"] == "queue"
    monkeypatch.setattr(m.manager._state, "save", original_save)
    m.manager.reload_download_queue()

    assert interrupted
    assert dbx_path in m.manager.download_queue
    assert m.get_state("recovery", "sync_reset") == {}


def test_sync_reset_keeps_marker_when_queue_reload_fails(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dbx_path = "/persisted"
    m._state.set("sync", "pending_downloads", [dbx_path])
    m._state.set("recovery", "sync_reset", {"kind": "sync", "phase": "queue"})
    original_reload = m.manager.download_queue.reload_persisted
    monkeypatch.setattr(
        m.manager.download_queue,
        "reload_persisted",
        mock.Mock(side_effect=OSError("queue reload failed")),
    )

    with pytest.raises(OSError, match="queue reload failed"):
        m.manager.reload_download_queue()

    assert m.get_state("recovery", "sync_reset")["phase"] == "queue"

    monkeypatch.setattr(
        m.manager.download_queue,
        "reload_persisted",
        original_reload,
    )
    m.manager.reload_download_queue()

    assert dbx_path in m.manager.download_queue
    assert m.get_state("recovery", "sync_reset") == {}


def test_sync_reset_restores_autostart_after_marker_clear_postcommit(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    m.manager.autostart.set()
    m._state.set("recovery", "sync_reset", {"kind": "sync", "phase": "queue"})
    original_save = m.manager._state.save
    interrupted = False

    def save_then_interrupt() -> None:
        nonlocal interrupted
        original_save()
        if not interrupted and m.get_state("recovery", "sync_reset") == {}:
            interrupted = True
            raise KeyboardInterrupt("after reset marker clear")

    monkeypatch.setattr(m.manager._state, "save", save_then_interrupt)

    with pytest.raises(KeyboardInterrupt, match="after reset marker clear"):
        m.manager.reset_sync_state()

    assert interrupted
    assert m.get_state("recovery", "sync_reset") == {}
    assert m.manager.autostart.is_set()


def test_manager_start_replays_pending_sync_reset(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    m.sync.dropbox_path = str(dropbox_path)
    m.sync.create_root_marker()
    m.sync.queue_targeted_download("/restore", "restore")
    m.sync._begin_sync_reset("sync")
    reload_queue = mock.Mock(wraps=m.manager.download_queue.reload_persisted)
    monkeypatch.setattr(m.manager.download_queue, "reload_persisted", reload_queue)
    monkeypatch.setattr(
        manager_module, "check_connection", mock.Mock(return_value=False)
    )

    m.manager.start()

    reload_queue.assert_called_once_with()
    assert m.get_state("recovery", "sync_reset") == {}
    assert "/restore" in m.manager.download_queue
    assert m.manager.autostart.is_set()
    assert not m.manager.running.is_set()


def test_persistent_queue_insert_postcommit_exception_keeps_live_item(
    m: Maestral, monkeypatch: pytest.MonkeyPatch
) -> None:
    queue = m.manager.download_queue
    dbx_path = "/committed-ordinary-insert"
    original_save = queue._persistent._conf.save

    def save_then_interrupt() -> int:
        original_save()
        raise KeyboardInterrupt("after queue commit")

    monkeypatch.setattr(queue._persistent._conf, "save", save_then_interrupt)

    with pytest.raises(KeyboardInterrupt, match="after queue commit"):
        queue.put(dbx_path)

    assert dbx_path in queue
    assert queue.qsize() == 1


def test_targeted_queue_insert_postcommit_base_exception_keeps_new_state(
    m: Maestral, monkeypatch: pytest.MonkeyPatch
) -> None:
    dbx_path = "/committed-insert"
    original_save = m.manager._state.save

    def save_then_interrupt() -> int:
        original_save()
        raise KeyboardInterrupt("after queue commit")

    monkeypatch.setattr(m.manager._state, "save", save_then_interrupt)

    with pytest.raises(KeyboardInterrupt, match="after queue commit"):
        m.sync.queue_targeted_download(dbx_path, "include")

    assert m.sync._validated_download_intents() == {dbx_path: "include"}
    assert m.get_state("sync", "pending_downloads") == [dbx_path]
    assert dbx_path in m.manager.download_queue
    assert m.manager.download_queue.qsize() == 1


def test_targeted_queue_completion_postcommit_base_exception_keeps_new_state(
    m: Maestral, monkeypatch: pytest.MonkeyPatch
) -> None:
    dbx_path = "/committed-completion"
    queue = m.manager.download_queue
    m.sync.queue_targeted_download(dbx_path, "include")
    assert queue.get() == dbx_path
    original_save = m.manager._state.save

    def save_then_interrupt() -> int:
        original_save()
        raise KeyboardInterrupt("after queue commit")

    monkeypatch.setattr(m.manager._state, "save", save_then_interrupt)

    with pytest.raises(KeyboardInterrupt, match="after queue commit"):
        queue.task_done(dbx_path, m.sync.finish_targeted_download)

    assert m.sync._validated_download_intents() == {}
    assert m.get_state("sync", "pending_downloads") == []
    assert dbx_path not in queue
    assert queue.qsize() == 0


def test_path_root_recovery_postcommit_base_exception_keeps_new_state(
    m: Maestral, monkeypatch: pytest.MonkeyPatch
) -> None:
    m.manager._state.set(
        "recovery",
        "local_paths",
        {
            "/old": {
                "path": "/Old",
                "identity": [1, 2, 3],
                "phase": "tracked",
                "source": "",
            }
        },
        save=False,
    )
    m.manager._state.set(
        "recovery",
        "download_intents",
        {"/old": "restore"},
        save=False,
    )
    migration = {
        "old_recovery_local_paths": {
            "/old": {
                "path": "/Old",
                "identity": [1, 2, 3],
                "phase": "tracked",
                "source": "",
            }
        },
        "new_recovery_local_paths": {
            "/new": {
                "path": "/New",
                "identity": [1, 2, 3],
                "phase": "tracked",
                "source": "",
            }
        },
        "old_download_intents": {"/old": "restore"},
        "new_download_intents": {"/new": "include"},
    }
    original_save = m.manager._state.save

    def save_then_interrupt() -> int:
        original_save()
        raise KeyboardInterrupt("after recovery commit")

    monkeypatch.setattr(m.manager._state, "save", save_then_interrupt)

    with pytest.raises(KeyboardInterrupt, match="after recovery commit"):
        m.manager._set_path_root_recovery_state(migration, use_new=True)

    assert m.sync._validated_recovered_local_paths() == {
        "/new": {
            "path": "/New",
            "identity": [1, 2, 3],
            "phase": "tracked",
            "source": "",
        }
    }
    assert m.sync._validated_download_intents() == {"/new": "include"}


def test_persistent_queue_keeps_request_added_during_active_work(
    m: Maestral,
) -> None:
    queue = m.manager.download_queue
    dbx_path = "/requested-again"
    queue.put(dbx_path)
    assert queue.get() == dbx_path

    queue.put(dbx_path)
    queue.task_done(dbx_path)

    assert dbx_path in queue
    assert queue.qsize() == 1
    assert queue.get() == dbx_path
    queue.task_done(dbx_path)
    assert dbx_path not in queue


def test_persistent_queue_keeps_request_during_dequeue_handoff(
    m: Maestral,
) -> None:
    queue = m.manager.download_queue
    dbx_path = "/requested-during-handoff"
    queue.put(dbx_path)
    dequeued: list[str] = []

    with queue._lock:
        getter = Thread(target=lambda: dequeued.append(queue.get()))
        getter.start()
        deadline = time.monotonic() + 2
        while queue._queue.qsize() > 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert queue._queue.qsize() == 0
        queue.put(dbx_path)

    getter.join(timeout=2)
    assert dequeued == [dbx_path]
    queue.task_done(dbx_path)

    assert queue.qsize() == 1
    assert queue.get() == dbx_path
    queue.task_done(dbx_path)


def test_newer_targeted_intent_survives_in_flight_completion(
    m: Maestral,
) -> None:
    manager = m.manager
    manager._connection_helper_stop.set()
    manager.connection_helper.join(timeout=2)
    dbx_path = "/requested-again-as-restore"
    running = manager.running
    startup_completed = manager.startup_completed
    autostart = manager.autostart
    download_started = Event()
    finish_download = Event()
    running.set()
    startup_completed.set()
    autostart.set()
    m.sync.queue_targeted_download(dbx_path, "include")

    def complete_old_download(path: str) -> bool:
        assert path == dbx_path
        download_started.set()
        finish_download.wait(timeout=2)
        return True

    def finish_rescan(path: str) -> bool:
        assert path == dbx_path
        running.clear()
        return True

    m.sync.get_remote_item = mock.Mock(  # type: ignore[method-assign]
        side_effect=complete_old_download
    )
    m.sync.rescan_dbx_path = mock.Mock(  # type: ignore[method-assign]
        side_effect=finish_rescan
    )
    worker = Thread(
        target=manager.download_worker_added_item,
        args=(running, startup_completed, autostart),
    )
    worker.start()

    try:
        assert download_started.wait(timeout=2)
        m.sync.queue_targeted_download(dbx_path, "restore")
    finally:
        finish_download.set()
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert m.sync._validated_download_intents() == {dbx_path: "restore"}
    assert manager.download_queue.qsize() == 1
    assert dbx_path in manager.download_queue


@pytest.mark.parametrize("worker_name", ["active", "startup"])
def test_targeted_rescan_estale_requeues_without_clearing_autostart(
    m: Maestral,
    tmp_path: Path,
    worker_name: str,
) -> None:
    manager = m.manager
    manager._connection_helper_stop.set()
    manager.connection_helper.join(timeout=2)
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    m.sync.dropbox_path = str(dropbox_path)
    m.sync.create_root_marker()
    local_path = dropbox_path / "stale.txt"
    local_path.write_text("stale")
    dbx_path = "/stale.txt"
    running = manager.running
    startup_completed = manager.startup_completed
    autostart = manager.autostart
    running.set()
    startup_completed.set()
    autostart.set()
    m.sync.queue_targeted_download(dbx_path, "include")
    m.sync.get_remote_item = mock.Mock(return_value=True)  # type: ignore[method-assign]
    m.sync._rescan_unindexed = mock.Mock(  # type: ignore[method-assign]
        side_effect=OSError(errno.ESTALE, "injected stale path")
    )

    if worker_name == "active":
        manager._wait_for_download_retry = mock.Mock(  # type: ignore[method-assign]
            side_effect=lambda _: running.clear() or False
        )
        manager.download_worker_added_item(
            running,
            startup_completed,
            autostart,
        )
    else:
        m.sync.client.get_space_usage = mock.Mock()  # type: ignore[method-assign]
        manager.check_and_update_path_root = mock.Mock(  # type: ignore[method-assign]
            return_value=False
        )
        m.sync.download_sync_cycle = mock.Mock(  # type: ignore[method-assign]
            side_effect=running.clear
        )
        manager.startup_worker(
            running,
            startup_completed,
            autostart,
        )

    assert autostart.is_set()
    assert m.sync._validated_download_intents() == {dbx_path: "include"}
    assert manager.download_queue.qsize() == 1
    assert dbx_path in manager.download_queue


def test_dropbox_path_save_failure_preserves_cached_state(
    m: Maestral,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = m.manager
    manager._connection_helper_stop.set()
    manager.connection_helper.join(timeout=2)
    old_path = tmp_path / "old-dropbox"
    old_path.mkdir()
    m.sync.dropbox_path = str(old_path)
    m.sync.create_root_marker()
    m.sync.ensure_dropbox_folder_present()
    old_cached_state = (
        m.sync._dropbox_path,
        m.sync._mignore_path,
        m.sync._file_cache_path,
        m.sync._confirmed_root_identity,
        m.sync._is_fs_case_sensitive,
    )
    check_case_sensitivity = mock.Mock(return_value=not m.sync._is_fs_case_sensitive)
    monkeypatch.setattr(m.sync, "_check_fs_case_sensitive", check_case_sensitivity)
    monkeypatch.setattr(
        m.sync._conf,
        "save",
        mock.Mock(side_effect=OSError("injected save failure")),
    )

    with pytest.raises(OSError, match="injected save failure"):
        m.sync.dropbox_path = str(tmp_path / "new-dropbox")

    assert (
        m.sync._dropbox_path,
        m.sync._mignore_path,
        m.sync._file_cache_path,
        m.sync._confirmed_root_identity,
        m.sync._is_fs_case_sensitive,
    ) == old_cached_state
    assert m.sync._conf.get("sync", "path") == str(old_path)


def test_unlink_reservation_refuses_a_restarted_manager(m: Maestral) -> None:
    stop_state = m.manager.pause_for_internal_operation()
    m.manager.running.set()
    try:
        with pytest.raises(MaestralApiError, match="Sync started again"):
            m.manager.begin_unlink_reset(
                stop_state,
                provider="dropbox",
                account_id="old-account",
                keyring="automatic",
                sync_mode="mirror",
                root_path="/old/dropbox",
                source_root_path="/old/dropbox",
                source_root_identity=None,
                root_marker_id="a" * 32,
                native_registration_committed=False,
            )
    finally:
        m.manager.running.clear()
        m.manager._finish_internal_operation(stop_state)

    assert m._state.get("recovery", "sync_reset") == {}


def test_unlink_reservation_blocks_a_later_start(m: Maestral) -> None:
    reserve_unlink_reset(m)

    with pytest.raises(MaestralApiError, match="pending reset"):
        m.manager.start()


@pytest.mark.parametrize("targeted", [False, True])
def test_unlink_reservation_blocks_late_download_publication(
    m: Maestral,
    targeted: bool,
) -> None:
    dbx_path = "/old-account-item"
    reserve_unlink_reset(m)

    with pytest.raises(CancelledError, match="unlink"):
        if targeted:
            m.sync.queue_targeted_download(dbx_path, "include")
        else:
            assert m.sync.download_callback is not None
            m.sync.download_callback(dbx_path)

    assert m.sync._validated_download_intents() == {}
    assert m.get_state("sync", "pending_downloads") == []
    assert dbx_path not in m.manager.download_queue


def test_selective_sync_rechecks_account_gate_after_unlink_reservation(
    m: Maestral,
) -> None:
    initial_check_complete = Event()
    unlink_reserved = Event()
    checks = 0
    errors: list[BaseException] = []

    def check_linked() -> None:
        nonlocal checks
        checks += 1
        if checks == 1:
            initial_check_complete.set()
            unlink_reserved.wait(5)

    m._check_linked = mock.Mock(side_effect=check_linked)  # type: ignore[method-assign]
    m._check_dropbox_dir = mock.Mock()  # type: ignore[method-assign]

    def change_selection() -> None:
        try:
            m.set_selective_sync("include", ["/old-account-item"])
        except BaseException as exc:
            errors.append(exc)

    thread = Thread(target=change_selection)
    thread.start()
    assert initial_check_complete.wait(5)
    reserve_unlink_reset(m)
    unlink_reserved.set()
    thread.join(5)

    assert not thread.is_alive()
    assert checks == 2
    assert len(errors) == 1
    assert isinstance(errors[0], CancelledError)
    assert m.sync.selective_sync_mode == "exclude"
    assert m.sync.selective_sync_paths == set()
    assert m.sync._validated_download_intents() == {}
    assert m.get_state("sync", "pending_downloads") == []


def test_normal_reset_holds_sync_gate_through_queue_replay(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    m.manager._connection_helper_stop.set()
    m.manager.connection_helper.join(timeout=2)
    reset_started = Event()
    finish_reset = Event()
    errors: list[BaseException] = []
    original_reset = m.sync.reset_sync_state

    def paused_reset() -> None:
        reset_started.set()
        finish_reset.wait(5)
        original_reset()

    monkeypatch.setattr(m.sync, "reset_sync_state", paused_reset)
    m._check_linked = mock.Mock()  # type: ignore[method-assign]
    m._check_dropbox_dir = mock.Mock()  # type: ignore[method-assign]

    def reset() -> None:
        try:
            m.manager.reset_sync_state()
        except BaseException as exc:
            errors.append(exc)

    thread = Thread(target=reset)
    thread.start()
    assert reset_started.wait(5)

    with pytest.raises(MaestralApiError, match="idle"):
        m.set_selective_sync("include", ["/new-selection"])

    finish_reset.set()
    thread.join(5)

    assert not thread.is_alive()
    assert errors == []
    assert m.sync.selective_sync_mode == "exclude"
    assert m.sync.selective_sync_paths == set()
    assert m.get_state("recovery", "sync_reset") == {}


@pytest.mark.parametrize("operation", ["stop", "worker-stop"])
def test_stop_waits_for_sync_gate_without_holding_manager_lock(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    m.manager._connection_helper_stop.set()
    m.manager.connection_helper.join(timeout=2)
    cancel_requested = Event()
    errors: list[BaseException] = []
    original_request_cancel = m.sync.request_cancel

    def request_cancel() -> None:
        original_request_cancel()
        cancel_requested.set()

    monkeypatch.setattr(m.sync, "request_cancel", request_cancel)
    m.manager.running.set()
    running = m.manager.running

    def stop() -> None:
        try:
            if operation == "stop":
                m.manager.stop()
            else:
                m.manager._signal_worker_stop(
                    running,
                    m.manager.autostart,
                    restart=False,
                )
        except BaseException as exc:
            errors.append(exc)

    with m.sync.sync_lock:
        thread = Thread(target=stop)
        thread.start()
        assert cancel_requested.wait(5)
        manager_lock_was_free = m.manager._lock.acquire(blocking=False)
        if manager_lock_was_free:
            m.manager._lock.release()

    thread.join(5)

    assert manager_lock_was_free
    assert not thread.is_alive()
    assert errors == []


def test_migration_owned_reset_finishes_while_stop_waits_for_sync_gate(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    m.manager._connection_helper_stop.set()
    m.manager.connection_helper.join(timeout=2)
    sync_gate_held = Event()
    begin_reset = Event()
    cancel_requested = Event()
    reset_finished = Event()
    errors: list[BaseException] = []
    original_request_cancel = m.sync.request_cancel

    def request_cancel() -> None:
        original_request_cancel()
        cancel_requested.set()

    monkeypatch.setattr(m.sync, "request_cancel", request_cancel)
    m.manager.running.set()

    def migrate_and_reset() -> None:
        try:
            with m.sync.sync_lock:
                sync_gate_held.set()
                begin_reset.wait(5)
                m.manager.reset_sync_state()
                reset_finished.set()
        except BaseException as exc:
            errors.append(exc)

    migration_thread = Thread(target=migrate_and_reset)
    migration_thread.start()
    assert sync_gate_held.wait(5)
    stop_thread = Thread(target=m.manager.stop)
    stop_thread.start()
    assert cancel_requested.wait(5)
    begin_reset.set()
    completed_without_cycle = reset_finished.wait(5)

    if not completed_without_cycle:
        with m.manager._lock:
            m.manager._stopping = False
            m.manager._stop_condition.notify_all()

    migration_thread.join(5)
    stop_thread.join(5)

    assert completed_without_cycle
    assert not migration_thread.is_alive()
    assert not stop_thread.is_alive()
    assert errors == []


@pytest.mark.parametrize("operation", ["rebuild", "path-root"])
def test_later_user_stop_cancels_internal_restart(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    m.manager._connection_helper_stop.set()
    m.manager.connection_helper.join(timeout=2)
    operation_started = Event()
    finish_operation = Event()
    errors: list[BaseException] = []
    m.manager.running.set()
    m.manager.autostart.set()
    m.manager.start = mock.Mock()  # type: ignore[method-assign]
    m.sync.ensure_dropbox_folder_present = mock.Mock()  # type: ignore[method-assign]
    m.manager._needs_path_root_update = mock.Mock(  # type: ignore[method-assign]
        return_value=True
    )

    def pause_operation() -> None:
        operation_started.set()
        finish_operation.wait(5)

    if operation == "rebuild":
        m.manager.reset_sync_state = mock.Mock(  # type: ignore[method-assign]
            side_effect=pause_operation
        )
        target = m.manager.rebuild_index
    else:
        m.manager._update_path_root = mock.Mock(  # type: ignore[method-assign]
            side_effect=pause_operation
        )
        target = m.manager.check_and_update_path_root

    def run_operation() -> None:
        try:
            target()
        except BaseException as exc:
            errors.append(exc)

    thread = Thread(target=run_operation)
    thread.start()
    assert operation_started.wait(5)
    m.manager.stop()
    finish_operation.set()
    thread.join(5)

    assert not thread.is_alive()
    assert errors == []
    m.manager.start.assert_not_called()
    assert not m.manager.autostart.is_set()


@pytest.mark.parametrize("operation", ["rebuild", "path-root"])
def test_user_stop_before_internal_pause_cancels_restart(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    m.manager._connection_helper_stop.set()
    m.manager.connection_helper.join(timeout=2)
    pause_called = Event()
    user_stop_finished = Event()
    errors: list[BaseException] = []
    m.manager.running.set()
    m.manager.autostart.set()
    m.manager.start = mock.Mock()  # type: ignore[method-assign]
    m.sync.ensure_dropbox_folder_present = mock.Mock()  # type: ignore[method-assign]
    m.manager._needs_path_root_update = mock.Mock(  # type: ignore[method-assign]
        return_value=True
    )
    m.manager.reset_sync_state = mock.Mock()  # type: ignore[method-assign]
    m.manager._update_path_root = mock.Mock()  # type: ignore[method-assign]
    original_pause = m.manager.pause_for_internal_operation

    def delayed_pause():
        pause_called.set()
        user_stop_finished.wait(5)
        return original_pause()

    monkeypatch.setattr(m.manager, "pause_for_internal_operation", delayed_pause)
    target = (
        m.manager.rebuild_index
        if operation == "rebuild"
        else m.manager.check_and_update_path_root
    )

    def run_operation() -> None:
        try:
            target()
        except BaseException as exc:
            errors.append(exc)

    thread = Thread(target=run_operation)
    thread.start()
    assert pause_called.wait(5)
    m.manager.stop()
    user_stop_finished.set()
    thread.join(5)

    assert not thread.is_alive()
    assert errors == []
    m.manager.start.assert_not_called()
    assert not m.manager.autostart.is_set()


def test_path_root_migration_reservation_blocks_a_concurrent_start(
    m: Maestral,
) -> None:
    m.manager._connection_helper_stop.set()
    m.manager.connection_helper.join(timeout=2)
    migration_started = Event()
    finish_migration = Event()
    errors: list[BaseException] = []
    m.sync.ensure_dropbox_folder_present = mock.Mock()  # type: ignore[method-assign]
    m.manager._needs_path_root_update = mock.Mock(  # type: ignore[method-assign]
        return_value=True
    )

    def update_path_root() -> None:
        migration_started.set()
        finish_migration.wait(5)

    m.manager._update_path_root = mock.Mock(  # type: ignore[method-assign]
        side_effect=update_path_root
    )

    def run_migration() -> None:
        try:
            m.manager.check_and_update_path_root()
        except BaseException as exc:
            errors.append(exc)

    thread = Thread(target=run_migration)
    thread.start()
    assert migration_started.wait(5)

    with pytest.raises(MaestralApiError, match="internal operation"):
        m.manager.start()

    finish_migration.set()
    thread.join(5)

    assert not thread.is_alive()
    assert errors == []


def test_path_root_worker_does_not_wait_on_a_stop_which_joins_it(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    m.manager._connection_helper_stop.set()
    m.manager.connection_helper.join(timeout=2)
    stop_started = Event()
    release_stop = Event()
    run_worker = Event()
    worker_errors: list[BaseException] = []
    stop_errors: list[BaseException] = []
    m.manager.running.set()
    m.sync.ensure_dropbox_folder_present = mock.Mock()  # type: ignore[method-assign]
    m.manager._needs_path_root_update = mock.Mock(  # type: ignore[method-assign]
        return_value=True
    )

    def cancel_sync() -> None:
        stop_started.set()
        release_stop.wait(5)

    monkeypatch.setattr(m.sync, "cancel_sync", cancel_sync)

    def run_path_root_worker() -> None:
        run_worker.wait(5)
        try:
            m.manager.check_and_update_path_root()
        except BaseException as exc:
            worker_errors.append(exc)

    worker = Thread(target=run_path_root_worker)
    m.manager.startup_thread = worker
    worker.start()

    def stop_manager() -> None:
        try:
            m.manager.stop()
        except BaseException as exc:
            stop_errors.append(exc)

    stop_thread = Thread(target=stop_manager)
    stop_thread.start()
    assert stop_started.wait(5)
    run_worker.set()
    worker.join(5)

    assert not worker.is_alive()
    assert len(worker_errors) == 1
    assert isinstance(worker_errors[0], CancelledError)

    release_stop.set()
    stop_thread.join(5)

    assert not stop_thread.is_alive()
    assert stop_errors == []


def test_stale_connection_restart_does_not_stop_an_internal_operation(
    m: Maestral,
) -> None:
    m.manager._connection_helper_stop.set()
    m.manager.connection_helper.join(timeout=2)
    stop_state = m.manager.pause_for_internal_operation()
    results: list[bool] = []
    m.manager._connection_helper_stop.clear()
    m.manager.autostart.set()

    thread = Thread(
        target=lambda: results.append(m.manager._stop_for_connection_restart())
    )
    with m.sync.sync_lock:
        thread.start()
        thread.join(2)
        assert not thread.is_alive()

    assert results == [False]
    assert not m.manager._stopping
    m.manager._finish_internal_operation(stop_state)


@pytest.mark.parametrize("operation", ["stop", "worker-stop"])
@pytest.mark.parametrize("failure", ["events", "observer"])
def test_stop_setup_failure_releases_the_stop_gate(
    m: Maestral,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    failure: str,
) -> None:
    error = RuntimeError("injected stop setup failure")
    observer = mock.Mock()
    if failure == "events":
        monkeypatch.setattr(
            m.sync.fs_events,
            "disable",
            mock.Mock(side_effect=error),
        )
    else:
        observer.stop.side_effect = error
        m.manager.local_observer_thread = observer

    m.manager.running.set()
    with pytest.raises(RuntimeError, match="injected stop setup failure"):
        if operation == "stop":
            m.manager.stop()
        else:
            m.manager._signal_worker_stop(
                m.manager.running,
                m.manager.autostart,
                restart=False,
            )

    assert not m.manager._stopping
    m.manager.local_observer_thread = None


def test_root_move_reservation_refuses_a_restarted_manager(m: Maestral) -> None:
    m.manager.running.set()
    try:
        with pytest.raises(MaestralApiError, match="Sync started again"):
            m.manager.begin_root_move(
                {
                    "old_path": "/old/dropbox",
                    "new_path": "/new/dropbox",
                    "identity": [1, 2, stat.S_IFDIR],
                    "phase": "planned",
                }
            )
    finally:
        m.manager.running.clear()

    assert m._state.get("recovery", "root_move") == {}


def test_root_move_reservation_blocks_a_later_start(m: Maestral) -> None:
    journal = {
        "old_path": "/old/dropbox",
        "new_path": "/new/dropbox",
        "identity": [1, 2, stat.S_IFDIR],
        "phase": "planned",
    }
    m.manager.begin_root_move(journal)

    with pytest.raises(MaestralApiError, match="folder move"):
        m.manager.start()
