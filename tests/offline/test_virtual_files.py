from __future__ import annotations

import threading
from pathlib import Path
from unittest.mock import Mock

import pytest

from maestral.config import MaestralConfig
from maestral.core import DeletedMetadata
from maestral.exceptions import (
    VirtualFileBusyError,
    VirtualFileNotFoundError,
    VirtualFileRevisionError,
    VirtualFilesUnsupportedError,
)
from maestral.main import Maestral
from maestral.rpc import JsonRpcDispatcher
from maestral.virtual_files import (
    HydrationState,
    VirtualFileController,
)

from .virtual_files_fakes import (
    FakeVirtualFileBackend,
    FakeVirtualProvider,
    make_folder,
)


def make_controller(
    config_name: str,
    tmp_path: Path,
    provider: FakeVirtualProvider,
    backend: FakeVirtualFileBackend,
) -> VirtualFileController:
    root = tmp_path / "virtual-root"
    root.mkdir(exist_ok=True)
    controller = VirtualFileController(
        config_name,
        provider,  # type: ignore[arg-type]
        backend,
        database_path=str(tmp_path / "virtual-files.db"),
        remote_polling=False,
    )
    controller.start(str(root))
    return controller


def test_hydration_state_persists_by_stable_provider_id(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    first = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("stable-1", "/Old Name.txt", "rev-1", b"one")
    first.reconcile_remote_batch([metadata], "cursor-1")
    backend.open("stable-1")

    moved = provider.add_file("stable-1", "/New Name.txt", "rev-1", b"one")
    first.reconcile_remote_batch(
        [
            DeletedMetadata(
                name="Old Name.txt",
                path_lower="/old name.txt",
                path_display="/Old Name.txt",
            ),
            moved,
        ],
        "cursor-2",
    )
    first.close()

    second = make_controller(config_name, tmp_path, provider, backend)
    try:
        status = second.get_status("stable-1")
        assert status["path"] == "/New Name.txt"
        assert status["hydration_state"] == "hydrated"
        assert status["materialized_revision"] == "rev-1"
        assert second.cursor == "cursor-2"
        assert backend.items["stable-1"].content == b"one"
    finally:
        second.close()


def test_pin_unpin_and_safe_eviction(config_name: str, tmp_path: Path) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    try:
        metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
        controller.reconcile_remote_change(metadata)

        pinned = controller.pin("file-1")
        assert pinned["pinned"] is True
        assert pinned["hydration_state"] == "hydrated"
        with pytest.raises(VirtualFileBusyError, match="pinned"):
            controller.evict("file-1")

        controller.unpin("file-1")
        backend.set_access_state("file-1", open_count=1)
        with pytest.raises(VirtualFileBusyError, match="open or has local changes"):
            controller.evict("file-1")

        backend.set_access_state("file-1", dirty=True)
        with pytest.raises(VirtualFileBusyError, match="open or has local changes"):
            controller.evict("file-1")

        backend.set_access_state("file-1")
        evicted = controller.evict("file-1")
        assert evicted["hydration_state"] == "online_only"
        assert backend.items["file-1"].content is None
    finally:
        controller.close()


def test_concurrent_open_requests_share_one_hydration(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    try:
        metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
        controller.reconcile_remote_change(metadata)
        provider.block_downloads()
        results: list[dict[str, object]] = []

        threads = [
            threading.Thread(target=lambda: results.append(backend.open("file-1")))
            for _ in range(2)
        ]
        for thread in threads:
            thread.start()
        assert provider.download_started.wait(2)
        provider.release_download.set()
        for thread in threads:
            thread.join(2)

        assert all(not thread.is_alive() for thread in threads)
        assert len(results) == 2
        assert provider.download_calls == [("file-1", "rev-1")]
        assert backend.items["file-1"].content == b"data"
    finally:
        controller.close()


def test_remote_revision_wins_hydration_race(config_name: str, tmp_path: Path) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    try:
        first = provider.add_file("file-1", "/file.txt", "rev-1", b"old")
        controller.reconcile_remote_change(first)
        provider.block_downloads()
        errors: list[BaseException] = []

        def open_file() -> None:
            try:
                backend.open("file-1")
            except BaseException as exc:
                errors.append(exc)

        thread = threading.Thread(target=open_file)
        thread.start()
        assert provider.download_started.wait(2)
        second = provider.add_file("file-1", "/file.txt", "rev-2", b"new")
        controller.reconcile_remote_change(second)
        provider.release_download.set()
        thread.join(2)

        assert not thread.is_alive()
        assert len(errors) == 1
        assert isinstance(errors[0], VirtualFileRevisionError)
        assert provider.download_calls == [("file-1", "rev-1")]
        assert backend.items["file-1"].content is None

        backend.open("file-1")

        assert provider.download_calls[-1] == ("file-1", "rev-2")
        assert backend.items["file-1"].content == b"new"
        assert controller.get_status("file-1")["revision"] == "rev-2"
    finally:
        controller.close()


def test_remote_deletion_cancels_in_flight_hydration(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    try:
        metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
        controller.reconcile_remote_change(metadata)
        provider.block_downloads()
        errors: list[BaseException] = []

        def open_file() -> None:
            try:
                backend.open("file-1")
            except BaseException as exc:
                errors.append(exc)

        thread = threading.Thread(target=open_file)
        thread.start()
        assert provider.download_started.wait(2)
        controller.reconcile_remote_change(
            DeletedMetadata(
                name="file.txt",
                path_lower="/file.txt",
                path_display="/file.txt",
            )
        )
        provider.release_download.set()
        thread.join(2)

        assert not thread.is_alive()
        assert len(errors) == 1
        assert isinstance(errors[0], VirtualFileNotFoundError)
        assert "file-1" not in backend.items
        with pytest.raises(VirtualFileNotFoundError):
            controller.get_status("file-1")
    finally:
        controller.close()


def test_remote_folder_move_and_deletion_reconcile_the_full_tree(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    try:
        controller.reconcile_remote_change(make_folder("folder-1", "/Folder"))
        child = provider.add_file("file-1", "/Folder/Child.txt", "rev-1", b"data")
        controller.reconcile_remote_change(child)
        backend.open("file-1")

        controller.reconcile_remote_change(make_folder("folder-1", "/Renamed"))

        child_status = controller.get_status("file-1")
        assert child_status["path"] == "/Renamed/Child.txt"
        assert child_status["hydration_state"] == "hydrated"
        assert backend.items["file-1"].descriptor.path == "/Renamed/Child.txt"

        controller.reconcile_remote_change(
            DeletedMetadata(
                name="Renamed",
                path_lower="/renamed",
                path_display="/Renamed",
            )
        )

        with pytest.raises(VirtualFileNotFoundError):
            controller.get_status("folder-1")
        with pytest.raises(VirtualFileNotFoundError):
            controller.get_status("file-1")
        assert backend.items == {}
    finally:
        controller.close()


def test_restart_resumes_interrupted_eviction(config_name: str, tmp_path: Path) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    first = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
    first.reconcile_remote_change(metadata)
    backend.open("file-1")
    with first._lock:
        record = first._get_store().get("file-1")
        assert record is not None
        first._get_store().put(
            first._copy_record(record, hydration_state=HydrationState.Evicting)
        )
    first.close()

    second = make_controller(config_name, tmp_path, provider, backend)
    try:
        status = second.get_status("file-1")
        assert status["hydration_state"] == "online_only"
        assert status["materialized_revision"] is None
        assert backend.items["file-1"].content is None
    finally:
        second.close()


def test_restart_retries_interrupted_pinned_hydration(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    first = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
    first.reconcile_remote_change(metadata)
    with first._lock:
        record = first._get_store().get("file-1")
        assert record is not None
        first._get_store().put(
            first._copy_record(
                record,
                hydration_state=HydrationState.Hydrating,
                pinned=1,
            )
        )
    first.close()

    second = make_controller(config_name, tmp_path, provider, backend)
    try:
        status = second.get_status("file-1")
        assert status["hydration_state"] == "hydrated"
        assert status["pinned"] is True
        assert backend.items["file-1"].content == b"data"
    finally:
        second.close()


def test_restart_finishes_folder_deletion_from_children_up(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    first = make_controller(config_name, tmp_path, provider, backend)
    first.reconcile_remote_change(make_folder("folder-1", "/Folder"))
    child = provider.add_file("file-1", "/Folder/Child.txt", "rev-1", b"data")
    first.reconcile_remote_change(child)
    with first._lock:
        records = first._get_store().all()
        first._get_store().records.update_many(
            [
                first._copy_record(
                    record,
                    hydration_state=HydrationState.Deleting,
                    generation=record.generation + 1,
                )
                for record in records
            ]
        )
    first.close()
    backend.require_empty_directories = True

    second = make_controller(config_name, tmp_path, provider, backend)
    try:
        assert second.list_status() == []
        assert backend.items == {}
        remove_calls = [call for call in backend.calls if call[0] == "remove"]
        assert remove_calls[-2:] == [
            ("remove", "file-1"),
            ("remove", "folder-1"),
        ]
    finally:
        second.close()


def test_restart_removes_native_items_missing_from_durable_state(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    first = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
    first.reconcile_remote_change(metadata)
    backend.open("file-1")
    first.close()
    first.reset()
    first.close()

    second = make_controller(config_name, tmp_path, provider, backend)
    try:
        assert second.list_status() == []
        assert backend.items == {}
    finally:
        second.close()


def test_virtual_mode_selection_rejects_an_unsupported_platform(
    config_name: str,
) -> None:
    maestral = Maestral(config_name)
    try:
        with pytest.raises(VirtualFilesUnsupportedError):
            maestral.set_sync_mode("virtual")

        assert maestral.sync_mode == "mirror"
        assert MaestralConfig(config_name).get("sync", "mode") == "mirror"
    finally:
        maestral.virtual_files.close()
        maestral.manager.shutdown()
        maestral.sync._connection.close()
        maestral.client.close()


def test_rpc_exposes_virtual_mode_and_file_methods(config_name: str) -> None:
    backend = FakeVirtualFileBackend()
    maestral = Maestral(
        config_name,
        virtual_file_backend=backend,
        virtual_file_remote_polling=False,
    )
    try:
        dispatcher = JsonRpcDispatcher(maestral)
        response = dispatcher.dispatch(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "set_sync_mode",
                "params": {"mode": "virtual"},
            }
        )
        snapshot = dispatcher.dispatch(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "get_app_snapshot",
            }
        )["result"]
        handshake = dispatcher.dispatch(
            {"jsonrpc": "2.0", "id": 3, "method": "rpc.handshake"}
        )["result"]

        assert response["result"] is None
        assert snapshot["sync_mode"] == "virtual"
        assert snapshot["virtual_files"]["backend"] == "fake_native"
        assert "pin_virtual_file" in handshake["methods"]
        assert "get_virtual_file_status" in handshake["methods"]
        assert "virtual_file_backend" in handshake["properties"]["read"]
    finally:
        maestral.virtual_files.close()
        maestral.manager.shutdown()
        maestral.sync._connection.close()
        maestral.client.close()


def test_virtual_start_never_starts_the_mirror_engine(
    config_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend = FakeVirtualFileBackend()
    maestral = Maestral(
        config_name,
        virtual_file_backend=backend,
        virtual_file_remote_polling=False,
    )
    try:
        maestral.set_sync_mode("virtual")
        maestral.sync._dropbox_path = "/native-virtual-root"
        monkeypatch.setattr(maestral, "_check_linked", Mock())
        monkeypatch.setattr(maestral, "_check_dropbox_dir", Mock())
        virtual_start = Mock()
        mirror_start = Mock()
        monkeypatch.setattr(maestral.virtual_files, "start", virtual_start)
        monkeypatch.setattr(maestral.manager, "start", mirror_start)

        maestral.start_sync()

        virtual_start.assert_called_once_with("/native-virtual-root")
        mirror_start.assert_not_called()
    finally:
        maestral.virtual_files.close()
        maestral.manager.shutdown()
        maestral.sync._connection.close()
        maestral.client.close()
