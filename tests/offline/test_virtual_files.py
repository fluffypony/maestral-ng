from __future__ import annotations

import os
import sqlite3
import stat
import subprocess
import sys
import threading
import time
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock

import pytest

from maestral.config import MaestralConfig
from maestral.core import DeletedMetadata, ListFolderResult
from maestral.exceptions import (
    CursorResetError,
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
    VirtualFileIdentity,
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


def test_hydration_uses_a_private_profile_stage(
    config_name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    root = tmp_path / "virtual-root"
    root.mkdir()
    controller = VirtualFileController(
        config_name,
        provider,  # type: ignore[arg-type]
        backend,
        database_path=str(tmp_path / "virtual-files.db"),
        remote_polling=False,
    )
    stage_directory = Path(controller._stage_directory)
    stage_directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    stale = stage_directory / f"maestral-{config_name}-hydrate-stale"
    stale.write_bytes(b"stale")
    stale.chmod(0o600)
    original_materialize = backend.materialize
    captured: dict[str, object] = {}

    def capture_stage(*args: object, **kwargs: object) -> None:
        staged_path = Path(str(args[1]))
        captured["path"] = staged_path
        captured["mode"] = stat.S_IMODE(os.lstat(staged_path).st_mode)
        captured["parent_mode"] = stat.S_IMODE(os.lstat(staged_path.parent).st_mode)
        original_materialize(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(backend, "materialize", capture_stage)
    controller.start(str(root))
    try:
        assert not stale.exists()
        metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
        controller.reconcile_remote_change(metadata)
        backend.open("file-1")

        staged_path = captured["path"]
        assert isinstance(staged_path, Path)
        assert staged_path.parent == stage_directory
        assert captured["mode"] == 0o600
        assert captured["parent_mode"] == 0o700
        assert not staged_path.exists()
    finally:
        controller.close()


def test_stage_cleanup_requires_the_profile_stage_lock(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    root = tmp_path / "virtual-root"
    root.mkdir()
    controller = VirtualFileController(
        config_name,
        provider,  # type: ignore[arg-type]
        backend,
        database_path=str(tmp_path / "virtual-files.db"),
        remote_polling=False,
    )
    stage_directory = Path(controller._stage_directory)
    stage_directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    live_stage = stage_directory / f"maestral-{config_name}-hydrate-live"
    live_stage.write_bytes(b"live")
    live_stage.chmod(0o600)
    ready = tmp_path / "stage-lock-ready"
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import pathlib,sys; from fasteners import InterProcessLock; "
                "lock=InterProcessLock(sys.argv[1]); assert lock.acquire(False); "
                "pathlib.Path(sys.argv[2]).write_text('ready'); "
                "sys.stdin.buffer.read(1); lock.release()"
            ),
            f"{stage_directory}.lock",
            str(ready),
        ],
        stdin=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 2
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.exists()
        with pytest.raises(VirtualFileBusyError, match="Another process owns"):
            controller.start(str(root))
        assert live_stage.read_bytes() == b"live"

        assert holder.stdin is not None
        holder.stdin.write(b"x")
        holder.stdin.close()
        holder.wait(timeout=2)
        controller.start(str(root))
        assert not live_stage.exists()
    finally:
        if holder.poll() is None:
            holder.terminate()
            holder.wait(timeout=2)
        controller.close()


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


def test_pin_download_does_not_block_remote_reconciliation(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    first = provider.add_file("file-1", "/file.txt", "rev-1", b"old")
    controller.reconcile_remote_change(first)
    provider.block_downloads()
    pin_errors: list[BaseException] = []
    reconcile_errors: list[BaseException] = []
    reconcile_done = threading.Event()

    def pin_file() -> None:
        try:
            controller.pin("file-1")
        except BaseException as exc:
            pin_errors.append(exc)

    def reconcile_file() -> None:
        try:
            second = provider.add_file("file-1", "/file.txt", "rev-2", b"new")
            controller.reconcile_remote_change(second)
        except BaseException as exc:
            reconcile_errors.append(exc)
        finally:
            reconcile_done.set()

    pin_thread = threading.Thread(target=pin_file)
    reconcile_thread = threading.Thread(target=reconcile_file)
    try:
        pin_thread.start()
        assert provider.download_started.wait(2)
        reconcile_thread.start()
        deadline = time.monotonic() + 2
        while (
            controller.get_status("file-1")["revision"] != "rev-2"
            and time.monotonic() < deadline
        ):
            time.sleep(0.01)
        assert controller.get_status("file-1")["revision"] == "rev-2"
    finally:
        provider.release_download.set()
        pin_thread.join(3)
        reconcile_thread.join(3)
        controller.close()

    assert not pin_thread.is_alive()
    assert not reconcile_thread.is_alive()
    assert pin_errors == []
    assert reconcile_errors == []
    assert provider.download_calls == [
        ("file-1", "rev-1"),
        ("file-1", "rev-2"),
    ]
    assert backend.items["file-1"].content == b"new"


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


def test_same_revision_cannot_change_hydrated_content_metadata(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    try:
        first = provider.add_file("file-1", "/file.txt", "rev-1", b"old")
        controller.reconcile_remote_change(first)
        backend.open("file-1")
        call_count = len(backend.calls)

        reused = provider.add_file("file-1", "/renamed.txt", "rev-1", b"different")
        with pytest.raises(ValueError, match="reused.*revision"):
            controller.reconcile_remote_change(reused)

        assert len(backend.calls) == call_count
        assert backend.items["file-1"].content == b"old"
        assert controller.get_status("file-1")["path"] == "/file.txt"
    finally:
        controller.close()


def test_hydration_rejects_same_revision_with_different_metadata(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    try:
        first = provider.add_file("file-1", "/file.txt", "rev-1", b"old")
        controller.reconcile_remote_change(first)
        provider.add_file("file-1", "/file.txt", "rev-1", b"different")

        with pytest.raises(VirtualFileRevisionError, match="different metadata"):
            backend.open("file-1")

        assert backend.items["file-1"].content is None
        assert controller.get_status("file-1")["hydration_state"] == "online_only"
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


def test_remote_changes_preserve_dirty_native_content(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    try:
        first = provider.add_file("file-1", "/file.txt", "rev-1", b"remote")
        controller.reconcile_remote_change(first)
        backend.open("file-1")
        backend.items["file-1"].content = b"local changes"
        backend.set_access_state("file-1", dirty=True)

        second = provider.add_file("file-1", "/file.txt", "rev-2", b"new remote")
        with pytest.raises(VirtualFileBusyError, match="content was preserved"):
            controller.reconcile_remote_change(second)

        assert controller.get_status("file-1")["revision"] == "rev-1"
        assert backend.items["file-1"].content == b"local changes"

        with pytest.raises(VirtualFileBusyError, match="content was preserved"):
            controller.reconcile_remote_change(
                DeletedMetadata(
                    name="file.txt",
                    path_lower="/file.txt",
                    path_display="/file.txt",
                )
            )

        assert controller.get_status("file-1")["revision"] == "rev-1"
        assert backend.items["file-1"].content == b"local changes"
    finally:
        controller.close()


def test_failed_staging_keeps_a_durable_move_journal(
    config_name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    first = provider.add_file("file-1", "/file.txt", "rev-1", b"old")
    controller.reconcile_remote_change(first)
    original_upsert = backend.upsert

    def fail_upsert(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise VirtualFileBusyError("Native item is busy", "Try again later.")

    monkeypatch.setattr(backend, "upsert", fail_upsert)
    second = provider.add_file("file-1", "/renamed.txt", "rev-2", b"new")
    try:
        with pytest.raises(VirtualFileBusyError):
            controller.reconcile_remote_change(second)
        assert str(controller.get_status("file-1")["path"]).startswith(
            "/.maestral-stage-"
        )
        assert controller.get_status("file-1")["revision"] == "rev-1"

        monkeypatch.setattr(backend, "upsert", original_upsert)
        controller.reconcile_remote_change(second)
        assert controller.get_status("file-1")["path"] == "/renamed.txt"
        assert backend.items["file-1"].descriptor.revision == "rev-2"
    finally:
        controller.close()


def test_upsert_uses_the_current_clean_native_identity(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    first = provider.add_file("file-1", "/file.txt", "rev-1", b"old")
    controller.reconcile_remote_change(first)
    backend.items["file-1"].descriptor = replace(
        backend.items["file-1"].descriptor,
        path="/native-drift.txt",
    )
    second = provider.add_file("file-1", "/file.txt", "rev-2", b"new")

    try:
        controller.reconcile_remote_change(second)
        assert backend.items["file-1"].descriptor.path == "/file.txt"
        assert backend.items["file-1"].descriptor.revision == "rev-2"
    finally:
        controller.close()


def test_lost_staging_response_retries_the_durable_move_journal(
    config_name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    first = provider.add_file("file-1", "/file.txt", "rev-1", b"old")
    controller.reconcile_remote_change(first)
    original_upsert = backend.upsert

    def lose_response(*args: object, **kwargs: object) -> None:
        original_upsert(*args, **kwargs)  # type: ignore[arg-type]
        raise RuntimeError("The native response was lost")

    monkeypatch.setattr(backend, "upsert", lose_response)
    second = provider.add_file("file-1", "/renamed.txt", "rev-2", b"new")
    try:
        with pytest.raises(RuntimeError, match="response was lost"):
            controller.reconcile_remote_change(second)
        assert str(controller.get_status("file-1")["path"]).startswith(
            "/.maestral-stage-"
        )

        monkeypatch.setattr(backend, "upsert", original_upsert)
        controller.reconcile_remote_change(second)
        assert controller.get_status("file-1")["path"] == "/renamed.txt"
        assert controller.get_status("file-1")["revision"] == "rev-2"
        assert backend.items["file-1"].descriptor.revision == "rev-2"
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


def test_batch_moves_a_live_child_before_its_parent_is_deleted(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    try:
        controller.reconcile_remote_change(make_folder("folder-1", "/Folder"))
        child = provider.add_file("file-1", "/Folder/Child.txt", "rev-1", b"data")
        controller.reconcile_remote_change(child)
        controller.pin("file-1")
        backend.require_empty_directories = True

        moved = provider.add_file("file-1", "/Moved.txt", "rev-1", b"data")
        controller.reconcile_remote_batch(
            [
                DeletedMetadata(
                    name="Folder",
                    path_lower="/folder",
                    path_display="/Folder",
                ),
                moved,
            ],
            "cursor-moved",
        )

        assert controller.get_status("file-1")["path"] == "/Moved.txt"
        assert controller.get_status("file-1")["pinned"] is True
        assert backend.items["file-1"].content == b"data"
        assert "folder-1" not in backend.items
        assert controller.cursor == "cursor-moved"
    finally:
        controller.close()


def test_batch_stages_an_unchanged_child_before_parent_replacement(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    try:
        old_parent = make_folder("folder-old", "/Folder")
        child = provider.add_file("file-1", "/Folder/Child.txt", "rev-1", b"data")
        controller.reconcile_remote_batch([old_parent, child], "cursor-1")
        controller.pin("file-1")
        backend.cascade_directory_removals = True

        new_parent = make_folder("folder-new", "/Folder")
        controller.reconcile_remote_batch(
            [
                DeletedMetadata(
                    name="Folder",
                    path_lower="/folder",
                    path_display="/Folder",
                ),
                new_parent,
                child,
            ],
            "cursor-2",
        )

        assert "folder-old" not in backend.items
        assert backend.items["folder-new"].descriptor.path == "/Folder"
        assert backend.items["file-1"].descriptor.path == "/Folder/Child.txt"
        assert backend.items["file-1"].content == b"data"
        assert controller.get_status("file-1")["pinned"] is True
        assert controller.cursor == "cursor-2"
    finally:
        controller.close()


def test_batch_stages_same_revision_path_swap(config_name: str, tmp_path: Path) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    try:
        first = provider.add_file("file-a", "/A.txt", "rev-a", b"A")
        second = provider.add_file("file-b", "/B.txt", "rev-b", b"B")
        controller.reconcile_remote_batch([first, second], "cursor-1")
        backend.open("file-a")
        backend.open("file-b")

        moved_first = provider.add_file("file-a", "/B.txt", "rev-a", b"A")
        moved_second = provider.add_file("file-b", "/A.txt", "rev-b", b"B")
        controller.reconcile_remote_batch([moved_second, moved_first], "cursor-2")

        assert controller.get_status("file-a")["path"] == "/B.txt"
        assert controller.get_status("file-b")["path"] == "/A.txt"
        assert backend.items["file-a"].content == b"A"
        assert backend.items["file-b"].content == b"B"
        assert controller.cursor == "cursor-2"
    finally:
        controller.close()


@pytest.mark.parametrize("staged_before_crash", [0, 1])
def test_restart_finishes_a_crash_during_native_move_staging(
    config_name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    staged_before_crash: int,
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    first = make_controller(config_name, tmp_path, provider, backend)
    file_a = provider.add_file("file-a", "/A.txt", "rev-a", b"A")
    file_b = provider.add_file("file-b", "/B.txt", "rev-b", b"B")
    first.reconcile_remote_batch([file_a, file_b], "cursor-1")
    backend.open("file-a")
    backend.open("file-b")
    original_upsert = backend.upsert
    staging_calls = 0

    def fail_staging(*args: object, **kwargs: object) -> None:
        nonlocal staging_calls
        item = args[0]
        if getattr(item, "path", "").startswith("/.maestral-stage-"):
            if staging_calls == staged_before_crash:
                raise RuntimeError("crash during native staging")
            staging_calls += 1
        original_upsert(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(backend, "upsert", fail_staging)
    moved_a = provider.add_file("file-a", "/B.txt", "rev-a", b"A")
    moved_b = provider.add_file("file-b", "/A.txt", "rev-b", b"B")
    with pytest.raises(RuntimeError, match="during native staging"):
        first.reconcile_remote_batch([moved_b, moved_a], "cursor-2")
    first.close()
    monkeypatch.setattr(backend, "upsert", original_upsert)

    second = make_controller(config_name, tmp_path, provider, backend)
    try:
        assert second.get_status("file-a")["path"] == "/B.txt"
        assert second.get_status("file-b")["path"] == "/A.txt"
        assert backend.items["file-a"].descriptor.path == "/B.txt"
        assert backend.items["file-b"].descriptor.path == "/A.txt"
        assert backend.items["file-a"].content == b"A"
        assert backend.items["file-b"].content == b"B"
        assert second.cursor == "cursor-1"
    finally:
        second.close()


def test_same_controller_retry_resumes_the_durable_move_journal(
    config_name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    file_a = provider.add_file("file-a", "/A.txt", "rev-a", b"A")
    file_b = provider.add_file("file-b", "/B.txt", "rev-b", b"B")
    controller.reconcile_remote_batch([file_a, file_b], "cursor-1")
    original_upsert = backend.upsert
    failed = False

    def fail_first_stage(*args: object, **kwargs: object) -> None:
        nonlocal failed
        item = args[0]
        if not failed and getattr(item, "path", "").startswith("/.maestral-stage-"):
            failed = True
            raise RuntimeError("crash during native staging")
        original_upsert(*args, **kwargs)  # type: ignore[arg-type]

    moved_a = provider.add_file("file-a", "/B.txt", "rev-a", b"A")
    moved_b = provider.add_file("file-b", "/A.txt", "rev-b", b"B")
    monkeypatch.setattr(backend, "upsert", fail_first_stage)
    with pytest.raises(RuntimeError, match="during native staging"):
        controller.reconcile_remote_batch([moved_b, moved_a], "cursor-2")
    monkeypatch.setattr(backend, "upsert", original_upsert)

    controller.reconcile_remote_batch([moved_b, moved_a], "cursor-2")

    assert controller.cursor == "cursor-2"
    assert controller.get_status("file-a")["path"] == "/B.txt"
    assert controller.get_status("file-b")["path"] == "/A.txt"
    with controller._lock:
        assert all(
            record.staging_source_path_cased is None
            for record in controller._get_store().all()
        )
    controller.close()


@pytest.mark.parametrize("deleted_kind", ["file", "directory"])
def test_restart_deletes_a_replaced_target_before_finalising_a_staged_move(
    config_name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    deleted_kind: str,
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    first = make_controller(config_name, tmp_path, provider, backend)
    source = provider.add_file("file-a", "/A.txt", "rev-a", b"A")
    destination_path = "/B.txt" if deleted_kind == "file" else "/B"
    if deleted_kind == "file":
        destination_entries = [
            provider.add_file("target", destination_path, "rev-b", b"B")
        ]
    else:
        destination_entries = [
            make_folder("target", destination_path),
            provider.add_file("old-child", "/B/Old.txt", "rev-c", b"C"),
        ]
        backend.require_empty_directories = True
    first.reconcile_remote_batch([source, *destination_entries], "cursor-1")
    original_remove = backend.remove

    def fail_target_removal(*args: object, **kwargs: object) -> None:
        identity = args[0]
        if (
            isinstance(identity, VirtualFileIdentity)
            and identity.provider_id == "target"
        ):
            raise RuntimeError("crash after native staging")
        original_remove(*args, **kwargs)  # type: ignore[arg-type]

    moved = provider.add_file("file-a", destination_path, "rev-a", b"A")
    deleted = DeletedMetadata(
        name=destination_path.rsplit("/", 1)[-1],
        path_lower=destination_path.lower(),
        path_display=destination_path,
    )
    monkeypatch.setattr(backend, "remove", fail_target_removal)
    with pytest.raises(RuntimeError, match="after native staging"):
        first.reconcile_remote_batch([deleted, moved], "cursor-2")
    first.close()
    monkeypatch.setattr(backend, "remove", original_remove)
    recovery_call = len(backend.calls)

    second = make_controller(config_name, tmp_path, provider, backend)
    try:
        recovery_calls = backend.calls[recovery_call:]
        removal_index = next(
            index
            for index, call in enumerate(recovery_calls)
            if call[0] == "remove" and call[1] == "target"
        )
        final_move_index = next(
            index
            for index, call in enumerate(recovery_calls)
            if call[0] == "upsert"
            and call[1] == "file-a"
            and isinstance(call[3], VirtualFileIdentity)
            and call[3].path.startswith("/.maestral-stage-")
        )
        assert removal_index < final_move_index
        assert set(backend.items) == {"file-a"}
        assert backend.items["file-a"].descriptor.path == destination_path
        assert second.get_status("file-a")["path"] == destination_path
        assert second.cursor == "cursor-1"
    finally:
        second.close()


@pytest.mark.parametrize("final_native_moves", [1, 2])
def test_restart_finishes_a_crash_during_final_native_moves(
    config_name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    final_native_moves: int,
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    first = make_controller(config_name, tmp_path, provider, backend)
    file_a = provider.add_file("file-a", "/A.txt", "rev-a", b"A")
    file_b = provider.add_file("file-b", "/B.txt", "rev-b", b"B")
    first.reconcile_remote_batch([file_a, file_b], "cursor-1")
    backend.open("file-a")
    backend.open("file-b")
    original_upsert = backend.upsert

    def fail_staging(*args: object, **kwargs: object) -> None:
        item = args[0]
        if getattr(item, "path", "").startswith("/.maestral-stage-"):
            raise RuntimeError("crash before native staging")
        original_upsert(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(backend, "upsert", fail_staging)
    moved_a = provider.add_file("file-a", "/B.txt", "rev-a", b"A")
    moved_b = provider.add_file("file-b", "/A.txt", "rev-b", b"B")
    with pytest.raises(RuntimeError, match="before native staging"):
        first.reconcile_remote_batch([moved_b, moved_a], "cursor-2")
    monkeypatch.setattr(backend, "upsert", original_upsert)

    with first._lock:
        staged_records = first._get_store().all()
    for staged in staged_records:
        assert staged.staging_source_path_cased is not None
        original_upsert(
            first._descriptor(staged),
            expected_identity=VirtualFileIdentity(
                staged.provider_id,
                staged.staging_source_path_cased,
                bool(staged.is_directory),
                staged.revision,
            ),
        )
    for staged in staged_records[:final_native_moves]:
        assert staged.staging_final_path_lower is not None
        assert staged.staging_final_path_cased is not None
        final = first._copy_record(
            staged,
            path_lower=staged.staging_final_path_lower,
            path_cased=staged.staging_final_path_cased,
            staging_source_path_cased=None,
            staging_final_path_lower=None,
            staging_final_path_cased=None,
        )
        original_upsert(
            first._descriptor(final),
            expected_identity=first._native_identity(staged),
        )
    first.close()

    second = make_controller(config_name, tmp_path, provider, backend)
    try:
        assert second.get_status("file-a")["path"] == "/B.txt"
        assert second.get_status("file-b")["path"] == "/A.txt"
        assert backend.items["file-a"].content == b"A"
        assert backend.items["file-b"].content == b"B"
        assert second.cursor == "cursor-1"
    finally:
        second.close()


def test_batch_keeps_order_when_live_revision_is_then_deleted(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    try:
        first = provider.add_file("file-1", "/file.txt", "rev-1", b"old")
        controller.reconcile_remote_batch([first], "cursor-1")
        second = provider.add_file("file-1", "/file.txt", "rev-2", b"new")

        controller.reconcile_remote_batch(
            [
                second,
                DeletedMetadata(
                    name="file.txt",
                    path_lower="/file.txt",
                    path_display="/file.txt",
                ),
            ],
            "cursor-2",
        )

        with pytest.raises(VirtualFileNotFoundError):
            controller.get_status("file-1")
        assert "file-1" not in backend.items
        assert controller.cursor == "cursor-2"
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


def test_restart_keeps_running_when_eager_pin_hydration_fails(
    config_name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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

    def fail_download(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise OSError("stage disk failed")

    monkeypatch.setattr(provider, "download", fail_download)
    second = make_controller(config_name, tmp_path, provider, backend)
    try:
        status = second.get_status("file-1")
        assert second.running is True
        assert status["hydration_state"] == "online_only"
        assert status["pinned"] is True
        assert status["error"] == "stage disk failed"
    finally:
        second.close()


def test_restart_preserves_dirty_pinned_content(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    first = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"remote")
    first.reconcile_remote_change(metadata)
    first.pin("file-1")
    backend.items["file-1"].content = b"local changes"
    backend.set_access_state("file-1", dirty=True)
    first.close()

    second = make_controller(config_name, tmp_path, provider, backend)
    try:
        status = second.get_status("file-1")
        assert status["hydration_state"] == "hydrated"
        assert status["pinned"] is True
        assert status["error"] == "Local changes were preserved after restart."
        assert backend.items["file-1"].content == b"local changes"
        with pytest.raises(VirtualFileBusyError, match="local changes"):
            second.hydrate("file-1")
    finally:
        second.close()


def test_failed_reset_clears_cursor_before_rows(
    config_name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
    controller.reconcile_remote_batch([metadata], "cursor-1")
    controller.stop()

    store = controller._get_store()
    monkeypatch.setattr(
        store.records,
        "delete",
        Mock(side_effect=RuntimeError("simulated database failure")),
    )

    try:
        with pytest.raises(RuntimeError, match="simulated database failure"):
            controller.reset()
        assert controller.cursor == ""
        assert controller.get_status("file-1")["revision"] == "rev-1"
    finally:
        controller.close()


@pytest.mark.parametrize("dirty,open_count", [(True, 0), (False, 1)])
def test_reset_preflights_all_native_items_before_mutation(
    config_name: str,
    tmp_path: Path,
    dirty: bool,
    open_count: int,
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
    controller.reconcile_remote_batch([metadata], "cursor-1")
    backend.open("file-1")
    backend.set_access_state("file-1", dirty=dirty, open_count=open_count)

    with pytest.raises(VirtualFileBusyError, match="content was preserved"):
        controller.reset()

    assert controller.cursor == "cursor-1"
    assert controller.get_status("file-1")["revision"] == "rev-1"
    assert backend.items["file-1"].content == b"data"
    controller.close()


def test_detach_preserves_hydrated_bytes_and_clears_core_state(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
    controller.reconcile_remote_batch([metadata], "cursor-1")
    backend.open("file-1")

    controller.detach()

    assert controller.cursor == ""
    assert controller.status_page()["items"] == []
    assert backend.items["file-1"].content == b"data"
    assert backend.started is False
    controller.close()


def test_status_pages_and_summary_cover_the_complete_index(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    entries = [
        provider.add_file("file-c", "/C.txt", "rev-c", b"C"),
        provider.add_file("file-a", "/A.txt", "rev-a", b"A"),
        provider.add_file("file-b", "/B.txt", "rev-b", b"B"),
    ]
    controller.reconcile_remote_batch(entries, "cursor-1")
    controller.pin("file-b")

    first = controller.status_page(limit=2)
    second = controller.status_page(
        cursor=first["cursor"],  # type: ignore[arg-type]
        limit=2,
    )
    summary = controller.summary()

    assert [item["path"] for item in first["items"]] == ["/A.txt", "/B.txt"]  # type: ignore[index]
    assert first["cursor"] == "/b.txt"
    assert [item["path"] for item in second["items"]] == ["/C.txt"]  # type: ignore[index]
    assert second["cursor"] is None
    assert summary["items"] == 3
    assert summary["pinned"] == 1
    assert summary["states"] == {
        "online_only": 2,
        "hydrating": 0,
        "hydrated": 1,
        "evicting": 0,
        "deleting": 0,
    }
    with pytest.raises(ValueError, match="status limit"):
        controller.status_page(limit=257)
    controller.close()


def test_close_waits_for_status_and_never_reopens_the_database(
    config_name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    store = controller._get_store()
    original_summary = store.summary
    status_started = threading.Event()
    release_status = threading.Event()
    status_errors: list[BaseException] = []

    def blocked_summary() -> object:
        status_started.set()
        if not release_status.wait(2):
            raise TimeoutError("the status call did not resume")
        return original_summary()

    monkeypatch.setattr(store, "summary", blocked_summary)

    def read_status() -> None:
        try:
            controller.summary()
        except BaseException as exc:
            status_errors.append(exc)

    status_thread = threading.Thread(target=read_status)
    close_thread = threading.Thread(target=controller.close)
    status_thread.start()
    assert status_started.wait(1)
    close_thread.start()
    assert close_thread.is_alive()
    release_status.set()
    status_thread.join(2)
    close_thread.join(2)

    assert not status_thread.is_alive()
    assert not close_thread.is_alive()
    assert status_errors == []
    assert controller._store is None
    with pytest.raises(VirtualFileBusyError, match="closed"):
        controller.status_page()
    assert controller._store is None


def test_stop_cancels_hydration_before_materialization(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
    controller.reconcile_remote_change(metadata)
    provider.block_downloads()
    hydration_errors: list[BaseException] = []
    stop_errors: list[BaseException] = []

    def hydrate_file() -> None:
        try:
            backend.open("file-1")
        except BaseException as exc:
            hydration_errors.append(exc)

    def stop_controller() -> None:
        try:
            controller.stop()
        except BaseException as exc:
            stop_errors.append(exc)

    hydration = threading.Thread(target=hydrate_file)
    hydration.start()
    assert provider.download_started.wait(2)
    stopping = threading.Thread(target=stop_controller)
    stopping.start()
    deadline = time.monotonic() + 2
    while controller.running and time.monotonic() < deadline:
        time.sleep(0.01)
    provider.release_download.set()
    hydration.join(2)
    stopping.join(2)

    assert not hydration.is_alive()
    assert not stopping.is_alive()
    assert stop_errors == []
    assert len(hydration_errors) == 1
    assert isinstance(hydration_errors[0], VirtualFileBusyError)
    assert backend.items["file-1"].content is None
    assert backend.started is False
    controller.close()


def test_stop_interrupts_the_idle_remote_poll(config_name: str, tmp_path: Path) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    first = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
    first.reconcile_remote_batch([metadata], "cursor-1")
    first.close()
    provider.pages = [ListFolderResult([], False, "cursor-2")]
    provider.change_poll_started.clear()

    second = VirtualFileController(
        config_name,
        provider,  # type: ignore[arg-type]
        backend,
        database_path=str(tmp_path / "virtual-files.db"),
        remote_polling=True,
    )
    second.start(str(tmp_path / "virtual-root"))
    assert provider.change_poll_started.wait(2)

    stopping = threading.Thread(target=second.stop)
    stopping.start()
    stopping.join(1)

    assert not stopping.is_alive()
    assert backend.started is False
    second.close()


def test_unchanged_metadata_does_not_restart_hydration(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
    controller.reconcile_remote_change(metadata)
    provider.block_downloads()
    errors: list[BaseException] = []

    def hydrate_file() -> None:
        try:
            backend.open("file-1")
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=hydrate_file)
    thread.start()
    assert provider.download_started.wait(2)
    generation = controller.get_status("file-1")["generation"]
    controller.reconcile_remote_change(metadata)
    provider.release_download.set()
    thread.join(2)

    assert errors == []
    assert provider.download_calls == [("file-1", "rev-1")]
    assert controller.get_status("file-1")["generation"] == generation
    assert backend.items["file-1"].content == b"data"
    controller.close()


def test_unapplied_metadata_repairs_a_missing_native_item(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
    controller.reconcile_remote_change(metadata)
    backend.items.pop("file-1")
    with controller._lock:
        record = controller._get_store().get("file-1")
        assert record is not None
        controller._get_store().put(
            controller._copy_record(record, native_applied_generation=0)
        )

    controller.reconcile_remote_change(metadata)

    assert backend.items["file-1"].descriptor.path == "/file.txt"
    controller.close()


def test_unchanged_metadata_repairs_a_stale_native_revision(
    config_name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    first = provider.add_file("file-1", "/file.txt", "rev-1", b"old")
    controller.reconcile_remote_change(first)
    original_upsert = backend.upsert
    failed = False

    def fail_first_new_revision(*args: object, **kwargs: object) -> None:
        nonlocal failed
        item = args[0]
        if not failed and getattr(item, "revision", None) == "rev-2":
            failed = True
            raise RuntimeError("injected native apply failure")
        original_upsert(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(backend, "upsert", fail_first_new_revision)
    second = provider.add_file("file-1", "/file.txt", "rev-2", b"new")
    with pytest.raises(RuntimeError, match="injected native apply failure"):
        controller.reconcile_remote_change(second)

    assert controller.get_status("file-1")["revision"] == "rev-2"
    assert backend.items["file-1"].descriptor.revision == "rev-1"

    monkeypatch.setattr(backend, "upsert", original_upsert)
    controller.reconcile_remote_change(second)

    assert backend.items["file-1"].descriptor.revision == "rev-2"
    controller.close()


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
        assert second.status_page()["items"] == []
        assert backend.items == {}
        remove_calls = [call for call in backend.calls if call[0] == "remove"]
        assert remove_calls[-2:] == [
            (
                "remove",
                "file-1",
                VirtualFileIdentity("file-1", "/Folder/Child.txt", False, "rev-1"),
            ),
            (
                "remove",
                "folder-1",
                VirtualFileIdentity("folder-1", "/Folder", True, "folder"),
            ),
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
    first.stop()
    first.reset()
    first.close()

    second = make_controller(config_name, tmp_path, provider, backend)
    try:
        assert second.status_page()["items"] == []
        assert backend.items == {}
    finally:
        second.close()


def test_restart_preserves_a_dirty_native_orphan(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    first = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"remote")
    first.reconcile_remote_change(metadata)
    backend.open("file-1")
    backend.items["file-1"].content = b"local changes"
    backend.set_access_state("file-1", dirty=True)
    first.stop()
    with first._lock:
        first._get_store().delete("file-1")
    first.close()

    root = tmp_path / "virtual-root"
    second = VirtualFileController(
        config_name,
        provider,  # type: ignore[arg-type]
        backend,
        database_path=str(tmp_path / "virtual-files.db"),
        remote_polling=False,
    )
    try:
        with pytest.raises(VirtualFileBusyError, match="content was preserved"):
            second.start(str(root))
        assert backend.items["file-1"].content == b"local changes"
        assert not any(call[:2] == ("remove", "file-1") for call in backend.calls)
    finally:
        second.close()


def test_restart_removes_clean_orphans_from_children_up(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    first = make_controller(config_name, tmp_path, provider, backend)
    first.reconcile_remote_change(make_folder("folder-1", "/Folder"))
    child = provider.add_file("file-1", "/Folder/Child.txt", "rev-1", b"data")
    first.reconcile_remote_change(child)
    first.stop()
    with first._lock:
        first._get_store().delete("file-1")
        first._get_store().delete("folder-1")
    first.close()
    backend.require_empty_directories = True

    second = make_controller(config_name, tmp_path, provider, backend)
    try:
        remove_calls = [call for call in backend.calls if call[0] == "remove"]
        assert remove_calls[-2:] == [
            (
                "remove",
                "file-1",
                VirtualFileIdentity("file-1", "/Folder/Child.txt", False, "rev-1"),
            ),
            (
                "remove",
                "folder-1",
                VirtualFileIdentity("folder-1", "/Folder", True, "folder"),
            ),
        ]
    finally:
        second.close()


def test_missing_database_rebuild_preserves_matching_hydrated_content(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    database_path = tmp_path / "virtual-files.db"
    first = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
    first.reconcile_remote_batch([metadata], "cursor-old")
    first.pin("file-1")
    first.close()
    database_path.unlink()
    provider.pages = [ListFolderResult([metadata], False, "cursor-new")]

    second = VirtualFileController(
        config_name,
        provider,  # type: ignore[arg-type]
        backend,
        database_path=str(database_path),
        remote_polling=False,
    )
    second.start(str(tmp_path / "virtual-root"))
    try:
        assert second.cursor == ""
        assert backend.items["file-1"].content == b"data"

        second.refresh_remote()

        status = second.get_status("file-1")
        assert status["hydration_state"] == "hydrated"
        assert status["pinned"] is True
        assert backend.items["file-1"].content == b"data"
        assert second.cursor == "cursor-new"
    finally:
        second.close()


def test_missing_database_stages_a_live_child_before_orphan_parent_removal(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    database_path = tmp_path / "virtual-files.db"
    first = make_controller(config_name, tmp_path, provider, backend)
    folder = make_folder("folder-1", "/Folder")
    child = provider.add_file("file-1", "/Folder/Child.txt", "rev-1", b"data")
    first.reconcile_remote_batch([folder, child], "cursor-old")
    first.pin("file-1")
    first.close()
    database_path.unlink()

    moved_child = provider.add_file("file-1", "/Moved.txt", "rev-1", b"data")
    provider.pages = [ListFolderResult([moved_child], False, "cursor-new")]
    backend.cascade_directory_removals = True
    second = VirtualFileController(
        config_name,
        provider,  # type: ignore[arg-type]
        backend,
        database_path=str(database_path),
        remote_polling=False,
    )
    second.start(str(tmp_path / "virtual-root"))
    try:
        second.refresh_remote()

        assert "folder-1" not in backend.items
        assert backend.items["file-1"].descriptor.path == "/Moved.txt"
        assert backend.items["file-1"].content == b"data"
        assert second.get_status("file-1")["pinned"] is True
        assert second.cursor == "cursor-new"
    finally:
        second.close()


def test_restored_database_uses_its_own_remote_cursor(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    database_path = tmp_path / "virtual-files.db"
    first = make_controller(config_name, tmp_path, provider, backend)
    old = provider.add_file("file-1", "/old.txt", "rev-1", b"old")
    first.reconcile_remote_batch([old], "cursor-old")
    first.close()
    old_database = database_path.read_bytes()

    second = make_controller(config_name, tmp_path, provider, backend)
    new = provider.add_file("file-2", "/new.txt", "rev-1", b"new")
    second.reconcile_remote_batch([new], "cursor-new")
    second.close()
    database_path.write_bytes(old_database)

    restored = make_controller(config_name, tmp_path, provider, backend)
    try:
        assert restored.cursor == "cursor-old"
        assert restored.get_status("file-1")["path"] == "/old.txt"
        with pytest.raises(VirtualFileNotFoundError):
            restored.get_status("file-2")
        assert set(backend.items) == {"file-1"}
    finally:
        restored.close()


@pytest.mark.parametrize("replacement", ["missing", "empty_sqlite"])
def test_database_loss_after_controller_creation_forces_safe_snapshot(
    config_name: str,
    tmp_path: Path,
    replacement: str,
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    database_path = tmp_path / "virtual-files.db"
    first = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
    first.reconcile_remote_batch([metadata], "cursor-old")
    first.pin("file-1")
    first.close()

    second = VirtualFileController(
        config_name,
        provider,  # type: ignore[arg-type]
        backend,
        database_path=str(database_path),
        remote_polling=False,
    )
    database_path.unlink()
    if replacement == "empty_sqlite":
        sqlite3.connect(database_path).close()
    provider.pages = [ListFolderResult([metadata], False, "cursor-new")]

    second.start(str(tmp_path / "virtual-root"))
    try:
        assert second.cursor == ""
        assert backend.items["file-1"].content == b"data"

        second.refresh_remote()

        assert second.cursor == "cursor-new"
        assert second.get_status("file-1")["hydration_state"] == "hydrated"
        assert backend.items["file-1"].content == b"data"
    finally:
        second.close()


@pytest.mark.parametrize("store_fault", ["initialising", "missing_records"])
def test_non_ready_database_sentinel_forces_safe_snapshot(
    config_name: str,
    tmp_path: Path,
    store_fault: str,
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    database_path = tmp_path / "virtual-files.db"
    first = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
    first.reconcile_remote_batch([metadata], "cursor-old")
    first.pin("file-1")
    first.close()

    connection = sqlite3.connect(database_path)
    try:
        if store_fault == "initialising":
            connection.execute(
                "UPDATE virtual_file_store_identity SET status = 'initialising'"
            )
        else:
            connection.execute("DROP TABLE virtual_files")
        connection.commit()
    finally:
        connection.close()
    provider.pages = [ListFolderResult([metadata], False, "cursor-new")]

    second = VirtualFileController(
        config_name,
        provider,  # type: ignore[arg-type]
        backend,
        database_path=str(database_path),
        remote_polling=False,
    )
    second.start(str(tmp_path / "virtual-root"))
    try:
        assert second.cursor == ""
        assert backend.items["file-1"].content == b"data"

        second.refresh_remote()

        assert second.cursor == "cursor-new"
        assert backend.items["file-1"].content == b"data"
    finally:
        second.close()


def test_cursor_reset_runs_a_full_snapshot(config_name: str, tmp_path: Path) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
    controller.reconcile_remote_batch([metadata], "cursor-old")
    provider.pages = [ListFolderResult([metadata], False, "cursor-new")]

    def reset_cursor(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise CursorResetError("The cursor was reset")
        yield

    provider.list_remote_changes_iterator = reset_cursor  # type: ignore[method-assign]

    controller.refresh_remote()

    assert controller.cursor == "cursor-new"
    assert controller.get_status("file-1")["revision"] == "rev-1"
    controller.close()


@pytest.mark.parametrize(
    "pages",
    [
        [],
        [ListFolderResult([], True, "cursor-next")],
        [
            ListFolderResult([], True, "same-cursor"),
            ListFolderResult([], False, "same-cursor"),
        ],
    ],
)
def test_incomplete_snapshot_never_deletes_native_items(
    config_name: str,
    tmp_path: Path,
    pages: list[ListFolderResult],
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
    controller.reconcile_remote_batch([metadata], "cursor-old")
    controller._set_checkpoint("")
    provider.pages = pages
    call_count = len(backend.calls)

    with pytest.raises(ValueError):
        controller.refresh_remote()

    assert len(backend.calls) == call_count
    assert "file-1" in backend.items
    assert controller.get_status("file-1")["revision"] == "rev-1"
    assert controller.cursor == ""
    controller.close()


def test_malformed_snapshot_fails_before_native_mutation(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"data")
    controller.reconcile_remote_batch([metadata], "cursor-old")
    controller._set_checkpoint("")
    malformed = provider.add_file("file-2", "/safe.txt", "rev-1", b"bad")
    malformed.path_lower = "/../escape"
    provider.pages = [ListFolderResult([malformed], False, "cursor-new")]
    call_count = len(backend.calls)

    with pytest.raises(ValueError, match="normalised path"):
        controller.refresh_remote()

    assert len(backend.calls) == call_count
    assert set(backend.items) == {"file-1"}
    assert controller.cursor == ""
    controller.close()


@pytest.mark.parametrize("update_kind", ["direct", "batch", "snapshot"])
def test_multibyte_component_over_filesystem_limit_fails_before_native_mutation(
    config_name: str,
    tmp_path: Path,
    update_kind: str,
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("long-1", f"/{'é' * 128}", "rev-1", b"bad")
    call_count = len(backend.calls)

    with pytest.raises(ValueError, match="normalised path"):
        if update_kind == "direct":
            controller.reconcile_remote_change(metadata)
        elif update_kind == "batch":
            controller.reconcile_remote_batch([metadata], "cursor-new")
        else:
            controller._set_checkpoint("")
            provider.pages = [ListFolderResult([metadata], False, "cursor-new")]
            controller.refresh_remote()

    assert len(backend.calls) == call_count
    assert backend.items == {}
    controller.close()


@pytest.mark.parametrize("update_kind", ["direct", "batch", "snapshot"])
@pytest.mark.parametrize("control", ["\n", "\x7f"])
def test_control_characters_in_paths_fail_before_native_mutation(
    config_name: str,
    tmp_path: Path,
    update_kind: str,
    control: str,
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file(
        "control-1", f"/unsafe{control}name.txt", "rev-1", b"bad"
    )
    call_count = len(backend.calls)

    with pytest.raises(ValueError, match="normalised path"):
        if update_kind == "direct":
            controller.reconcile_remote_change(metadata)
        elif update_kind == "batch":
            controller.reconcile_remote_batch([metadata], "cursor-new")
        else:
            controller._set_checkpoint("")
            provider.pages = [ListFolderResult([metadata], False, "cursor-new")]
            controller.refresh_remote()

    assert len(backend.calls) == call_count
    assert backend.items == {}
    assert controller.status_page()["items"] == []
    controller.close()


@pytest.mark.parametrize(
    ("field", "value"),
    [("id", "é" * 513), ("rev", "revision\x7f")],
)
def test_native_token_limits_fail_before_database_or_native_mutation(
    config_name: str,
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("file-1", "/file.txt", "rev-1", b"bad")
    setattr(metadata, field, value)
    call_count = len(backend.calls)

    with pytest.raises(ValueError, match="provider identity|revision"):
        controller.reconcile_remote_batch([metadata], "cursor-new")

    assert len(backend.calls) == call_count
    assert backend.items == {}
    assert controller.status_page()["items"] == []
    controller.close()


@pytest.mark.parametrize(
    "path",
    ["/.maestral-root", "/.MAESTRAL-ROOT", "/.~maestral-root-owned"],
)
@pytest.mark.parametrize("update_kind", ["direct", "batch", "snapshot"])
def test_reserved_root_marker_paths_fail_before_native_mutation(
    config_name: str,
    tmp_path: Path,
    path: str,
    update_kind: str,
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    metadata = provider.add_file("reserved-1", path, "rev-1", b"bad")
    call_count = len(backend.calls)

    with pytest.raises(ValueError, match="reserved root-marker"):
        if update_kind == "direct":
            controller.reconcile_remote_change(metadata)
        elif update_kind == "batch":
            controller.reconcile_remote_batch([metadata], "cursor-new")
        else:
            controller._set_checkpoint("")
            provider.pages = [ListFolderResult([metadata], False, "cursor-new")]
            controller.refresh_remote()

    assert len(backend.calls) == call_count
    assert backend.items == {}
    controller.close()


@pytest.mark.parametrize("invalid_parent", ["missing", "file"])
def test_malformed_snapshot_tree_fails_before_native_mutation(
    config_name: str,
    tmp_path: Path,
    invalid_parent: str,
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    old = provider.add_file("old-1", "/old.txt", "rev-1", b"old")
    controller.reconcile_remote_batch([old], "cursor-old")
    controller._set_checkpoint("")
    child = provider.add_file("child-1", "/Parent/Child.txt", "rev-1", b"child")
    entries = [child]
    if invalid_parent == "file":
        entries.insert(
            0,
            provider.add_file("parent-1", "/Parent", "rev-1", b"file"),
        )
    provider.pages = [ListFolderResult(entries, False, "cursor-new")]
    call_count = len(backend.calls)

    with pytest.raises(ValueError, match="missing parent|descendant below a file"):
        controller.refresh_remote()

    assert len(backend.calls) == call_count
    assert set(backend.items) == {"old-1"}
    assert controller.cursor == ""
    controller.close()


def test_full_snapshot_cannot_borrow_a_parent_from_stale_rows(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    controller = make_controller(config_name, tmp_path, provider, backend)
    parent = make_folder("parent-old", "/Parent")
    controller.reconcile_remote_batch([parent], "cursor-old")
    controller._set_checkpoint("")
    child = provider.add_file("child-new", "/Parent/Child.txt", "rev-1", b"child")
    provider.pages = [ListFolderResult([child], False, "cursor-new")]
    call_count = len(backend.calls)

    with pytest.raises(ValueError, match="missing parent directory"):
        controller.refresh_remote()

    assert len(backend.calls) == call_count
    assert set(backend.items) == {"parent-old"}
    assert controller.cursor == ""
    controller.close()


def test_corrupt_store_tree_fails_before_native_mutation(
    config_name: str, tmp_path: Path
) -> None:
    provider = FakeVirtualProvider()
    backend = FakeVirtualFileBackend()
    first = make_controller(config_name, tmp_path, provider, backend)
    folder = make_folder("folder-1", "/Folder")
    child = provider.add_file("child-1", "/Folder/Child.txt", "rev-1", b"child")
    first.reconcile_remote_batch([folder, child], "cursor-old")
    first.stop()
    with first._lock:
        first._get_store().delete("folder-1")
    first.close()
    prior_mutations = [
        call for call in backend.calls if call[0] in {"upsert", "remove"}
    ]

    second = VirtualFileController(
        config_name,
        provider,  # type: ignore[arg-type]
        backend,
        database_path=str(tmp_path / "virtual-files.db"),
        remote_polling=False,
    )
    with pytest.raises(ValueError, match="missing parent directory"):
        second.start(str(tmp_path / "virtual-root"))

    mutations = [call for call in backend.calls if call[0] in {"upsert", "remove"}]
    assert mutations == prior_mutations
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
