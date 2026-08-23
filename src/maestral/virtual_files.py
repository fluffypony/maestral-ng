"""Core coordination for a separate native virtual-file root."""

from __future__ import annotations

import enum
import os
import platform
import sqlite3
import tempfile
import threading
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import BinaryIO, Protocol, cast

from .config import MaestralState
from .core import DeletedMetadata, FileMetadata, FolderMetadata, Metadata
from .database.core import Database
from .database.orm import Column, Manager, Model, NonNullColumn
from .database.query import AllQuery, MatchQuery
from .database.types import SqlEnum, SqlInt, SqlPath, SqlString
from .exceptions import (
    MaestralApiError,
    VirtualFileBusyError,
    VirtualFileNotFoundError,
    VirtualFileRevisionError,
    VirtualFilesUnsupportedError,
)
from .providers.base import RemoteProvider
from .utils.appdirs import get_data_path

MIRROR_MODE = "mirror"
VIRTUAL_MODE = "virtual"
SYNC_MODES = (MIRROR_MODE, VIRTUAL_MODE)


def normalise_sync_mode(value: str) -> str:
    """Return one canonical local sync mode."""
    if not isinstance(value, str):
        raise ValueError("The sync mode must be a string")
    mode = value.strip().lower().replace("-", "_").replace(" ", "_")
    if mode not in SYNC_MODES:
        choices = ", ".join(SYNC_MODES)
        raise ValueError(f"Unknown sync mode {value!r}. Choose one of: {choices}")
    return mode


class HydrationState(enum.Enum):
    """Durable state of one native virtual file."""

    OnlineOnly = "online_only"
    Hydrating = "hydrating"
    Hydrated = "hydrated"
    Evicting = "evicting"
    Deleting = "deleting"


class _VirtualFileRecord(Model):
    """One desired virtual item, keyed by its stable provider identity."""

    __tablename__ = "virtual_files"

    provider_id = NonNullColumn(SqlString(), primary_key=True)
    path_lower = NonNullColumn(SqlPath(), unique=True)
    path_cased = NonNullColumn(SqlPath(), index=True)
    is_directory = NonNullColumn(SqlInt())
    revision = NonNullColumn(SqlString())
    content_hash = Column(SqlString())
    size = NonNullColumn(SqlInt())
    symlink_target = Column(SqlPath())
    hydration_state = NonNullColumn(SqlEnum(HydrationState))
    pinned = NonNullColumn(SqlInt())
    materialized_revision = Column(SqlString())
    generation = NonNullColumn(SqlInt())
    last_error = Column(SqlString())


@dataclass(frozen=True)
class VirtualFileDescriptor:
    """Provider metadata required by a native placeholder backend."""

    provider_id: str
    path: str
    is_directory: bool
    revision: str
    content_hash: str | None
    size: int
    symlink_target: str | None
    pinned: bool


@dataclass(frozen=True)
class NativeFileState:
    """Native state used for recovery and safe eviction checks."""

    hydrated_revision: str | None
    pinned: bool
    dirty: bool = False
    open_count: int = 0


@dataclass(frozen=True)
class NativeFileRecord:
    """One native placeholder returned by crash recovery enumeration."""

    provider_id: str
    path: str
    revision: str
    hydrated_revision: str | None
    pinned: bool
    dirty: bool = False
    open_count: int = 0


HydrationRequest = Callable[[str, str], dict[str, object]]


class VirtualFileBackend(Protocol):
    """Atomic boundary implemented by each native virtual-file adapter.

    Every mutation resolves an item by ``provider_id``. Implementations must apply an
    operation atomically or leave the previous native state unchanged.
    """

    backend_id: str
    supported: bool

    def start(self, root_path: str, request_hydration: HydrationRequest) -> None: ...

    def stop(self) -> None: ...

    def upsert(self, item: VirtualFileDescriptor) -> None: ...

    def remove(self, provider_id: str) -> None: ...

    def set_pinned(self, provider_id: str, pinned: bool) -> None: ...

    def materialize(
        self,
        item: VirtualFileDescriptor,
        staged_path: str,
        *,
        expected_revision: str,
    ) -> None: ...

    def evict(self, provider_id: str, *, expected_revision: str) -> None: ...

    def inspect(self, provider_id: str) -> NativeFileState | None: ...

    def recover(self) -> Sequence[NativeFileRecord]: ...


class UnsupportedVirtualFileBackend:
    """Inactive backend used only while the profile remains in mirror mode."""

    backend_id = "unsupported"
    supported = False

    def _unsupported(self) -> VirtualFilesUnsupportedError:
        return VirtualFilesUnsupportedError(
            "Native virtual files are unavailable",
            f"No virtual-file backend is installed for {platform.system()}.",
        )

    def start(self, root_path: str, request_hydration: HydrationRequest) -> None:
        del root_path, request_hydration
        raise self._unsupported()

    def stop(self) -> None:
        return

    def upsert(self, item: VirtualFileDescriptor) -> None:
        del item
        raise self._unsupported()

    def remove(self, provider_id: str) -> None:
        del provider_id
        raise self._unsupported()

    def set_pinned(self, provider_id: str, pinned: bool) -> None:
        del provider_id, pinned
        raise self._unsupported()

    def materialize(
        self,
        item: VirtualFileDescriptor,
        staged_path: str,
        *,
        expected_revision: str,
    ) -> None:
        del item, staged_path, expected_revision
        raise self._unsupported()

    def evict(self, provider_id: str, *, expected_revision: str) -> None:
        del provider_id, expected_revision
        raise self._unsupported()

    def inspect(self, provider_id: str) -> NativeFileState | None:
        del provider_id
        raise self._unsupported()

    def recover(self) -> Sequence[NativeFileRecord]:
        raise self._unsupported()


def create_native_virtual_file_backend() -> VirtualFileBackend:
    """Select an installed native backend.

    Platform implementations deliberately live outside this core checkpoint. They must
    replace this selection point when their native client is added.
    """
    backend = UnsupportedVirtualFileBackend()
    raise backend._unsupported()


class _VirtualFileStore:
    def __init__(self, path: str) -> None:
        self.connection = sqlite3.connect(path, check_same_thread=False)
        self.database = Database(self.connection)
        self.records = Manager(self.database, _VirtualFileRecord)

    def close(self) -> None:
        self.connection.close()

    def get(self, provider_id: str) -> _VirtualFileRecord | None:
        return self.records.get(provider_id)

    def get_by_path(self, path_lower: str) -> _VirtualFileRecord | None:
        matches = self.records.select(
            MatchQuery(_VirtualFileRecord.path_lower, path_lower)
        )
        return matches[0] if matches else None

    def all(self) -> list[_VirtualFileRecord]:
        return self.records.select(AllQuery())

    def tree(self, path_lower: str) -> list[_VirtualFileRecord]:
        prefix = "/" if path_lower == "/" else path_lower.rstrip("/") + "/"
        return [
            record
            for record in self.all()
            if record.path_lower == path_lower or record.path_lower.startswith(prefix)
        ]

    def put(self, record: _VirtualFileRecord) -> None:
        self.records.update(record)

    def delete(self, provider_id: str) -> None:
        self.records.delete_primary_key(provider_id)


class VirtualFileController:
    """Coordinate remote metadata, hydration, and one native virtual root."""

    def __init__(
        self,
        config_name: str,
        provider: RemoteProvider,
        backend: VirtualFileBackend,
        *,
        database_path: str | None = None,
        remote_polling: bool = True,
    ) -> None:
        self.config_name = config_name
        self.provider = provider
        self._backend = backend
        self._database_path = database_path or get_data_path(
            "maestral", f"{config_name}.virtual-files"
        )
        self._state = MaestralState(config_name)
        self._store: _VirtualFileStore | None = None
        self._lock = threading.RLock()
        self._lifecycle_lock = threading.Lock()
        self._remote_lock = threading.Lock()
        self._operation_locks: dict[str, threading.Lock] = {}
        self._running = threading.Event()
        self._ready = threading.Event()
        self._stop_requested = threading.Event()
        self._worker: threading.Thread | None = None
        self._remote_polling = remote_polling
        self._root_path = ""
        self._connected = False
        self._last_worker_error = ""

    @property
    def backend_id(self) -> str:
        return self._backend.backend_id

    @property
    def supported(self) -> bool:
        return self._backend.supported

    @property
    def running(self) -> bool:
        return self._running.is_set()

    @property
    def connected(self) -> bool:
        return self.running and self._connected

    @property
    def cursor(self) -> str:
        cursor = self._state.get("virtual_files", "cursor")
        if not isinstance(cursor, str):
            raise ValueError("The virtual-file cursor is invalid")
        return cursor

    def replace_backend(self, backend: VirtualFileBackend) -> VirtualFileBackend:
        """Replace the inactive native backend before a root is started."""
        with self._lock:
            if self.running:
                raise VirtualFileBusyError(
                    "Cannot replace the virtual-file backend",
                    "Stop the virtual root first.",
                )
            old_backend = self._backend
            self._backend = backend
            return old_backend

    def replace_provider(self, provider: RemoteProvider) -> None:
        """Replace the provider before any virtual identity has been stored."""
        with self._lock:
            has_database = os.path.exists(self._database_path)
            has_records = has_database and bool(self._get_store().all())
            if self.running or has_records:
                raise VirtualFileBusyError(
                    "Cannot replace the virtual-file provider",
                    "Reset the virtual root first.",
                )
            self.provider = provider

    def reset(self) -> None:
        """Remove all local virtual state without changing remote content."""
        with self._lifecycle_lock:
            backend_started = self.running
            if backend_started:
                self._stop_requested.set()
                self._ready.clear()
                self._running.clear()
                worker = self._worker
                if worker is not None and worker is not threading.current_thread():
                    worker.join(timeout=3)
                    if worker.is_alive():
                        raise VirtualFileBusyError(
                            "Cannot reset the virtual root",
                            "A remote refresh is still active. Try again shortly.",
                        )
                self._worker = None

            try:
                with self._remote_lock:
                    # Publish an empty cursor before any destructive reset work. If the
                    # process stops after this save, the next start performs a full
                    # remote snapshot and rebuilds any missing native state.
                    self._state.set("virtual_files", "cursor", "")
                    with self._lock:
                        records = sorted(
                            self._get_store().all(),
                            key=lambda item: item.path_lower.count("/"),
                            reverse=True,
                        )
                    if backend_started:
                        for record in records:
                            self._backend.remove(record.provider_id)
                    with self._lock:
                        self._get_store().records.delete(AllQuery())
                        self._last_worker_error = ""
            finally:
                if backend_started:
                    self._connected = False
                    self._backend.stop()

    def start(self, root_path: str) -> None:
        """Start the native root and recover all durable operations."""
        if not self.supported:
            raise UnsupportedVirtualFileBackend()._unsupported()
        if not root_path:
            raise ValueError("A virtual root path is required")

        with self._lifecycle_lock:
            with self._lock:
                if self.running:
                    return
                self._root_path = root_path
                self._stop_requested.clear()
                self._ready.clear()
                self._running.set()
            try:
                self._backend.start(root_path, self._hydrate_on_open)
                pinned_to_hydrate = self._recover()
                self._ready.set()
                for provider_id in pinned_to_hydrate:
                    try:
                        self.hydrate(provider_id)
                    except MaestralApiError:
                        pass
            except BaseException:
                self._ready.clear()
                self._running.clear()
                try:
                    self._backend.stop()
                except Exception:
                    pass
                raise
            with self._lock:
                self._connected = not self._remote_polling
            if self._remote_polling:
                self._worker = threading.Thread(
                    target=self._remote_worker,
                    name=f"maestral-virtual-files-{self.config_name}",
                    daemon=True,
                )
                self._worker.start()

    def stop(self) -> None:
        """Stop remote polling and the native root."""
        with self._lifecycle_lock:
            self._stop_requested.set()
            self._ready.clear()
            self._running.clear()
            worker = self._worker
            if worker is not None and worker is not threading.current_thread():
                worker.join(timeout=3)
                if worker.is_alive():
                    raise VirtualFileBusyError(
                        "Cannot stop the virtual root",
                        "A remote refresh is still active. Try again shortly.",
                    )
            self._worker = None
            self._connected = False
            self._backend.stop()

    def close(self) -> None:
        """Stop the controller and close its lazy database connection."""
        self.stop()
        with self._lock:
            if self._store is not None:
                self._store.close()
                self._store = None

    def _get_store(self) -> _VirtualFileStore:
        if self._store is None:
            self._store = _VirtualFileStore(self._database_path)
        return self._store

    def _operation_lock(self, provider_id: str) -> threading.Lock:
        with self._lock:
            return self._operation_locks.setdefault(provider_id, threading.Lock())

    @staticmethod
    def _descriptor(record: _VirtualFileRecord) -> VirtualFileDescriptor:
        return VirtualFileDescriptor(
            provider_id=record.provider_id,
            path=record.path_cased,
            is_directory=bool(record.is_directory),
            revision=record.revision,
            content_hash=record.content_hash,
            size=record.size,
            symlink_target=record.symlink_target,
            pinned=bool(record.pinned),
        )

    @staticmethod
    def _status(record: _VirtualFileRecord) -> dict[str, object]:
        return {
            "provider_id": record.provider_id,
            "path": record.path_cased,
            "is_directory": bool(record.is_directory),
            "revision": record.revision,
            "hydration_state": record.hydration_state.value,
            "pinned": bool(record.pinned),
            "materialized_revision": record.materialized_revision,
            "generation": record.generation,
            "error": record.last_error or "",
        }

    def get_status(self, provider_id: str) -> dict[str, object]:
        """Return durable status for one stable provider identity."""
        with self._lock:
            record = self._get_store().get(provider_id)
            if record is None:
                raise VirtualFileNotFoundError(
                    "Virtual file not found",
                    f"No virtual file has provider ID {provider_id!r}.",
                )
            return self._status(record)

    def get_status_for_path(self, path_lower: str) -> dict[str, object]:
        """Return durable status for one normalised remote path."""
        with self._lock:
            record = self._get_store().get_by_path(path_lower)
            if record is None:
                raise VirtualFileNotFoundError(
                    "Virtual file not found",
                    f"No virtual file exists at {path_lower!r}.",
                )
            return self._status(record)

    def list_status(self) -> list[dict[str, object]]:
        """Return all virtual items in deterministic path order."""
        with self._lock:
            records = sorted(self._get_store().all(), key=lambda item: item.path_lower)
            return [self._status(record) for record in records]

    def summary(self) -> dict[str, object]:
        """Return a JSON-safe virtual-root status summary."""
        with self._lock:
            records = self._get_store().all() if self.supported else []
            states = {state.value: 0 for state in HydrationState}
            for record in records:
                states[record.hydration_state.value] += 1
            return {
                "backend": self.backend_id,
                "supported": self.supported,
                "running": self.running,
                "connected": self.connected,
                "cursor": self.cursor if self.supported else "",
                "items": len(records),
                "pinned": sum(bool(record.pinned) for record in records),
                "states": states,
                "error": self._last_worker_error,
            }

    def reconcile_remote_batch(self, entries: Sequence[Metadata], cursor: str) -> None:
        """Apply one ordered remote page, then publish its opaque cursor."""
        if not isinstance(cursor, str):
            raise ValueError("The virtual-file cursor must be a string")
        with self._remote_lock:
            self._reconcile_remote_batch(entries, cursor)

    def _reconcile_remote_batch(self, entries: Sequence[Metadata], cursor: str) -> None:
        """Apply a batch while preserving identities shown as delete and add."""
        live_ids = {
            entry.id
            for entry in entries
            if isinstance(entry, (FileMetadata, FolderMetadata))
        }
        for entry in entries:
            if isinstance(entry, DeletedMetadata):
                with self._lock:
                    deleted_record = self._get_store().get_by_path(entry.path_lower)
                if (
                    deleted_record is not None
                    and deleted_record.provider_id in live_ids
                ):
                    continue
            self.reconcile_remote_change(entry)
        self._state.set("virtual_files", "cursor", cursor)

    def reconcile_remote_change(self, metadata: Metadata) -> None:
        """Apply one remote change to durable state and the native root."""
        if isinstance(metadata, DeletedMetadata):
            with self._lock:
                record = self._get_store().get_by_path(metadata.path_lower)
            if record is not None:
                self._delete_remote_record(record.provider_id)
            return

        if not isinstance(metadata, (FileMetadata, FolderMetadata)):
            raise TypeError(f"Unsupported remote metadata: {type(metadata).__name__}")

        provider_id = metadata.id
        with self._lock:
            store = self._get_store()
            old = store.get(provider_id)
            destination = store.get_by_path(metadata.path_lower)

        if destination is not None and destination.provider_id != provider_id:
            self._delete_remote_record(destination.provider_id)

        is_directory = isinstance(metadata, FolderMetadata)
        if isinstance(metadata, FolderMetadata):
            revision = "folder"
            content_hash = None
            size = 0
            symlink_target = None
        else:
            revision = metadata.rev
            content_hash = metadata.content_hash
            size = metadata.size
            symlink_target = metadata.symlink_target
        if old is not None and (
            old.path_lower != metadata.path_lower
            or old.path_cased != metadata.path_display
            or bool(old.is_directory) != is_directory
            or old.revision != revision
            or old.content_hash != content_hash
            or old.size != size
            or old.symlink_target != symlink_target
        ):
            self._ensure_native_item_mutable(
                provider_id, "Cannot apply the remote file change"
            )
        native_only = is_directory or symlink_target is not None
        unchanged_revision = old is not None and old.revision == revision
        if old is not None and unchanged_revision:
            state = old.hydration_state
            materialized_revision = old.materialized_revision
        else:
            state = (
                HydrationState.Hydrated if native_only else HydrationState.OnlineOnly
            )
            materialized_revision = revision if native_only else None
        record = _VirtualFileRecord(
            provider_id=provider_id,
            path_lower=metadata.path_lower,
            path_cased=metadata.path_display,
            is_directory=int(is_directory),
            revision=revision,
            content_hash=content_hash,
            size=size,
            symlink_target=symlink_target,
            hydration_state=state,
            pinned=old.pinned if old is not None else 0,
            materialized_revision=materialized_revision,
            generation=(old.generation + 1) if old is not None else 1,
            last_error=None,
        )

        moved_descendants: list[_VirtualFileRecord] = []
        if (
            old is not None
            and bool(old.is_directory)
            and (
                old.path_lower != record.path_lower
                or old.path_cased != record.path_cased
            )
        ):
            with self._lock:
                descendants = [
                    item
                    for item in self._get_store().tree(old.path_lower)
                    if item.provider_id != old.provider_id
                ]
            for descendant in descendants:
                lower_suffix = descendant.path_lower[len(old.path_lower) :]
                cased_suffix = descendant.path_cased[len(old.path_cased) :]
                moved_descendants.append(
                    self._copy_record(
                        descendant,
                        path_lower=record.path_lower + lower_suffix,
                        path_cased=record.path_cased + cased_suffix,
                        generation=descendant.generation + 1,
                        last_error=None,
                    )
                )

        with self._lock:
            if moved_descendants:
                self._get_store().records.update_many([record, *moved_descendants])
            else:
                self._get_store().put(record)

        try:
            self._backend.upsert(self._descriptor(record))
            self._backend.set_pinned(provider_id, bool(record.pinned))
            for descendant in moved_descendants:
                self._backend.upsert(self._descriptor(descendant))
                self._backend.set_pinned(
                    descendant.provider_id, bool(descendant.pinned)
                )
        except BaseException as exc:
            self._save_error(provider_id, record.generation, exc)
            raise

        if bool(record.pinned) and not native_only and not unchanged_revision:
            self.hydrate(provider_id)

    def _delete_remote_record(self, provider_id: str) -> None:
        with self._lock:
            store = self._get_store()
            record = store.get(provider_id)
            if record is None:
                return
            records = (
                store.tree(record.path_lower) if bool(record.is_directory) else [record]
            )
            for item in records:
                self._ensure_native_item_mutable(
                    item.provider_id, "Cannot apply the remote deletion"
                )
            deleting_records = [
                self._copy_record(
                    item,
                    hydration_state=HydrationState.Deleting,
                    generation=item.generation + 1,
                    last_error=None,
                )
                for item in records
            ]
            store.records.update_many(deleting_records)

        for deleting in sorted(
            deleting_records,
            key=lambda item: item.path_lower.count("/"),
            reverse=True,
        ):
            try:
                self._backend.remove(deleting.provider_id)
            except BaseException as exc:
                self._save_error(deleting.provider_id, deleting.generation, exc)
                raise
            with self._lock:
                current = self._get_store().get(deleting.provider_id)
                if current is not None and current.generation == deleting.generation:
                    self._get_store().delete(deleting.provider_id)

    def _hydrate_on_open(
        self, provider_id: str, expected_revision: str
    ) -> dict[str, object]:
        return self.hydrate(provider_id, expected_revision=expected_revision)

    def hydrate(
        self, provider_id: str, *, expected_revision: str | None = None
    ) -> dict[str, object]:
        """Hydrate the newest revision, coalescing concurrent open requests."""
        if not self.running or not self._ready.is_set():
            raise VirtualFileBusyError(
                "Virtual root is stopped",
                "Start sync and wait for root recovery before hydration.",
            )

        lock = self._operation_lock(provider_id)
        with lock:
            while True:
                with self._lock:
                    store = self._get_store()
                    record = store.get(provider_id)
                    if (
                        record is None
                        or record.hydration_state is HydrationState.Deleting
                    ):
                        raise VirtualFileNotFoundError(
                            "Virtual file not found",
                            f"No virtual file has provider ID {provider_id!r}.",
                        )
                    if bool(record.is_directory) or record.symlink_target is not None:
                        return self._status(record)
                    if (
                        expected_revision is not None
                        and record.revision != expected_revision
                    ):
                        raise VirtualFileRevisionError(
                            "Could not hydrate this file",
                            "The native open request has an old revision.",
                        )
                    native = self._backend.inspect(provider_id)
                    if native is not None and native.dirty:
                        raise VirtualFileBusyError(
                            "Could not hydrate this file",
                            "The native file has local changes which must be preserved.",
                        )
                    if (
                        record.hydration_state is HydrationState.Hydrated
                        and record.materialized_revision == record.revision
                        and native is not None
                        and native.hydrated_revision == record.revision
                    ):
                        return self._status(record)
                    target = self._copy_record(
                        record,
                        hydration_state=HydrationState.Hydrating,
                        last_error=None,
                    )
                    store.put(target)

                try:
                    staged_path, metadata = self._download_to_stage(target)
                    try:
                        with self._lock:
                            current = self._get_store().get(provider_id)
                            if current is None or (
                                current.hydration_state is HydrationState.Deleting
                            ):
                                raise VirtualFileNotFoundError(
                                    "Virtual file was deleted",
                                    "The remote item was deleted during hydration.",
                                )
                            if (
                                current.generation != target.generation
                                or current.revision != target.revision
                            ):
                                if expected_revision is not None:
                                    raise VirtualFileRevisionError(
                                        "Could not hydrate this file",
                                        "The remote revision changed during hydration.",
                                    )
                                continue
                            descriptor = self._descriptor(current)

                        self._backend.materialize(
                            descriptor,
                            staged_path,
                            expected_revision=target.revision,
                        )

                        with self._lock:
                            current = self._get_store().get(provider_id)
                            if current is None or (
                                current.generation != target.generation
                                or current.revision != metadata.rev
                            ):
                                continue
                            hydrated = self._copy_record(
                                current,
                                hydration_state=HydrationState.Hydrated,
                                materialized_revision=current.revision,
                                last_error=None,
                            )
                            self._get_store().put(hydrated)
                            return self._status(hydrated)
                    finally:
                        try:
                            os.unlink(staged_path)
                        except FileNotFoundError:
                            pass
                except VirtualFileRevisionError:
                    with self._lock:
                        current = self._get_store().get(provider_id)
                        if (
                            current is not None
                            and current.generation != target.generation
                        ):
                            continue
                    raise
                except BaseException as exc:
                    self._save_hydration_failure(provider_id, target.generation, exc)
                    raise

    def pin(self, provider_id: str) -> dict[str, object]:
        """Persist a pin and hydrate its newest remote revision."""
        lock = self._operation_lock(provider_id)
        with lock:
            with self._lock:
                record = self._require_record(provider_id)
                if bool(record.is_directory):
                    raise VirtualFileBusyError(
                        "Cannot pin this virtual item",
                        "Pin files individually in this core version.",
                    )
                pinned = self._copy_record(record, pinned=1, last_error=None)
                self._get_store().put(pinned)
            if self.running:
                self._backend.set_pinned(provider_id, True)

        if self.running:
            return self.hydrate(provider_id)
        return self.get_status(provider_id)

    def unpin(self, provider_id: str) -> dict[str, object]:
        """Remove a durable pin without evicting cached content."""
        lock = self._operation_lock(provider_id)
        with lock:
            with self._lock:
                record = self._require_record(provider_id)
                unpinned = self._copy_record(record, pinned=0, last_error=None)
                self._get_store().put(unpinned)
            if self.running:
                self._backend.set_pinned(provider_id, False)
            return self._status(unpinned)

    def evict(self, provider_id: str) -> dict[str, object]:
        """Evict clean, closed, unpinned content through the native backend."""
        if not self.running:
            raise VirtualFileBusyError(
                "Virtual root is stopped", "Start sync before content is evicted."
            )
        lock = self._operation_lock(provider_id)
        with lock:
            with self._lock:
                record = self._require_record(provider_id)
                if bool(record.pinned):
                    raise VirtualFileBusyError(
                        "Cannot evict a pinned file", "Unpin the file first."
                    )
                if bool(record.is_directory):
                    raise VirtualFileBusyError(
                        "Cannot evict this virtual item",
                        "Only file content can be evicted.",
                    )
                native = self._backend.inspect(provider_id)
                if native is None or native.hydrated_revision is None:
                    online = self._copy_record(
                        record,
                        hydration_state=HydrationState.OnlineOnly,
                        materialized_revision=None,
                        last_error=None,
                    )
                    self._get_store().put(online)
                    return self._status(online)
                if native.dirty or native.open_count:
                    raise VirtualFileBusyError(
                        "Cannot evict this file",
                        "The native file is open or has local changes.",
                    )
                if native.hydrated_revision != record.revision:
                    raise VirtualFileRevisionError(
                        "Cannot evict this file",
                        "The native revision does not match the remote revision.",
                    )
                evicting = self._copy_record(
                    record,
                    hydration_state=HydrationState.Evicting,
                    last_error=None,
                )
                self._get_store().put(evicting)

            try:
                self._backend.evict(provider_id, expected_revision=evicting.revision)
            except BaseException as exc:
                self._save_evict_failure(provider_id, evicting.generation, exc)
                raise

            with self._lock:
                current = self._require_record(provider_id)
                if current.generation != evicting.generation:
                    return self._status(current)
                online = self._copy_record(
                    current,
                    hydration_state=HydrationState.OnlineOnly,
                    materialized_revision=None,
                    last_error=None,
                )
                self._get_store().put(online)
                return self._status(online)

    def _download_to_stage(
        self, record: _VirtualFileRecord
    ) -> tuple[str, FileMetadata]:
        staged_path = ""
        try:
            with tempfile.NamedTemporaryFile(
                mode="w+b",
                prefix=f"maestral-{self.config_name}-hydrate-",
                delete=False,
            ) as staged:
                staged_path = staged.name
                metadata = self.provider.download(
                    record.path_cased,
                    cast(BinaryIO, staged),
                    rev=record.revision,
                    provider_id=record.provider_id,
                )
                staged.flush()
                os.fsync(staged.fileno())
            if metadata.id != record.provider_id or metadata.rev != record.revision:
                raise VirtualFileRevisionError(
                    "Could not hydrate this file",
                    "The provider returned a different file revision.",
                )
            return staged_path, metadata
        except BaseException:
            if staged_path:
                try:
                    os.unlink(staged_path)
                except FileNotFoundError:
                    pass
            raise

    def _recover(self) -> list[str]:
        """Replay desired native state and settle interrupted operations."""
        pinned_to_hydrate: list[str] = []
        records = sorted(
            self._get_store().all(), key=lambda item: item.path_lower.count("/")
        )
        desired_ids = {record.provider_id for record in records}
        native_records = self._backend.recover()
        native_ids: set[str] = set()
        native_by_id: dict[str, NativeFileRecord] = {}
        for native_record in native_records:
            if not native_record.provider_id or native_record.provider_id in native_ids:
                raise ValueError("The native recovery result has invalid identities")
            native_ids.add(native_record.provider_id)
            native_by_id[native_record.provider_id] = native_record
        orphan_records = [
            native_by_id[provider_id] for provider_id in native_ids - desired_ids
        ]
        if any(record.dirty or record.open_count for record in orphan_records):
            raise VirtualFileBusyError(
                "Cannot recover the virtual root",
                "An untracked native item has local changes or is open. Its content "
                "was preserved.",
            )
        for orphan in sorted(
            orphan_records,
            key=lambda item: item.path.count("/"),
            reverse=True,
        ):
            self._backend.remove(orphan.provider_id)

        deleting = [
            record
            for record in records
            if record.hydration_state is HydrationState.Deleting
        ]
        for record in reversed(deleting):
            self._backend.remove(record.provider_id)
            self._get_store().delete(record.provider_id)

        for record in records:
            if record.hydration_state is HydrationState.Deleting:
                continue
            recovered_native = native_by_id.get(record.provider_id)
            if recovered_native is not None and recovered_native.dirty:
                preserved = self._copy_record(
                    record,
                    hydration_state=HydrationState.Hydrated,
                    materialized_revision=recovered_native.hydrated_revision,
                    last_error="Local changes were preserved after restart.",
                )
                self._get_store().put(preserved)
                continue
            descriptor = self._descriptor(record)
            self._backend.upsert(descriptor)
            self._backend.set_pinned(record.provider_id, bool(record.pinned))
            native = self._backend.inspect(record.provider_id)

            if record.hydration_state is HydrationState.Evicting:
                if native is not None and native.hydrated_revision is not None:
                    if native.dirty or native.open_count or bool(record.pinned):
                        settled = self._copy_record(
                            record,
                            hydration_state=HydrationState.Hydrated,
                            materialized_revision=record.revision,
                            last_error="An interrupted eviction is no longer safe.",
                        )
                    else:
                        self._backend.evict(
                            record.provider_id,
                            expected_revision=record.revision,
                        )
                        settled = self._copy_record(
                            record,
                            hydration_state=HydrationState.OnlineOnly,
                            materialized_revision=None,
                            last_error=None,
                        )
                else:
                    settled = self._copy_record(
                        record,
                        hydration_state=HydrationState.OnlineOnly,
                        materialized_revision=None,
                        last_error=None,
                    )
                self._get_store().put(settled)
                continue

            hydrated = (
                bool(record.is_directory)
                or record.symlink_target is not None
                or (
                    native is not None
                    and native.hydrated_revision == record.revision
                    and not native.dirty
                )
            )
            settled = self._copy_record(
                record,
                hydration_state=(
                    HydrationState.Hydrated if hydrated else HydrationState.OnlineOnly
                ),
                materialized_revision=(record.revision if hydrated else None),
                last_error=None,
            )
            self._get_store().put(settled)
            if bool(settled.pinned) and not hydrated and not bool(settled.is_directory):
                pinned_to_hydrate.append(settled.provider_id)

        return pinned_to_hydrate

    def refresh_remote(self) -> None:
        """Apply either an initial remote snapshot or incremental change pages."""
        with self._remote_lock:
            cursor = self.cursor
            if not cursor:
                existing_ids = {
                    record.provider_id for record in self._get_store().all()
                }
                seen_ids: set[str] = set()
                latest_cursor = ""
                for page in self.provider.list_folder_iterator("", recursive=True):
                    for metadata in page.entries:
                        if isinstance(metadata, (FileMetadata, FolderMetadata)):
                            seen_ids.add(metadata.id)
                        self.reconcile_remote_change(metadata)
                    latest_cursor = page.cursor
                for provider_id in sorted(existing_ids - seen_ids):
                    self._delete_remote_record(provider_id)
                self._state.set("virtual_files", "cursor", latest_cursor)
                return

            indexed_paths = {
                record.provider_id: record.path_lower
                for record in self._get_store().all()
            }
            pending_entries: list[Metadata] = []
            latest_cursor = cursor
            for page in self.provider.list_remote_changes_iterator(
                cursor, indexed_paths=indexed_paths
            ):
                pending_entries.extend(page.entries)
                latest_cursor = page.cursor
            self._reconcile_remote_batch(pending_entries, latest_cursor)

    def _remote_worker(self) -> None:
        while self._running.is_set() and not self._stop_requested.is_set():
            try:
                self.refresh_remote()
            except Exception as exc:
                self._connected = False
                self._last_worker_error = str(exc)
                self._stop_requested.wait(2)
                continue

            self._connected = True
            self._last_worker_error = ""
            cursor = self.cursor
            if not cursor:
                self._stop_requested.wait(1)
                continue
            try:
                changed = self.provider.wait_for_remote_changes(cursor, timeout=1)
            except Exception as exc:
                self._connected = False
                self._last_worker_error = str(exc)
                self._stop_requested.wait(2)
            else:
                if not changed:
                    self._stop_requested.wait(0.1)

    def _require_record(self, provider_id: str) -> _VirtualFileRecord:
        record = self._get_store().get(provider_id)
        if record is None or record.hydration_state is HydrationState.Deleting:
            raise VirtualFileNotFoundError(
                "Virtual file not found",
                f"No virtual file has provider ID {provider_id!r}.",
            )
        return record

    def _ensure_native_item_mutable(self, provider_id: str, operation: str) -> None:
        """Reject a native mutation which could destroy local content or an open file."""
        if not self.running:
            return
        native = self._backend.inspect(provider_id)
        if native is not None and (native.dirty or native.open_count):
            raise VirtualFileBusyError(
                operation,
                "The native item has local changes or is open. Its content was "
                "preserved.",
            )

    @staticmethod
    def _copy_record(
        record: _VirtualFileRecord, **changes: object
    ) -> _VirtualFileRecord:
        values = {
            column.name: getattr(record, column.name) for column in record.__columns__
        }
        values.update(changes)
        return _VirtualFileRecord(**values)

    def _save_error(
        self, provider_id: str, generation: int, exc: BaseException
    ) -> None:
        with self._lock:
            record = self._get_store().get(provider_id)
            if record is not None and record.generation == generation:
                self._get_store().put(self._copy_record(record, last_error=str(exc)))

    def _save_hydration_failure(
        self, provider_id: str, generation: int, exc: BaseException
    ) -> None:
        with self._lock:
            record = self._get_store().get(provider_id)
            if record is not None and record.generation == generation:
                self._get_store().put(
                    self._copy_record(
                        record,
                        hydration_state=HydrationState.OnlineOnly,
                        materialized_revision=None,
                        last_error=str(exc),
                    )
                )

    def _save_evict_failure(
        self, provider_id: str, generation: int, exc: BaseException
    ) -> None:
        with self._lock:
            record = self._get_store().get(provider_id)
            if record is not None and record.generation == generation:
                self._get_store().put(
                    self._copy_record(
                        record,
                        hydration_state=HydrationState.Hydrated,
                        materialized_revision=record.revision,
                        last_error=str(exc),
                    )
                )


def iter_virtual_file_status(
    controller: VirtualFileController,
) -> Iterator[dict[str, object]]:
    """Yield a stable status snapshot for transport helpers."""
    yield from controller.list_status()
