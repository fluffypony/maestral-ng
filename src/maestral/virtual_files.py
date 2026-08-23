"""Core coordination for a separate native virtual-file root."""

from __future__ import annotations

import enum
import hashlib
import os
import platform
import posixpath
import re
import sqlite3
import tempfile
import threading
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from stat import S_ISREG
from typing import BinaryIO, Protocol, cast

from .config import MaestralState
from .constants import ROOT_MARKER_FILE, ROOT_MARKER_TEMP_PREFIX
from .core import (
    DeletedMetadata,
    FileMetadata,
    FolderMetadata,
    ListFolderResult,
    Metadata,
)
from .database.core import Database
from .database.orm import Column, Manager, Model, NonNullColumn
from .database.query import AllQuery, MatchQuery
from .database.types import SqlEnum, SqlInt, SqlPath, SqlString
from .exceptions import (
    CursorResetError,
    VirtualFileBusyError,
    VirtualFileNotFoundError,
    VirtualFileRevisionError,
    VirtualFilesUnsupportedError,
)
from .providers.base import RemoteProvider
from .utils.appdirs import get_data_path
from .utils.path import is_fs_link, normalize

MIRROR_MODE = "mirror"
VIRTUAL_MODE = "virtual"
SYNC_MODES = (MIRROR_MODE, VIRTUAL_MODE)
_MAX_VIRTUAL_PATH_LENGTH = 16_384
_MAX_VIRTUAL_TOKEN_LENGTH = 1_024
_MAX_REMOTE_PAGE_ITEMS = 4_096
_MAX_REMOTE_PAGES = 100_000
_MAX_REMOTE_BATCH_ITEMS = 10_000_000
_MAX_STATUS_PAGE_ITEMS = 256
_CONTENT_HASH_PATTERN = re.compile(r"(?:[0-9a-f]{32}|[0-9a-f]{64})")


def _validate_virtual_token(value: object, name: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > _MAX_VIRTUAL_TOKEN_LENGTH
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError(f"The virtual-file {name} is invalid")
    return value


def _validate_virtual_path(
    path: object, name: str, *, allow_staging: bool = False
) -> str:
    if (
        not isinstance(path, str)
        or path == "/"
        or not path.startswith("/")
        or len(path) > _MAX_VIRTUAL_PATH_LENGTH
        or "\\" in path
        or "\x00" in path
        or posixpath.normpath(path) != path
    ):
        raise ValueError(f"The virtual-file {name} is invalid")
    components = path[1:].split("/")
    if any(
        component in {"", ".", ".."} or len(component) > 255 for component in components
    ):
        raise ValueError(f"The virtual-file {name} is invalid")
    if components[0].casefold() == ROOT_MARKER_FILE.casefold() or components[
        0
    ].casefold().startswith(ROOT_MARKER_TEMP_PREFIX.casefold()):
        raise ValueError("The virtual-file path uses a reserved root-marker name")
    if not allow_staging and components[0].casefold().startswith(".maestral-stage-"):
        raise ValueError("The virtual-file path uses a reserved staging name")
    return path


def _validate_tree_structure(paths: Mapping[str, bool], name: str) -> None:
    """Require each non-root parent to exist and to be a directory."""
    for path in paths:
        parent = posixpath.dirname(path)
        while parent != "/":
            is_directory = paths.get(parent)
            if is_directory is None:
                raise ValueError(f"The {name} has a missing parent directory")
            if not is_directory:
                raise ValueError(f"The {name} has a descendant below a file")
            parent = posixpath.dirname(parent)


def _validated_database_identity(path: str) -> tuple[int, int, int] | None:
    """Return a regular no-follow database identity after checking all ancestors."""
    absolute = os.path.abspath(path)
    candidate = absolute
    while True:
        if os.path.lexists(candidate):
            item_stat = os.lstat(candidate)
            if is_fs_link(item_stat):
                raise ValueError("The virtual-file database path uses a link")
            if candidate == absolute and not S_ISREG(item_stat.st_mode):
                raise ValueError("The virtual-file database is not a regular file")
        parent = os.path.dirname(candidate)
        if parent == candidate:
            break
        candidate = parent
    if not os.path.exists(absolute):
        return None
    item_stat = os.lstat(absolute)
    return item_stat.st_dev, item_stat.st_ino, item_stat.st_mode


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
    native_applied_generation = NonNullColumn(SqlInt())
    staging_source_path_cased = Column(SqlPath())
    staging_final_path_lower = Column(SqlPath())
    staging_final_path_cased = Column(SqlPath())
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

    identity: NativeFileIdentity
    hydrated_revision: str | None
    pinned: bool
    dirty: bool = False
    open_count: int = 0


@dataclass(frozen=True)
class NativeFileIdentity:
    """Immutable native fields required by a destructive compare-and-swap."""

    provider_id: str
    path: str
    is_directory: bool
    revision: str


@dataclass(frozen=True)
class NativeFileRecord:
    """One native placeholder returned by crash recovery enumeration."""

    provider_id: str
    path: str
    is_directory: bool
    revision: str
    hydrated_revision: str | None
    pinned: bool
    dirty: bool = False
    open_count: int = 0


@dataclass
class _KeyedOperationLock:
    lock: threading.Lock
    users: int = 0


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

    def upsert(
        self,
        item: VirtualFileDescriptor,
        *,
        expected: NativeFileIdentity | None,
    ) -> None: ...

    def remove(self, provider_id: str, *, expected: NativeFileIdentity) -> None: ...

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

    def upsert(
        self,
        item: VirtualFileDescriptor,
        *,
        expected: NativeFileIdentity | None,
    ) -> None:
        del item, expected
        raise self._unsupported()

    def remove(self, provider_id: str, *, expected: NativeFileIdentity) -> None:
        del provider_id, expected
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
    _SENTINEL_TABLE = "virtual_file_store_identity"
    _SENTINEL_VALUE = "maestral-virtual-files-v1"

    def __init__(self, path: str) -> None:
        expected_identity = _validated_database_identity(path)
        self.connection = sqlite3.connect(path, check_same_thread=False)
        actual_identity = _validated_database_identity(path)
        if actual_identity is None or (
            expected_identity is not None and actual_identity != expected_identity
        ):
            self.connection.close()
            raise ValueError("The virtual-file database changed while it was opened")
        self.identity = actual_identity
        self.database = Database(self.connection)
        try:
            with self.connection:
                self.connection.execute(
                    f"CREATE TABLE IF NOT EXISTS {self._SENTINEL_TABLE} "
                    "(identity TEXT PRIMARY KEY NOT NULL, status TEXT NOT NULL)"
                )
                sentinel = self.connection.execute(
                    f"SELECT identity, status FROM {self._SENTINEL_TABLE}"
                ).fetchall()
                records_table_exists = (
                    self.connection.execute(
                        "SELECT 1 FROM sqlite_master WHERE type = 'table' "
                        "AND name = 'virtual_files'"
                    ).fetchone()
                    is not None
                )
                if not sentinel:
                    self.connection.execute(
                        f"INSERT INTO {self._SENTINEL_TABLE} "
                        "(identity, status) VALUES (?, 'initialising')",
                        (self._SENTINEL_VALUE,),
                    )
                    self.recreated = True
                elif (
                    len(sentinel) != 1
                    or sentinel[0]["identity"] != self._SENTINEL_VALUE
                    or sentinel[0]["status"] not in {"initialising", "ready"}
                ):
                    raise ValueError("The virtual-file database identity is invalid")
                else:
                    self.recreated = (
                        sentinel[0]["status"] != "ready" or not records_table_exists
                    )
                    if self.recreated and sentinel[0]["status"] == "ready":
                        self.connection.execute(
                            f"UPDATE {self._SENTINEL_TABLE} "
                            "SET status = 'initialising'"
                        )
        except BaseException:
            self.connection.close()
            raise
        self.records = Manager(self.database, _VirtualFileRecord)
        columns = {
            row["name"]
            for row in self.database.execute(
                "PRAGMA table_info(virtual_files)"
            ).fetchall()
        }
        if "native_applied_generation" not in columns:
            self.database.execute(
                "ALTER TABLE virtual_files ADD COLUMN "
                "native_applied_generation INTEGER NOT NULL DEFAULT 0"
            )
            self.records.clear_cache()
        staging_columns = {
            "staging_source_path_cased": "BLOB",
            "staging_final_path_lower": "BLOB",
            "staging_final_path_cased": "BLOB",
        }
        for column, affinity in staging_columns.items():
            if column not in columns:
                self.database.execute(
                    f"ALTER TABLE virtual_files ADD COLUMN {column} {affinity}"
                )
                self.records.clear_cache()

    def mark_ready(self) -> None:
        """Commit schema readiness only after full-snapshot recovery is durable."""
        with self.connection:
            changed = self.connection.execute(
                f"UPDATE {self._SENTINEL_TABLE} SET status = 'ready' "
                "WHERE identity = ? AND status = 'initialising'",
                (self._SENTINEL_VALUE,),
            ).rowcount
            if changed != 1:
                row = self.connection.execute(
                    f"SELECT identity, status FROM {self._SENTINEL_TABLE}"
                ).fetchone()
                if (
                    row is None
                    or row["identity"] != self._SENTINEL_VALUE
                    or row["status"] != "ready"
                ):
                    raise ValueError("The virtual-file database identity is invalid")

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

    def status_page(
        self, after_path: str | None, limit: int
    ) -> tuple[list[_VirtualFileRecord], str | None]:
        """Return one deterministic path-keyset page and its next cursor."""
        if after_path is None:
            sql = "ORDER BY path_lower LIMIT ?"
            args: tuple[object, ...] = (limit + 1,)
        else:
            sql = "WHERE path_lower > ? ORDER BY path_lower LIMIT ?"
            args = (os.fsencode(after_path), limit + 1)
        records = self.records.select_sql(sql, *args)
        has_more = len(records) > limit
        page = records[:limit]
        next_cursor = page[-1].path_lower if has_more and page else None
        return page, next_cursor

    def summary(self) -> tuple[int, int, dict[HydrationState, int]]:
        """Return bounded SQL aggregates for the complete virtual index."""
        totals = self.database.execute(
            "SELECT COUNT(*) AS items, "
            "COALESCE(SUM(CASE WHEN pinned != 0 THEN 1 ELSE 0 END), 0) AS pinned "
            "FROM virtual_files"
        ).fetchone()
        assert totals is not None
        counts = {state: 0 for state in HydrationState}
        rows = self.database.execute(
            "SELECT hydration_state, COUNT(*) AS items FROM virtual_files "
            "GROUP BY hydration_state"
        ).fetchall()
        for row in rows:
            try:
                state = HydrationState[row["hydration_state"]]
            except (KeyError, TypeError) as exc:
                raise ValueError(
                    "The virtual-file hydration summary is invalid"
                ) from exc
            counts[state] = row["items"]
        return totals["items"], totals["pinned"], counts

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
        database_missing = not os.path.exists(self._database_path)
        needs_full_snapshot = self._state.get("virtual_files", "needs_full_snapshot")
        if not isinstance(needs_full_snapshot, bool):
            raise ValueError("The virtual-file snapshot state is invalid")
        if database_missing:
            with self._state._lock:
                self._state.set("virtual_files", "cursor", "", save=False)
                self._state.set(
                    "virtual_files", "needs_full_snapshot", True, save=False
                )
                self._state.save()
            needs_full_snapshot = True
        self._needs_full_snapshot = needs_full_snapshot
        self._snapshot_native_by_id: dict[str, NativeFileRecord] = {}
        self._store: _VirtualFileStore | None = None
        self._lock = threading.RLock()
        self._lifecycle_lock = threading.Lock()
        self._remote_lock = threading.RLock()
        self._operation_locks: dict[str, _KeyedOperationLock] = {}
        self._operation_condition = threading.Condition(self._lock)
        self._active_operations = 0
        self._generation = 0
        self._running = threading.Event()
        self._ready = threading.Event()
        self._worker: threading.Thread | None = None
        self._worker_stop: threading.Event | None = None
        self._backend_started = False
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
            if (
                self.running
                or self._backend_started
                or self._worker is not None
                or self._active_operations
            ):
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
            if (
                self.running
                or self._backend_started
                or self._worker is not None
                or self._active_operations
                or has_records
            ):
                raise VirtualFileBusyError(
                    "Cannot replace the virtual-file provider",
                    "Reset the virtual root first.",
                )
            self.provider = provider

    def reset(self) -> None:
        """Remove all local virtual state without changing remote content."""
        with self._lifecycle_lock:
            self._quiesce()
            try:
                with self._remote_lock:
                    with self._lock:
                        records = sorted(
                            self._get_store().all(),
                            key=lambda item: item.path_lower.count("/"),
                            reverse=True,
                        )
                    if self._backend_started:
                        for record in records:
                            native = self._inspect_native(record.provider_id)
                            if native is not None and (
                                native.dirty or native.open_count
                            ):
                                raise VirtualFileBusyError(
                                    "Cannot reset the virtual root",
                                    "A native item has local changes or is open. Its "
                                    "content was preserved.",
                                )
                    # Publish an empty cursor before any destructive reset work. If the
                    # process stops after this save, the next start performs a full
                    # remote snapshot and rebuilds any missing native state.
                    self._state.set("virtual_files", "cursor", "")
                    if self._backend_started:
                        for record in records:
                            self._backend.remove(
                                record.provider_id,
                                expected=self._native_identity(record),
                            )
                    with self._lock:
                        self._get_store().records.delete(AllQuery())
                        self._last_worker_error = ""
            finally:
                if self._backend_started:
                    self._backend.stop()
                    self._backend_started = False

    def detach(self) -> None:
        """Forget the virtual root while preserving every native item."""
        with self._lifecycle_lock:
            self._quiesce()
            try:
                with self._remote_lock:
                    self._state.set("virtual_files", "cursor", "")
                    with self._lock:
                        self._get_store().records.delete(AllQuery())
                        self._last_worker_error = ""
            finally:
                if self._backend_started:
                    self._backend.stop()
                    self._backend_started = False

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
                if self._worker is not None:
                    if self._worker.is_alive():
                        raise VirtualFileBusyError(
                            "Cannot start the virtual root",
                            "The prior virtual worker has not stopped.",
                        )
                    self._worker = None
                if self._backend_started:
                    raise VirtualFileBusyError(
                        "Cannot start the virtual root",
                        "Finish stopping the prior native root first.",
                    )
                self._root_path = root_path
                self._generation += 1
                generation = self._generation
                self._ready.clear()
                self._running.set()
            try:
                self._backend.start(root_path, self._hydrate_on_open)
                self._backend_started = True
                pinned_to_hydrate = self._recover()
                self._ready.set()
            except BaseException as start_error:
                self._ready.clear()
                self._running.clear()
                if self._backend_started:
                    try:
                        self._backend.stop()
                    except BaseException as stop_error:
                        self._last_worker_error = (
                            f"Root recovery failed: {start_error}. Native cleanup "
                            f"also failed: {stop_error}."
                        )
                        raise VirtualFileBusyError(
                            "Could not clean up the virtual root",
                            "Native cleanup failed after root recovery. Retry stop "
                            "before you start or replace the root.",
                        ) from stop_error
                    else:
                        self._backend_started = False
                raise
            with self._lock:
                self._connected = not self._remote_polling
            if self._remote_polling:
                worker_stop = threading.Event()
                self._worker_stop = worker_stop
                self._worker = threading.Thread(
                    target=self._remote_worker,
                    args=(worker_stop, generation),
                    name=f"maestral-virtual-files-{self.config_name}",
                    daemon=True,
                )
                self._worker.start()

        for provider_id in pinned_to_hydrate:
            try:
                self.hydrate(provider_id)
            except Exception:
                pass

    def stop(self) -> None:
        """Stop remote polling and the native root."""
        with self._lifecycle_lock:
            self._quiesce()
            with self._remote_lock:
                if self._backend_started:
                    self._backend.stop()
                    self._backend_started = False

    def close(self) -> None:
        """Stop the controller and close its lazy database connection."""
        self.stop()
        with self._lock:
            if self._store is not None:
                self._store.close()
                self._store = None

    def _get_store(self) -> _VirtualFileStore:
        with self._lock:
            if self._store is None:
                store = _VirtualFileStore(self._database_path)
                if store.recreated:
                    try:
                        with self._state._lock:
                            self._state.set("virtual_files", "cursor", "", save=False)
                            self._state.set(
                                "virtual_files",
                                "needs_full_snapshot",
                                True,
                                save=False,
                            )
                            self._state.save()
                    except BaseException:
                        store.close()
                        raise
                    self._needs_full_snapshot = True
                    self._snapshot_native_by_id.clear()
                    try:
                        store.mark_ready()
                    except BaseException:
                        store.close()
                        raise
                self._store = store
            return self._store

    @contextmanager
    def _item_operation(self, provider_id: str) -> Iterator[None]:
        """Serialize one identity and release its keyed lock after the last user."""
        with self._lock:
            entry = self._operation_locks.get(provider_id)
            if entry is None:
                entry = _KeyedOperationLock(threading.Lock())
                self._operation_locks[provider_id] = entry
            entry.users += 1
        try:
            with entry.lock:
                yield
        finally:
            with self._lock:
                entry.users -= 1
                if entry.users == 0 and self._operation_locks.get(provider_id) is entry:
                    del self._operation_locks[provider_id]

    @contextmanager
    def _native_operation(self) -> Iterator[int]:
        """Track native work so shutdown cannot close its backend."""
        with self._operation_condition:
            if not self.running or not self._ready.is_set():
                raise VirtualFileBusyError(
                    "Virtual root is stopped",
                    "Start sync and wait for root recovery before this operation.",
                )
            generation = self._generation
            self._active_operations += 1
        try:
            yield generation
        finally:
            with self._operation_condition:
                self._active_operations -= 1
                self._operation_condition.notify_all()

    def _require_active_generation(self, generation: int) -> None:
        """Reject native work after its controller generation starts stopping."""
        with self._lock:
            if not self.running or generation != self._generation:
                raise VirtualFileBusyError(
                    "Virtual root is stopping",
                    "The native operation was cancelled before it changed local "
                    "content.",
                )

    def _quiesce(self) -> None:
        """Cancel one generation and wait for all work to leave the backend."""
        with self._operation_condition:
            self._ready.clear()
            self._running.clear()
            self._connected = False
            worker_stop = self._worker_stop
            if worker_stop is not None:
                worker_stop.set()
            worker = self._worker

        if worker is not None and worker is not threading.current_thread():
            worker.join()
            with self._lock:
                if self._worker is worker:
                    self._worker = None
                    self._worker_stop = None

        with self._operation_condition:
            while self._active_operations:
                self._operation_condition.wait()

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
    def _native_identity(
        record: _VirtualFileRecord | NativeFileRecord | VirtualFileDescriptor,
    ) -> NativeFileIdentity:
        return NativeFileIdentity(
            provider_id=record.provider_id,
            path=(
                record.path_cased
                if isinstance(record, _VirtualFileRecord)
                else record.path
            ),
            is_directory=bool(record.is_directory),
            revision=record.revision,
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

    def status_page(
        self, cursor: str | None = None, limit: int = 200
    ) -> dict[str, object]:
        """Return one bounded virtual-status page in deterministic path order."""
        if cursor is not None:
            _validate_virtual_path(cursor, "status cursor", allow_staging=True)
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= _MAX_STATUS_PAGE_ITEMS
        ):
            raise ValueError(
                f"The virtual-file status limit must be from 1 to "
                f"{_MAX_STATUS_PAGE_ITEMS}"
            )
        with self._lock:
            records, next_cursor = self._get_store().status_page(cursor, limit)
            return {
                "items": [self._status(record) for record in records],
                "cursor": next_cursor,
            }

    def summary(self) -> dict[str, object]:
        """Return a JSON-safe virtual-root status summary."""
        with self._lock:
            if self.supported:
                items, pinned, state_counts = self._get_store().summary()
            else:
                items = 0
                pinned = 0
                state_counts = {state: 0 for state in HydrationState}
            return {
                "backend": self.backend_id,
                "supported": self.supported,
                "running": self.running,
                "connected": self.connected,
                "cursor": self.cursor if self.supported else "",
                "items": items,
                "pinned": pinned,
                "states": {
                    state.value: state_counts[state] for state in HydrationState
                },
                "error": self._last_worker_error,
            }

    def reconcile_remote_batch(self, entries: Sequence[Metadata], cursor: str) -> None:
        """Apply one ordered remote page, then publish its opaque cursor."""
        if not isinstance(cursor, str):
            raise ValueError("The virtual-file cursor must be a string")
        with self._remote_lock:
            if not self.running or not self._ready.is_set():
                raise VirtualFileBusyError(
                    "Virtual root is stopped",
                    "Start sync before remote changes are applied.",
                )
            self._reconcile_remote_batch(entries, cursor)

    def _reconcile_remote_batch(
        self,
        entries: Sequence[Metadata],
        cursor: str,
        *,
        publish_cursor: bool = True,
    ) -> set[str]:
        """Apply final batch targets in dependency order, then publish the cursor."""
        for entry in entries:
            self._validate_remote_metadata(entry)
        live_entries, deleted_ids, final_tree = self._fold_remote_batch(entries)
        _validate_tree_structure(final_tree, "remote batch")
        live_ids = set(live_entries)
        with self._lock:
            store = self._get_store()
            for entry in live_entries.values():
                old = store.get(entry.id)
                if old is not None:
                    self._validate_revision_binding(old, entry)

        self._stage_live_moves(tuple(live_entries.values()), deleted_ids)
        for provider_id in deleted_ids:
            self._delete_remote_record(provider_id, preserve_ids=live_ids)
        for entry in sorted(
            live_entries.values(),
            key=lambda item: (
                item.path_lower.count("/"),
                not isinstance(item, FolderMetadata),
                item.path_lower,
                item.id,
            ),
        ):
            self._reconcile_remote_change(entry)
        if publish_cursor:
            self._state.set("virtual_files", "cursor", cursor)
        return live_ids

    def _fold_remote_batch(self, entries: Sequence[Metadata]) -> tuple[
        dict[str, FileMetadata | FolderMetadata],
        list[str],
        dict[str, bool],
    ]:
        """Fold ordered remote events into their final identity and path state."""
        with self._lock:
            records = self._get_store().all()

        state_by_id = {
            record.provider_id: (record.path_lower, bool(record.is_directory))
            for record in records
        }
        id_by_path = {record.path_lower: record.provider_id for record in records}
        live_entries: dict[str, FileMetadata | FolderMetadata] = {}
        deleted_ids: dict[str, None] = {}

        def remove_identity(provider_id: str) -> None:
            state = state_by_id.get(provider_id)
            if state is None:
                return
            path, is_directory = state
            removed_ids = [provider_id]
            if is_directory:
                prefix = path.rstrip("/") + "/"
                removed_ids.extend(
                    child_id
                    for child_id, (
                        child_path,
                        _child_is_directory,
                    ) in state_by_id.items()
                    if child_id != provider_id and child_path.startswith(prefix)
                )
            for removed_id in sorted(
                removed_ids,
                key=lambda item: state_by_id[item][0].count("/"),
                reverse=True,
            ):
                removed_path, _removed_is_directory = state_by_id.pop(removed_id)
                if id_by_path.get(removed_path) == removed_id:
                    id_by_path.pop(removed_path)
                live_entries.pop(removed_id, None)
                deleted_ids[removed_id] = None

        for entry in entries:
            if isinstance(entry, DeletedMetadata):
                provider_id = id_by_path.get(entry.path_lower)
                if provider_id is None:
                    continue
                remove_identity(provider_id)
                continue
            if not isinstance(entry, (FileMetadata, FolderMetadata)):
                raise TypeError(f"Unsupported remote metadata: {type(entry).__name__}")

            is_directory = isinstance(entry, FolderMetadata)
            old_state = state_by_id.get(entry.id)
            if old_state is not None:
                old_path, old_is_directory = old_state
                if old_is_directory and not is_directory:
                    remove_identity(entry.id)
                    old_state = None
                elif old_is_directory and old_path != entry.path_lower:
                    old_prefix = old_path.rstrip("/") + "/"
                    descendants = [
                        (child_id, child_path, child_is_directory)
                        for child_id, (
                            child_path,
                            child_is_directory,
                        ) in state_by_id.items()
                        if child_id != entry.id and child_path.startswith(old_prefix)
                    ]
                    for child_id, child_path, child_is_directory in descendants:
                        id_by_path.pop(child_path, None)
                        suffix = child_path[len(old_path) :]
                        new_child_path = entry.path_lower + suffix
                        displaced_child = id_by_path.get(new_child_path)
                        if displaced_child is not None and displaced_child != child_id:
                            remove_identity(displaced_child)
                        state_by_id[child_id] = (
                            new_child_path,
                            child_is_directory,
                        )
                        id_by_path[new_child_path] = child_id
                if id_by_path.get(old_path) == entry.id:
                    id_by_path.pop(old_path)

            displaced_id = id_by_path.get(entry.path_lower)
            if displaced_id is not None and displaced_id != entry.id:
                remove_identity(displaced_id)

            state_by_id[entry.id] = (entry.path_lower, is_directory)
            id_by_path[entry.path_lower] = entry.id
            live_entries[entry.id] = entry
            deleted_ids.pop(entry.id, None)

        final_tree = {path: is_directory for path, is_directory in state_by_id.values()}
        return live_entries, list(deleted_ids), final_tree

    def _stage_live_moves(
        self,
        entries: Sequence[FileMetadata | FolderMetadata],
        deleting_ids: Sequence[str] = (),
    ) -> None:
        """Move changing native trees aside before final batch paths are applied."""
        targets_by_id = {entry.id: entry for entry in entries}
        moved_records: list[_VirtualFileRecord] = []
        with self._lock:
            deleting_directories = [
                record
                for provider_id in deleting_ids
                if (record := self._get_store().get(provider_id)) is not None
                and bool(record.is_directory)
            ]
            for entry in entries:
                record = self._get_store().get(entry.id)
                if record is not None and (
                    record.path_lower != entry.path_lower
                    or record.path_cased != entry.path_display
                    or any(
                        record.provider_id != deleting.provider_id
                        and record.path_lower.startswith(
                            deleting.path_lower.rstrip("/") + "/"
                        )
                        for deleting in deleting_directories
                    )
                ):
                    moved_records.append(record)
        moved_directories = [
            record for record in moved_records if bool(record.is_directory)
        ]
        roots = [
            record
            for record in moved_records
            if not any(
                record.provider_id != parent.provider_id
                and record.path_lower.startswith(parent.path_lower.rstrip("/") + "/")
                for parent in moved_directories
            )
        ]

        with self._lock:
            occupied_paths = {record.path_lower for record in self._get_store().all()}
        plans: list[tuple[list[_VirtualFileRecord], list[_VirtualFileRecord]]] = []
        for root in sorted(roots, key=lambda item: item.path_lower):
            with self._lock:
                records = (
                    self._get_store().tree(root.path_lower)
                    if bool(root.is_directory)
                    else [root]
                )
            for record in records:
                self._ensure_native_item_mutable(
                    record.provider_id,
                    "Cannot stage the remote file move",
                )

            digest = hashlib.sha256(root.provider_id.encode("utf-8")).hexdigest()[:16]
            counter = 0
            while True:
                suffix = f"-{counter}" if counter else ""
                temp_cased = f"/.maestral-stage-{digest}{suffix}"
                temp_lower = temp_cased.lower()
                if temp_lower not in occupied_paths:
                    break
                counter += 1
            occupied_paths.add(temp_lower)

            root_target = targets_by_id.get(root.provider_id)
            staged_records: list[_VirtualFileRecord] = []
            for record in records:
                target = targets_by_id.get(record.provider_id)
                if target is not None:
                    final_lower = target.path_lower
                    final_cased = target.path_display
                elif root_target is not None:
                    final_lower = (
                        root_target.path_lower
                        + record.path_lower[len(root.path_lower) :]
                    )
                    final_cased = (
                        root_target.path_display
                        + record.path_cased[len(root.path_cased) :]
                    )
                else:  # pragma: no cover - every staged root has a live target
                    final_lower = record.path_lower
                    final_cased = record.path_cased
                staged_records.append(
                    self._copy_record(
                        record,
                        path_lower=temp_lower
                        + record.path_lower[len(root.path_lower) :],
                        path_cased=temp_cased
                        + record.path_cased[len(root.path_cased) :],
                        generation=record.generation + 1,
                        staging_source_path_cased=record.path_cased,
                        staging_final_path_lower=final_lower,
                        staging_final_path_cased=final_cased,
                        last_error=None,
                    )
                )
            plans.append((records, staged_records))

        all_staged = [record for _old, staged in plans for record in staged]
        with self._lock:
            self._get_store().records.update_many(all_staged)

        staged_by_id = {record.provider_id: record for record in all_staged}
        for old in sorted(roots, key=lambda item: item.path_lower.count("/")):
            staged = staged_by_id[old.provider_id]
            self._backend.upsert(
                self._descriptor(staged),
                expected=self._native_identity(old),
            )
            inspected = self._inspect_native(old.provider_id)
            if inspected is None or inspected.identity != self._native_identity(staged):
                raise VirtualFileRevisionError(
                    "Cannot stage the remote file move",
                    "The native directory move did not rebase its full tree.",
                )
        for staged in all_staged:
            inspected = self._inspect_native(staged.provider_id)
            if inspected is None or inspected.identity != self._native_identity(staged):
                raise VirtualFileRevisionError(
                    "Cannot stage the remote file move",
                    "The native directory move did not rebase its full tree.",
                )
        self._mark_native_applied(all_staged)

    def _prepare_missing_database_snapshot(self, entries: Sequence[Metadata]) -> None:
        """Free native paths before a missing index applies its remote snapshot."""
        live_entries = {
            entry.id: entry
            for entry in entries
            if isinstance(entry, (FileMetadata, FolderMetadata))
        }
        final_paths = {entry.path_lower for entry in live_entries.values()}
        for entry in entries:
            self._validate_remote_metadata(entry)
        if len(final_paths) != len(live_entries):
            raise ValueError("The remote snapshot has duplicate final paths")
        _validate_tree_structure(
            {
                entry.path_lower: isinstance(entry, FolderMetadata)
                for entry in live_entries.values()
            },
            "remote snapshot",
        )

        native_records = list(self._snapshot_native_by_id.values())
        for native in native_records:
            target = live_entries.get(native.provider_id)
            target_identity = (
                self._metadata_identity(target) if target is not None else None
            )
            if (native.dirty or native.open_count) and (
                target_identity is None
                or self._native_identity(native) != target_identity
            ):
                raise VirtualFileBusyError(
                    "Cannot rebuild the virtual root",
                    "A native item has local changes or is open at a different remote "
                    "identity. Its content was preserved.",
                )

        orphans = [
            native
            for native in native_records
            if native.provider_id not in live_entries
        ]
        moved = [
            native
            for native in native_records
            if native.provider_id in live_entries
            and (
                native.path != live_entries[native.provider_id].path_display
                or native.is_directory
                != isinstance(live_entries[native.provider_id], FolderMetadata)
                or any(
                    native.provider_id != orphan.provider_id
                    and normalize(native.path).startswith(
                        normalize(orphan.path).rstrip("/") + "/"
                    )
                    for orphan in orphans
                    if orphan.is_directory
                )
            )
        ]
        occupied_paths = {
            normalize(native.path) for native in self._snapshot_native_by_id.values()
        } | final_paths
        for native in sorted(
            moved,
            key=lambda item: item.path.count("/"),
            reverse=True,
        ):
            target = live_entries[native.provider_id]
            digest = hashlib.sha256(native.provider_id.encode("utf-8")).hexdigest()[:16]
            counter = 0
            while True:
                suffix = f"-{counter}" if counter else ""
                staged_path = f"/.maestral-stage-{digest}{suffix}"
                if normalize(staged_path) not in occupied_paths:
                    break
                counter += 1
            occupied_paths.add(normalize(staged_path))
            descriptor = self._metadata_descriptor(
                target,
                pinned=native.pinned,
                path=staged_path,
            )
            self._backend.upsert(
                descriptor,
                expected=self._native_identity(native),
            )
            current = self._inspect_native(native.provider_id)
            if current is None:
                raise VirtualFileNotFoundError(
                    "Cannot rebuild the virtual root",
                    "The staged native item disappeared.",
                )
            self._snapshot_native_by_id[native.provider_id] = NativeFileRecord(
                provider_id=current.identity.provider_id,
                path=current.identity.path,
                is_directory=current.identity.is_directory,
                revision=current.identity.revision,
                hydrated_revision=current.hydrated_revision,
                pinned=current.pinned,
                dirty=current.dirty,
                open_count=current.open_count,
            )

        for orphan in sorted(
            orphans,
            key=lambda item: item.path.count("/"),
            reverse=True,
        ):
            self._backend.remove(
                orphan.provider_id,
                expected=self._native_identity(orphan),
            )
            self._snapshot_native_by_id.pop(orphan.provider_id, None)

    @staticmethod
    def _metadata_identity(
        metadata: FileMetadata | FolderMetadata,
    ) -> NativeFileIdentity:
        return NativeFileIdentity(
            provider_id=metadata.id,
            path=metadata.path_display,
            is_directory=isinstance(metadata, FolderMetadata),
            revision=(
                "folder" if isinstance(metadata, FolderMetadata) else metadata.rev
            ),
        )

    @staticmethod
    def _metadata_descriptor(
        metadata: FileMetadata | FolderMetadata,
        *,
        pinned: bool,
        path: str | None = None,
    ) -> VirtualFileDescriptor:
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
        return VirtualFileDescriptor(
            provider_id=metadata.id,
            path=path or metadata.path_display,
            is_directory=isinstance(metadata, FolderMetadata),
            revision=revision,
            content_hash=content_hash,
            size=size,
            symlink_target=symlink_target,
            pinned=pinned,
        )

    @classmethod
    def _validate_revision_binding(
        cls,
        old: _VirtualFileRecord,
        metadata: FileMetadata | FolderMetadata,
    ) -> None:
        """Require one provider revision to bind immutable content metadata."""
        descriptor = cls._metadata_descriptor(metadata, pinned=bool(old.pinned))
        if old.revision == descriptor.revision and (
            bool(old.is_directory) != descriptor.is_directory
            or old.content_hash != descriptor.content_hash
            or old.size != descriptor.size
            or old.symlink_target != descriptor.symlink_target
        ):
            raise ValueError(
                "The provider reused a virtual-file revision for different content"
            )

    def reconcile_remote_change(self, metadata: Metadata) -> None:
        """Apply one remote change to durable state and the native root."""
        with self._remote_lock:
            if not self.running or not self._ready.is_set():
                raise VirtualFileBusyError(
                    "Virtual root is stopped",
                    "Start sync before remote changes are applied.",
                )
            self._reconcile_remote_batch([metadata], self.cursor, publish_cursor=False)

    def _reconcile_remote_change(self, metadata: Metadata) -> None:
        """Apply one remote change while holding the remote-operation lock."""
        self._validate_remote_metadata(metadata)
        if isinstance(metadata, DeletedMetadata):
            with self._lock:
                record = self._get_store().get_by_path(metadata.path_lower)
            if record is not None:
                self._delete_remote_record(record.provider_id)
            return

        if not isinstance(metadata, (FileMetadata, FolderMetadata)):
            raise TypeError(f"Unsupported remote metadata: {type(metadata).__name__}")

        hydrate_required = self._upsert_remote_metadata(metadata)
        if hydrate_required:
            self.hydrate(metadata.id)

    def _upsert_remote_metadata(self, metadata: FileMetadata | FolderMetadata) -> bool:
        """Publish one validated live target while its identity is serialized."""
        provider_id = metadata.id
        with self._lock:
            store = self._get_store()
            old = store.get(provider_id)
            destination = store.get_by_path(metadata.path_lower)

        if old is not None:
            self._validate_revision_binding(old, metadata)

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
        metadata_changed = old is None or (
            old.path_lower != metadata.path_lower
            or old.path_cased != metadata.path_display
            or bool(old.is_directory) != is_directory
            or old.revision != revision
            or old.content_hash != content_hash
            or old.size != size
            or old.symlink_target != symlink_target
        )
        if old is not None and not metadata_changed:
            hydrate_required = (
                bool(old.pinned)
                and not bool(old.is_directory)
                and old.symlink_target is None
                and old.materialized_revision != old.revision
            )
            if old.native_applied_generation != old.generation:
                self._repair_native_tree(old)
            return hydrate_required
        if old is not None:
            self._ensure_native_item_mutable(
                provider_id, "Cannot apply the remote file change"
            )
        native_only = is_directory or symlink_target is not None
        unchanged_revision = old is not None and old.revision == revision
        recovered_native = (
            self._snapshot_native_by_id.get(provider_id) if old is None else None
        )
        target_identity = NativeFileIdentity(
            provider_id=provider_id,
            path=metadata.path_display,
            is_directory=is_directory,
            revision=revision,
        )
        recovered_matches = (
            recovered_native is not None
            and self._native_identity(recovered_native) == target_identity
        )
        recovered_hydrated = (
            recovered_matches
            and recovered_native is not None
            and recovered_native.hydrated_revision == revision
        )
        if old is not None and unchanged_revision:
            state = old.hydration_state
            materialized_revision = old.materialized_revision
        else:
            state = (
                HydrationState.Hydrated
                if native_only or recovered_hydrated
                else HydrationState.OnlineOnly
            )
            materialized_revision = (
                revision if native_only or recovered_hydrated else None
            )
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
            pinned=(
                old.pinned
                if old is not None
                else int(recovered_native.pinned) if recovered_native is not None else 0
            ),
            materialized_revision=materialized_revision,
            generation=(old.generation + 1) if old is not None else 1,
            native_applied_generation=(
                old.native_applied_generation if old is not None else 0
            ),
            staging_source_path_cased=None,
            staging_final_path_lower=None,
            staging_final_path_cased=None,
            last_error=(
                "Local changes or an open native file were preserved during the full "
                "snapshot."
                if recovered_native is not None
                and (recovered_native.dirty or recovered_native.open_count)
                else None
            ),
        )

        descendants: list[_VirtualFileRecord] = []
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
                        staging_source_path_cased=None,
                        staging_final_path_lower=None,
                        staging_final_path_cased=None,
                        last_error=None,
                    )
                )

        with self._lock:
            if moved_descendants:
                self._get_store().records.update_many([record, *moved_descendants])
            else:
                self._get_store().put(record)

        try:
            self._backend.upsert(
                self._descriptor(record),
                expected=(
                    self._native_identity(old)
                    if old is not None
                    else (
                        self._native_identity(self._snapshot_native_by_id[provider_id])
                        if provider_id in self._snapshot_native_by_id
                        else None
                    )
                ),
            )
            for descendant in moved_descendants:
                inspected = self._inspect_native(descendant.provider_id)
                if inspected is None or inspected.identity != self._native_identity(
                    descendant
                ):
                    raise VirtualFileRevisionError(
                        "Cannot apply the remote directory move",
                        "The native directory move did not rebase its full tree.",
                    )
        except BaseException as exc:
            self._save_error(provider_id, record.generation, exc)
            raise

        self._mark_native_applied([record, *moved_descendants])

        return bool(record.pinned) and not native_only and not unchanged_revision

    @staticmethod
    def _validate_remote_page(
        page: object, page_name: str
    ) -> tuple[list[Metadata], str, bool]:
        """Return one bounded provider page after strict shape validation."""
        if not isinstance(page, ListFolderResult):
            raise TypeError(f"The remote {page_name} result is invalid")
        if type(page.entries) is not list:
            raise TypeError(f"The remote {page_name} entries are invalid")
        if len(page.entries) > _MAX_REMOTE_PAGE_ITEMS:
            raise ValueError(f"The remote {page_name} has too many entries")
        if not isinstance(page.has_more, bool):
            raise ValueError(f"The remote {page_name} state is invalid")
        cursor = _validate_virtual_token(page.cursor, f"{page_name} cursor")
        for metadata in page.entries:
            VirtualFileController._validate_remote_metadata(metadata)
        return page.entries, cursor, page.has_more

    @staticmethod
    def _validate_full_snapshot(entries: Sequence[Metadata]) -> None:
        """Validate one authoritative live tree without consulting stored rows."""
        provider_ids: set[str] = set()
        paths: dict[str, bool] = {}
        for entry in entries:
            if not isinstance(entry, (FileMetadata, FolderMetadata)):
                raise ValueError("The remote snapshot contains a non-live entry")
            if entry.id in provider_ids:
                raise ValueError("The remote snapshot has duplicate provider IDs")
            if entry.path_lower in paths:
                raise ValueError("The remote snapshot has duplicate final paths")
            provider_ids.add(entry.id)
            paths[entry.path_lower] = isinstance(entry, FolderMetadata)
        _validate_tree_structure(paths, "remote snapshot")

    @staticmethod
    def _validate_remote_metadata(metadata: Metadata) -> None:
        """Reject remote metadata which cannot map to one contained native item."""
        if not isinstance(metadata, (DeletedMetadata, FileMetadata, FolderMetadata)):
            raise TypeError(f"Unsupported remote metadata: {type(metadata).__name__}")
        path_lower = _validate_virtual_path(metadata.path_lower, "normalised path")
        path_display = _validate_virtual_path(metadata.path_display, "display path")
        if normalize(path_display) != path_lower:
            raise ValueError("The virtual-file path forms do not match")
        if metadata.name != posixpath.basename(path_display):
            raise ValueError("The virtual-file name does not match its path")
        if isinstance(metadata, DeletedMetadata):
            return

        _validate_virtual_token(metadata.id, "provider identity")
        if isinstance(metadata, FolderMetadata):
            return

        _validate_virtual_token(metadata.rev, "revision")
        if (
            not isinstance(metadata.size, int)
            or isinstance(metadata.size, bool)
            or metadata.size < 0
            or metadata.size > 2**63 - 1
        ):
            raise ValueError("The virtual-file size is invalid")
        if (
            metadata.content_hash is not None
            and _CONTENT_HASH_PATTERN.fullmatch(metadata.content_hash) is None
        ):
            raise ValueError("The virtual-file content hash is invalid")
        if metadata.symlink_target is not None:
            raise VirtualFilesUnsupportedError(
                "Remote symbolic links are unavailable",
                "The virtual root does not create symbolic links.",
            )

    @staticmethod
    def _validate_native_record(record: NativeFileRecord) -> None:
        """Reject untrusted adapter recovery data before any native mutation."""
        VirtualFileController._validate_native_identity(
            VirtualFileController._native_identity(record)
        )
        if record.hydrated_revision is not None:
            _validate_virtual_token(
                record.hydrated_revision, "native hydrated revision"
            )
        if not isinstance(record.pinned, bool) or not isinstance(record.dirty, bool):
            raise ValueError("The native access state is invalid")
        if (
            not isinstance(record.open_count, int)
            or isinstance(record.open_count, bool)
            or record.open_count < 0
        ):
            raise ValueError("The native open count is invalid")

    @staticmethod
    def _validate_native_identity(identity: NativeFileIdentity) -> None:
        _validate_virtual_token(identity.provider_id, "native provider identity")
        _validate_virtual_path(identity.path, "native path", allow_staging=True)
        if not isinstance(identity.is_directory, bool):
            raise ValueError("The native item type is invalid")
        _validate_virtual_token(identity.revision, "native revision")

    @staticmethod
    def _validate_store_records(records: Sequence[_VirtualFileRecord]) -> None:
        """Validate the complete durable target before any native recovery call."""
        provider_ids: set[str] = set()
        paths: set[str] = set()
        path_types: dict[str, bool] = {}
        projected_path_types: dict[str, bool] = {}
        for record in records:
            provider_id = _validate_virtual_token(
                record.provider_id, "stored provider identity"
            )
            path_lower = _validate_virtual_path(
                record.path_lower,
                "stored normalised path",
                allow_staging=True,
            )
            path_cased = _validate_virtual_path(
                record.path_cased,
                "stored display path",
                allow_staging=True,
            )
            if normalize(path_cased) != path_lower:
                raise ValueError("The stored virtual-file path forms do not match")
            if provider_id in provider_ids or path_lower in paths:
                raise ValueError("The virtual-file database has duplicate identities")
            provider_ids.add(provider_id)
            paths.add(path_lower)
            if record.is_directory not in {0, 1} or record.pinned not in {0, 1}:
                raise ValueError("The stored virtual-file flags are invalid")
            path_types[path_lower] = bool(record.is_directory)
            staging_paths = (
                record.staging_source_path_cased,
                record.staging_final_path_lower,
                record.staging_final_path_cased,
            )
            if any(path is not None for path in staging_paths):
                if not all(isinstance(path, str) for path in staging_paths):
                    raise ValueError("The stored move journal is incomplete")
                source_path = _validate_virtual_path(
                    record.staging_source_path_cased,
                    "stored move source",
                )
                final_lower = _validate_virtual_path(
                    record.staging_final_path_lower,
                    "stored move target",
                )
                final_cased = _validate_virtual_path(
                    record.staging_final_path_cased,
                    "stored move display target",
                )
                if normalize(final_cased) != final_lower:
                    raise ValueError("The stored move target forms do not match")
                if not path_lower.startswith("/.maestral-stage-"):
                    raise ValueError("The stored move staging path is invalid")
                if source_path == final_cased and path_cased == final_cased:
                    raise ValueError("The stored move journal has no staged path")
                projected_path = final_lower
            else:
                projected_path = path_lower
            if projected_path in projected_path_types:
                raise ValueError("The stored move targets have duplicate paths")
            projected_path_types[projected_path] = bool(record.is_directory)
            _validate_virtual_token(record.revision, "stored revision")
            if bool(record.is_directory) != (record.revision == "folder"):
                raise ValueError("The stored folder revision is invalid")
            if (
                not isinstance(record.size, int)
                or record.size < 0
                or record.size > 2**63 - 1
            ):
                raise ValueError("The stored virtual-file size is invalid")
            if (
                record.content_hash is not None
                and _CONTENT_HASH_PATTERN.fullmatch(record.content_hash) is None
            ):
                raise ValueError("The stored virtual-file content hash is invalid")
            if record.symlink_target is not None:
                raise ValueError("Stored virtual symbolic links are unsupported")
            if not isinstance(record.hydration_state, HydrationState):
                raise ValueError("The stored hydration state is invalid")
            if record.materialized_revision is not None:
                _validate_virtual_token(
                    record.materialized_revision, "stored materialized revision"
                )
            if (
                not isinstance(record.generation, int)
                or record.generation < 1
                or not isinstance(record.native_applied_generation, int)
                or record.native_applied_generation < 0
                or record.native_applied_generation > record.generation
            ):
                raise ValueError("The stored virtual-file generation is invalid")
            if record.last_error is not None and (
                not isinstance(record.last_error, str)
                or len(record.last_error) > 4096
                or "\x00" in record.last_error
            ):
                raise ValueError("The stored virtual-file error is invalid")
        _validate_tree_structure(path_types, "virtual-file database")
        _validate_tree_structure(projected_path_types, "stored move targets")

    def _delete_remote_record(
        self, provider_id: str, *, preserve_ids: set[str] | None = None
    ) -> None:
        with self._lock:
            store = self._get_store()
            record = store.get(provider_id)
            if record is None:
                return
            records = (
                store.tree(record.path_lower) if bool(record.is_directory) else [record]
            )
            if preserve_ids:
                records = [
                    item for item in records if item.provider_id not in preserve_ids
                ]
            if not records:
                return
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
                self._backend.remove(
                    deleting.provider_id,
                    expected=self._native_identity(deleting),
                )
            except BaseException as exc:
                self._save_error(deleting.provider_id, deleting.generation, exc)
                raise
            with self._lock:
                current = self._get_store().get(deleting.provider_id)
                if current is not None and current.generation == deleting.generation:
                    self._get_store().delete(deleting.provider_id)

    def _repair_native_tree(self, record: _VirtualFileRecord) -> None:
        """Replay a durable target after an interrupted native publication."""
        with self._lock:
            records = (
                self._get_store().tree(record.path_lower)
                if bool(record.is_directory)
                else [record]
            )
        for target in sorted(records, key=lambda item: item.path_lower.count("/")):
            if target.native_applied_generation == target.generation:
                continue
            native = self._inspect_native(target.provider_id)
            self._backend.upsert(
                self._descriptor(target),
                expected=native.identity if native is not None else None,
            )
        self._mark_native_applied(records)

    def _mark_native_applied(self, records: Sequence[_VirtualFileRecord]) -> None:
        """Persist native publication only for unchanged durable generations."""
        applied: list[_VirtualFileRecord] = []
        with self._lock:
            for target in records:
                current = self._get_store().get(target.provider_id)
                if current is not None and current.generation == target.generation:
                    applied.append(
                        self._copy_record(
                            current,
                            native_applied_generation=current.generation,
                            last_error=None,
                        )
                    )
            self._get_store().records.update_many(applied)

    def _hydrate_on_open(
        self, provider_id: str, expected_revision: str
    ) -> dict[str, object]:
        return self.hydrate(provider_id, expected_revision=expected_revision)

    def hydrate(
        self, provider_id: str, *, expected_revision: str | None = None
    ) -> dict[str, object]:
        """Hydrate the newest revision, coalescing concurrent open requests."""
        with self._native_operation() as operation_generation:
            return self._hydrate_active(
                provider_id,
                expected_revision=expected_revision,
                operation_generation=operation_generation,
            )

    def _hydrate_active(
        self,
        provider_id: str,
        *,
        expected_revision: str | None,
        operation_generation: int,
    ) -> dict[str, object]:
        with self._item_operation(provider_id):
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
                    native = self._inspect_native(provider_id)
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
                        self._require_active_generation(operation_generation)
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
                except VirtualFileRevisionError as exc:
                    with self._lock:
                        current = self._get_store().get(provider_id)
                        if (
                            current is not None
                            and current.generation != target.generation
                        ):
                            continue
                    self._save_hydration_failure(provider_id, target.generation, exc)
                    raise
                except BaseException as exc:
                    self._save_hydration_failure(provider_id, target.generation, exc)
                    raise

    def pin(self, provider_id: str) -> dict[str, object]:
        """Persist a pin and hydrate its newest remote revision."""
        with self._remote_lock:
            status = self._pin(provider_id)
            hydrate = self.running
        if hydrate:
            return self.hydrate(provider_id)
        return status

    def _pin(self, provider_id: str) -> dict[str, object]:
        """Pin one item while remote reconciliation is excluded."""
        if self.running and not self._ready.is_set():
            raise VirtualFileBusyError(
                "Virtual root is starting",
                "Wait for root recovery before you pin a file.",
            )
        with self._item_operation(provider_id):
            with self._lock:
                record = self._require_record(provider_id)
                if bool(record.is_directory):
                    raise VirtualFileBusyError(
                        "Cannot pin this virtual item",
                        "Pin files individually in this core version.",
                    )
                pinned = self._copy_record(
                    record,
                    pinned=1,
                    generation=record.generation + 1,
                    last_error=None,
                )
                self._get_store().put(pinned)
            if self.running:
                with self._native_operation() as operation_generation:
                    self._require_active_generation(operation_generation)
                    self._backend.set_pinned(provider_id, True)
                    self._mark_native_applied([pinned])

        return self.get_status(provider_id)

    def unpin(self, provider_id: str) -> dict[str, object]:
        """Remove a durable pin without evicting cached content."""
        with self._remote_lock:
            return self._unpin(provider_id)

    def _unpin(self, provider_id: str) -> dict[str, object]:
        """Unpin one item while remote reconciliation is excluded."""
        if self.running and not self._ready.is_set():
            raise VirtualFileBusyError(
                "Virtual root is starting",
                "Wait for root recovery before you unpin a file.",
            )
        with self._item_operation(provider_id):
            with self._lock:
                record = self._require_record(provider_id)
                unpinned = self._copy_record(
                    record,
                    pinned=0,
                    generation=record.generation + 1,
                    last_error=None,
                )
                self._get_store().put(unpinned)
            if self.running:
                with self._native_operation() as operation_generation:
                    self._require_active_generation(operation_generation)
                    self._backend.set_pinned(provider_id, False)
                    self._mark_native_applied([unpinned])
            return self._status(unpinned)

    def evict(self, provider_id: str) -> dict[str, object]:
        """Evict clean, closed, unpinned content through the native backend."""
        with self._native_operation() as operation_generation:
            return self._evict_active(provider_id, operation_generation)

    def _evict_active(
        self, provider_id: str, operation_generation: int
    ) -> dict[str, object]:
        with self._item_operation(provider_id):
            with self._lock:
                record = self._require_record(provider_id)
                if bool(record.pinned):
                    raise VirtualFileBusyError(
                        "Cannot evict a pinned file", "Unpin the file first."
                    )
                if bool(record.is_directory) or record.symlink_target is not None:
                    raise VirtualFileBusyError(
                        "Cannot evict this virtual item",
                        "Only regular file content can be evicted.",
                    )
                native = self._inspect_native(provider_id)
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
                self._require_active_generation(operation_generation)
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
            if not isinstance(metadata, FileMetadata):
                raise VirtualFileRevisionError(
                    "Could not hydrate this file",
                    "The provider returned a non-file item.",
                )
            if metadata.id != record.provider_id or metadata.rev != record.revision:
                raise VirtualFileRevisionError(
                    "Could not hydrate this file",
                    "The provider returned a different file revision.",
                )
            try:
                self._validate_revision_binding(record, metadata)
            except ValueError:
                raise VirtualFileRevisionError(
                    "Could not hydrate this file",
                    "The provider returned different metadata for this revision.",
                ) from None
            return staged_path, metadata
        except BaseException:
            if staged_path:
                try:
                    os.unlink(staged_path)
                except FileNotFoundError:
                    pass
            raise

    @staticmethod
    def _native_record_from_state(state: NativeFileState) -> NativeFileRecord:
        return NativeFileRecord(
            provider_id=state.identity.provider_id,
            path=state.identity.path,
            is_directory=state.identity.is_directory,
            revision=state.identity.revision,
            hydrated_revision=state.hydrated_revision,
            pinned=state.pinned,
            dirty=state.dirty,
            open_count=state.open_count,
        )

    def _settle_staged_moves(
        self,
        records: Sequence[_VirtualFileRecord],
        native_by_id: dict[str, NativeFileRecord],
    ) -> list[_VirtualFileRecord]:
        """Finish durable path staging before the root becomes available."""
        staged_records = [
            record for record in records if record.staging_source_path_cased is not None
        ]
        if not staged_records:
            return list(records)

        final_records: dict[str, _VirtualFileRecord] = {}
        for record in staged_records:
            assert record.staging_source_path_cased is not None
            assert record.staging_final_path_lower is not None
            assert record.staging_final_path_cased is not None
            final_records[record.provider_id] = self._copy_record(
                record,
                path_lower=record.staging_final_path_lower,
                path_cased=record.staging_final_path_cased,
                staging_source_path_cased=None,
                staging_final_path_lower=None,
                staging_final_path_cased=None,
                native_applied_generation=0,
                last_error=None,
            )

        staged_directories = [
            record for record in staged_records if bool(record.is_directory)
        ]
        staged_roots = [
            record
            for record in staged_records
            if not any(
                record.provider_id != parent.provider_id
                and record.path_lower.startswith(parent.path_lower.rstrip("/") + "/")
                for parent in staged_directories
            )
        ]

        for record in sorted(
            staged_roots,
            key=lambda item: item.path_lower.count("/"),
        ):
            native = native_by_id.get(record.provider_id)
            if native is None:
                raise VirtualFileNotFoundError(
                    "Cannot recover a staged virtual move",
                    "The staged native item is missing.",
                )
            if native.dirty or native.open_count:
                raise VirtualFileBusyError(
                    "Cannot recover a staged virtual move",
                    "A staged native item has local changes or is open.",
                )
            source_path = record.staging_source_path_cased
            assert source_path is not None
            source_identity = NativeFileIdentity(
                provider_id=record.provider_id,
                path=source_path,
                is_directory=bool(record.is_directory),
                revision=record.revision,
            )
            native_identity = self._native_identity(native)
            final_identity = self._native_identity(final_records[record.provider_id])
            if native_identity == source_identity:
                self._backend.upsert(
                    self._descriptor(record),
                    expected=source_identity,
                )
                inspected = self._inspect_native(record.provider_id)
                if inspected is None:
                    raise VirtualFileNotFoundError(
                        "Cannot recover a staged virtual move",
                        "The native item disappeared during staging.",
                    )
                native = self._native_record_from_state(inspected)
                native_by_id[record.provider_id] = native
                native_identity = self._native_identity(native)
            if native_identity not in {self._native_identity(record), final_identity}:
                raise VirtualFileRevisionError(
                    "Cannot recover a staged virtual move",
                    "The native move identity changed after the crash.",
                )

        settled_records: list[_VirtualFileRecord] = []
        for record in sorted(
            staged_records,
            key=lambda item: final_records[item.provider_id].path_lower.count("/"),
        ):
            final = final_records[record.provider_id]
            inspected = self._inspect_native(record.provider_id)
            if inspected is None:
                raise VirtualFileNotFoundError(
                    "Cannot recover a staged virtual move",
                    "The native item disappeared before its final move.",
                )
            if inspected.dirty or inspected.open_count:
                raise VirtualFileBusyError(
                    "Cannot recover a staged virtual move",
                    "A staged native item has local changes or is open.",
                )
            native_identity = inspected.identity
            final_identity = self._native_identity(final)
            source_path = record.staging_source_path_cased
            assert source_path is not None
            allowed_identities = {
                NativeFileIdentity(
                    provider_id=record.provider_id,
                    path=source_path,
                    is_directory=bool(record.is_directory),
                    revision=record.revision,
                ),
                self._native_identity(record),
                final_identity,
            }
            for ancestor in staged_directories:
                if record.provider_id == ancestor.provider_id:
                    continue
                ancestor_prefix = ancestor.path_cased.rstrip("/") + "/"
                if not record.path_cased.startswith(ancestor_prefix):
                    continue
                suffix = record.path_cased[len(ancestor.path_cased) :]
                ancestor_final = final_records[ancestor.provider_id]
                allowed_identities.add(
                    NativeFileIdentity(
                        provider_id=record.provider_id,
                        path=ancestor_final.path_cased + suffix,
                        is_directory=bool(record.is_directory),
                        revision=record.revision,
                    )
                )
            if native_identity not in allowed_identities:
                raise VirtualFileRevisionError(
                    "Cannot recover a staged virtual move",
                    "The native move identity changed after the crash.",
                )
            if native_identity != final_identity:
                self._backend.upsert(
                    self._descriptor(final),
                    expected=native_identity,
                )
                inspected = self._inspect_native(record.provider_id)
                if inspected is None or inspected.identity != final_identity:
                    raise VirtualFileRevisionError(
                        "Cannot recover a staged virtual move",
                        "The native move did not reach its final path.",
                    )
                native_by_id[record.provider_id] = self._native_record_from_state(
                    inspected
                )
            settled_records.append(
                self._copy_record(
                    final,
                    native_applied_generation=final.generation,
                )
            )

        with self._lock:
            self._get_store().records.update_many(settled_records)

        all_records = self._get_store().all()
        self._validate_store_records(all_records)
        return all_records

    def _recover(self) -> list[str]:
        """Replay desired native state and settle interrupted operations."""
        pinned_to_hydrate: list[str] = []
        with self._lock:
            records = sorted(
                self._get_store().all(), key=lambda item: item.path_lower.count("/")
            )
        self._validate_store_records(records)
        native_records = self._backend.recover()
        native_ids: set[str] = set()
        native_paths: set[str] = set()
        native_path_types: dict[str, bool] = {}
        native_by_id: dict[str, NativeFileRecord] = {}
        for native_record in native_records:
            self._validate_native_record(native_record)
            native_path_lower = normalize(native_record.path)
            if (
                native_record.provider_id in native_ids
                or native_path_lower in native_paths
            ):
                raise ValueError("The native recovery result has invalid identities")
            native_ids.add(native_record.provider_id)
            native_paths.add(native_path_lower)
            native_path_types[native_path_lower] = native_record.is_directory
            native_by_id[native_record.provider_id] = native_record
        _validate_tree_structure(native_path_types, "native recovery result")
        records = self._settle_staged_moves(records, native_by_id)
        desired_ids = {record.provider_id for record in records}
        if self._needs_full_snapshot and not records and not native_records:
            self._state.set("virtual_files", "needs_full_snapshot", False)
            self._needs_full_snapshot = False
        if self._needs_full_snapshot:
            self._snapshot_native_by_id = native_by_id.copy()
        orphan_records = [
            native_by_id[provider_id] for provider_id in native_ids - desired_ids
        ]
        if not self._needs_full_snapshot and any(
            record.dirty or record.open_count for record in orphan_records
        ):
            raise VirtualFileBusyError(
                "Cannot recover the virtual root",
                "An untracked native item has local changes or is open. Its content "
                "was preserved.",
            )
        if not self._needs_full_snapshot:
            for orphan in sorted(
                orphan_records,
                key=lambda item: item.path.count("/"),
                reverse=True,
            ):
                self._backend.remove(
                    orphan.provider_id,
                    expected=self._native_identity(orphan),
                )

        deleting = [
            record
            for record in records
            if record.hydration_state is HydrationState.Deleting
        ]
        for record in sorted(
            deleting,
            key=lambda item: item.path_lower.count("/"),
            reverse=True,
        ):
            recovered_native = native_by_id.get(record.provider_id)
            if recovered_native is not None and (
                recovered_native.dirty or recovered_native.open_count
            ):
                preserved = self._copy_record(
                    record,
                    hydration_state=(
                        HydrationState.Hydrated
                        if recovered_native.hydrated_revision is not None
                        else HydrationState.OnlineOnly
                    ),
                    materialized_revision=recovered_native.hydrated_revision,
                    native_applied_generation=0,
                    last_error=(
                        "An interrupted deletion found local changes or an open file. "
                        "Its content was preserved."
                    ),
                )
                with self._lock:
                    self._get_store().put(preserved)
                continue
            if recovered_native is not None:
                self._backend.remove(
                    record.provider_id,
                    expected=self._native_identity(recovered_native),
                )
            with self._lock:
                self._get_store().delete(record.provider_id)

        for record in records:
            if record.hydration_state is HydrationState.Deleting:
                continue
            recovered_native = native_by_id.get(record.provider_id)
            if recovered_native is not None and (
                recovered_native.dirty or recovered_native.open_count
            ):
                preserved = self._copy_record(
                    record,
                    hydration_state=(
                        HydrationState.Hydrated
                        if recovered_native.hydrated_revision is not None
                        else HydrationState.OnlineOnly
                    ),
                    materialized_revision=recovered_native.hydrated_revision,
                    native_applied_generation=(
                        record.generation
                        if self._native_identity(recovered_native)
                        == self._native_identity(record)
                        else 0
                    ),
                    last_error=(
                        "Local changes were preserved after restart."
                        if recovered_native.dirty
                        else "An open native file was preserved after restart."
                    ),
                )
                with self._lock:
                    self._get_store().put(preserved)
                continue
            descriptor = self._descriptor(record)
            self._backend.upsert(
                descriptor,
                expected=(
                    self._native_identity(recovered_native)
                    if recovered_native is not None
                    else None
                ),
            )
            self._mark_native_applied([record])
            native = self._inspect_native(record.provider_id)

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
                with self._lock:
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
            with self._lock:
                self._get_store().put(settled)
            if bool(settled.pinned) and not hydrated and not bool(settled.is_directory):
                pinned_to_hydrate.append(settled.provider_id)

        return pinned_to_hydrate

    def refresh_remote(self) -> None:
        """Apply either an initial remote snapshot or incremental change pages."""
        with self._remote_lock:
            if not self.running or not self._ready.is_set():
                raise VirtualFileBusyError(
                    "Virtual root is stopped",
                    "Start sync before remote metadata is refreshed.",
                )
            cursor = self.cursor
            if not cursor:
                with self._lock:
                    existing_records = self._get_store().all()
                existing_ids = {record.provider_id for record in existing_records}
                latest_cursor = ""
                snapshot_entries: list[Metadata] = []
                saw_page = False
                snapshot_complete = False
                snapshot_cursors: set[str] = set()
                snapshot_pages = 0
                for page in self.provider.list_folder_iterator("", recursive=True):
                    if snapshot_complete:
                        raise ValueError("The remote snapshot continued after its end")
                    snapshot_pages += 1
                    if snapshot_pages > _MAX_REMOTE_PAGES:
                        raise ValueError("The remote snapshot has too many pages")
                    page_entries, page_cursor, has_more = self._validate_remote_page(
                        page, "snapshot page"
                    )
                    if page_cursor in snapshot_cursors:
                        raise ValueError("The remote snapshot cursor did not advance")
                    snapshot_cursors.add(page_cursor)
                    if (
                        len(snapshot_entries) + len(page_entries)
                        > _MAX_REMOTE_BATCH_ITEMS
                    ):
                        raise ValueError("The remote snapshot has too many entries")
                    latest_cursor = page_cursor
                    saw_page = True
                    snapshot_entries.extend(page_entries)
                    snapshot_complete = not has_more
                if not saw_page or not snapshot_complete:
                    raise ValueError("The remote snapshot is incomplete")
                self._validate_full_snapshot(snapshot_entries)
                if self._needs_full_snapshot:
                    self._prepare_missing_database_snapshot(snapshot_entries)
                seen_ids = self._reconcile_remote_batch(
                    snapshot_entries,
                    latest_cursor,
                    publish_cursor=False,
                )
                existing_by_id = {
                    record.provider_id: record for record in existing_records
                }
                for provider_id in sorted(
                    existing_ids - seen_ids,
                    key=lambda item: existing_by_id[item].path_lower.count("/"),
                    reverse=True,
                ):
                    self._delete_remote_record(provider_id)
                snapshot_orphans = [
                    native
                    for provider_id, native in self._snapshot_native_by_id.items()
                    if provider_id not in seen_ids
                ]
                if any(
                    native.dirty or native.open_count for native in snapshot_orphans
                ):
                    raise VirtualFileBusyError(
                        "Cannot rebuild the virtual root",
                        "An untracked native item has local changes or is open. Its "
                        "content was preserved.",
                    )
                for orphan in sorted(
                    snapshot_orphans,
                    key=lambda item: item.path.count("/"),
                    reverse=True,
                ):
                    self._backend.remove(
                        orphan.provider_id,
                        expected=self._native_identity(orphan),
                    )
                with self._state._lock:
                    self._state.set(
                        "virtual_files", "cursor", latest_cursor, save=False
                    )
                    self._state.set(
                        "virtual_files", "needs_full_snapshot", False, save=False
                    )
                    self._state.save()
                self._needs_full_snapshot = False
                self._snapshot_native_by_id.clear()
                return

            with self._lock:
                indexed_paths = {
                    record.provider_id: record.path_lower
                    for record in self._get_store().all()
                }
            pending_entries: list[Metadata] = []
            latest_cursor = cursor
            saw_page = False
            changes_complete = False
            change_cursors: set[str] = set()
            change_pages = 0
            try:
                for page in self.provider.list_remote_changes_iterator(
                    cursor, indexed_paths=indexed_paths
                ):
                    if changes_complete:
                        raise ValueError(
                            "The remote change stream continued after its end"
                        )
                    change_pages += 1
                    if change_pages > _MAX_REMOTE_PAGES:
                        raise ValueError("The remote change stream has too many pages")
                    page_entries, page_cursor, has_more = self._validate_remote_page(
                        page, "change page"
                    )
                    if page_cursor in change_cursors:
                        raise ValueError("The remote change cursor did not advance")
                    change_cursors.add(page_cursor)
                    if (
                        len(pending_entries) + len(page_entries)
                        > _MAX_REMOTE_BATCH_ITEMS
                    ):
                        raise ValueError(
                            "The remote change stream has too many entries"
                        )
                    latest_cursor = page_cursor
                    saw_page = True
                    pending_entries.extend(page_entries)
                    changes_complete = not has_more
            except CursorResetError:
                self._state.set("virtual_files", "cursor", "")
                self.refresh_remote()
                return
            if not saw_page or not changes_complete:
                raise ValueError("The remote change stream is incomplete")
            self._reconcile_remote_batch(pending_entries, latest_cursor)

    def _remote_worker(self, stop_requested: threading.Event, generation: int) -> None:
        while (
            self._running.is_set()
            and generation == self._generation
            and not stop_requested.is_set()
        ):
            try:
                self.refresh_remote()
            except Exception as exc:
                self._connected = False
                self._last_worker_error = str(exc)
                stop_requested.wait(2)
                continue

            self._connected = True
            self._last_worker_error = ""
            stop_requested.wait(10)

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
        native = self._inspect_native(provider_id)
        if native is not None and (native.dirty or native.open_count):
            raise VirtualFileBusyError(
                operation,
                "The native item has local changes or is open. Its content was "
                "preserved.",
            )

    def _inspect_native(self, provider_id: str) -> NativeFileState | None:
        """Inspect one adapter item and validate its untrusted identity and state."""
        native = self._backend.inspect(provider_id)
        if native is None:
            return None
        self._validate_native_identity(native.identity)
        if native.identity.provider_id != provider_id:
            raise ValueError("The native inspection returned a different identity")
        if native.hydrated_revision is not None:
            _validate_virtual_token(
                native.hydrated_revision, "native hydrated revision"
            )
        if not isinstance(native.pinned, bool) or not isinstance(native.dirty, bool):
            raise ValueError("The native inspection state is invalid")
        if (
            not isinstance(native.open_count, int)
            or isinstance(native.open_count, bool)
            or native.open_count < 0
        ):
            raise ValueError("The native inspection open count is invalid")
        return native

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
