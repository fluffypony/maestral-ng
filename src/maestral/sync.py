"""This module contains the main syncing functionality."""

from __future__ import annotations

# system imports
import copy
import ctypes
import enum
import errno
import gc
import hashlib
import json
import ntpath
import os
import os.path as osp
import posixpath
import random
import sqlite3
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pprint import pformat
from queue import Empty, Queue
from stat import S_ISDIR, S_ISREG
from threading import Condition, Event, RLock, current_thread
from typing import (
    Any,
    BinaryIO,
    Callable,
    Collection,
    Iterable,
    Iterator,
    Sequence,
    Type,
    TypeVar,
    cast,
    overload,
)
from uuid import uuid4

# external imports
from pathspec import PathSpec
from pathspec.pattern import Pattern
from typing_extensions import ParamSpec, TypeGuard
from watchdog.events import (
    EVENT_TYPE_CREATED,
    EVENT_TYPE_DELETED,
    EVENT_TYPE_MODIFIED,
    EVENT_TYPE_MOVED,
    DirCreatedEvent,
    DirDeletedEvent,
    DirModifiedEvent,
    DirMovedEvent,
    FileCreatedEvent,
    FileDeletedEvent,
    FileModifiedEvent,
    FileMovedEvent,
    FileSystemEvent,
    FileSystemEventHandler,
    FileSystemMovedEvent,
)

# local imports
from .config import MaestralConfig, MaestralState
from .constants import (
    CASE_CHANGE_TEMP_PREFIX,
    EXCLUDED_DIR_NAMES,
    EXCLUDED_FILE_NAMES,
    FILE_CACHE,
    IDLE,
    MIGNORE_FILE,
    MOVE_TEMP_PREFIX,
    PATH_ROOT_MIGRATION_PREFIX,
    PATH_ROOT_RECOVERY_PREFIX,
    REMOVE_TEMP_PREFIX,
    ROOT_MARKER_FILE,
    ROOT_MARKER_TEMP_PREFIX,
)
from .core import (
    DeletedMetadata,
    FileMetadata,
    FolderMetadata,
    ListFolderResult,
    Metadata,
    WriteMode,
)
from .database.core import Database
from .database.migrations import migrate_index_table
from .database.orm import Manager
from .database.query import (
    AllQuery,
    AndQuery,
    MatchQuery,
    OrQuery,
    PathTreeQuery,
    Query,
)
from .errorhandling import convert_api_errors, os_to_maestral_error
from .exceptions import (
    CacheDirError,
    CancelledError,
    DatabaseError,
    DataChangedError,
    FileConflictError,
    FolderConflictError,
    InvalidDbidError,
    IsAFolderError,
    MaestralApiError,
    NoDropboxDirError,
    NotAFolderError,
    NotFoundError,
    PathError,
    SymlinkError,
    SyncError,
)
from .logging import scoped_logger
from .models import (
    ChangeType,
    HashCacheEntry,
    IndexEntry,
    ItemType,
    SyncDirection,
    SyncErrorEntry,
    SyncEvent,
    SyncStatus,
)
from .providers.base import RemoteProvider
from .utils import exc_info_tuple, sanitize_string
from .utils.appdirs import get_data_path
from .utils.caches import LRUCache
from .utils.hashing import DropboxContentHasher
from .utils.integration import CPU_CORE_COUNT, cpu_usage_percent
from .utils.path import (
    RootedTemporaryFile,
    TreeSnapshotIdentity,
    content_hash,
    create_rooted_tempfile,
    delete,
    equal_but_for_unicode_norm,
    exists,
    generate_cc_name,
    get_existing_equivalent_paths,
    get_local_change_time,
    get_symlink_target,
    getsize,
    is_child,
    is_equal_or_child,
    is_fs_case_sensitive,
    is_fs_link,
    isdir,
    isfile,
)
from .utils.path import mkdir as rooted_mkdir
from .utils.path import (
    move,
    normalize,
    normalize_case,
    normalize_unicode,
    open_rooted_file,
    opener_no_symlink,
)
from .utils.path import rmdir as rooted_rmdir
from .utils.path import (
    rooted_item_snapshot,
    rooted_name_has_exact_case,
    rooted_tree_snapshot,
    rooted_walk,
)
from .utils.path import symlink as rooted_symlink
from .utils.path import (
    to_existing_unnormalized_path,
)
from .utils.path import unlink as rooted_unlink

__all__ = [
    "Conflict",
    "SyncDirection",
    "FSEventHandler",
    "SyncEngine",
    "ActivityNode",
    "ActivityTree",
    "pf_repr",
]

umask = os.umask(0o22)
os.umask(umask)

NUM_THREADS = min(24, CPU_CORE_COUNT)

_WINDOWS_INVALID_FILENAME_CHARS = frozenset('<>:"/\\|?*')
_WINDOWS_RESERVED_FILENAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "CLOCK$",
    "CONIN$",
    "CONOUT$",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
    *(f"COM{i}" for i in "¹²³"),
    *(f"LPT{i}" for i in "¹²³"),
}


def _snapshot_symlink_target(identity: TreeSnapshotIdentity) -> str | None:
    """Return a snapshot's link target, including for Windows reparse points."""
    content_identity = identity[6]
    if isinstance(content_identity, str) and content_identity.startswith("symlink:"):
        return content_identity.removeprefix("symlink:")
    return None


def _snapshot_is_directory(identity: TreeSnapshotIdentity) -> bool:
    """Return whether a snapshot identifies a directory rather than a link."""
    return S_ISDIR(identity[2]) and _snapshot_symlink_target(identity) is None


def _is_invalid_windows_filename(component: str) -> bool:
    if component.endswith((" ", ".")):
        return True
    if any(char in _WINDOWS_INVALID_FILENAME_CHARS for char in component):
        return True
    if any(ord(char) < 32 for char in component):
        return True

    stem = component.split(".", maxsplit=1)[0].rstrip(" ").upper()
    return stem in _WINDOWS_RESERVED_FILENAMES


P = ParamSpec("P")
T = TypeVar("T")

# ======================================================================================
# Syncing functionality
# ======================================================================================


class Conflict(enum.Enum):
    """Enumeration of sync conflict types"""

    RemoteNewer = "remote newer"
    Conflict = "conflict"
    Identical = "identical"
    LocalNewerOrIdentical = "local newer or identical"


@dataclass(frozen=True)
class _LocalDeletionResult:
    """Outcome of a local tree deletion which preserves ignored content."""

    error: Exception | None = None
    preserved: bool = False
    changed: bool = False
    conflict: bool = False


class _Ignore:
    def __init__(
        self,
        event: FileSystemEvent,
        start_time: float,
        ttl: float | None,
        recursive: bool,
    ) -> None:
        self.event = event
        self.start_time = start_time
        self.ttl = ttl
        self.recursive = recursive

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__}(event={self.event}, "
            f"recursive={self.recursive}, ttl={self.ttl})>"
        )


class FSEventHandler(FileSystemEventHandler):
    """A local file event handler

    Handles captured file events and adds them to :class:`SyncEngine`'s file event queue
    to be uploaded by :meth:`upload_worker`. This acts as a translation layer between
    :class:`watchdog.Observer` and :class:`SyncEngine`.

    White lists of event types to handle are supplied as ``file_event_types`` and
    ``dir_event_types``. This is for forward compatibility as additional event types
    may be added to watchdog in the future.

    :param file_event_types: Types of file events to handle. Acts as an allow list.
    :param dir_event_types: Types of folder events to handle. Acts as an allow list.

    :cvar float ignore_timeout: Timeout in seconds after which filters for ignored
        events will expire.
    """

    _ignored_events: set[_Ignore]
    local_file_event_queue: Queue[FileSystemEvent]

    def __init__(
        self,
        file_event_types: tuple[str, ...] = (
            EVENT_TYPE_CREATED,
            EVENT_TYPE_DELETED,
            EVENT_TYPE_MODIFIED,
            EVENT_TYPE_MOVED,
        ),
        dir_event_types: tuple[str, ...] = (
            EVENT_TYPE_CREATED,
            EVENT_TYPE_DELETED,
            EVENT_TYPE_MOVED,
        ),
    ) -> None:
        super().__init__()

        self._enabled = False
        self.has_events = Condition()

        self.file_event_types = file_event_types
        self.dir_event_types = dir_event_types

        self.file_event_types = file_event_types
        self.dir_event_types = dir_event_types

        self._ignored_events = set()
        self.ignore_timeout = 2.0
        self.local_file_event_queue = Queue()

    @property
    def enabled(self) -> bool:
        """Whether queuing of events is enabled."""
        return self._enabled

    def enable(self) -> None:
        """Turn on queueing of events."""
        self._enabled = True

    def disable(self) -> None:
        """Turn off queueing of new events and remove all events from queue."""
        self._enabled = False

        while True:
            try:
                self.local_file_event_queue.get_nowait()
            except Empty:
                break

    @contextmanager
    def ignore(
        self, *events: FileSystemEvent, recursive: bool = True
    ) -> Iterator[None]:
        """A context manager to ignore local file events

        Once a matching event has been registered, further matching events will no
        longer be ignored unless ``recursive`` is ``True``. If no matching event has
        occurred before leaving the context, the event will be ignored for
        :attr:`ignore_timeout` sec after leaving then context and then discarded. This
        accounts for possible delays in the emission of local file system events.

        This context manager is used to filter out file system events caused by maestral
        itself, for instance during a download or when moving a conflict.

        :Example:

            Prevent triggereing a sync event when creating a local file:

            >>> from watchdog.events import FileCreatedEvent
            >>> from maestral.main import Maestral
            >>> m = Maestral()
            >>> with m.sync.fs_events.ignore(FileCreatedEvent('path')):
            ...     open('path').close()

        :param events: Local events to ignore.
        :param recursive: If ``True``, all child events of a directory event will be
            ignored as well. This parameter will be ignored for file events.
        """
        now = time.time()
        new_ignores = set()
        for e in events:
            new_ignores.add(
                _Ignore(
                    event=e,
                    start_time=now,
                    ttl=None,
                    recursive=recursive and e.is_directory,
                )
            )
        self._ignored_events.update(new_ignores)

        try:
            yield
        finally:
            for ignore in new_ignores:
                ignore.ttl = time.time() + self.ignore_timeout

    def expire_ignored_events(self) -> None:
        """Removes all expired ignore entries."""
        now = time.time()
        for ignore in self._ignored_events.copy():
            if ignore.ttl and ignore.ttl < now:
                self._ignored_events.discard(ignore)

    def _is_ignored(self, event: FileSystemEvent) -> bool:
        """
        Checks if a file system event should be explicitly ignored because it was
        triggered by Maestral itself.

        :param event: Local file system event.
        :returns: Whether the event should be ignored.
        """
        for ignore in self._ignored_events.copy():
            # Remove expired ignores.
            if ignore.ttl and ignore.ttl < time.time():
                self._ignored_events.discard(ignore)
                continue

            ignore_event = ignore.event
            recursive = ignore.recursive

            # Check if event is directly marked as to ignore.
            if event == ignore_event:
                if not recursive:
                    self._ignored_events.discard(ignore)
                return True

            # Check if event is marked as to ignore by a recursive parent. For moved
            # events, source and destination path most both be children of the
            # respective paths of the event to ignore.
            elif recursive:
                if not is_equal_or_child(event.src_path, ignore_event.src_path):
                    continue

                if isinstance(event, FileSystemMovedEvent) and isinstance(
                    ignore_event, FileSystemMovedEvent
                ):
                    if not is_equal_or_child(event.dest_path, ignore_event.dest_path):
                        continue

                return True

        return False

    def on_any_event(self, event: FileSystemEvent) -> None:
        """
        Checks if the system file event should be ignored. If not, adds it to the queue
        for events to upload. If syncing is paused or stopped, all events will be
        ignored.

        :param event: Watchdog file event.
        """
        # Ignore events if asked to do so.
        if not self._enabled:
            return

        # Handle only whitelisted dir event types.
        if event.is_directory and event.event_type not in self.dir_event_types:
            return

        # Handle only whitelisted file event types.
        if not event.is_directory and event.event_type not in self.file_event_types:
            return

        # Ignore moves onto itself, they may be erroneously emitted on older versions of
        # macOS. See https://github.com/samschott/maestral/issues/671.
        if is_moved(event) and event.src_path == event.dest_path:
            return

        event_paths = [event.src_path]
        if isinstance(event, FileSystemMovedEvent):
            event_paths.append(event.dest_path)
        if any(
            osp.basename(os.fsdecode(path)).startswith(
                (
                    ROOT_MARKER_TEMP_PREFIX,
                    CASE_CHANGE_TEMP_PREFIX,
                    MOVE_TEMP_PREFIX,
                    REMOVE_TEMP_PREFIX,
                )
            )
            for path in event_paths
        ):
            return

        # Check if event should be ignored.
        if self._is_ignored(event):
            return

        self.queue_event(event)

    def queue_event(self, event: FileSystemEvent) -> None:
        """
        Queues an individual file system event. Notifies / wakes up all threads that are
        waiting with :meth:`wait_for_event`.

        :param event: File system event to queue.
        """
        with self.has_events:
            self.local_file_event_queue.put(event)
            self.has_events.notify_all()

    def wait_for_event(self, timeout: float = 40) -> bool:
        """
        Blocks until an event is available in the queue or a timeout occurs, whichever
        comes first. You can use with method to wait for file system events in another
        thread.

        .. note:: If there are multiple threads waiting for events, all of them will be
            notified. If one of those threads starts getting events from
            :attr:`local_file_event_queue`, other threads may find that the queue is
            empty despite being woken. You should therefore be prepared to handle an
            empty queue even if this method returns ``True``.

        :param timeout: Maximum time to block in seconds.
        :returns: ``True`` if an event is available, ``False`` if the call returns due
            to a timeout.
        """
        with self.has_events:
            if self.local_file_event_queue.qsize() > 0:
                return True
            self.has_events.wait(timeout)
            return self.local_file_event_queue.qsize() > 0


class ActivityNode:
    """A node in a sparse tree to represent syncing activity.

    Each node represents an item in the local Dropbox folder. Apart from the root node,
    items will only be present if they or any of their children have any sync activity.

    :attr children: All children with sync activity. Leaf nodes must represent items
        that are being uploaded, downloaded, or have a sync error.
    :attr sync_events: All SyncEvents of this node and its children.
    """

    __slots__ = ["name", "parent", "children", "sync_events"]

    def __init__(
        self,
        name: str,
        sync_events: Iterable[SyncEvent] = (),
        parent: ActivityNode | None = None,
    ) -> None:
        self.name = name
        self.parent = parent
        self.children: dict[str, ActivityNode] = {}
        self.sync_events = set(sync_events)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(name={self.name}, children={self.children}, sync_events={self.sync_events})>"


class ActivityTree(ActivityNode):
    """The root node in a sync activity tree. Represents Dropbox root."""

    def __init__(self) -> None:
        super().__init__(name="/")
        self._lock = RLock()

    def add(self, event: SyncEvent) -> None:
        with self._lock:
            parts = event.dbx_path.lstrip("/").split("/")

            # Remove any failures at this path.
            node = self.get_node(event.dbx_path)
            if node:
                failed = [e for e in node.sync_events if e.status is SyncStatus.Failed]
                for fail in failed:
                    self.remove(fail)

            # Traverse tree and create children as required.
            # Insert SyncEvent for each parent as we move down.
            current_node: ActivityNode = self
            current_node.sync_events.add(event)

            for part in parts:
                try:
                    child_node = current_node.children[part]
                except KeyError:
                    child_node = ActivityNode(part, parent=current_node)
                    current_node.children[part] = child_node
                child_node.sync_events.add(event)
                current_node = child_node

    def remove(self, event: SyncEvent) -> None:
        with self._lock:
            node = self.get_node(event.dbx_path)
            if not node:
                raise KeyError(f"No node at path {event.dbx_path}")
            if event not in node.sync_events:
                raise KeyError(f"SyncEvent not found at path {event.dbx_path}")

            # Walk tree upwards. Remove nodes if no SyncEvents remain.
            node.sync_events.remove(event)
            parent = node.parent

            while parent:
                # Remove event from parent.
                parent.sync_events.remove(event)
                # Remove node from tree if it is empty.
                if len(node.sync_events) == 0:
                    parent.children.pop(node.name)
                # Move up.
                node = parent
                parent = node.parent

    def discard(self, event: SyncEvent) -> None:
        try:
            self.remove(event)
        except KeyError:
            pass

    def has_path(self, dbx_path: str) -> bool:
        return self.get_node(dbx_path) is not None

    def get_events(self, dbx_path: str = "/") -> tuple[SyncEvent, ...]:
        """Return a stable snapshot of activity at *dbx_path*."""
        with self._lock:
            node = self.get_node(dbx_path)
            return tuple(node.sync_events) if node else ()

    def get_node(self, dbx_path: str) -> ActivityNode | None:
        if dbx_path == "/":
            return self

        with self._lock:
            parts = dbx_path.lstrip("/").split("/")
            node: ActivityNode = self
            for part in parts:
                try:
                    node = node.children[part]
                except KeyError:
                    return None

            return node


class SyncEngine:
    """Class that handles syncing with Dropbox

    Provides methods to wait for local or remote changes and sync them, including
    conflict resolution and updates to our index.

    :param client: Dropbox API client instance.
    :param event_callback: Called with each completed batch of sync events.
    """

    _max_history = 1000

    def __init__(
        self,
        client: RemoteProvider,
        event_callback: Callable[[Sequence[SyncEvent]], None] | None = None,
    ) -> None:
        self.client = client
        self.config_name = self.client.config_name
        self.fs_events = FSEventHandler()
        self._logger = scoped_logger(__name__, self.config_name)
        self._ignored_symlinks_lock = RLock()
        self._local_mutation_lock = RLock()
        self._confirmed_root_identity: tuple[int, int, int] | None = None

        self._conf = MaestralConfig(self.config_name)
        self._state = MaestralState(self.config_name)
        self._migrate_remote_cursor()
        self.reload_cached_config()

        self.event_callback = event_callback
        self.download_callback: Callable[[str], None] | None = None
        self.targeted_download_callback: Callable[[str, str], None] | None = None

        # Synchronization
        self.sync_lock = RLock()  # Upload and download cycles.
        self._db_lock = RLock()  # DB access.
        self._tree_traversal = RLock()  # Sync activity across multiple levels.
        max_parallel_downloads = self._conf.get("app", "max_parallel_downloads")
        self._parallel_down_semaphore = threading.Semaphore(max_parallel_downloads)
        max_parallel_uploads = self._conf.get("app", "max_parallel_uploads")
        self._parallel_up_semaphore = threading.Semaphore(max_parallel_uploads)

        # Data structures for internal communication.
        self._cancel_requested = Event()

        # Data structures for user information.
        self.activity = ActivityTree()

        # Initialize SQLite database.
        self._db_path = get_data_path("maestral", f"{self.config_name}.db")

        if not exists(self._db_path):
            # Reset the sync state if DB is missing.
            self.remote_cursor = ""
            self.local_cursor = 0.0

        self._connection = sqlite3.connect(self._db_path, check_same_thread=False)
        self._db = Database(self._connection)

        try:
            migrate_index_table(self._connection)
        except BaseException:
            self._connection.close()
            raise

        try:
            self._create_or_load_db()
        except DatabaseError:
            # Reading tables may fail if the DB was corrupted. This should happen very
            # rarely, see https://sqlite.org/howtocorrupt.html. Attempt to recover by
            # deleting DB and resetting sync state to trigger reindexing.

            self._logger.warning("Database error, resetting sync state")

            self._state.reset_to_defaults("sync")
            self.reload_cached_config()
            delete(self._db_path)

            self._create_or_load_db()

        self._complete_pending_sync_reset()

        # Caches.
        self._case_conversion_cache = LRUCache(capacity=5000)
        self._recover_pending_case_changes()
        self.clean_cache_dir(raise_error=False)

    def _create_or_load_db(self) -> None:
        with self._database_access():
            self._index_table = Manager(self._db, IndexEntry)
            self._history_table = Manager(self._db, SyncEvent)
            self._hash_table = Manager(self._db, HashCacheEntry)
            self._sync_errors_table = Manager(self._db, SyncErrorEntry)

    def reload_cached_config(self) -> None:
        """
        Reloads all config and state values that are otherwise cached by this class for
        faster access. Call this method if config or state values where modified
        directly instead of using :class:`SyncEngine` APIs.
        """
        dropbox_path: str = self._conf.get("sync", "path")
        if getattr(self, "_dropbox_path", None) != dropbox_path:
            self._confirmed_root_identity = None
        self._dropbox_path = dropbox_path
        self._mignore_path: str = osp.join(self._dropbox_path, MIGNORE_FILE)
        self._file_cache_path: str = osp.join(self._dropbox_path, FILE_CACHE)

        self._selective_sync_mode: str = self._conf.get("sync", "selective_sync_mode")
        if self._selective_sync_mode not in {"exclude", "include"}:
            raise ValueError(
                "Selective sync mode must be either 'exclude' or 'include'"
            )
        self._selective_sync_paths = self.clean_selective_sync_paths(
            self._conf.get("sync", "selective_sync_paths"),
            validate_local=self._selective_sync_mode != "exclude",
        )
        self._ignore_symlinks: bool = self._conf.get("sync", "ignore_symlinks")
        self._ignored_symlink_paths = self.clean_selective_sync_paths(
            self._state.get("sync", "ignored_symlink_paths")
        )
        self._max_cpu_percent: float = (
            self._conf.get("sync", "max_cpu_percent") * CPU_CORE_COUNT
        )
        self._local_cursor: float = self._state.get("sync", "lastsync")

        self._is_fs_case_sensitive = self._check_fs_case_sensitive()

        self.load_mignore_file()

    def _check_fs_case_sensitive(self) -> bool:
        try:
            return is_fs_case_sensitive(self._dropbox_path)
        except (FileNotFoundError, NotADirectoryError, ValueError):
            # Fall back to the assumption of a case-sensitive file system.
            return True

    # ==== Config access ===============================================================

    @property
    def dropbox_path(self) -> str:
        """
        Path to local Dropbox folder, as loaded from the config file. Before changing
        :attr:`dropbox_path`, make sure that syncing is paused. Move the dropbox folder
        to the new location before resuming the sync. Changes are saved to the config
        file.
        """
        return self._dropbox_path

    @dropbox_path.setter
    def dropbox_path(self, path: str) -> None:
        """Setter: dropbox_path"""

        self.set_dropbox_path(path)

    def set_dropbox_path(
        self,
        path: str,
        *,
        expected_root_identity: tuple[int, int, int] | None = None,
    ) -> None:
        """Save a Dropbox path, optionally bound to a proved directory identity."""

        path = osp.normpath(osp.abspath(osp.expanduser(path)))
        if expected_root_identity is not None:
            self._snapshot_local_item(
                path,
                path,
                expected_root_identity=expected_root_identity,
            )

        with self.sync_lock, self._conf._lock:
            save_generation = self._conf.save_generation
            try:
                self._conf.set("sync", "path", path)
            except BaseException:
                if self._conf.save_committed_since(save_generation):
                    self._publish_dropbox_path(path, expected_root_identity)
                raise
            self._publish_dropbox_path(path, expected_root_identity)

        if expected_root_identity is not None:
            self._snapshot_local_item(
                path,
                path,
                expected_root_identity=expected_root_identity,
            )

    def _publish_dropbox_path(
        self,
        path: str,
        confirmed_root_identity: tuple[int, int, int] | None = None,
    ) -> None:
        """Publish a Dropbox path which has committed to the config file."""
        self._dropbox_path = path
        self._confirmed_root_identity = confirmed_root_identity
        self._mignore_path = osp.join(self._dropbox_path, MIGNORE_FILE)
        self._file_cache_path = osp.join(self._dropbox_path, FILE_CACHE)
        self._is_fs_case_sensitive = self._check_fs_case_sensitive()

    @property
    def confirmed_root_identity(self) -> tuple[int, int, int]:
        """Return the marker-validated identity of the configured Dropbox root."""
        if self._confirmed_root_identity is None:
            self.ensure_dropbox_folder_present()
        if self._confirmed_root_identity is None:  # pragma: no cover - defensive
            raise NoDropboxDirError(
                "Dropbox folder not confirmed",
                "The configured Dropbox root could not be validated.",
            )
        return self._confirmed_root_identity

    @property
    def is_fs_case_sensitive(self) -> bool:
        """Whether the local Dropbox directory lies on a case-sensitive file system."""
        return self._is_fs_case_sensitive

    @property
    def database_path(self) -> str:
        """Path SQLite database."""
        return self._db_path

    @property
    def file_cache_path(self) -> str:
        """Path to cache folder for temporary files (read only). The cache folder
        '.maestral.cache' is located inside the local Dropbox folder to prevent file
        transfer between different partitions or drives during sync."""
        return self._file_cache_path

    @property
    def selective_sync_mode(self) -> str:
        """Selective-sync mode, either ``exclude`` or ``include``."""
        return self._selective_sync_mode

    @property
    def selective_sync_paths(self) -> set[str]:
        """Canonical paths selected by :attr:`selective_sync_mode`."""
        return self._selective_sync_paths.copy()

    def set_selective_sync(self, mode: str, dbx_paths: Collection[str]) -> None:
        """Atomically replace the selective-sync mode and its selected paths."""
        if mode not in {"exclude", "include"}:
            raise ValueError("Mode must be either 'exclude' or 'include'")

        paths = self.clean_selective_sync_paths(
            dbx_paths,
            validate_local=mode != "exclude",
        )

        with self.sync_lock, self._conf._lock:
            old_mode = self._selective_sync_mode
            old_paths = self._selective_sync_paths
            save_generation = self._conf.save_generation

            try:
                self._conf.set("sync", "selective_sync_mode", mode, save=False)
                self._conf.set(
                    "sync", "selective_sync_paths", sorted(paths), save=False
                )
                self._conf.save()
            except BaseException:
                if not self._conf.save_committed_since(save_generation):
                    self._conf.set("sync", "selective_sync_mode", old_mode, save=False)
                    self._conf.set(
                        "sync",
                        "selective_sync_paths",
                        sorted(old_paths),
                        save=False,
                    )
                else:
                    self._selective_sync_mode = mode
                    self._selective_sync_paths = paths
                raise
            else:
                self._selective_sync_mode = mode
                self._selective_sync_paths = paths

    def _validated_download_intents(self) -> dict[str, str]:
        """Return validated targeted-download intents."""
        intents = self._state.get("recovery", "download_intents")
        if not isinstance(intents, dict):
            raise ValueError("Targeted download intents must be a dictionary")
        validated: dict[str, str] = {}
        for path, intent in intents.items():
            if (
                not isinstance(path, str)
                or normalize(path) != path
                or intent not in {"include", "restore"}
            ):
                raise ValueError("Targeted download intent is invalid")
            validated[path] = intent
        return validated

    @property
    def targeted_download_paths(self) -> set[str]:
        """Return paths with durable targeted-download intents."""
        with self._local_mutation_lock:
            return set(self._validated_download_intents())

    def queue_targeted_download(self, dbx_path: str, intent: str) -> None:
        """Persist and queue an include or forced-restore download."""
        if intent not in {"include", "restore"}:
            raise ValueError("Targeted download intent must be include or restore")
        dbx_path_lower = next(
            iter(
                self.clean_selective_sync_paths(
                    [dbx_path],
                    validate_local=False,
                )
            )
        )
        if self.targeted_download_callback:
            self.targeted_download_callback(dbx_path_lower, intent)
            return

        with self._local_mutation_lock:
            self._ensure_unlink_not_pending()
            intents = self._validated_download_intents()
            current = intents.get(dbx_path_lower)
            if current != "restore":
                intents[dbx_path_lower] = intent
            self._state.set("recovery", "download_intents", intents)
            if self.download_callback:
                self.download_callback(dbx_path_lower)

    def finish_targeted_download(self, dbx_path: str) -> None:
        """Atomically clear a completed targeted download and queue record."""
        dbx_path_lower = normalize(dbx_path)
        with self._local_mutation_lock, self._state._lock:
            previous_intents = self._validated_download_intents()
            previous_pending = list(self._state.get("sync", "pending_downloads"))
            intents = previous_intents.copy()
            intents.pop(dbx_path_lower, None)
            pending = [path for path in previous_pending if path != dbx_path_lower]
            save_generation = self._state.save_generation
            try:
                self._state.set(
                    "recovery",
                    "download_intents",
                    intents,
                    save=False,
                )
                self._state.set(
                    "sync",
                    "pending_downloads",
                    pending,
                    save=False,
                )
                self._state.save()
            except BaseException:
                if not self._state.save_committed_since(save_generation):
                    self._state.set(
                        "recovery",
                        "download_intents",
                        previous_intents,
                        save=False,
                    )
                    self._state.set(
                        "sync",
                        "pending_downloads",
                        previous_pending,
                        save=False,
                    )
                raise

    def _targeted_download_intent(self, dbx_path_lower: str) -> str | None:
        """Return the nearest active targeted-download intent for a path."""
        with self._local_mutation_lock:
            intents = self._validated_download_intents()
        matching = {
            path: intent
            for path, intent in intents.items()
            if is_equal_or_child(dbx_path_lower, path)
        }
        if not matching:
            return None
        nearest_path = max(matching, key=lambda path: path.count("/"))
        return matching[nearest_path]

    def _validated_recovered_local_paths(self) -> dict[str, dict[str, Any]]:
        """Return validated visible recovery paths which still need upload."""
        paths = self._state.get("recovery", "local_paths")
        if not isinstance(paths, dict) or not all(
            isinstance(path_lower, str)
            and normalize(path_lower) == path_lower
            and isinstance(entry, dict)
            and isinstance(entry.get("path"), str)
            and self._is_valid_cased_dbx_path(entry["path"])
            and normalize(entry["path"]) == path_lower
            and isinstance(entry.get("identity"), list)
            and len(entry["identity"]) == 3
            and all(isinstance(value, int) for value in entry["identity"])
            and entry.get("phase") in {"reserved", "tracked"}
            and isinstance(entry.get("source"), str)
            and (not entry["source"] or self._is_valid_cased_dbx_path(entry["source"]))
            and (entry["phase"] == "reserved") == bool(entry["source"])
            for path_lower, entry in paths.items()
        ):
            raise ValueError("Recovered local paths are invalid")
        return copy.deepcopy(paths)

    def _is_valid_cased_dbx_path(self, dbx_path: str) -> bool:
        """Return whether a cased Dropbox path is safe to map below the sync root."""
        if "\0" in dbx_path:
            return False
        try:
            self.to_local_path_from_cased(dbx_path)
        except ValueError:
            return False
        return True

    def _record_recovered_local_path(
        self,
        local_path: str,
        expected_identity: Collection[int],
        *,
        source_path: str | None = None,
    ) -> str:
        """Persist a visible recovery path before moving local data there."""
        identity = list(tuple(expected_identity)[:3])
        if len(identity) != 3 or any(not isinstance(value, int) for value in identity):
            raise ValueError("Recovered local path identity is invalid")
        dbx_path_lower = self.to_dbx_path_lower(local_path)
        dbx_path_cased = self.to_dbx_path(local_path)
        source_dbx_path = self.to_dbx_path(source_path) if source_path else ""
        with self._local_mutation_lock:
            self._ensure_unlink_not_pending()
            paths = self._validated_recovered_local_paths()
            existing = paths.get(dbx_path_lower)
            if source_path and existing is not None:
                if (
                    existing["phase"] == "reserved"
                    and existing["identity"] == identity
                    and existing["source"] == source_dbx_path
                ):
                    return dbx_path_lower
                raise FileExistsError(local_path)
            paths[dbx_path_lower] = {
                "path": dbx_path_cased,
                "identity": identity,
                "phase": "reserved" if source_path else "tracked",
                "source": source_dbx_path,
            }
            self._state.set("recovery", "local_paths", paths)
        return dbx_path_lower

    def _forget_recovered_local_path(
        self,
        dbx_path_lower: str,
        *,
        expected_identity: Collection[int] | None = None,
        expected_source: str | None = None,
    ) -> bool:
        """Remove one visible recovery path after its upload or failed reservation."""
        with self._local_mutation_lock:
            paths = self._validated_recovered_local_paths()
            entry = paths.get(dbx_path_lower)
            if entry is None:
                return False
            if expected_identity is not None and entry["identity"] != list(
                tuple(expected_identity)[:3]
            ):
                return False
            if expected_source is not None:
                if entry["source"] != expected_source:
                    return False
            paths.pop(dbx_path_lower)
            self._state.set("recovery", "local_paths", paths)
            return True

    def _activate_recovered_local_path(
        self,
        dbx_path_lower: str,
        *,
        expected_identity: Collection[int] | None = None,
        expected_source: str | None = None,
    ) -> bool:
        """Mark a filled recovery reservation as a visible tracked item."""
        with self._local_mutation_lock:
            paths = self._validated_recovered_local_paths()
            entry = paths.get(dbx_path_lower)
            if entry is None:
                return False
            if expected_identity is not None and entry["identity"] != list(
                tuple(expected_identity)[:3]
            ):
                return False
            if expected_source is not None:
                if entry["source"] != expected_source:
                    return False
            if entry["phase"] == "tracked":
                return True
            entry["phase"] = "tracked"
            entry["source"] = ""
            paths[dbx_path_lower] = entry
            self._state.set("recovery", "local_paths", paths)
            return True

    def _recovered_path_identity(
        self,
        dbx_path_cased: str,
    ) -> TreeSnapshotIdentity | None:
        """Read one recovery path identity and propagate uncertain I/O errors."""
        local_path = self.to_local_path_from_cased(dbx_path_cased)
        try:
            return self._snapshot_local_item(
                local_path,
                self.dropbox_path,
                expected_root_identity=self.confirmed_root_identity,
            )
        except (FileNotFoundError, NotADirectoryError):
            return None

    def _recovered_local_path_matches(
        self,
        entry: dict[str, Any],
    ) -> bool:
        """Return whether a recovery path still names its recorded item."""
        identity = self._recovered_path_identity(entry["path"])
        return identity is not None and list(identity[:3]) == entry["identity"]

    def _reserved_recovered_local_root(self, dbx_path_lower: str) -> str | None:
        """Return the nearest recovery reservation, including an unfilled one."""
        with self._local_mutation_lock:
            paths = self._validated_recovered_local_paths()
        matching = {path for path in paths if is_equal_or_child(dbx_path_lower, path)}
        return max(matching, key=lambda path: path.count("/"), default=None)

    def _recovered_local_root(self, dbx_path_lower: str) -> str | None:
        """Return the nearest visible recovery root for a Dropbox path."""
        with self._local_mutation_lock:
            paths = self._validated_recovered_local_paths()
        matching = {
            path
            for path, entry in paths.items()
            if is_equal_or_child(dbx_path_lower, path)
            and self._recovered_local_path_matches(entry)
        }
        return max(matching, key=lambda path: path.count("/"), default=None)

    def _has_recovered_local_descendant(self, dbx_path_lower: str) -> bool:
        """Return whether a durable visible recovery root is below a path."""
        with self._local_mutation_lock:
            paths = self._validated_recovered_local_paths()
        return any(
            is_child(path, dbx_path_lower) and self._recovered_local_path_matches(entry)
            for path, entry in paths.items()
        )

    def _tracked_recovered_local_root(self, dbx_path_lower: str) -> str | None:
        """Return the nearest saved tracked root without a pathname identity check."""
        with self._local_mutation_lock:
            paths = self._validated_recovered_local_paths()
        matching = {
            path
            for path, entry in paths.items()
            if entry["phase"] == "tracked" and is_equal_or_child(dbx_path_lower, path)
        }
        return max(matching, key=lambda path: path.count("/"), default=None)

    def _has_tracked_recovered_local_descendant(self, dbx_path_lower: str) -> bool:
        """Return whether a saved tracked recovery root is below a path."""
        with self._local_mutation_lock:
            paths = self._validated_recovered_local_paths()
        return any(
            entry["phase"] == "tracked" and is_child(path, dbx_path_lower)
            for path, entry in paths.items()
        )

    def _transfer_recovered_local_paths(
        self,
        source_lower: str,
        destination_lower: str,
        destination_cased: str,
    ) -> list[str]:
        """Move saved recovery roots with a locally moved ancestor."""
        if normalize(destination_cased) != destination_lower:
            raise ValueError("The recovery destination casing is invalid")
        source_depth = len([part for part in source_lower.split("/") if part])
        destination_parts = [part for part in destination_cased.split("/") if part]

        def moved_path(path_cased: str) -> str:
            path_parts = [part for part in path_cased.split("/") if part]
            parts = [*destination_parts, *path_parts[source_depth:]]
            return "/" + "/".join(parts) if parts else "/"

        with self._local_mutation_lock:
            paths = self._validated_recovered_local_paths()
            moving = {
                path: entry
                for path, entry in paths.items()
                if is_equal_or_child(path, source_lower)
            }
            if not moving:
                return []

            for path in moving:
                paths.pop(path)

            local_paths: list[str] = []
            for old_path_lower, entry in moving.items():
                new_path_cased = moved_path(entry["path"])
                new_path_lower = normalize(new_path_cased)
                old_identity = self._recovered_path_identity(entry["path"])
                new_identity = self._recovered_path_identity(new_path_cased)
                expected_identity = entry["identity"]
                old_matches = (
                    old_identity is not None
                    and list(old_identity[:3]) == expected_identity
                )
                new_matches = (
                    new_identity is not None
                    and list(new_identity[:3]) == expected_identity
                )

                if old_matches:
                    paths[old_path_lower] = entry
                elif old_identity is not None:
                    rebound_old_entry = entry.copy()
                    rebound_old_entry["identity"] = list(old_identity[:3])
                    rebound_old_entry["phase"] = "tracked"
                    rebound_old_entry["source"] = ""
                    paths[old_path_lower] = rebound_old_entry
                    local_paths.append(self.to_local_path_from_cased(entry["path"]))

                if new_identity is not None and (not old_matches or new_matches):
                    new_entry = entry.copy()
                    new_entry["path"] = new_path_cased
                    if new_matches:
                        source = new_entry["source"]
                        if source and is_equal_or_child(
                            normalize(source), source_lower
                        ):
                            new_entry["source"] = moved_path(source)
                    else:
                        new_entry["identity"] = list(new_identity[:3])
                        new_entry["phase"] = "tracked"
                        new_entry["source"] = ""
                    paths[new_path_lower] = new_entry
                    local_paths.append(self.to_local_path_from_cased(new_path_cased))
                elif old_identity is None:
                    paths[old_path_lower] = entry

            self._state.set("recovery", "local_paths", paths)
            return list(dict.fromkeys(local_paths))

    def rescan_recovered_local_paths(self) -> None:
        """Queue every durable visible recovery path before startup downloads."""
        for dbx_path_lower, entry in tuple(
            self._validated_recovered_local_paths().items()
        ):
            identity = self._recovered_path_identity(entry["path"])
            identity_matches = (
                identity is not None and list(identity[:3]) == entry["identity"]
            )
            if identity_matches:
                self._activate_recovered_local_path(
                    dbx_path_lower,
                    expected_identity=entry["identity"],
                    expected_source=entry["source"],
                )
                local_path = self.to_local_path_from_cased(entry["path"])
                self.rescan(local_path)
            elif entry["phase"] == "tracked":
                local_path = self.to_local_path_from_cased(entry["path"])
                if identity is None:
                    self._forget_recovered_local_path(
                        dbx_path_lower,
                        expected_identity=entry["identity"],
                        expected_source=entry["source"],
                    )
                else:
                    self._record_recovered_local_path(
                        local_path,
                        identity[:3],
                    )
                self.rescan(local_path)
            else:
                source_identity = self._recovered_path_identity(entry["source"])
                source_matches = (
                    source_identity is not None
                    and list(source_identity[:3]) == entry["identity"]
                )
                if source_matches or identity is None:
                    self._forget_recovered_local_path(
                        dbx_path_lower,
                        expected_identity=entry["identity"],
                        expected_source=entry["source"],
                    )
                else:
                    local_path = self.to_local_path_from_cased(entry["path"])
                    self._record_recovered_local_path(
                        local_path,
                        identity[:3],
                    )
                    self.rescan(local_path)

    def _maybe_finish_recovered_local_path(self, dbx_path_lower: str) -> None:
        """Clear every proved recovery ancestor after a successful upload."""
        paths = self._validated_recovered_local_paths()
        recovery_roots = sorted(
            (
                path
                for path, entry in paths.items()
                if is_equal_or_child(dbx_path_lower, path)
                and self._recovered_local_path_matches(entry)
            ),
            key=lambda path: path.count("/"),
            reverse=True,
        )
        for recovery_root in recovery_roots:
            entry = paths[recovery_root]
            if self.sync_errors_for_path(
                recovery_root,
                direction=SyncDirection.Up,
            ):
                continue
            root_entry = self.get_index_entry(recovery_root)
            if root_entry is None:
                continue
            local_root = self.to_local_path_from_cased(root_entry.dbx_path_cased)
            try:
                snapshot = self._snapshot_local_tree(local_root)
            except OSError:
                continue
            if not snapshot or not self._snapshot_matches_index(snapshot, local_root):
                continue
            with self._local_mutation_lock:
                cleared = self._forget_recovered_local_path(
                    recovery_root,
                    expected_identity=entry["identity"],
                    expected_source=entry["source"],
                )
                if not cleared:
                    continue
                try:
                    current_snapshot = self._snapshot_local_tree(local_root)
                except OSError:
                    self._record_recovered_local_path(
                        local_root,
                        entry["identity"],
                    )
                    self.rescan(local_root)
                    continue
                if current_snapshot == snapshot:
                    continue
                current_identity = current_snapshot.get(local_root)
                if current_identity is not None:
                    self._record_recovered_local_path(
                        local_root,
                        current_identity[:3],
                    )
                    self.rescan(local_root)

    def clean_selective_sync_paths(
        self,
        dbx_paths: Collection[str],
        *,
        validate_local: bool = True,
    ) -> set[str]:
        """Validate and normalise Dropbox paths without redundant children."""
        paths: set[str] = set()

        for path in dbx_paths:
            if not isinstance(path, str):
                raise TypeError("Selective-sync paths must be strings")
            if path == "/":
                paths.add(path)
                continue
            if (
                not path.startswith("/")
                or path.endswith("/")
                or "//" in path
                or "\\" in path
                or "\0" in path
            ):
                raise ValueError(f"Invalid Dropbox path: {path!r}")

            parts = path[1:].split("/")
            if any(part in {"", ".", ".."} for part in parts):
                raise ValueError(f"Invalid Dropbox path: {path!r}")

            if validate_local:
                # Apply the platform-specific checks used for local path conversion.
                self.to_local_path_from_cased(path)
            paths.add(normalize(path))

        for path in paths.copy():
            paths = {candidate for candidate in paths if not is_child(candidate, path)}

        return paths

    @staticmethod
    def _is_path_excluded(
        dbx_path_lower: str, mode: str, selected_paths: Collection[str]
    ) -> bool:
        if mode == "exclude":
            return any(
                is_equal_or_child(dbx_path_lower, path) for path in selected_paths
            )

        is_selected = any(
            is_equal_or_child(dbx_path_lower, path) for path in selected_paths
        )
        is_required_parent = any(
            is_child(path, dbx_path_lower) for path in selected_paths
        )
        return not (is_selected or is_required_parent)

    @staticmethod
    def _is_path_managed(
        dbx_path_lower: str, mode: str, selected_paths: Collection[str]
    ) -> bool:
        """Return whether a path and its descendants are fully managed."""
        if SyncEngine._is_path_excluded(dbx_path_lower, mode, selected_paths):
            return False
        if mode == "exclude":
            return True
        return any(is_equal_or_child(dbx_path_lower, path) for path in selected_paths)

    def _is_required_selective_sync_parent(self, dbx_path_lower: str) -> bool:
        """Return whether an include-mode path only leads to a selected item."""
        return (
            self.selective_sync_mode == "include"
            and not self.is_excluded_by_selective_sync(dbx_path_lower)
            and not self._is_path_managed(
                dbx_path_lower,
                self.selective_sync_mode,
                self.selective_sync_paths,
            )
        )

    @property
    def ignore_symlinks(self) -> bool:
        """Whether local symbolic links remain unmanaged."""
        return self._ignore_symlinks

    @ignore_symlinks.setter
    def ignore_symlinks(self, ignore: bool) -> None:
        """Set whether local symbolic links remain unmanaged."""
        with self.sync_lock, self._conf._lock:
            save_generation = self._conf.save_generation
            try:
                self._conf.set("sync", "ignore_symlinks", ignore)
            except BaseException:
                if self._conf.save_committed_since(save_generation):
                    self._publish_ignore_symlinks(ignore)
                raise
            self._publish_ignore_symlinks(ignore)

    def _publish_ignore_symlinks(self, ignore: bool) -> None:
        """Publish an ignored-link policy which has committed to the config file."""
        self._ignore_symlinks = ignore

        if ignore:
            self.refresh_ignored_symlinks()
        else:
            ignored_paths = self._ignored_symlink_paths.copy()
            self._set_ignored_symlink_paths(set())
            for dbx_path in ignored_paths:
                self.rescan(self._local_path_for_ignored_symlink(dbx_path))

    def _set_ignored_symlink_paths(self, dbx_paths: Collection[str]) -> None:
        paths = self.clean_selective_sync_paths(dbx_paths)
        with self._ignored_symlinks_lock:
            self._state.set("sync", "ignored_symlink_paths", sorted(paths))
            self._ignored_symlink_paths = paths

    def _remember_ignored_symlink(self, dbx_path: str) -> str:
        dbx_path_lower = "/" + normalize(dbx_path).strip("/")
        with self._ignored_symlinks_lock:
            paths = self._ignored_symlink_paths | {dbx_path_lower}
            paths = self.clean_selective_sync_paths(paths)
            if paths != self._ignored_symlink_paths:
                self._state.set("sync", "ignored_symlink_paths", sorted(paths))
                self._ignored_symlink_paths = paths
        return dbx_path_lower

    def _forget_ignored_symlink(self, dbx_path: str) -> None:
        with self._ignored_symlinks_lock:
            paths = {
                path
                for path in self._ignored_symlink_paths
                if not is_equal_or_child(path, dbx_path)
            }
            if paths != self._ignored_symlink_paths:
                self._state.set("sync", "ignored_symlink_paths", sorted(paths))
                self._ignored_symlink_paths = paths

    def _stored_ignored_symlink_for_path(self, dbx_path: str) -> str | None:
        dbx_path_lower = normalize(dbx_path)
        with self._ignored_symlinks_lock:
            return next(
                (
                    path
                    for path in self._ignored_symlink_paths
                    if is_equal_or_child(dbx_path_lower, path)
                ),
                None,
            )

    def _stored_ignored_symlinks_below(self, dbx_path: str) -> set[str]:
        dbx_path_lower = normalize(dbx_path)
        with self._ignored_symlinks_lock:
            return {
                path
                for path in self._ignored_symlink_paths
                if is_equal_or_child(path, dbx_path_lower)
            }

    def _local_path_for_ignored_symlink(self, dbx_path_lower: str) -> str:
        local_path = self.to_local_path_from_cased(dbx_path_lower)
        try:
            return to_existing_unnormalized_path(local_path, root=self.dropbox_path)
        except (FileNotFoundError, NotADirectoryError):
            return local_path

    @staticmethod
    def _is_local_link(local_path: str | bytes) -> bool:
        try:
            return is_fs_link(os.lstat(local_path))
        except (FileNotFoundError, NotADirectoryError):
            return False

    def _find_local_symlink(
        self, local_path: str | bytes, *, remember: bool = True
    ) -> str | None:
        """Return the Dropbox path of a symlink at or above ``local_path``."""
        path = os.fsdecode(local_path)
        try:
            relative_path = osp.relpath(path, self.dropbox_path)
        except ValueError:
            return None

        if relative_path == osp.pardir or relative_path.startswith(
            osp.pardir + osp.sep
        ):
            return None

        try:
            root_stat = os.lstat(self.dropbox_path)
        except (FileNotFoundError, NotADirectoryError):
            return None
        if is_fs_link(root_stat):
            return "/"

        current_path = self.dropbox_path
        for part in relative_path.split(osp.sep):
            if part in {"", "."}:
                continue
            current_path = osp.join(current_path, part)
            try:
                stat = os.lstat(current_path)
            except (FileNotFoundError, NotADirectoryError):
                return None
            if is_fs_link(stat):
                dbx_path_lower = self.to_dbx_path_lower(current_path)
                if remember:
                    return self._remember_ignored_symlink(dbx_path_lower)
                return dbx_path_lower

        return None

    def _ignored_symlink_for_local_path(self, local_path: str | bytes) -> str | None:
        if not self.ignore_symlinks:
            return None

        dbx_path_lower = self.to_dbx_path_lower(local_path)
        stored_path = self._stored_ignored_symlink_for_path(dbx_path_lower)

        if stored_path:
            stored_local_path = self._local_path_for_ignored_symlink(stored_path)
            if self._is_local_link(stored_local_path):
                if self._is_managed_symlink(stored_local_path, stored_path):
                    self._forget_ignored_symlink(stored_path)
                    if stored_path == dbx_path_lower:
                        return None
                else:
                    return stored_path

            # The overlay has gone. The current remote event can restore the path, but
            # its old index revision must not make the download look redundant.
            self.remove_node_from_index(stored_path)
            self._forget_ignored_symlink(stored_path)

        if self._is_managed_symlink(os.fsdecode(local_path), dbx_path_lower):
            self._forget_ignored_symlink(dbx_path_lower)
            return None

        return self._find_local_symlink(local_path)

    def _queue_remote_restore(self, dbx_path: str) -> None:
        """Queue a remote restore without uploading the corresponding deletion."""
        self.clear_sync_errors_for_path(dbx_path, recursive=True)
        self.queue_targeted_download(dbx_path, "restore")
        self.remove_node_from_index(dbx_path)
        self._forget_ignored_symlink(dbx_path)

    @staticmethod
    def _local_stat_identity(
        stat_result: os.stat_result,
        content_identity: str | None = None,
    ) -> TreeSnapshotIdentity:
        """Return fields which change when a local item or its content changes."""
        return (
            stat_result.st_dev,
            stat_result.st_ino,
            stat_result.st_mode,
            stat_result.st_size,
            stat_result.st_mtime_ns,
            stat_result.st_ctime_ns,
            content_identity,
        )

    def _snapshot_local_tree(self, local_path: str) -> dict[str, TreeSnapshotIdentity]:
        """Capture identities for a local item and every descendant."""
        try:
            return rooted_tree_snapshot(
                local_path,
                self.dropbox_path,
                expected_root_identity=self.confirmed_root_identity,
                content_hasher_factory=self.client.content_hasher_factory,
            )
        except (FileNotFoundError, NotADirectoryError):
            return {}

    def _snapshot_local_item(
        self,
        local_path: str,
        root_path: str,
        *,
        expected_root_identity: tuple[int, ...] | None = None,
    ) -> TreeSnapshotIdentity:
        """Capture one item with the selected provider's content hash."""
        return rooted_item_snapshot(
            local_path,
            root_path,
            expected_root_identity=expected_root_identity,
            content_hasher_factory=self.client.content_hasher_factory,
        )

    def _stable_download_conflict(
        self, event: SyncEvent
    ) -> tuple[Conflict, dict[str, TreeSnapshotIdentity]]:
        """Check a download conflict against one stable local-tree snapshot."""
        for _ in range(3):
            try:
                before = self._snapshot_local_tree(event.local_path)
                conflict = self._check_download_conflict(event, before)
                after = self._snapshot_local_tree(event.local_path)
            except OSError:
                continue
            if before == after:
                return conflict, after
        raise FileConflictError(
            "Cannot apply remote change",
            "The local item kept changing while Maestral checked it.",
            dbx_path=event.dbx_path,
            local_path=event.local_path,
        )

    def _snapshot_has_selectively_excluded_descendant(
        self,
        snapshot: dict[str, TreeSnapshotIdentity],
        root_path: str,
    ) -> bool:
        """Return whether a local snapshot contains data outside its selection."""
        for local_path in snapshot:
            if local_path == root_path:
                continue
            dbx_path_lower = self.to_dbx_path_lower(local_path)
            if self.is_excluded_by_selective_sync(dbx_path_lower):
                return True
        return False

    def _snapshot_has_unmanaged_symlink(
        self,
        snapshot: dict[str, TreeSnapshotIdentity],
        index_entries: dict[str, IndexEntry] | None = None,
    ) -> bool:
        """Return whether a snapshot contains a link not backed by the index."""
        for local_path, identity in snapshot.items():
            content_identity = identity[6]
            if not (
                isinstance(content_identity, str)
                and content_identity.startswith("symlink:")
            ):
                continue
            dbx_path_lower = self.to_dbx_path_lower(local_path)
            entry = (
                index_entries.get(dbx_path_lower)
                if index_entries is not None
                else self.get_index_entry(dbx_path_lower)
            )
            if (
                self._stored_ignored_symlink_for_path(dbx_path_lower)
                or entry is None
                or entry.symlink_target is None
                or content_identity != f"symlink:{entry.symlink_target}"
            ):
                return True
        return False

    def _snapshot_matches_index(
        self,
        snapshot: dict[str, TreeSnapshotIdentity],
        local_root: str,
        *,
        allow_ignored_link_overlays: bool = False,
        allow_selective_exclusions: bool = False,
    ) -> bool:
        """Return whether a complete local snapshot matches the sync index."""
        snapshot_paths = {
            self.to_dbx_path_lower(local_path): (local_path, identity)
            for local_path, identity in snapshot.items()
        }
        root_lower = self.to_dbx_path_lower(local_root)
        index_entries = {
            entry.dbx_path_lower: entry
            for entry in self.iter_index()
            if is_equal_or_child(entry.dbx_path_lower, root_lower)
        }

        if allow_ignored_link_overlays:
            ignored_roots = {
                dbx_path_lower
                for dbx_path_lower, (_, identity) in snapshot_paths.items()
                if _snapshot_symlink_target(identity) is not None
                and self._stored_ignored_symlink_for_path(dbx_path_lower)
                == dbx_path_lower
            }
            snapshot_paths = {
                path: value
                for path, value in snapshot_paths.items()
                if not any(is_equal_or_child(path, root) for root in ignored_roots)
            }
            index_entries = {
                path: entry
                for path, entry in index_entries.items()
                if not any(is_equal_or_child(path, root) for root in ignored_roots)
            }

        if allow_selective_exclusions:
            excluded_roots = {
                dbx_path_lower
                for dbx_path_lower in snapshot_paths
                if dbx_path_lower != root_lower
                and self.is_excluded_by_selective_sync(dbx_path_lower)
            }
            snapshot_paths = {
                path: value
                for path, value in snapshot_paths.items()
                if not any(is_equal_or_child(path, root) for root in excluded_roots)
            }

        if snapshot_paths.keys() != index_entries.keys():
            return False

        for dbx_path_lower, (_, identity) in snapshot_paths.items():
            entry = index_entries[dbx_path_lower]
            content_identity = identity[6]
            if _snapshot_symlink_target(identity) is not None:
                if (
                    self._stored_ignored_symlink_for_path(dbx_path_lower)
                    or entry.symlink_target is None
                    or content_identity != f"symlink:{entry.symlink_target}"
                ):
                    return False
            elif _snapshot_is_directory(identity):
                if not entry.is_directory:
                    return False
            elif not entry.is_file or entry.symlink_target is not None:
                return False
            elif content_identity != entry.content_hash:
                return False
        return True

    @staticmethod
    def _rebase_dbx_path(
        dbx_path: str,
        source_root: str,
        destination_root: str,
    ) -> str:
        relative_path = posixpath.relpath(dbx_path, source_root)
        if relative_path == ".":
            return destination_root
        return normalize(posixpath.join(destination_root, relative_path))

    def _snapshot_matches_index_tree(
        self,
        snapshot: dict[str, TreeSnapshotIdentity],
        local_root: str,
        index_root: str,
        index_entries: dict[str, IndexEntry],
    ) -> bool:
        """Compare a local tree at a new name with index rows at its old name."""
        snapshot_entries: dict[str, TreeSnapshotIdentity] = {}
        for local_path, identity in snapshot.items():
            relative_path = osp.relpath(local_path, local_root)
            dbx_path_lower = (
                index_root
                if relative_path == osp.curdir
                else normalize(
                    posixpath.join(
                        index_root,
                        *relative_path.split(osp.sep),
                    )
                )
            )
            snapshot_entries[dbx_path_lower] = identity

        if snapshot_entries.keys() != index_entries.keys():
            return False

        for dbx_path_lower, identity in snapshot_entries.items():
            entry = index_entries[dbx_path_lower]
            content_identity = identity[6]
            if _snapshot_symlink_target(identity) is not None:
                if content_identity != f"symlink:{entry.symlink_target}":
                    return False
            elif _snapshot_is_directory(identity):
                if not entry.is_directory:
                    return False
            elif (
                not entry.is_file
                or entry.symlink_target is not None
                or content_identity != entry.content_hash
            ):
                return False

        return True

    def _bind_local_snapshot_for_index(
        self,
        local_path: str,
        expected_snapshot: dict[str, TreeSnapshotIdentity],
    ) -> bool:
        """Bind and recheck a local tree before an index row can refer to it."""
        expected_identity = expected_snapshot.get(local_path)
        if expected_identity is None:
            return False

        dbx_path_lower = self._record_recovered_local_path(
            local_path,
            expected_identity[:3],
        )
        actual_snapshot = self._snapshot_local_tree(local_path)
        if actual_snapshot == expected_snapshot:
            return True

        actual_identity = actual_snapshot.get(local_path)
        if actual_identity is None:
            self._forget_recovered_local_path(
                dbx_path_lower,
                expected_identity=expected_identity[:3],
                expected_source="",
            )
        else:
            self._record_recovered_local_path(
                local_path,
                actual_identity[:3],
            )
        self.rescan(local_path)
        return False

    def _finish_local_index_publication(
        self,
        local_path: str,
        expected_identity: TreeSnapshotIdentity,
    ) -> bool:
        """Finish or repair a local-present index publication."""
        dbx_path_lower = self.to_dbx_path_lower(local_path)
        entry = self._validated_recovered_local_paths().get(dbx_path_lower)
        if entry is None:
            return False
        actual_identity = self._recovered_path_identity(entry["path"])
        if actual_identity is not None and list(actual_identity[:3]) == list(
            expected_identity[:3]
        ):
            self._maybe_finish_recovered_local_path(dbx_path_lower)
            return True

        if actual_identity is None:
            self._forget_recovered_local_path(
                dbx_path_lower,
                expected_identity=entry["identity"],
                expected_source=entry["source"],
            )
        else:
            self._record_recovered_local_path(
                local_path,
                actual_identity[:3],
            )
        self.rescan(local_path)
        return False

    def _publish_sync_event_for_local_snapshot(
        self,
        event: SyncEvent,
        expected_snapshot: dict[str, TreeSnapshotIdentity],
    ) -> bool:
        """Publish one remote event only while its local tree remains proved."""
        if not self._bind_local_snapshot_for_index(
            event.local_path,
            expected_snapshot,
        ):
            return False
        self.update_index_from_sync_event(event)
        return self._finish_local_index_publication(
            event.local_path,
            expected_snapshot[event.local_path],
        )

    def _publish_metadata_for_local_snapshot(
        self,
        md: Metadata,
        local_path: str,
        expected_snapshot: dict[str, TreeSnapshotIdentity],
    ) -> bool:
        """Publish remote metadata only while its local tree remains proved."""
        if not self._bind_local_snapshot_for_index(local_path, expected_snapshot):
            return False
        self.update_index_from_dbx_metadata(md)
        return self._finish_local_index_publication(
            local_path,
            expected_snapshot[local_path],
        )

    def _remote_move_matches_index_tree(
        self,
        remote_entries: dict[str, Metadata],
        remote_root: str,
        index_root: str,
        index_entries: dict[str, IndexEntry],
    ) -> bool:
        """Compare a remotely moved tree with its source rows in the sync index."""
        expected_remote_paths = {
            self._rebase_dbx_path(path, index_root, remote_root)
            for path in index_entries
        }
        if remote_entries.keys() != expected_remote_paths:
            return False

        for remote_path, md in remote_entries.items():
            source_path = self._rebase_dbx_path(
                remote_path,
                remote_root,
                index_root,
            )
            entry = index_entries[source_path]
            if isinstance(md, FolderMetadata):
                if not entry.is_directory or entry.provider_id != md.id:
                    return False
            elif isinstance(md, FileMetadata):
                if (
                    not entry.is_file
                    or entry.provider_id != md.id
                    or entry.rev != md.rev
                    or entry.content_hash != md.content_hash
                    or entry.symlink_target != md.symlink_target
                ):
                    return False
            else:
                return False

        return True

    def _remote_metadata_tree(self, root_md: Metadata) -> dict[str, Metadata]:
        """Return metadata for a remote item and all folder descendants."""
        entries = {root_md.path_lower: root_md}
        if isinstance(root_md, FolderMetadata):
            result = self.client.list_folder(root_md.path_lower, recursive=True)
            for md in result.entries:
                if isinstance(md, DeletedMetadata) or not is_equal_or_child(
                    md.path_lower,
                    root_md.path_lower,
                ):
                    raise FileConflictError(
                        "Cannot verify remote move",
                        "Dropbox returned an inconsistent moved folder tree.",
                        dbx_path=root_md.path_display,
                    )
                entries[md.path_lower] = md
        return entries

    def remove_local_after_selective_sync(self, dbx_path_cased: str) -> SyncStatus:
        """Unindex and remove one excluded tree without losing local changes."""
        if dbx_path_cased == "/":
            return SyncStatus.Skipped
        local_path = self.to_local_path_from_cased(dbx_path_cased)
        dbx_path_lower = normalize(dbx_path_cased)
        try:
            snapshot = self._snapshot_local_tree(local_path)
        except OSError as exc:
            if exc.errno != errno.ELOOP:
                raise
            self.remove_node_from_index(dbx_path_lower)
            return SyncStatus.Skipped
        index_entries = {
            entry.dbx_path_lower: entry
            for entry in self.iter_index()
            if is_equal_or_child(entry.dbx_path_lower, dbx_path_lower)
        }
        if not snapshot:
            self.remove_node_from_index(dbx_path_lower)
            return SyncStatus.Skipped

        if self._snapshot_has_unmanaged_symlink(snapshot):
            if self.ignore_symlinks:
                self.remove_node_from_index(dbx_path_lower)
                result = self._delete_local_path_preserving_ignored_symlinks(
                    local_path,
                    snapshot,
                    index_entries=index_entries,
                )
                if result.error is not None:
                    if result.changed:
                        self.rescan(local_path)
                    if isinstance(result.error, OSError):
                        raise os_to_maestral_error(
                            result.error,
                            dbx_path=dbx_path_cased,
                            local_path=local_path,
                        ) from result.error
                    raise result.error
                if not result.preserved:
                    raise FileConflictError(
                        "Cannot exclude local item",
                        "The ignored symbolic link changed during exclusion.",
                        dbx_path=dbx_path_cased,
                        local_path=local_path,
                    )
                if result.conflict:
                    self.rescan(local_path)
                    return SyncStatus.Conflict
                return SyncStatus.Done
            raise SymlinkError(
                "Cannot exclude local item",
                "The local item contains an unmanaged symbolic link.",
                dbx_path=dbx_path_cased,
                local_path=local_path,
            )

        matches_index = self._snapshot_matches_index(snapshot, local_path)
        deleted_md = DeletedMetadata(
            name=posixpath.basename(dbx_path_cased),
            path_lower=normalize(dbx_path_cased),
            path_display=dbx_path_cased,
        )
        event = SyncEvent.from_metadata(deleted_md, self)
        root_identity = snapshot[local_path]
        event_cls = (
            DirDeletedEvent
            if _snapshot_is_directory(root_identity)
            else FileDeletedEvent
        )
        # Make the old selection recover the remote item after a crash before the
        # local evacuation or the selection commit.
        self.remove_node_from_index(event.dbx_path_lower)
        with self.fs_events.ignore(event_cls(local_path)):
            token, backup_path = self._evacuate_local_item(event)

        if matches_index and self._retain_local_evacuation(
            event,
            token,
            backup_path,
            snapshot,
        ):
            return SyncStatus.Done

        self._preserve_evacuated_conflict(event, token, backup_path)
        return SyncStatus.Conflict

    def _delete_local_path_preserving_ignored_symlinks(
        self,
        local_path: str,
        expected_snapshot: dict[str, TreeSnapshotIdentity] | None = None,
        *,
        change_dbid: str | None = None,
        index_entries: dict[str, IndexEntry] | None = None,
        record_partial_failure: bool = True,
    ) -> _LocalDeletionResult:
        """Evacuate managed content but keep ignored links and required parents."""
        if expected_snapshot is None:
            try:
                expected_snapshot = self._snapshot_local_tree(local_path)
            except OSError as exc:
                return _LocalDeletionResult(error=exc)

        if index_entries is None:
            dbx_path_lower = self.to_dbx_path_lower(local_path)
            index_entries = {
                entry.dbx_path_lower: entry
                for entry in self.iter_index()
                if is_equal_or_child(entry.dbx_path_lower, dbx_path_lower)
            }

        expected_identity = expected_snapshot.get(local_path)
        if expected_identity is None:
            return _LocalDeletionResult(
                error=FileNotFoundError(
                    errno.ENOENT,
                    os.strerror(errno.ENOENT),
                    local_path,
                )
            )

        try:
            actual_identity = self._snapshot_local_item(
                local_path,
                self.dropbox_path,
                expected_root_identity=self.confirmed_root_identity,
            )
        except OSError as exc:
            return _LocalDeletionResult(error=exc)

        if actual_identity != expected_identity:
            return _LocalDeletionResult(
                error=OSError(
                    errno.ESTALE,
                    "Local item changed before deletion",
                    local_path,
                )
            )

        subtree_snapshot = {
            path: identity
            for path, identity in expected_snapshot.items()
            if path == local_path
            or is_child(path, local_path, self.is_fs_case_sensitive)
        }
        has_unmanaged_link = self._snapshot_has_unmanaged_symlink(
            subtree_snapshot,
            index_entries,
        )
        if has_unmanaged_link and not self.ignore_symlinks:
            return _LocalDeletionResult(
                error=OSError(
                    errno.ELOOP,
                    "Symbolic link in local path",
                    local_path,
                ),
                preserved=True,
            )

        content_identity = actual_identity[6]
        if isinstance(content_identity, str) and content_identity.startswith(
            "symlink:"
        ):
            if has_unmanaged_link:
                self._remember_ignored_symlink(self.to_dbx_path(local_path))
                return _LocalDeletionResult(preserved=True)

        if not has_unmanaged_link:
            dbx_path_cased = self.to_dbx_path(local_path)
            dbx_path_lower = normalize(dbx_path_cased)
            subtree_index_entries = {
                path: entry
                for path, entry in index_entries.items()
                if is_equal_or_child(path, dbx_path_lower)
            }
            matches_index = self._snapshot_matches_index_tree(
                subtree_snapshot,
                local_path,
                dbx_path_lower,
                subtree_index_entries,
            )
            evacuation_event = SyncEvent(
                direction=SyncDirection.Down,
                item_type=(
                    ItemType.Folder
                    if _snapshot_is_directory(actual_identity)
                    else ItemType.File
                ),
                sync_time=0,
                dbx_path=dbx_path_cased,
                dbx_path_lower=normalize(dbx_path_cased),
                local_path=local_path,
                content_hash=None,
                symlink_target=None,
                change_type=ChangeType.Removed,
                change_time=None,
                change_dbid=change_dbid,
                status=SyncStatus.Queued,
                size=0,
                completed=0,
            )
            try:
                token, backup_path = self._evacuate_local_item(evacuation_event)
                if matches_index and self._retain_local_evacuation(
                    evacuation_event,
                    token,
                    backup_path,
                    subtree_snapshot,
                ):
                    return _LocalDeletionResult(changed=True)
                self._preserve_evacuated_conflict(
                    evacuation_event,
                    token,
                    backup_path,
                )
            except Exception as exc:
                return _LocalDeletionResult(error=exc)
            return _LocalDeletionResult(
                changed=True,
                conflict=True,
            )

        if not _snapshot_is_directory(actual_identity):
            return _LocalDeletionResult(
                error=OSError(
                    errno.ESTALE,
                    "The ignored symbolic link tree changed",
                    local_path,
                )
            )

        preserved = False
        changed = False
        conflict = False

        expected_children = sorted(
            path for path in expected_snapshot if osp.dirname(path) == local_path
        )
        for child_path in expected_children:
            child_result = self._delete_local_path_preserving_ignored_symlinks(
                child_path,
                expected_snapshot,
                change_dbid=change_dbid,
                index_entries=index_entries,
                record_partial_failure=False,
            )
            if child_result.error is not None:
                changed = changed or child_result.changed
                if changed and record_partial_failure:
                    try:
                        current_identity = self._snapshot_local_item(
                            local_path,
                            self.dropbox_path,
                            expected_root_identity=self.confirmed_root_identity,
                        )
                    except OSError:
                        pass
                    else:
                        self._record_recovered_local_path(
                            local_path,
                            current_identity[:3],
                        )
                return _LocalDeletionResult(
                    error=child_result.error,
                    preserved=preserved or child_result.preserved,
                    changed=changed,
                    conflict=conflict or child_result.conflict,
                )
            preserved = preserved or child_result.preserved
            changed = changed or child_result.changed
            conflict = conflict or child_result.conflict

        if preserved:
            return _LocalDeletionResult(
                preserved=True,
                changed=changed,
                conflict=conflict,
            )
        return _LocalDeletionResult(
            error=OSError(
                errno.ESTALE,
                "The ignored symbolic link disappeared during deletion",
                local_path,
            ),
            changed=changed,
            conflict=conflict,
        )

    def _find_local_symlink_below(self, local_path: str) -> str | None:
        """Return the Dropbox path of a symlink below a local directory."""
        if not isdir(local_path):
            return None

        for child_path, stat in rooted_walk(
            local_path,
            self.dropbox_path,
            expected_root_identity=self.confirmed_root_identity,
        ):
            if is_fs_link(stat):
                dbx_path = self.to_dbx_path_lower(child_path)
                if self.ignore_symlinks:
                    return self._remember_ignored_symlink(dbx_path)
                return dbx_path

        return None

    def _is_managed_symlink_at_event_path(self, event: SyncEvent) -> bool:
        """Return whether an event path still matches an indexed Dropbox link."""
        return self._is_managed_symlink(
            event.local_path,
            event.dbx_path_lower,
        )

    def _is_managed_symlink(
        self,
        local_path: str,
        dbx_path_lower: str,
    ) -> bool:
        """Return whether a local path matches its indexed Dropbox link."""
        entry = self.get_index_entry(dbx_path_lower)
        if entry is None or entry.symlink_target is None:
            return False
        try:
            path_stat = os.lstat(local_path)
            if not is_fs_link(path_stat):
                return False
            if self._find_local_symlink(
                osp.dirname(local_path),
                remember=False,
            ):
                return False
            return get_symlink_target(local_path) == entry.symlink_target
        except OSError:
            return False

    def _is_indexed_symlink_at_event_path(self, event: SyncEvent) -> bool:
        """Return whether the exact event path is an indexed, non-overlay link."""
        entry = self.get_index_entry(event.dbx_path_lower)
        if entry is None or entry.symlink_target is None:
            return False
        if self._stored_ignored_symlink_for_path(event.dbx_path_lower):
            return False
        try:
            path_stat = os.lstat(event.local_path)
        except (FileNotFoundError, NotADirectoryError):
            return False
        if not is_fs_link(path_stat):
            return False
        return (
            self._find_local_symlink(
                osp.dirname(event.local_path),
                remember=False,
            )
            is None
        )

    def _raise_for_remote_event_symlink(
        self,
        event: SyncEvent,
        title: str,
    ) -> None:
        """Reject unsafe ancestors and unmanaged links in a replaced local tree."""
        self._raise_for_local_ancestor_symlink(
            osp.dirname(event.local_path),
            event.dbx_path,
            title,
        )
        snapshot = self._snapshot_local_tree(event.local_path)
        if self._snapshot_has_unmanaged_symlink(snapshot):
            raise SymlinkError(
                title,
                "The local item contains an unmanaged symbolic link.",
                dbx_path=event.dbx_path,
                local_path=event.local_path,
            )

    def _raise_for_local_symlink_at_or_below(
        self,
        local_path: str,
        dbx_path: str,
        title: str,
    ) -> None:
        """Reject an app move which would relocate a symbolic link."""
        self.ensure_dropbox_folder_present()
        with convert_api_errors(dbx_path=dbx_path, local_path=local_path):
            symlink_path = self._find_local_symlink(
                local_path,
                remember=self.ignore_symlinks,
            ) or self._find_local_symlink_below(local_path)

        if symlink_path:
            raise SymlinkError(
                title,
                f'The local path contains the symbolic link "{symlink_path}".',
                dbx_path=dbx_path,
                local_path=local_path,
            )

    def _raise_for_local_ancestor_symlink(
        self,
        local_path: str,
        dbx_path: str,
        title: str,
    ) -> None:
        """Reject a path whose existing ancestor is a symbolic link."""
        self.ensure_dropbox_folder_present()
        with convert_api_errors(dbx_path=dbx_path, local_path=local_path):
            symlink_path = self._find_local_symlink(
                local_path,
                remember=self.ignore_symlinks,
            )
        if symlink_path:
            raise SymlinkError(
                title,
                f'The local path contains the symbolic link "{symlink_path}".',
                dbx_path=dbx_path,
                local_path=local_path,
            )

    def _raise_for_local_move_symlink(
        self,
        source_path: str,
        destination_path: str,
        dbx_path: str,
        title: str,
    ) -> None:
        """Reject a local move when either endpoint contains a symbolic link."""
        self._raise_for_local_symlink_at_or_below(source_path, dbx_path, title)
        self._raise_for_local_symlink_at_or_below(destination_path, dbx_path, title)

    def refresh_ignored_symlinks(self) -> None:
        """Find local symlink overlays and clear their upload errors."""
        if not self.ignore_symlinks or not isdir(self.dropbox_path):
            return

        paths: set[str] = set()
        for local_path, stat in rooted_walk(
            self.dropbox_path,
            self.dropbox_path,
            expected_root_identity=self.confirmed_root_identity,
        ):
            if is_fs_link(stat):
                dbx_path_lower = self.to_dbx_path_lower(local_path)
                if not self._is_managed_symlink(local_path, dbx_path_lower):
                    paths.add(dbx_path_lower)

        self._set_ignored_symlink_paths(paths)
        for dbx_path in paths:
            self.clear_sync_errors_for_path(dbx_path, recursive=True)

    @property
    def max_cpu_percent(self) -> float:
        """Maximum CPU usage for parallel downloads or uploads in percent of the total
        available CPU time per core. Individual workers in a thread pool will pause
        until the usage drops below this value. Tasks in the main thread such as
        indexing file changes may still use more CPU time. Setting this to 200% means
        that two full logical CPU core can be used."""
        return self._max_cpu_percent

    @max_cpu_percent.setter
    def max_cpu_percent(self, percent: float) -> None:
        """Setter: max_cpu_percent."""
        self._max_cpu_percent = percent
        self._conf.set("sync", "max_cpu_percent", percent // CPU_CORE_COUNT)

    # ==== Sync state ==================================================================

    @property
    def remote_cursor(self) -> str:
        """Opaque cursor from the last sync with the selected provider."""
        cursors = self._state.get("sync", "cursors")
        if not isinstance(cursors, dict) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in cursors.items()
        ):
            raise ValueError("The provider cursor state is invalid")
        return cursors.get(self.client.provider_id, "")

    @remote_cursor.setter
    def remote_cursor(self, cursor: str) -> None:
        """Setter: last_cursor"""
        if not isinstance(cursor, str):
            raise TypeError("The remote cursor must be a string")
        with self.sync_lock:
            cursors = self._state.get("sync", "cursors")
            if not isinstance(cursors, dict):
                raise ValueError("The provider cursor state is invalid")
            updated_cursors = cursors.copy()
            updated_cursors[self.client.provider_id] = cursor
            with self._state._lock:
                snapshot = self._state._configuration_snapshot()
                save_generation = self._state.save_generation
                try:
                    self._state.set("sync", "cursors", updated_cursors, save=False)
                    if self.client.provider_id == "dropbox":
                        self._state.set("sync", "cursor", cursor, save=False)
                    self._state.save()
                except BaseException:
                    if not self._state.save_committed_since(save_generation):
                        self._state._restore_configuration_snapshot(snapshot)
                    raise

        self._logger.debug("Remote cursor saved: %s", cursor)

    def _migrate_remote_cursor(self) -> None:
        """Move the legacy Dropbox cursor to provider-scoped state once."""
        cursors = self._state.get("sync", "cursors")
        legacy_cursor = self._state.get("sync", "cursor")
        if not isinstance(cursors, dict):
            raise ValueError("The provider cursor state is invalid")
        if (
            self.client.provider_id == "dropbox"
            and isinstance(legacy_cursor, str)
            and legacy_cursor
            and "dropbox" not in cursors
        ):
            migrated = cursors.copy()
            migrated["dropbox"] = legacy_cursor
            self._state.set("sync", "cursors", migrated)

    @property
    def local_cursor(self) -> float:
        """Time stamp from last sync with remote Dropbox. The value is updated and saved
        to the config file on every successful upload of local changes."""
        return self._local_cursor

    @local_cursor.setter
    def local_cursor(self, last_sync: float) -> None:
        """Setter: local_cursor"""
        with self.sync_lock:
            self._local_cursor = last_sync
            self._state.set("sync", "lastsync", last_sync)

        self._logger.debug("Local cursor saved: %s", last_sync)

    @property
    def last_change(self) -> float:
        """The time stamp of the last file change or 0.0 if there are no file changes in
        our history."""
        with self._database_access():
            row = self._db.execute("SELECT MAX(last_sync) FROM 'index'").fetchone()
            if not row:
                return 0.0
            try:
                return row[0] or 0.0
            except IndexError:
                return 0.0

    @property
    def last_reindex(self) -> float:
        """Time stamp of last full indexing. This is used to determine when the next
        full indexing should take place."""
        return self._state.get("sync", "last_reindex")

    def get_history(self, dbx_path: str | None = None) -> list[SyncEvent]:
        """A list of the last SyncEvents in our history. History will be kept for the
        interval specified by the config value ``keep_history`` (defaults to two weeks)
        but at most 1,000 events will be kept."""
        with self._database_access():
            query: Query
            if dbx_path is None:
                query = AllQuery()
            else:
                query = MatchQuery(SyncEvent.dbx_path, dbx_path)

            order_expr = "IFNULL(change_time, sync_time)"
            sync_events = self._history_table.select(query.order_by(order_expr))
            return sync_events

    def reset_sync_state(self) -> None:
        """Resets all saved sync state. Settings are not affected."""
        if self.busy():
            raise RuntimeError("Cannot reset sync state while syncing.")

        self._begin_sync_reset("sync")
        self._complete_pending_sync_reset()

        self._logger.debug("Sync state reset")

    def _validated_sync_reset(self) -> dict[str, Any]:
        """Return the durable reset request, or an empty dictionary."""
        journal = self._state.get("recovery", "sync_reset")
        if journal == {}:
            return {}
        if not isinstance(journal, dict):
            raise ValueError("The sync reset journal is invalid")
        kind = journal.get("kind")
        phase = journal.get("phase")
        # Remove this one-shot upgrade after pre-provider state files are unsupported.
        legacy_unlink_keys = {
            "kind",
            "phase",
            "account_id",
            "keyring",
            "credentials_deleted",
            "root_path",
            "root_marker_id",
        }
        if kind == "unlink" and set(journal) == legacy_unlink_keys:
            journal["provider"] = "dropbox"
            self._state.set("recovery", "sync_reset", journal)
        if kind == "sync":
            if set(journal) != {"kind", "phase"} or phase not in {
                "pending",
                "queue",
            }:
                raise ValueError("The sync reset journal is invalid")
        elif kind == "unlink":
            if (
                set(journal)
                != {
                    "kind",
                    "phase",
                    "provider",
                    "account_id",
                    "keyring",
                    "credentials_deleted",
                    "root_path",
                    "root_marker_id",
                }
                or phase not in {"pending", "config", "queue"}
                or journal.get("provider") not in {"dropbox", "google_drive"}
                or not isinstance(journal.get("account_id"), str)
                or not isinstance(journal.get("keyring"), str)
                or not isinstance(journal.get("credentials_deleted"), bool)
                or not isinstance(journal.get("root_path"), str)
                or not isinstance(journal.get("root_marker_id"), str)
            ):
                raise ValueError("The sync reset journal is invalid")
        else:
            raise ValueError("The sync reset journal is invalid")
        return journal.copy()

    def _root_bound_recovery_names(self) -> tuple[str, ...]:
        """Return recovery journals which belong to the configured account root."""
        values = {
            "local evacuations": self._state.get("recovery", "local_evacuations"),
            "local paths": self._state.get("recovery", "local_paths"),
            "case changes": self._state.get("recovery", "case_changes"),
            "root move": self._state.get("recovery", "root_move"),
            "path-root migration": self._state.get("account", "path_root_migration"),
        }
        return tuple(name for name, value in values.items() if value != {})

    def _ensure_unlink_recovery_clear(self) -> None:
        """Refuse an account boundary while old-root recovery work remains."""
        pending = self._root_bound_recovery_names()
        if pending:
            raise MaestralApiError(
                "Cannot unlink Dropbox account",
                "Finish the pending local recovery first: " + ", ".join(pending),
            )

    def _ensure_unlink_not_pending(self) -> None:
        """Refuse creation of root-bound work after an unlink has started."""
        reset = self._state.get("recovery", "sync_reset")
        if isinstance(reset, dict) and reset.get("kind") == "unlink":
            raise CancelledError("Account unlink is in progress")

    def _begin_sync_reset(
        self,
        kind: str,
        *,
        provider: str = "",
        account_id: str = "",
        keyring: str = "automatic",
        root_path: str = "",
        root_marker_id: str = "",
    ) -> None:
        """Persist a reset request before any database or account change."""
        if kind not in {"sync", "unlink"}:
            raise ValueError("The sync reset kind is invalid")
        with self._local_mutation_lock, self._state._lock:
            journal = self._validated_sync_reset()
            if journal and journal["kind"] != kind:
                raise RuntimeError("A different sync reset is already pending")
            if not journal:
                if kind == "unlink":
                    self._ensure_unlink_recovery_clear()
                journal = {"kind": kind, "phase": "pending"}
                if kind == "unlink":
                    if provider not in {"dropbox", "google_drive"}:
                        raise ValueError("The unlink provider is invalid")
                    journal.update(
                        provider=provider,
                        account_id=account_id,
                        keyring=keyring,
                        credentials_deleted=False,
                        root_path=root_path,
                        root_marker_id=root_marker_id,
                    )
                self._state.set("recovery", "sync_reset", journal)

    def _clear_sync_database(self) -> None:
        """Clear all sync tables in one SQLite transaction."""
        tables = (
            self._index_table,
            self._history_table,
            self._sync_errors_table,
            self._hash_table,
        )
        with self._database_access():
            self._db.execute_batch(
                (f"DELETE FROM {table.table_name}", ()) for table in tables
            )
            for table in tables:
                table.clear_cache()

    def _complete_pending_sync_reset(self) -> None:
        """Replay durable database, state, and config reset phases."""
        journal = self._validated_sync_reset()
        if not journal:
            return

        kind = journal["kind"]
        phase = journal["phase"]

        if kind == "unlink":
            self._ensure_unlink_recovery_clear()

        if phase == "pending":
            path_root_migration = self._state.get("account", "path_root_migration")
            self._clear_sync_database()

            with self._state._lock:
                snapshot = self._state._configuration_snapshot()
                save_generation = self._state.save_generation
                try:
                    download_intents = self._validated_download_intents()
                    self._state.reset_to_defaults("sync", save=False)
                    if kind == "sync":
                        self._state.set(
                            "sync",
                            "pending_downloads",
                            sorted(download_intents),
                            save=False,
                        )
                        journal["phase"] = "queue"
                    else:
                        if not path_root_migration:
                            self._state.reset_to_defaults("account", save=False)
                        self._state.set(
                            "recovery",
                            "download_intents",
                            {},
                            save=False,
                        )
                        journal["phase"] = "config"
                    self._state.set(
                        "recovery",
                        "sync_reset",
                        journal,
                        save=False,
                    )
                    self._state.save()
                except BaseException:
                    if not self._state.save_committed_since(save_generation):
                        self._state._restore_configuration_snapshot(snapshot)
                    raise
            phase = journal["phase"]

        if kind == "unlink" and phase == "config":
            with self._conf._lock:
                snapshot = self._conf._configuration_snapshot()
                save_generation = self._conf.save_generation
                try:
                    self._conf.reset_to_defaults(save=False)
                    self._conf.set("auth", "provider", journal["provider"], save=False)
                    self._conf.set("auth", "provider_selected", False, save=False)
                    self._conf.save()
                except BaseException:
                    if not self._conf.save_committed_since(save_generation):
                        self._conf._restore_configuration_snapshot(snapshot)
                    raise

            journal["phase"] = "queue"
            self._state.set("recovery", "sync_reset", journal)

        self.reload_cached_config()

    def _finish_sync_reset_queue(self) -> None:
        """Clear a reset marker after the live queue matches persistent state."""
        journal = self._validated_sync_reset()
        if not journal or journal["phase"] != "queue":
            return
        if journal["kind"] == "unlink":
            if not journal["credentials_deleted"]:
                return
            if self._root_bound_recovery_names():
                return
        self._state.set("recovery", "sync_reset", {})

    # ==== Sync error management =======================================================

    @property
    def sync_errors(self) -> list[SyncErrorEntry]:
        """Returns a list of all sync errors."""
        with self._database_access():
            return self._sync_errors_table.select(AllQuery())

    @property
    def upload_errors(self) -> list[SyncErrorEntry]:
        """Returns a list of all upload errors."""
        with self._database_access():
            query = MatchQuery(SyncErrorEntry.direction, SyncDirection.Up)
            return self._sync_errors_table.select(query)

    @property
    def download_errors(self) -> list[SyncErrorEntry]:
        """Returns a list of all download errors."""
        with self._database_access():
            query = MatchQuery(SyncErrorEntry.direction, SyncDirection.Down)
            return self._sync_errors_table.select(query)

    def has_sync_errors(self) -> bool:
        """Returns ``True`` in case of sync errors, ``False`` otherwise."""
        with self._database_access():
            return self._sync_errors_table.count() > 0

    def sync_errors_for_path(
        self, dbx_path_lower: str, direction: SyncDirection | None = None
    ) -> list[SyncErrorEntry]:
        """
        Returns a list of all sync errors for the given path and its children.

        :param dbx_path_lower: Normalised Dropbox path.
        :param direction: Direction to filter sync errors. If not given, both upload
            and download errors will be returned.
        :returns: List of sync errors.
        """
        with self._database_access():
            path_tree_query = PathTreeQuery(
                SyncErrorEntry.dbx_path_lower, dbx_path_lower
            )

            if direction:
                direction_query = MatchQuery(SyncErrorEntry.direction, direction)
                joint_query = AndQuery(path_tree_query, direction_query)
                errors = self._sync_errors_table.select(joint_query)
            else:
                errors = self._sync_errors_table.select(path_tree_query)

            return errors

    def clear_sync_errors_for_path(
        self, dbx_path_lower: str, recursive: bool = False
    ) -> None:
        """
        Clear all sync errors for a path after a successful sync event.

        :param dbx_path_lower: Normalised Dropbox path to clear.
        :param recursive: Whether to clear sync errors for children of the given path.
        """
        with self._database_access():
            self._sync_errors_table.delete_primary_key(dbx_path_lower)

            if recursive:
                query = PathTreeQuery(SyncErrorEntry.dbx_path_lower, dbx_path_lower)
                self._sync_errors_table.delete(query)

    def clear_sync_errors_from_event(self, event: SyncEvent) -> None:
        """Clears sync errors corresponding to a sync event."""
        recursive = event.is_moved or event.is_deleted or event.is_file

        self.clear_sync_errors_for_path(event.dbx_path_lower, recursive)
        if event.dbx_path_from_lower is not None:
            self.clear_sync_errors_for_path(event.dbx_path_from_lower, recursive)

    # ==== Index access and management =================================================

    def get_index(self) -> list[IndexEntry]:
        """
        Returns a copy of the local index of synced files and folders.

        :returns: List of index entries.
        """
        with self._database_access():
            return self._index_table.select(AllQuery())

    def get_index_entry(self, dbx_path_lower: str) -> IndexEntry | None:
        """
        Gets the index entry for the given Dropbox path.

        :param dbx_path_lower: Normalized lower case Dropbox path.
        :returns: Index entry or ``None`` if no entry exists for the given path.
        """
        with self._database_access():
            query = MatchQuery(IndexEntry.dbx_path_lower, dbx_path_lower)
            entries = self._index_table.select(query)
            return entries[0] if entries else None

    def get_index_entry_for_local_path(self, local_path: str) -> IndexEntry | None:
        """
        Gets the index entry for the given local path. Ensures that the index entry has
        the correct casing but ignore unicode normalisation differences. Dropbox always
        normalizes to composed (NFC) on upload, even in the display_path, so we cannot
        distinguish.

        :param local_path: Local path as returned by file system APIs.
        :returns: Index entry or ``None`` if no entry exists for the given path.
        """
        dbx_path_cased = self.to_dbx_path(local_path)
        dbx_path_lower = self.to_dbx_path_lower(local_path)

        index_entry = self.get_index_entry(dbx_path_lower)

        if not index_entry:
            return None

        if equal_but_for_unicode_norm(index_entry.dbx_path_cased, dbx_path_cased):
            return index_entry

        return None

    def iter_index(self) -> Iterator[IndexEntry]:
        """
        Returns an iterator over the local index of synced files and folders.

        :returns: Iterator over index entries.
        """
        with self._database_access():
            for entries in self._index_table.select_iter(AllQuery()):
                yield from entries

    def index_count(self) -> int:
        """
        Returns the number of items in our index without loading any items.

        :returns: Number of index entries.
        """
        with self._database_access():
            return self._index_table.count()

    def get_local_rev(self, dbx_path_lower: str) -> str | None:
        """
        Gets revision number of local file.

        :param dbx_path_lower: Normalized lower case Dropbox path.
        :returns: Revision number as str or ``None`` if no local revision number has
            been saved.
        """
        entry = self.get_index_entry(dbx_path_lower)

        if entry:
            return entry.rev
        else:
            return None

    def get_last_sync(self, dbx_path_lower: str) -> float:
        """
        Returns the timestamp of last sync for an individual path.

        :param dbx_path_lower: Normalized lower case Dropbox path.
        :returns: Time of last sync.
        """
        entry = self.get_index_entry(dbx_path_lower)

        if entry:
            last_sync = entry.last_sync or 0.0
        else:
            last_sync = 0.0

        return max(last_sync, self.local_cursor)

    def update_index_from_sync_event(
        self, event: SyncEvent, *, local_present: bool = True
    ) -> None:
        """
        Updates the local index from a SyncEvent.

        :param event: SyncEvent from download.
        """
        if event.change_type is not ChangeType.Removed and not event.rev:
            raise ValueError("Rev required to update index")

        dbx_path_lower = event.dbx_path_lower

        with self._database_access():
            # Remove any entries for deleted or moved items.

            if event.change_type is ChangeType.Removed:
                self.remove_node_from_index(dbx_path_lower)
            elif event.change_type is ChangeType.Moved:
                assert event.dbx_path_from_lower is not None
                self.remove_node_from_index(event.dbx_path_from_lower)

            # Add or update entries for created or modified items.

            if event.change_type is not ChangeType.Removed:
                # Create or update entry.
                entry = IndexEntry(
                    dbx_path_cased=event.dbx_path,
                    dbx_path_lower=dbx_path_lower,
                    provider_id=event.dbx_id,
                    item_type=event.item_type,
                    last_sync=(
                        self._get_change_time(event.local_path)
                        if local_present
                        else None
                    ),
                    rev=event.rev,
                    content_hash=event.content_hash,
                    symlink_target=event.symlink_target,
                )

                self._replace_index_entry(entry)

    def update_index_from_dbx_metadata(self, md: Metadata) -> None:
        """
        Updates the local index from Dropbox metadata.

        :param md: Dropbox metadata.
        """
        with self._database_access():
            if isinstance(md, DeletedMetadata):
                return self.remove_node_from_index(md.path_lower)

            if isinstance(md, FileMetadata):
                rev = md.rev
                hash_str = md.content_hash
                item_type = ItemType.File
                symlink_target = md.symlink_target
                md_id = md.id
            elif isinstance(md, FolderMetadata):
                rev = "folder"
                hash_str = "folder"
                item_type = ItemType.Folder
                symlink_target = None
                md_id = md.id
            else:
                raise RuntimeError(f"Unknown metadata type: {md}")

            # Construct correct display path from ancestors.
            dbx_path_cased = self.correct_case(md.path_display)

            # Update existing entry or create new entry.
            entry = IndexEntry(
                dbx_path_cased=dbx_path_cased,
                dbx_path_lower=md.path_lower,
                provider_id=md_id,
                item_type=item_type,
                last_sync=None,
                rev=rev,
                content_hash=hash_str,
                symlink_target=symlink_target,
            )

            self._replace_index_entry(entry)

    def _replace_index_entry(self, entry: IndexEntry) -> None:
        """Replace an index row by stable identity and destination path."""
        destination_entry = self.get_index_entry(entry.dbx_path_lower)
        replaces_destination_tree = entry.is_file or (
            destination_entry is not None
            and destination_entry.provider_id != entry.provider_id
        )
        if replaces_destination_tree:
            destination_query: Query = PathTreeQuery(
                IndexEntry.dbx_path_lower, entry.dbx_path_lower
            )
        else:
            destination_query = MatchQuery(
                IndexEntry.dbx_path_lower, entry.dbx_path_lower
            )

        query = OrQuery(
            MatchQuery(IndexEntry.provider_id, entry.provider_id),
            destination_query,
        )
        self._index_table.replace_matching(query, entry)

    def remove_node_from_index(self, dbx_path_lower: str) -> None:
        """
        Removes any local index entries for the given path and all its children.

        :param dbx_path_lower: Normalized lower case Dropbox path.
        """
        with self._database_access():
            query = PathTreeQuery(IndexEntry.dbx_path_lower, dbx_path_lower)
            self._index_table.delete(query)

    def remove_index_entry(self, dbx_path_lower: str) -> None:
        """Remove only the exact path from the local index."""
        with self._database_access():
            query = MatchQuery(IndexEntry.dbx_path_lower, dbx_path_lower)
            self._index_table.delete(query)

    # ==== Content hashing =============================================================

    def get_local_hash(self, local_path: str | bytes) -> str | None:
        """
        Computes content hash of a local file.

        :param local_path: Absolute path on local drive.
        :returns: Content hash to compare with Dropbox's content hash, or 'folder' if
            the path points to a directory. ``None`` if there is nothing at the path.
        """
        local_path = os.fsdecode(local_path)

        try:
            stat = os.lstat(local_path)
        except (FileNotFoundError, NotADirectoryError):
            # Remove all cache entries for local_path and return None.
            with self._database_access():
                query = MatchQuery(HashCacheEntry.local_path, local_path)
                self._hash_table.delete(query)
            return None
        except OSError as err:
            if err.errno == errno.ENAMETOOLONG:
                return None
            raise os_to_maestral_error(err)

        if is_fs_link(stat):
            return None

        if S_ISDIR(stat.st_mode):
            return "folder"

        mtime: float | None = stat.st_mtime

        with self._database_access():
            # Check cache for an up-to-date content hash and return if it exists.
            cache_entry = self._hash_table.get(stat.st_ino)

            if cache_entry and cache_entry.mtime == mtime:
                return cache_entry.hash_str

        with convert_api_errors(local_path=local_path):
            hash_str, mtime = content_hash(
                local_path,
                content_hasher_factory=self.client.content_hasher_factory,
            )

        self._save_local_hash(stat.st_ino, local_path, hash_str, mtime)

        return hash_str

    def _save_local_hash(
        self,
        inode: int,
        local_path: str,
        hash_str: str | None,
        mtime: float | None,
    ) -> None:
        """
        Save the content hash for a file in our cache.

        :param inode: Inode of the file.
        :param local_path: Absolute path on local drive.
        :param hash_str: Hash string to save. If None, the existing cache entry will be
            deleted.
        :param mtime: Mtime of the file when the hash was computed.
        """
        with self._database_access():
            if hash_str:
                cache_entry = HashCacheEntry(
                    inode=inode,
                    local_path=local_path,
                    hash_str=hash_str,
                    mtime=mtime,
                )
                self._hash_table.update(cache_entry)

            else:
                self._hash_table.delete_primary_key(inode)

    # ==== Mignore management ==========================================================

    @property
    def mignore_path(self) -> str:
        """Path to mignore file on local drive (read only)."""
        return self._mignore_path

    @property
    def mignore_rules(self) -> PathSpec[Pattern]:
        """List of mignore rules following git wildmatch syntax (read only)."""
        return self._mignore_rules

    def load_mignore_file(self) -> None:
        """
        Loads rules from mignore file. No rules are loaded if the file does
        not exist or cannot be read.

        :returns: PathSpec instance with ignore patterns.
        """
        try:
            with open(self.mignore_path, opener=opener_no_symlink) as f:
                spec = f.read()
        except OSError as err:
            self._logger.debug("Could not load mignore rules: %s", err.strerror)
            spec = ""

        self._mignore_rules = PathSpec.from_lines("gitwildmatch", spec.splitlines())

    # ==== Helper functions ============================================================

    def _root_marker_content(self, *, create: bool = False) -> bytes | None:
        """Return the content which binds the root marker to this profile."""
        with self._conf._lock:
            marker_id = self._conf.get("sync", "root_marker_id")
            marker_id_is_valid = (
                isinstance(marker_id, str)
                and len(marker_id) == 32
                and all(char in "0123456789abcdef" for char in marker_id)
            )
            if not marker_id_is_valid:
                if not create:
                    return None
                marker_id = uuid4().hex
                self._conf.set("sync", "root_marker_id", marker_id)

        return f"maestral-root-v1:{marker_id}\n".encode("ascii")

    @staticmethod
    def _root_marker_matches(
        marker_path: str,
        expected_content: bytes,
        *,
        root_path: str | None = None,
        expected_root_identity: tuple[int, ...] | None = None,
    ) -> bool:
        if root_path is not None:
            hasher = DropboxContentHasher()
            hasher.update(expected_content)
            try:
                snapshot = rooted_tree_snapshot(
                    marker_path,
                    root_path,
                    expected_root_identity=expected_root_identity,
                )
            except (FileNotFoundError, NotADirectoryError, OSError):
                return False
            marker_identity = snapshot.get(marker_path)
            return bool(
                marker_identity
                and S_ISREG(marker_identity[2])
                and marker_identity[3] == len(expected_content)
                and marker_identity[6] == hasher.hexdigest()
            )

        try:
            marker_stat = os.lstat(marker_path)
            if not S_ISREG(marker_stat.st_mode) or is_fs_link(marker_stat):
                return False
            with open(marker_path, "rb", opener=opener_no_symlink) as marker_file:
                content = marker_file.read(len(expected_content) + 1)
        except (FileNotFoundError, NotADirectoryError, OSError):
            return False
        return content == expected_content

    def ensure_dropbox_folder_present(self, require_marker: bool = True) -> None:
        """
        Checks if the Dropbox folder still exists where we expect it to be.

        :param require_marker: Whether the root marker must be present.
        :raises NoDropboxDirError: When local Dropbox directory does not exist.
        """
        exception = NoDropboxDirError(
            "Dropbox folder missing",
            "Please move the Dropbox folder back to its original location "
            "or restart Maestral to set up a new folder.",
        )

        try:
            root_stat = os.lstat(self.dropbox_path)
        except (FileNotFoundError, NotADirectoryError):
            raise exception

        if not S_ISDIR(root_stat.st_mode) or is_fs_link(root_stat):
            raise exception

        # If the file system is not case-sensitive but preserving, a path which was
        # renamed with a case change only will still exist at the old location. We
        # therefore explicitly check for case changes here.
        if not self.is_fs_case_sensitive:
            try:
                cased_path = to_existing_unnormalized_path(self.dropbox_path)
            except (FileNotFoundError, NotADirectoryError):
                raise exception

            if cased_path != self.dropbox_path:
                title = "Dropbox folder renamed"
                msg = (
                    "Please move the Dropbox folder back to its original location "
                    "or restart Maestral to set up a new folder."
                )
                raise NoDropboxDirError(title, msg)

        marker_path = osp.join(self.dropbox_path, ROOT_MARKER_FILE)
        expected_marker = self._root_marker_content()
        root_identity = (root_stat.st_dev, root_stat.st_ino, root_stat.st_mode)
        if (
            self._confirmed_root_identity is not None
            and root_identity != self._confirmed_root_identity
        ):
            raise exception
        marker_is_valid = expected_marker is not None and self._root_marker_matches(
            marker_path,
            expected_marker,
            root_path=self.dropbox_path,
            expected_root_identity=root_identity,
        )

        if require_marker and not marker_is_valid:
            raise NoDropboxDirError(
                "Dropbox folder not confirmed",
                f'The folder does not contain a valid "{ROOT_MARKER_FILE}" for this '
                "Maestral profile. Check that the correct drive or network mount is "
                "available. If this is an existing Dropbox folder, confirm it before "
                "syncing.",
            )

        self._confirmed_root_identity = root_identity

    def create_root_marker(self) -> None:
        """Creates the marker which identifies the configured Dropbox folder."""
        self.ensure_dropbox_folder_present(require_marker=False)

        marker_path = osp.join(self.dropbox_path, ROOT_MARKER_FILE)
        marker_content = self._root_marker_content(create=True)
        assert marker_content is not None

        marker_snapshot = self._snapshot_local_tree(marker_path)
        marker_identity = marker_snapshot.get(marker_path)
        if marker_identity is not None:
            if not S_ISREG(marker_identity[2]):
                raise FileExistsError(marker_path)
            if self._root_marker_matches(
                marker_path,
                marker_content,
                root_path=self.dropbox_path,
                expected_root_identity=self.confirmed_root_identity,
            ):
                return
            raise FileExistsError(
                errno.EEXIST,
                f'Path "{marker_path}" already contains another file.',
                marker_path,
            )

        temp_file = create_rooted_tempfile(
            self.dropbox_path,
            self.dropbox_path,
            expected_root_identity=self.confirmed_root_identity,
            prefix=ROOT_MARKER_TEMP_PREFIX,
            mode=0o600,
        )
        try:
            with temp_file.open("wb") as marker_file:
                marker_file.write(marker_content)
                marker_file.flush()
                os.fsync(marker_file.fileno())

            temp_file.close()
            temp_identity = self._snapshot_local_item(
                temp_file.path,
                self.dropbox_path,
                expected_root_identity=self.confirmed_root_identity,
            )[:6]
            with self.fs_events.ignore(
                FileCreatedEvent(marker_path),
                FileModifiedEvent(marker_path),
                FileMovedEvent(temp_file.path, marker_path),
            ):
                move(
                    temp_file.path,
                    marker_path,
                    replace=False,
                    raise_error=True,
                    root_path=self.dropbox_path,
                    expected_root_identity=self.confirmed_root_identity,
                    expected_source_identity=temp_identity,
                )
        finally:
            temp_file.close()
            delete(
                temp_file.path,
                root_path=self.dropbox_path,
                expected_root_identity=self.confirmed_root_identity,
                expected_target_identity=temp_file.identity,
            )

    def ensure_cache_dir_present(self) -> None:
        """
        Checks for or creates a directory at :attr:`file_cache_path`.

        :raises CacheDirError: When local cache directory cannot be created.
        """
        retries = 0
        max_retries = 10

        while True:
            self.ensure_dropbox_folder_present()
            try:
                cache_stat = os.lstat(self.file_cache_path)
            except (FileNotFoundError, NotADirectoryError):
                try:
                    rooted_mkdir(
                        self.file_cache_path,
                        root_path=self.dropbox_path,
                        expected_root_identity=self.confirmed_root_identity,
                    )
                except FileExistsError:
                    pass
                except NotADirectoryError:
                    self.ensure_dropbox_folder_present()
                except OSError as err:
                    raise CacheDirError(
                        f"Cannot create cache directory: {err.strerror}",
                        "Please check if you have write permissions for "
                        f"{self._file_cache_path}.",
                    )
            else:
                if S_ISDIR(cache_stat.st_mode) and not is_fs_link(cache_stat):
                    return
                raise CacheDirError(
                    "Cannot use cache directory",
                    "The cache path is a symbolic link or is not a directory.",
                )

            if retries >= max_retries:
                raise CacheDirError(
                    "Cannot create cache directory",
                    "Exceeded maximum number of retries",
                )

            time.sleep(0.01)
            retries += 1

    def clean_cache_dir(self, raise_error: bool = True) -> None:
        """
        Removes unclaimed items in the cache directory.

        :param raise_error: Whether errors should be raised or only logged.
        """
        with self.sync_lock:
            if not self.dropbox_path:
                return
            try:
                self.ensure_dropbox_folder_present()
            except NoDropboxDirError:
                if raise_error:
                    raise
                return

            try:
                self._recover_pending_local_evacuations()
            except OSError as err:
                raise CacheDirError(
                    f"Cannot recover local item: {err.strerror}",
                    "Maestral kept the recovery data in its internal cache.",
                ) from err

            try:
                pending_names: set[str] = set()
                for token, entry in self._validated_local_evacuations().items():
                    pending_names.add(entry["backup_name"])
                    if entry["phase"] == "discarding":
                        pending_names.add(f"discard-{token}")
                cache_snapshot = self._snapshot_local_tree(self._file_cache_path)
                cache_identity = cache_snapshot.get(self._file_cache_path)
                if cache_identity is None:
                    return
                if not _snapshot_is_directory(cache_identity):
                    raise OSError(
                        errno.ENOTDIR,
                        "The cache path is not a safe directory",
                        self._file_cache_path,
                    )
                child_paths = {
                    path
                    for path in cache_snapshot
                    if osp.dirname(path) == self._file_cache_path
                }
                for child_path in child_paths:
                    child_name = osp.basename(child_path)
                    if child_name not in pending_names:
                        if child_name.startswith(("evac-", "discard-")):
                            raise CacheDirError(
                                "Cannot clean cache directory",
                                "An unjournalled local recovery item is present.",
                            )
                        child_snapshot = {
                            path: identity
                            for path, identity in cache_snapshot.items()
                            if path == child_path
                            or is_child(
                                path,
                                child_path,
                                self.is_fs_case_sensitive,
                            )
                        }
                        delete(
                            child_path,
                            raise_error=True,
                            root_path=self.dropbox_path,
                            expected_root_identity=self.confirmed_root_identity,
                            expected_target_identity=cache_snapshot[child_path][:6],
                            expected_tree_snapshot=child_snapshot,
                            content_hasher_factory=(self.client.content_hasher_factory),
                        )
                rooted_rmdir(
                    self._file_cache_path,
                    root_path=self.dropbox_path,
                    expected_root_identity=self.confirmed_root_identity,
                    expected_target_identity=cache_identity[:3],
                )
            except (FileNotFoundError, NotADirectoryError):
                pass
            except OSError as err:
                exc = CacheDirError(
                    f"Cannot clean cache directory: {err.strerror}",
                    "Please check if you have write permissions for "
                    f"{self._file_cache_path}.",
                )

                if raise_error:
                    raise exc

                self._logger.error(exc.title, exc_info=exc_info_tuple(exc))

    def _validated_local_evacuations(self) -> dict[str, dict[str, Any]]:
        """Return a validated copy of the durable local-evacuation journal."""
        journal = self._state.get("recovery", "local_evacuations")
        if not isinstance(journal, dict):
            raise CacheDirError(
                "Cannot read local recovery journal",
                "The saved local recovery journal has an invalid format.",
            )

        validated: dict[str, dict[str, Any]] = {}
        for token, entry in journal.items():
            if not isinstance(token, str) or not isinstance(entry, dict):
                raise CacheDirError(
                    "Cannot read local recovery journal",
                    "The saved local recovery journal has an invalid entry.",
                )
            backup_name = entry.get("backup_name")
            dbx_path = entry.get("dbx_path")
            change_dbid = entry.get("change_dbid")
            identity = entry.get("identity")
            phase = entry.get("phase")
            retained_boot_id = entry.get("retained_boot_id")
            snapshot_digest = entry.get("snapshot_digest")
            recovered_source = entry.get("recovered_source", "")
            visible_path = entry.get("visible_path", "")
            if (
                not token
                or not isinstance(backup_name, str)
                or backup_name != f"evac-{token}"
                or osp.basename(backup_name) != backup_name
                or not isinstance(dbx_path, str)
                or not isinstance(change_dbid, str)
                or not isinstance(identity, list)
                or len(identity) != 3
                or any(not isinstance(value, int) for value in identity)
                or phase
                not in {"reserved", "moved", "retained", "visible", "discarding"}
                or not isinstance(retained_boot_id, str)
                or not isinstance(snapshot_digest, str)
                or not isinstance(recovered_source, str)
                or not isinstance(visible_path, str)
                or (
                    recovered_source and normalize(recovered_source) != recovered_source
                )
                or (
                    phase in {"retained", "discarding"}
                    and (not retained_boot_id or len(snapshot_digest) != 64)
                )
                or (phase == "visible" and not visible_path)
            ):
                raise CacheDirError(
                    "Cannot read local recovery journal",
                    "The saved local recovery journal has an unsafe entry.",
                )
            try:
                self.to_local_path_from_cased(dbx_path)
                if visible_path:
                    self.to_local_path_from_cased(visible_path)
            except ValueError as exc:
                raise CacheDirError(
                    "Cannot read local recovery journal",
                    "The saved local recovery path is invalid.",
                ) from exc
            validated[token] = {
                "backup_name": backup_name,
                "dbx_path": dbx_path,
                "change_dbid": change_dbid,
                "identity": identity,
                "phase": phase,
                "retained_boot_id": retained_boot_id,
                "snapshot_digest": snapshot_digest,
                "recovered_source": recovered_source,
                "visible_path": visible_path,
            }
        return validated

    @staticmethod
    def _journal_item_identity(stat_result: os.stat_result) -> list[int]:
        """Return the stable identity used to bind an evacuation journal entry."""
        return [
            stat_result.st_dev,
            stat_result.st_ino,
            stat_result.st_mode,
        ]

    @staticmethod
    def _system_boot_id() -> str:
        """Return a stable identifier for the current operating-system boot."""
        if sys.platform.startswith("linux"):
            with open(
                "/proc/sys/kernel/random/boot_id",
                encoding="ascii",
            ) as boot_id_file:
                return f"linux:{boot_id_file.read().strip()}"

        if sys.platform == "darwin":

            class Timeval(ctypes.Structure):
                _fields_ = [
                    ("seconds", ctypes.c_long),
                    ("microseconds", ctypes.c_int),
                ]

            boot_time = Timeval()
            size = ctypes.c_size_t(ctypes.sizeof(boot_time))
            libc = ctypes.CDLL(None, use_errno=True)
            result = libc.sysctlbyname(
                b"kern.boottime",
                ctypes.byref(boot_time),
                ctypes.byref(size),
                None,
                0,
            )
            if result != 0:
                error_number = ctypes.get_errno()
                raise OSError(error_number, os.strerror(error_number))
            return f"darwin:{boot_time.seconds}:{boot_time.microseconds}"

        if sys.platform == "win32":  # pragma: no cover - Windows only

            class SystemTimeOfDayInformation(ctypes.Structure):
                _fields_ = [
                    ("boot_time", ctypes.c_longlong),
                    ("current_time", ctypes.c_longlong),
                    ("time_zone_bias", ctypes.c_longlong),
                    ("time_zone_id", ctypes.c_ulong),
                    ("reserved", ctypes.c_ulong),
                    ("boot_time_bias", ctypes.c_ulonglong),
                    ("sleep_time_bias", ctypes.c_ulonglong),
                ]

            info = SystemTimeOfDayInformation()
            ntdll = ctypes.WinDLL("ntdll")
            status = ntdll.NtQuerySystemInformation(
                3,
                ctypes.byref(info),
                ctypes.sizeof(info),
                None,
            )
            if status != 0:
                raise OSError(f"NtQuerySystemInformation failed: {status:#x}")
            return f"windows:{info.boot_time}"

        raise OSError("Cannot determine the operating-system boot identity")

    @staticmethod
    def _snapshot_digest(
        snapshot: dict[str, TreeSnapshotIdentity],
        root_path: str,
    ) -> str:
        """Return a stable digest for a rooted local-tree snapshot."""
        serialised = []
        for path, identity in sorted(snapshot.items()):
            stable_identity = list(identity)
            if path == root_path:
                stable_identity[5] = 0
            serialised.append((osp.relpath(path, root_path), stable_identity))
        payload = json.dumps(
            serialised,
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")
        return hashlib.sha256(payload).hexdigest()

    def _record_local_evacuation(
        self,
        local_path: str,
        dbx_path: str,
        change_dbid: str | None,
    ) -> tuple[str, str]:
        """Reserve and journal an internal path before moving local data."""
        self.ensure_cache_dir_present()
        source_identity = list(
            self._snapshot_local_item(
                local_path,
                self.dropbox_path,
                expected_root_identity=self.confirmed_root_identity,
            )[:3]
        )
        with self._local_mutation_lock:
            self._ensure_unlink_not_pending()
            journal = self._validated_local_evacuations()
            source_lower = self.to_dbx_path_lower(local_path)
            while True:
                token = uuid4().hex
                backup_name = f"evac-{token}"
                backup_path = osp.join(self.file_cache_path, backup_name)
                if token not in journal and not osp.lexists(backup_path):
                    break
            journal[token] = {
                "backup_name": backup_name,
                "dbx_path": dbx_path,
                "change_dbid": change_dbid or "",
                "identity": source_identity,
                "phase": "reserved",
                "retained_boot_id": "",
                "snapshot_digest": "",
                "recovered_source": (
                    source_lower
                    if self._recovered_local_root(source_lower) == source_lower
                    else ""
                ),
                "visible_path": "",
            }
            self._state.set("recovery", "local_evacuations", journal)
        return token, backup_path

    def _bind_local_evacuation(self, token: str, backup_path: str) -> None:
        """Bind a moved cache item identity to its durable journal entry."""
        backup_snapshot = self._snapshot_local_tree(backup_path)
        backup_identity = backup_snapshot.get(backup_path)
        with self._local_mutation_lock:
            journal = self._validated_local_evacuations()
            entry = journal.get(token)
            if entry is None:
                raise CacheDirError(
                    "Cannot preserve local item",
                    "The local recovery journal entry is missing.",
                )
            if (
                backup_identity is None
                or list(backup_identity[:3]) != entry["identity"]
            ):
                raise CacheDirError(
                    "Cannot preserve local item",
                    "The moved local recovery item does not match its journal.",
                )
            recovered_source = entry["recovered_source"]
            entry["phase"] = "moved"
            entry["recovered_source"] = ""
            journal[token] = entry
            self._state.set("recovery", "local_evacuations", journal)
            if recovered_source:
                self._forget_recovered_local_path(
                    recovered_source,
                    expected_identity=entry["identity"],
                    expected_source="",
                )

    def _retain_local_evacuation(
        self,
        event: SyncEvent,
        token: str,
        backup_path: str,
        expected_snapshot: dict[str, TreeSnapshotIdentity],
        *,
        original_path: str | None = None,
    ) -> bool:
        """Retain an unchanged inode until no old writer handle can survive."""
        snapshot = self._snapshot_local_tree(backup_path)
        if not self._evacuated_item_matches_snapshot(
            event,
            backup_path,
            expected_snapshot,
            original_path=original_path,
            actual_snapshot=snapshot,
        ):
            return False
        digest = self._snapshot_digest(snapshot, backup_path)
        boot_id = self._system_boot_id()
        with self._local_mutation_lock:
            journal = self._validated_local_evacuations()
            entry = journal.get(token)
            if entry is None:
                raise CacheDirError(
                    "Cannot retain local item",
                    "The local recovery journal entry is missing.",
                )
            entry["phase"] = "retained"
            entry["retained_boot_id"] = boot_id
            entry["snapshot_digest"] = digest
            journal[token] = entry
            self._state.set("recovery", "local_evacuations", journal)
        return True

    def _clear_local_evacuation(self, token: str) -> None:
        """Remove one completed local evacuation from the durable journal."""
        with self._local_mutation_lock:
            journal = self._validated_local_evacuations()
            if token in journal:
                journal.pop(token)
                self._state.set("recovery", "local_evacuations", journal)

    def _reserve_local_evacuation_visible_path(
        self,
        token: str,
        local_path: str,
    ) -> str:
        """Journal a visible recovery destination before moving its backup."""
        with self._local_mutation_lock:
            journal = self._validated_local_evacuations()
            entry = journal.get(token)
            if entry is None:
                raise CacheDirError(
                    "Cannot preserve local item",
                    "The local recovery journal entry is missing.",
                )
            previous_phase = entry["phase"]
            entry["phase"] = "visible"
            entry["visible_path"] = self.to_dbx_path(local_path)
            journal[token] = entry
            self._state.set("recovery", "local_evacuations", journal)
        return previous_phase

    def _cancel_local_evacuation_visible_path(
        self,
        token: str,
        previous_phase: str,
    ) -> None:
        """Undo a visible-path reservation after an atomic name collision."""
        with self._local_mutation_lock:
            journal = self._validated_local_evacuations()
            entry = journal.get(token)
            if entry is None:
                return
            entry["phase"] = previous_phase
            entry["visible_path"] = ""
            journal[token] = entry
            self._state.set("recovery", "local_evacuations", journal)

    def _recovery_conflict_base(self, dbx_path: str) -> str:
        """Return a safe visible base path for a recovered local item."""
        original_path = self.to_local_path_from_cased(dbx_path)
        parent_path = osp.dirname(original_path)
        while is_equal_or_child(
            parent_path,
            self.dropbox_path,
            self.is_fs_case_sensitive,
        ):
            try:
                parent_stat = os.lstat(parent_path)
            except (FileNotFoundError, NotADirectoryError):
                pass
            else:
                if S_ISDIR(parent_stat.st_mode) and not is_fs_link(parent_stat):
                    if not self._find_local_symlink(parent_path, remember=False):
                        return osp.join(parent_path, osp.basename(original_path))
            if parent_path == self.dropbox_path:
                break
            parent_path = osp.dirname(parent_path)
        return osp.join(self.dropbox_path, osp.basename(original_path))

    def _recover_pending_local_evacuations(self) -> None:
        """Restore crash-held local items as visible conflict copies."""
        with self._local_mutation_lock:
            journal = self._validated_local_evacuations()
            for token, entry in tuple(journal.items()):
                backup_path = osp.join(
                    self.file_cache_path,
                    entry["backup_name"],
                )
                backup_snapshot = self._snapshot_local_tree(backup_path)
                backup_identity = backup_snapshot.get(backup_path)
                discard_path = osp.join(self.file_cache_path, f"discard-{token}")
                discard_snapshot = self._snapshot_local_tree(discard_path)
                discard_identity = discard_snapshot.get(discard_path)
                visible_path = (
                    self.to_local_path_from_cased(entry["visible_path"])
                    if entry["visible_path"]
                    else ""
                )
                visible_snapshot = (
                    self._snapshot_local_tree(visible_path) if visible_path else {}
                )
                visible_identity = visible_snapshot.get(visible_path)

                if entry["phase"] != "discarding" and discard_identity is not None:
                    raise CacheDirError(
                        "Cannot recover local item",
                        "An unjournalled discard item occupies the recovery path.",
                    )

                if entry["phase"] == "visible" and visible_identity is not None:
                    if backup_identity is not None:
                        if list(visible_identity[:3]) == entry["identity"]:
                            raise CacheDirError(
                                "Cannot recover local item",
                                "Both the internal and visible recovery items match "
                                "the journal.",
                            )
                        self._forget_recovered_local_path(
                            self.to_dbx_path_lower(visible_path),
                            expected_identity=entry["identity"],
                            expected_source=self.to_dbx_path(backup_path),
                        )
                        entry["phase"] = "moved"
                        entry["visible_path"] = ""
                        journal[token] = entry
                        self._state.set("recovery", "local_evacuations", journal)
                        visible_path = ""
                        visible_identity = None
                    else:
                        if list(visible_identity[:3]) != entry["identity"]:
                            raise CacheDirError(
                                "Cannot recover local item",
                                "The visible recovery item does not match its journal.",
                            )
                        self._record_recovered_local_path(
                            visible_path,
                            visible_identity[:3],
                        )
                        journal.pop(token)
                        self.rescan(visible_path)
                        self._logger.warning(
                            'Recovered interrupted local change as "%s"',
                            visible_path,
                        )
                        continue

                if entry["phase"] == "retained" and backup_identity is not None:
                    if list(backup_identity[:3]) != entry["identity"]:
                        raise CacheDirError(
                            "Cannot recover local item",
                            "An internal recovery item no longer matches its journal.",
                        )
                    if entry["recovered_source"]:
                        recovered_source = entry["recovered_source"]
                        entry["recovered_source"] = ""
                        journal[token] = entry
                        self._state.set("recovery", "local_evacuations", journal)
                        self._forget_recovered_local_path(
                            recovered_source,
                            expected_identity=entry["identity"],
                            expected_source="",
                        )
                    actual_digest = self._snapshot_digest(
                        backup_snapshot,
                        backup_path,
                    )
                    if actual_digest != entry["snapshot_digest"]:
                        entry["phase"] = "moved"
                        journal[token] = entry
                        self._state.set("recovery", "local_evacuations", journal)
                    elif entry["retained_boot_id"] == self._system_boot_id():
                        continue
                    else:
                        entry["phase"] = "discarding"
                        journal[token] = entry
                        self._state.set("recovery", "local_evacuations", journal)

                if entry["phase"] == "discarding":
                    if backup_identity is not None and discard_identity is not None:
                        raise CacheDirError(
                            "Cannot recover local item",
                            "Both recovery discard paths exist.",
                        )
                    if backup_identity is None and discard_identity is None:
                        journal.pop(token)
                        self._state.set("recovery", "local_evacuations", journal)
                        continue

                    held_path = (
                        backup_path if backup_identity is not None else discard_path
                    )
                    held_snapshot = (
                        backup_snapshot
                        if backup_identity is not None
                        else discard_snapshot
                    )
                    held_identity = held_snapshot[held_path]
                    if list(held_identity[:3]) != entry["identity"]:
                        raise CacheDirError(
                            "Cannot recover local item",
                            "The discard item does not match its journal.",
                        )
                    if entry["recovered_source"]:
                        recovered_source = entry["recovered_source"]
                        entry["recovered_source"] = ""
                        journal[token] = entry
                        self._state.set("recovery", "local_evacuations", journal)
                        self._forget_recovered_local_path(
                            recovered_source,
                            expected_identity=entry["identity"],
                            expected_source="",
                        )

                    held_digest = self._snapshot_digest(
                        held_snapshot,
                        held_path,
                    )
                    if held_digest != entry["snapshot_digest"]:
                        if held_path == discard_path:
                            move(
                                discard_path,
                                backup_path,
                                replace=False,
                                raise_error=True,
                                root_path=self.dropbox_path,
                                expected_root_identity=(self.confirmed_root_identity),
                                expected_source_identity=held_identity[:6],
                            )
                            backup_snapshot = self._snapshot_local_tree(backup_path)
                            backup_identity = backup_snapshot.get(backup_path)
                        entry["phase"] = "moved"
                        journal[token] = entry
                        self._state.set("recovery", "local_evacuations", journal)
                    elif entry["retained_boot_id"] == self._system_boot_id():
                        continue
                    else:
                        try:
                            delete(
                                held_path,
                                raise_error=True,
                                root_path=self.dropbox_path,
                                expected_root_identity=(self.confirmed_root_identity),
                                expected_target_identity=held_identity[:6],
                                expected_tree_snapshot=held_snapshot,
                                quarantine_path=discard_path,
                                content_hasher_factory=(
                                    self.client.content_hasher_factory
                                ),
                            )
                        except OSError as deletion_error:
                            backup_snapshot = self._snapshot_local_tree(backup_path)
                            backup_identity = backup_snapshot.get(backup_path)
                            discard_snapshot = self._snapshot_local_tree(discard_path)
                            discard_identity = discard_snapshot.get(discard_path)
                            if (
                                backup_identity is not None
                                and discard_identity is not None
                            ):
                                raise CacheDirError(
                                    "Cannot recover local item",
                                    "Both recovery discard paths exist.",
                                ) from deletion_error
                            if backup_identity is None and discard_identity is None:
                                journal.pop(token)
                                self._state.set(
                                    "recovery",
                                    "local_evacuations",
                                    journal,
                                )
                                continue

                            remaining_path = (
                                backup_path
                                if backup_identity is not None
                                else discard_path
                            )
                            remaining_snapshot = (
                                backup_snapshot
                                if backup_identity is not None
                                else discard_snapshot
                            )
                            remaining_identity = remaining_snapshot[remaining_path]
                            if list(remaining_identity[:3]) != entry["identity"]:
                                raise CacheDirError(
                                    "Cannot recover local item",
                                    "The remaining discard item does not match its journal.",
                                ) from deletion_error
                            remaining_digest = self._snapshot_digest(
                                remaining_snapshot,
                                remaining_path,
                            )
                            if remaining_digest == entry["snapshot_digest"]:
                                raise
                            if remaining_path == discard_path:
                                move(
                                    discard_path,
                                    backup_path,
                                    replace=False,
                                    raise_error=True,
                                    root_path=self.dropbox_path,
                                    expected_root_identity=(
                                        self.confirmed_root_identity
                                    ),
                                    expected_source_identity=(remaining_identity[:6]),
                                )
                                backup_snapshot = self._snapshot_local_tree(backup_path)
                                backup_identity = backup_snapshot.get(backup_path)
                            entry["phase"] = "moved"
                            journal[token] = entry
                            self._state.set("recovery", "local_evacuations", journal)
                        else:
                            journal.pop(token)
                            self._state.set("recovery", "local_evacuations", journal)
                            continue

                if backup_identity is None:
                    if entry["phase"] == "reserved":
                        source_path = self.to_local_path_from_cased(entry["dbx_path"])
                        source_snapshot = self._snapshot_local_tree(source_path)
                        source_identity = source_snapshot.get(source_path)
                        if (
                            source_identity is not None
                            and list(source_identity[:3]) == entry["identity"]
                        ):
                            journal.pop(token)
                            continue
                    raise CacheDirError(
                        "Cannot recover local item",
                        "A journalled local recovery item is missing.",
                    )

                actual_identity = list(backup_identity[:3])
                if actual_identity != entry["identity"]:
                    raise CacheDirError(
                        "Cannot recover local item",
                        "An internal recovery item no longer matches its journal.",
                    )
                if entry["recovered_source"]:
                    recovered_source = entry["recovered_source"]
                    entry["recovered_source"] = ""
                    journal[token] = entry
                    self._state.set("recovery", "local_evacuations", journal)
                    self._forget_recovered_local_path(
                        recovered_source,
                        expected_identity=entry["identity"],
                        expected_source="",
                    )
                if entry["phase"] == "reserved":
                    entry["phase"] = "moved"
                    journal[token] = entry
                    self._state.set("recovery", "local_evacuations", journal)

                had_visible_reservation = entry["phase"] == "visible"
                candidate_destinations: tuple[str, ...]
                if had_visible_reservation:
                    destination = visible_path
                    self._record_recovered_local_path(
                        destination,
                        entry["identity"],
                        source_path=backup_path,
                    )
                    candidate_destinations = (destination,)
                else:
                    conflict_base = self._recovery_conflict_base(entry["dbx_path"])
                    candidate_destinations = tuple(
                        generate_cc_name(
                            conflict_base,
                            (
                                "recovered local change"
                                if reservation == 0
                                else f"recovered local change {reservation}"
                            ),
                        )
                        for reservation in range(100)
                    )

                destination = ""
                for candidate in candidate_destinations:
                    destination_lower = self.to_dbx_path_lower(candidate)
                    if not had_visible_reservation and (
                        self.get_index_entry(destination_lower) is not None
                        or self._reserved_recovered_local_root(destination_lower)
                        is not None
                    ):
                        continue
                    if not had_visible_reservation:
                        try:
                            self._record_recovered_local_path(
                                candidate,
                                entry["identity"],
                                source_path=backup_path,
                            )
                        except FileExistsError:
                            continue
                        previous_phase = self._reserve_local_evacuation_visible_path(
                            token,
                            candidate,
                        )
                        journal = self._validated_local_evacuations()
                        entry = journal[token]
                    else:
                        previous_phase = "moved"
                    try:
                        move(
                            backup_path,
                            candidate,
                            replace=False,
                            raise_error=True,
                            root_path=self.dropbox_path,
                            expected_root_identity=self.confirmed_root_identity,
                            expected_source_identity=tuple(entry["identity"]),
                        )
                    except FileExistsError:
                        if had_visible_reservation:
                            raise CacheDirError(
                                "Cannot recover local item",
                                "The reserved visible recovery path is occupied.",
                            )
                        self._cancel_local_evacuation_visible_path(
                            token,
                            previous_phase,
                        )
                        self._forget_recovered_local_path(
                            destination_lower,
                            expected_identity=entry["identity"],
                            expected_source=self.to_dbx_path(backup_path),
                        )
                        journal = self._validated_local_evacuations()
                        entry = journal[token]
                        continue
                    destination = candidate
                    break
                if not destination:
                    raise CacheDirError(
                        "Cannot recover local item",
                        "Could not reserve a visible recovery path.",
                    )

                moved_snapshot = self._snapshot_local_tree(destination)
                moved_identity = moved_snapshot.get(destination)
                if (
                    moved_identity is None
                    or list(moved_identity[:3]) != entry["identity"]
                ):
                    raise CacheDirError(
                        "Cannot recover local item",
                        "The visible recovery item does not match its journal.",
                    )

                self._activate_recovered_local_path(
                    self.to_dbx_path_lower(destination),
                    expected_identity=entry["identity"],
                    expected_source=self.to_dbx_path(backup_path),
                )

                journal = self._validated_local_evacuations()
                journal.pop(token)
                self._state.set("recovery", "local_evacuations", journal)
                self.rescan(destination)
                self._logger.warning(
                    'Recovered interrupted local change as "%s"',
                    destination,
                )
            self._state.set("recovery", "local_evacuations", journal)

    def _new_tmp_file(self) -> RootedTemporaryFile:
        """Create and hold a temporary file in the validated cache directory."""
        self.ensure_cache_dir_present()
        try:
            cache_snapshot = self._snapshot_local_tree(self.file_cache_path)
            cache_identity = cache_snapshot.get(self.file_cache_path)
            if cache_identity is None or not _snapshot_is_directory(cache_identity):
                raise CacheDirError(
                    "Cannot use cache directory",
                    "The cache path is a symbolic link or is not a directory.",
                )
            tmp_file = create_rooted_tempfile(
                self.file_cache_path,
                self.dropbox_path,
                expected_root_identity=self.confirmed_root_identity,
                mode=0o666 & ~umask,
            )
            return tmp_file
        except OSError as err:
            raise CacheDirError(
                f"Cannot create cache directory: {err.strerror}",
                "Please check if you have write permissions for "
                f"{self._file_cache_path}.",
            )

    def _discard_tmp_file(self, tmp_file: RootedTemporaryFile) -> None:
        """Close and delete a proof-bound temporary file."""
        tmp_file.close()
        delete(
            tmp_file.path,
            root_path=self.dropbox_path,
            expected_root_identity=self.confirmed_root_identity,
            expected_target_identity=tmp_file.identity,
        )

    def correct_case(self, dbx_path_basename_cased: str) -> str:
        """
        Converts a Dropbox path with correctly cased basename to a fully cased path.

        This is useful because the Dropbox API guarantees the correct casing for the
        basename only. In practice, casing of parent directories is often incorrect.
        This method retrieves the correct casing of all ancestors in the path, either
        from our cache, our database, or from Dropbox servers.

        Performance may vary significantly with the number of parent folders and the
        method used to resolve the casing of all parent directory names:

        1) If the parent directory is already in our cache, performance is O(1).
        2) If the parent directory is already in our sync index, performance is slower
           because it requires a sqlite query but still O(1).
        3) If the parent directory is unknown to us, its metadata (including the correct
           casing of the directory's basename) is queried from Dropbox. This is used to
           construct a correctly cased path by calling :meth:`correct_case` again. At
           best, performance will be of O(2) if the parent directory is known to us, at
           worst it will be of order O(N) involving queries to Dropbox servers for each
           of the N parent directories.

        When calling :meth:`correct_case` repeatedly for paths from the same tree, it is
        therefore best to do so in top-down traversal.

        :param dbx_path_basename_cased: Dropbox path with correctly cased basename, as
            provided by :attr:`dropbox.files.Metadata.path_display` or
            :attr:`dropbox.files.Metadata.name`.
        :returns: Correctly cased Dropbox path.
        """
        dbx_path_lower = normalize(dbx_path_basename_cased)

        dirname, basename = posixpath.split(dbx_path_basename_cased)
        dirname_lower = posixpath.dirname(dbx_path_lower)

        dirname_cased = self._correct_case_helper(dirname, dirname_lower)
        path_cased = posixpath.join(dirname_cased, basename)

        # Add our result to the cache.
        self._case_conversion_cache.put(dbx_path_lower, path_cased)

        return path_cased

    def _correct_case_helper(self, dbx_path: str, dbx_path_lower: str) -> str:
        """
        :param dbx_path: Uncased or randomly cased Dropbox path.
        :param dbx_path_lower: Normalized fully lower cased Dropbox path.
        :returns: Correctly cased Dropbox path.
        """
        # Check for root folder.
        if dbx_path == "/":
            return dbx_path

        # Check in our conversion cache.
        dbx_path_cased = self._case_conversion_cache.get(dbx_path_lower)

        if dbx_path_cased:
            return dbx_path_cased

        # Try to get casing from our index, this is slower.
        with self._database_access():
            entry = self.get_index_entry(dbx_path_lower)

        if entry:
            dbx_path_cased = entry.dbx_path_cased
        else:
            # Fall back to querying from server.
            md = self.client.get_metadata(dbx_path)
            if md:
                # Recurse over parent directories.
                dbx_path_cased = self.correct_case(md.path_display)
            else:
                # Give up.
                dbx_path_cased = dbx_path

        # Add our result to the cache.
        self._case_conversion_cache.put(dbx_path_lower, dbx_path_cased)

        return dbx_path_cased

    def to_dbx_path(self, local_path: str | bytes) -> str:
        """
        Converts a local path to a path relative to the Dropbox folder. Casing of the
        given ``local_path`` will be preserved.

        :param local_path: Absolute path on local drive.
        :returns: Relative path with respect to Dropbox folder.
        :raises ValueError: When the path lies outside the local Dropbox folder.
        """
        path = os.fsdecode(local_path)

        if not is_equal_or_child(path, self.dropbox_path, self.is_fs_case_sensitive):
            raise ValueError(f'"{path}" is not in "{self.dropbox_path}"')
        relative_path = osp.relpath(path, self.dropbox_path)
        if relative_path == osp.curdir:
            return "/"
        if osp.sep != "/":
            relative_path = relative_path.replace(osp.sep, "/")
        return "/" + relative_path

    def to_dbx_path_lower(self, local_path: str | bytes) -> str:
        """
        Converts a local path to a path relative to the Dropbox folder. The path will be
        normalized as on Dropbox servers (lower case and some additional
        normalisations).

        :param local_path: Absolute path on local drive.
        :returns: Relative path with respect to Dropbox folder.
        :raises ValueError: When the path lies outside the local Dropbox folder.
        """
        return normalize(self.to_dbx_path(local_path))

    def to_local_path_from_cased(self, dbx_path_cased: str) -> str:
        """
        Converts a correctly cased Dropbox path to the corresponding local path. This is
        more efficient than :meth:`to_local_path` which accepts uncased paths.

        :param dbx_path_cased: Path relative to Dropbox folder, correctly cased.
        :returns: Corresponding local path on drive.
        """
        if dbx_path_cased in ("", "/"):
            return self.dropbox_path
        if not dbx_path_cased.startswith("/"):
            raise ValueError(f"Invalid Dropbox path: {dbx_path_cased!r}")

        parts = dbx_path_cased[1:].split("/")
        if any(part in ("", ".", "..") or osp.basename(part) != part for part in parts):
            raise ValueError(f"Invalid Dropbox path: {dbx_path_cased!r}")
        if osp is ntpath and any(_is_invalid_windows_filename(part) for part in parts):
            raise ValueError(f"Invalid Dropbox path: {dbx_path_cased!r}")

        return osp.join(self.dropbox_path, *parts)

    def to_local_path(self, dbx_path: str) -> str:
        """
        Converts a Dropbox path to the corresponding local path. Only the basename must
        be correctly cased, as guaranteed by the Dropbox API for the ``display_path``
        attribute of file or folder metadata.

        This method slower than :meth:`to_local_path_from_cased`.

        :param dbx_path: Path relative to Dropbox folder, must be correctly cased in its
            basename.
        :returns: Corresponding local path on drive.
        """
        dbx_path_cased = self.correct_case(dbx_path)
        return self.to_local_path_from_cased(dbx_path_cased)

    def is_excluded(self, path: str | bytes) -> bool:
        """
        Checks if a file is excluded from sync. Certain file names are always excluded
        from syncing, following the Dropbox support article:

        https://help.dropbox.com/installs-integrations/sync-uploads/files-not-syncing

        This includes file system files such as 'desktop.ini' and '.DS_Store' and some
        temporary files as well as caches used by Dropbox or Maestral. `is_excluded`
        accepts both local and Dropbox paths.

        :param path: Can be an absolute path, a path relative to the Dropbox folder or
            just a file name. Does not need to be normalized.
        :returns: Whether the path is excluded from syncing.
        """
        path = os.fsdecode(path)
        dirname, basename = osp.split(path)

        # Is in excluded files?
        if basename in EXCLUDED_FILE_NAMES or basename.startswith(
            (
                PATH_ROOT_MIGRATION_PREFIX,
                PATH_ROOT_RECOVERY_PREFIX,
                ROOT_MARKER_TEMP_PREFIX,
                CASE_CHANGE_TEMP_PREFIX,
                MOVE_TEMP_PREFIX,
                REMOVE_TEMP_PREFIX,
            )
        ):
            return True

        # Is in excluded dirs?
        try:
            dbx_dirname = self.to_dbx_path(dirname)
        except ValueError:
            # Path is already relative to Dropbox.
            dbx_dirname = dirname

        root_dir = next(iter(part for part in dbx_dirname.split("/", 2) if part), "")

        if root_dir in EXCLUDED_DIR_NAMES or root_dir.startswith(
            (PATH_ROOT_MIGRATION_PREFIX, PATH_ROOT_RECOVERY_PREFIX)
        ):
            return True

        # Is temporary file?
        if "~" in basename:
            # 1) Office temporary files
            if basename.startswith("~$"):
                return True
            if basename.startswith(".~"):
                return True
            # 2) Other temporary files
            if basename.startswith("~") and basename.endswith(".tmp"):
                return True

        return False

    def is_excluded_by_selective_sync(self, dbx_path_lower: str) -> bool:
        """
        Check if a path is outside the current selective-sync selection.

        :param dbx_path_lower: Normalised lower case Dropbox path.
        :returns: Whether the path is excluded from download syncing by the user.
        """
        return self._is_path_excluded(
            normalize(dbx_path_lower),
            self.selective_sync_mode,
            self.selective_sync_paths,
        )

    def selective_sync_status(self, dbx_path: str) -> str:
        """Return ``included``, ``partially included``, or ``excluded``."""
        dbx_path_lower = "/" + normalize(dbx_path).strip("/")

        if self.is_excluded_by_selective_sync(dbx_path_lower):
            return "excluded"

        if self.selective_sync_mode == "include":
            if any(
                is_equal_or_child(dbx_path_lower, path)
                for path in self.selective_sync_paths
            ):
                return "included"
            return "partially included"

        if any(is_child(path, dbx_path_lower) for path in self.selective_sync_paths):
            return "partially included"

        return "included"

    def is_mignore(self, event: SyncEvent) -> bool:
        """
        Check if local file change has been excluded by a mignore pattern.

        :param event: SyncEvent for local file event.
        :returns: Whether the path is excluded from upload syncing by the user.
        """
        if len(self.mignore_rules.patterns) == 0:
            return False

        return self._is_mignore_path(
            event.dbx_path, is_dir=event.is_directory
        ) and not self.get_local_rev(event.dbx_path_lower)

    def _is_mignore_path(self, dbx_path: str, is_dir: bool = False) -> bool:
        relative_path = dbx_path.lstrip("/")

        if is_dir:
            relative_path = f"{relative_path}/"
        return self.mignore_rules.match_file(relative_path)

    def _slow_down(self) -> None:
        """
        Pauses if CPU usage is too high if called from one of our thread pools.
        """
        if self._max_cpu_percent == 100 * CPU_CORE_COUNT:
            return

        cpu_usage = cpu_usage_percent()

        if cpu_usage > self._max_cpu_percent:
            thread_name = current_thread().name
            self._logger.debug(f"{thread_name}: {cpu_usage}% CPU usage - throttling")

            while cpu_usage > self._max_cpu_percent:
                cpu_usage = cpu_usage_percent(0.5 + 2 * random.random())

            self._logger.debug(
                f"{thread_name}: {cpu_usage}% CPU usage - end throttling"
            )

    def request_cancel(self) -> None:
        """Ask all active sync work to stop without waiting for the sync lock."""
        self._cancel_requested.set()

    def cancel_sync(self) -> None:
        """
        Raises a :exc:`maestral.exceptions.CancelledError` in all sync threads and waits
        for them to shut down.
        """
        self.request_cancel()

        # Wait until we can acquire the sync lock => we are idle.
        self.sync_lock.acquire()
        self.sync_lock.release()

        self._cancel_requested.clear()

        self._logger.info("Sync aborted")

    def busy(self) -> bool:
        """
        Checks if we are currently syncing.

        :returns: ``True`` if :attr:`sync_lock` cannot be acquired, ``False`` otherwise.
        """
        idle = self.sync_lock.acquire(blocking=False)
        if idle:
            self.sync_lock.release()

        return not idle

    def _handle_sync_error(self, err: SyncError, direction: SyncDirection) -> None:
        """
        Handles a sync error. Fills out any missing path information and adds the error
        to the persistent state for later resync.

        :param err: The sync error to handle.
        :param direction: The sync direction (up or down) for which the error occurred.
        """
        # Fill in missing dbx_path or local_path.
        if err.dbx_path and not err.local_path:
            err.local_path = self.to_local_path_from_cased(err.dbx_path)
        if err.local_path and not err.dbx_path:
            err.dbx_path = self.to_dbx_path(err.local_path)

        # Fill in missing dbx_path_from or local_path_from.
        if err.dbx_path_from and not err.local_path_from:
            err.local_path_from = self.to_local_path_from_cased(err.dbx_path_from)
        if err.local_path_from and not err.dbx_path_from:
            err.dbx_path_from = self.to_dbx_path(err.local_path_from)

        if not err.dbx_path:
            raise ValueError("Sync error has an unknown path")

        printable_file_name = sanitize_string(posixpath.basename(err.dbx_path))

        self._logger.info("Could not sync %s", printable_file_name, exc_info=True)

        # Save sync errors to retry later.

        dbx_path_lower = normalize(err.dbx_path)
        if err.dbx_path_from:
            dbx_path_from_lower = normalize(err.dbx_path_from)
        else:
            dbx_path_from_lower = None

        with self._database_access():
            self._sync_errors_table.update(
                SyncErrorEntry(
                    dbx_path=err.dbx_path,
                    dbx_path_lower=dbx_path_lower,
                    dbx_path_from=err.dbx_path_from,
                    dbx_path_from_lower=dbx_path_from_lower,
                    local_path=err.local_path,
                    local_path_from=err.local_path_from,
                    direction=direction,
                    title=err.title,
                    message=err.message,
                    type=err.__class__.__name__,
                )
            )

    @contextmanager
    def _database_access(self, raise_error: bool = True) -> Iterator[None]:
        """
        A context manager to synchronises access to the SQLite database. Catches
        exceptions raised by sqlite3 and converts them to a MaestralApiError if we know
        how to handle them.

        :param raise_error: Whether errors should be raised or logged.
        """
        title = ""
        msg = ""
        new_exc = None

        try:
            with self._db_lock:
                yield
        except sqlite3.OperationalError as exc:
            title = "Database transaction error"
            msg = (
                f'The index file at "{self._db_path}" cannot be read. '
                "Please check that you have sufficient permissions and "
                "restart Maestral to continue syncing."
            )
            new_exc = DatabaseError(title, msg).with_traceback(exc.__traceback__)
        except sqlite3.IntegrityError as exc:
            title = "Database integrity error"
            msg = "Please restart Maestral to continue syncing."
            new_exc = DatabaseError(title, msg).with_traceback(exc.__traceback__)
        except sqlite3.DatabaseError as exc:
            title = "Database transaction error"
            msg = (
                "Please restart Maestral to continue syncing. "
                "Rebuild the index if this issue persists."
            )
            new_exc = DatabaseError(title, msg).with_traceback(exc.__traceback__)

        if new_exc:
            if raise_error:
                raise new_exc
            self._logger.error(title, exc_info=exc_info_tuple(new_exc))

    def _clear_caches(self) -> None:
        """
        Frees memory by clearing internal caches.
        """
        self._case_conversion_cache.clear()
        self.fs_events.expire_ignored_events()

    def _sync_event_from_fs_event(self, fs_event: FileSystemEvent) -> SyncEvent:
        event = SyncEvent.from_file_system_event(
            fs_event,
            self,
            skip_local_access=True,
        )
        try:
            identity = self._snapshot_local_item(
                event.local_path,
                self.dropbox_path,
                expected_root_identity=self.confirmed_root_identity,
            )
        except (FileNotFoundError, NotADirectoryError, OSError):
            return event

        event.size = 0 if _snapshot_is_directory(identity) else identity[3]
        event.change_time = (
            identity[4] if sys.platform == "win32" else identity[5]
        ) / 1_000_000_000
        content_identity = identity[6]
        symlink_target = _snapshot_symlink_target(identity)
        if symlink_target is not None:
            event.item_type = ItemType.File
            event.content_hash = None
            event.symlink_target = symlink_target
        elif _snapshot_is_directory(identity):
            event.item_type = ItemType.Folder
            event.content_hash = "folder"
            event.symlink_target = None
        else:
            event.item_type = ItemType.File
            event.content_hash = content_identity
            event.symlink_target = None
        return event

    def _filter_local_events(
        self, fs_events: Collection[FileSystemEvent]
    ) -> list[FileSystemEvent]:
        """Remove events outside include mode and events from ignored symlinks."""
        filtered_events: list[FileSystemEvent] = []

        for event in fs_events:
            dbx_src_path = self.to_dbx_path_lower(event.src_path)
            dbx_dest_path = (
                self.to_dbx_path_lower(event.dest_path) if is_moved(event) else None
            )
            if dbx_src_path == "/" and is_created(event):
                continue
            moved_recovery_paths: list[str] = []
            if is_moved(event) and dbx_dest_path:
                moved_recovery_paths = self._transfer_recovered_local_paths(
                    dbx_src_path,
                    dbx_dest_path,
                    self.to_dbx_path(event.dest_path),
                )

            if self.ignore_symlinks:
                stored_source = self._stored_ignored_symlink_for_path(dbx_src_path)
                stored_descendants = self._stored_ignored_symlinks_below(dbx_src_path)

                if is_deleted(event) and (stored_source or stored_descendants):
                    self._queue_remote_restore(stored_source or dbx_src_path)
                    continue

                if is_moved(event):
                    dest_symlink = self._find_local_symlink(event.dest_path)

                    if stored_source or stored_descendants or dest_symlink:
                        restore_path = stored_source or dbx_src_path
                        self._queue_remote_restore(restore_path)
                        self.refresh_ignored_symlinks()
                        self.rescan(event.dest_path)
                        for local_path in moved_recovery_paths:
                            self.rescan(local_path)
                        continue

                elif self._ignored_symlink_for_local_path(event.src_path):
                    continue

            source_is_required_parent = self._is_required_selective_sync_parent(
                dbx_src_path
            )
            destination_is_required_parent = bool(
                dbx_dest_path and self._is_required_selective_sync_parent(dbx_dest_path)
            )
            if is_moved(event) and (
                source_is_required_parent or destination_is_required_parent
            ):
                self._queue_remote_restore(dbx_src_path)
                if destination_is_required_parent and dbx_dest_path:
                    self._queue_remote_restore(dbx_dest_path)
                for local_path in moved_recovery_paths:
                    self.rescan(local_path)
                continue

            if source_is_required_parent:
                if is_deleted(event) or is_moved(event):
                    self._queue_remote_restore(dbx_src_path)
                    continue
                if not (
                    is_created(event)
                    and event.is_directory
                    and self.get_index_entry(dbx_src_path) is None
                ):
                    continue

            selective_filter_active = (
                self.selective_sync_mode == "include"
                or "/" in self.selective_sync_paths
            )
            source_is_excluded = selective_filter_active and (
                self.is_excluded_by_selective_sync(dbx_src_path)
            )
            destination_is_excluded = bool(
                selective_filter_active
                and dbx_dest_path
                and self.is_excluded_by_selective_sync(dbx_dest_path)
            )
            if (
                is_moved(event)
                and moved_recovery_paths
                and (source_is_excluded or destination_is_excluded)
            ):
                if not source_is_excluded:
                    self._queue_remote_restore(dbx_src_path)
                for local_path in moved_recovery_paths:
                    self.rescan(local_path)
                continue

            if (
                source_is_excluded
                and self._tracked_recovered_local_root(dbx_src_path) is None
            ):
                continue

            filtered_events.append(event)

        return filtered_events

    def _sync_events_from_fs_events(
        self, fs_events: list[FileSystemEvent]
    ) -> list[SyncEvent]:
        """Convert local file system events to sync events. This is done in a thread
        pool to parallelize content hashing."""
        fs_events = self._filter_local_events(fs_events)
        res = do_parallel(
            self._sync_event_from_fs_event,
            fs_events,
            thread_name_prefix="maestral-local-indexer",
        )
        return list(res)

    # ==== Upload sync =================================================================

    def upload_local_changes_while_inactive(self) -> None:
        """
        Collects changes while sync has not been running and uploads them to Dropbox.
        Call this method when resuming sync.
        """
        self.ensure_dropbox_folder_present()

        with self.sync_lock:
            # Delete upload sync errors before starting indexing. This prevents errors
            # from now deleted or ignored (.mignore) items from lingering on. All other
            # sync errors will be retried automatically by comparing local items against
            # our index.
            self._logger.debug("Pruning sync errors")

            with self._database_access():
                query = MatchQuery(SyncErrorEntry.direction, SyncDirection.Up)
                self._sync_errors_table.delete(query)

            self._logger.info("Indexing local changes...")

            try:
                events, local_cursor = self._get_local_changes_while_inactive()
            except OSError as err:
                if err.filename == self.dropbox_path:
                    self.ensure_dropbox_folder_present()

                raise os_to_maestral_error(err)

            events = self._clean_local_events(events)

            sync_events = self._sync_events_from_fs_events(events)
            del events

            if len(sync_events) > 0:
                self.apply_local_changes(sync_events)
                self._logger.debug("Uploaded local changes while inactive")
            else:
                self._logger.debug("No local changes while inactive")

            del sync_events
            gc.collect()

            self.local_cursor = local_cursor

            self._clear_caches()

    def _get_local_changes_while_inactive(self) -> tuple[list[FileSystemEvent], float]:
        """
        Retrieves all local changes since the last sync by performing a full scan of the
        local folder. Changes are detected by comparing the new directory snapshot to
        our index.

        Added items: Are present in the snapshot but not in our index.
        Deleted items: Are present in our index but not in the snapshot.
        Modified items: Are present in both but have a mtime newer than the last sync.

        Note that the client sets mtimes for files explicitly but never to a value in
        the future. mtime > last_sync therefore indicates a recent content change. We do
        not use the ctime here to avoid resyncing the entire folder after it has been
        moved (moving between partitions and on some file systems can change the ctime).

        :returns: Tuple containing local file system events and a cursor / timestamp
            for the changes.
        """
        changes: list[FileSystemEvent] = []
        snapshot_time = time.time()

        # Get modified or added items.
        for local_path, stat in rooted_walk(
            self.dropbox_path,
            self.dropbox_path,
            expected_root_identity=self.confirmed_root_identity,
            should_recurse=self._include_rooted_walk_entry,
        ):
            if not self._include_rooted_walk_entry(local_path, stat):
                continue
            is_dir = S_ISDIR(stat.st_mode) and not is_fs_link(stat)
            index_entry = self.get_index_entry_for_local_path(local_path)

            if index_entry:
                is_new = False
                last_sync = index_entry.last_sync or 0.0
            else:
                is_new = True
                last_sync = 0.0

            last_sync = max(last_sync, self.local_cursor)

            # Check if item was created or modified since the last sync,
            # but before we started the FileEventHandler (~snapshot_time).
            mtime_check = snapshot_time > stat.st_mtime > last_sync

            # Always upload untracked items, check mtime of tracked items.
            is_modified = mtime_check and not is_new

            event: FileSystemEvent
            event0: FileSystemEvent
            event1: FileSystemEvent

            if is_new:
                if is_dir:
                    event = DirCreatedEvent(local_path)
                else:
                    event = FileCreatedEvent(local_path)
                changes.append(event)

            elif is_modified:
                if is_dir and index_entry.is_directory:  # type: ignore
                    # We don't emit `DirModifiedEvent`s.
                    pass
                elif not is_dir and not index_entry.is_directory:  # type: ignore
                    event = FileModifiedEvent(local_path)
                    changes.append(event)
                elif is_dir:
                    event0 = FileDeletedEvent(local_path)
                    event1 = DirCreatedEvent(local_path)
                    changes += [event0, event1]
                elif not is_dir:
                    event0 = DirDeletedEvent(local_path)
                    event1 = FileCreatedEvent(local_path)
                    changes += [event0, event1]

        # Get deleted items.
        restore_paths: set[str] = set()
        invalid_index_paths: set[str] = set()
        for entry in self.iter_index():
            try:
                local_path_indexed = self.to_local_path_from_cased(entry.dbx_path_cased)
            except ValueError:
                invalid_index_paths.add(entry.dbx_path_lower)
                continue
            is_mignore = self._is_mignore_path(entry.dbx_path_cased, entry.is_directory)
            ignored_descendants: set[str] = set()

            if self.ignore_symlinks:
                ignored_path = self._stored_ignored_symlink_for_path(
                    entry.dbx_path_lower
                )
                ignored_descendants = self._stored_ignored_symlinks_below(
                    entry.dbx_path_lower
                )

                if ignored_path:
                    ignored_local_path = self._local_path_for_ignored_symlink(
                        ignored_path
                    )
                    if self._is_local_link(ignored_local_path):
                        continue
                    if not exists(ignored_local_path):
                        restore_paths.add(ignored_path)
                        continue
                    self.remove_node_from_index(ignored_path)
                    self._forget_ignored_symlink(ignored_path)
                    self.rescan(ignored_local_path)
                    continue

            elif self._find_local_symlink(local_path_indexed, remember=False):
                continue

            local_exists = self._exists_with_given_casing(local_path_indexed)

            if not local_exists and ignored_descendants:
                restore_paths.add(entry.dbx_path_lower)
                continue

            if is_mignore or not local_exists:
                if entry.is_directory:
                    event = DirDeletedEvent(local_path_indexed)
                else:
                    event = FileDeletedEvent(local_path_indexed)
                changes.append(event)

        for dbx_path in invalid_index_paths:
            self.remove_node_from_index(dbx_path)
            self.clear_sync_errors_for_path(dbx_path, recursive=True)

        for dbx_path in self.clean_selective_sync_paths(restore_paths):
            self._queue_remote_restore(dbx_path)

        # Ensure that the local Dropbox folder still exists before returning changes.
        # This prevents a deletion of the Dropbox folder from being incorrectly
        # processed as individual file deletions.
        self.ensure_dropbox_folder_present()

        duration = time.time() - snapshot_time
        self._logger.debug("Local indexing completed in %s sec", round(duration, 4))
        self._logger.debug("Retrieved local changes:\n%s", pf_repr(changes))

        return changes, snapshot_time

    def _exists_with_given_casing(self, local_path: str) -> bool:
        """
        On case-insensitive but case preserving file systems, a `os.path.exists`
        call will return true even if the local casing differs from the
        indexed casing. This method returns False if the display casing differs from the
        given input, even if paths refer to the same file.
        """
        if self.is_fs_case_sensitive:
            return exists(local_path)

        if not exists(local_path):
            return False

        try:
            local_path_displayed = to_existing_unnormalized_path(local_path)
            return equal_but_for_unicode_norm(local_path_displayed, local_path)
        except (FileNotFoundError, NotADirectoryError):
            return False

    def wait_for_local_changes(self, timeout: float = 40) -> bool:
        """
        Blocks until local changes are available.

        :param timeout: Maximum time in seconds to wait.
        :returns: ``True`` if changes are available, ``False`` otherwise.
        """
        self._logger.debug(
            "Waiting for local changes since cursor: %s", self.local_cursor
        )
        return self.fs_events.wait_for_event(timeout)

    def upload_sync_cycle(self) -> None:
        """
        Performs a full upload sync cycle by calling in order:

            1) :meth:`list_local_changes`
            2) :meth:`apply_local_changes`

        Handles updating the local cursor for you. If monitoring for local file events
        was interrupted, call :meth:`upload_local_changes_while_inactive` instead.
        """
        self.ensure_dropbox_folder_present()

        with self.sync_lock:
            changes, cursor = self.list_local_changes()
            self.apply_local_changes(changes)

            self.local_cursor = cursor

            # Free memory early to prevent fragmentation.
            del changes
            self._clear_caches()
            gc.collect()

            if self._cancel_requested.is_set():
                raise CancelledError("Sync cancelled")

    def list_local_changes(self, delay: float = 1) -> tuple[list[SyncEvent], float]:
        """
        Returns a list of local changes with at most one entry per path.

        :param delay: Delay in sec to wait for subsequent changes before returning.
        :returns: (list of sync times events, time_stamp)
        """
        events = []
        local_cursor = time.time()

        # Keep collecting events until idle for `delay`.
        while True:
            try:
                event = self.fs_events.local_file_event_queue.get(timeout=delay)
                events.append(event)
                local_cursor = time.time()
            except Empty:
                break

        self._logger.debug("Retrieved local file events:\n%s", pf_repr(events))

        events = self._clean_local_events(events)
        sync_events = self._sync_events_from_fs_events(events)

        # Free memory early to prevent fragmentation.
        del events
        gc.collect()

        return sync_events, local_cursor

    def apply_local_changes(
        self, sync_events: Collection[SyncEvent]
    ) -> list[SyncEvent]:
        """
        Applies locally detected changes to the remote Dropbox. Changes which should be
        ignored (mignore or always ignored files) are skipped.

        :param sync_events: List of local file system events.
        """
        results: list[SyncEvent] = []

        if len(sync_events) == 0:
            return results

        # Sort all sync events into deleted, dir_moved and other. Discard items
        # which are excluded by mignore or the internal exclusion list. Deleted and
        # dir_moved events will never be nested (we have already combined such nested
        # events) but all other events might be. We order and apply them hierarchically.

        deleted: list[SyncEvent] = []
        dir_moved: list[SyncEvent] = []
        other: defaultdict[int, list[SyncEvent]] = defaultdict(list)

        for event in sync_events:
            source_path = (
                event.dbx_path_from_lower
                if event.is_moved and event.dbx_path_from_lower
                else event.dbx_path_lower
            )
            moved_recovery_paths: list[str] = []
            if event.is_moved and event.dbx_path_from_lower:
                moved_recovery_paths = self._transfer_recovered_local_paths(
                    event.dbx_path_from_lower,
                    event.dbx_path_lower,
                    event.dbx_path,
                )

            selective_filter_active = (
                self.selective_sync_mode == "include"
                or "/" in self.selective_sync_paths
            )
            source_is_selectively_excluded = selective_filter_active and (
                self.is_excluded_by_selective_sync(source_path)
            )
            destination_is_selectively_excluded = selective_filter_active and (
                self.is_excluded_by_selective_sync(event.dbx_path_lower)
            )
            source_is_required_parent = self._is_required_selective_sync_parent(
                source_path
            )
            destination_is_required_parent = (
                event.is_moved
                and self._is_required_selective_sync_parent(event.dbx_path_lower)
            )
            if (
                event.is_moved
                and moved_recovery_paths
                and (
                    source_is_selectively_excluded
                    or destination_is_selectively_excluded
                    or source_is_required_parent
                    or destination_is_required_parent
                )
            ):
                if not source_is_selectively_excluded:
                    self._queue_remote_restore(source_path)
                if destination_is_required_parent:
                    self._queue_remote_restore(event.dbx_path_lower)
                for local_path in moved_recovery_paths:
                    self.rescan(local_path)
                continue

            if (
                self.is_excluded(event.local_path)
                or self.is_mignore(event)
                or (
                    (
                        self.selective_sync_mode == "include"
                        or "/" in self.selective_sync_paths
                    )
                    and self.is_excluded_by_selective_sync(event.dbx_path_lower)
                    and self._tracked_recovered_local_root(event.dbx_path_lower) is None
                )
            ):
                continue

            stored_symlink = self._stored_ignored_symlink_for_path(source_path)
            stored_descendants = self._stored_ignored_symlinks_below(source_path)
            if self.ignore_symlinks and (event.is_deleted or event.is_moved):
                if stored_symlink or stored_descendants:
                    self._queue_remote_restore(stored_symlink or source_path)
                    continue

            if event.is_moved and (
                source_is_required_parent or destination_is_required_parent
            ):
                self._queue_remote_restore(source_path)
                if destination_is_required_parent:
                    self._queue_remote_restore(event.dbx_path_lower)
                continue

            if source_is_required_parent:
                if event.is_deleted or event.is_moved:
                    self._queue_remote_restore(source_path)
                    continue
                if not (
                    event.is_added
                    and event.is_directory
                    and self.get_index_entry(source_path) is None
                ):
                    continue

            if self.ignore_symlinks and (
                event.symlink_target is not None
                or self._ignored_symlink_for_local_path(event.local_path)
            ):
                if event.symlink_target is not None:
                    self._remember_ignored_symlink(event.dbx_path_lower)
                continue

            if event.is_deleted:
                deleted.append(event)
            elif event.is_directory and event.is_moved:
                dir_moved.append(event)
            else:
                level = event.dbx_path.count("/")
                other[level].append(event)

            # Housekeeping.
            self.activity.add(event)

        self._logger.debug("Filtered deleted events:\n%s", pf_repr(deleted))
        self._logger.debug("Filtered dir moved events:\n%s", pf_repr(dir_moved))
        self._logger.debug("Filtered other events:\n%s", pf_repr(other))

        # Apply deleted events first, folder moved events second.
        # Neither event type requires an actual upload.
        if deleted:
            self._logger.info("Uploading deletions...")

        res = do_parallel(
            self._create_remote_entry,
            deleted,
            on_progress=lambda x, y: self._logger.info(f"Deleting {x}/{y}"),
            thread_name_prefix="maestral-upload-pool",
        )
        results.extend(res)

        if dir_moved:
            self._logger.info("Moving folders...")

        for event in dir_moved:
            self._logger.info(f"Moving {event.dbx_path_from}")
            r = self._create_remote_entry(event)
            results.append(r)

        # Apply other events in parallel, processing each hierarchy level successively.
        for level in sorted(other):
            res = do_parallel(
                self._create_remote_entry,
                other[level],
                on_progress=lambda x, y: self._logger.info(f"Syncing ↑ {x}/{y}"),
                thread_name_prefix="maestral-upload-pool",
            )
            results.extend(res)

        self._clean_history()

        completed = [
            event for event in results if event.status is not SyncStatus.Skipped
        ]
        if self.event_callback and completed:
            self.event_callback(completed)

        return results

    def _clean_local_events(
        self, events: Collection[FileSystemEvent]
    ) -> list[FileSystemEvent]:
        """
        Takes local file events and cleans them up as follows:

        1) Keep only a single event per path, unless the item type changed (e.g., from
           file to folder).
        2) Collapses moved and deleted events of folders with those of their children.

        The order of events will be preserved according to the first event registered
        for that path.

        :param events: Iterable of :class:`watchdog.FileSystemEvent`.
        :returns: List of :class:`watchdog.FileSystemEvent`.
        """
        # COMBINE EVENTS TO ONE EVENT PER PATH

        # Move events are difficult to combine with other event types, we split them
        # into deleted and created events and recombine them later if neither the source
        # of the destination path of has other events associated with it or is excluded
        # from sync.

        # mapping of path -> event history
        events_for_path: defaultdict[str | bytes, list[FileSystemEvent]] = defaultdict(
            list
        )

        # mapping of source deletion event -> destination creation event
        moved_from_to: dict[FileSystemEvent, FileSystemEvent] = {}
        moved_events_to_recombine: list[FileSystemEvent] = []

        for event in events:
            if is_moved(event):
                deleted, created = split_moved_event(event)
                moved_from_to[deleted] = created

                events_for_path[deleted.src_path].append(deleted)
                events_for_path[created.src_path].append(created)
            else:
                events_for_path[event.src_path].append(event)

        # For every path, keep only a single event which represents all changes,
        # unless we deal with a type change.
        for path in list(events_for_path):
            events = events_for_path[path]

            if len(events) == 1:
                event = events[0]
                # Mark moved events if there is a single event only at the src
                # and dest paths, to be recombined later.
                if event in moved_from_to:
                    dest_path = moved_from_to[event].src_path
                    dest_events = events_for_path.get(dest_path)
                    if dest_events is not None and len(dest_events) == 1:
                        moved_events_to_recombine.append(event)

            else:
                # Count how often the file / folder was created vs deleted.
                # Remember if it was first created or deleted.

                n_created = 0
                n_deleted = 0

                first_created_index = -1
                first_deleted_index = -1

                for i in reversed(range(len(events))):
                    event = events[i]

                    if is_created(event):
                        n_created += 1
                        first_created_index = i

                    if is_deleted(event):
                        n_deleted += 1
                        first_deleted_index = i

                if n_created > n_deleted:  # Item was created.
                    if events[-1].is_directory:
                        events_for_path[path] = [DirCreatedEvent(path)]
                    else:
                        events_for_path[path] = [FileCreatedEvent(path)]

                elif n_created < n_deleted:  # Item was deleted.
                    if events[0].is_directory:
                        events_for_path[path] = [DirDeletedEvent(path)]
                    else:
                        events_for_path[path] = [FileDeletedEvent(path)]

                else:  # Same number of deleted and created events.
                    if n_created == 0 or first_deleted_index < first_created_index:
                        # Item was modified.
                        if events[0].is_directory and events[-1].is_directory:
                            # Both first and last events are from folders.
                            events_for_path[path] = [DirModifiedEvent(path)]
                        elif not events[0].is_directory and not events[-1].is_directory:
                            # Both first and last events are from files.
                            events_for_path[path] = [FileModifiedEvent(path)]
                        elif events[0].is_directory:
                            # Type change folder -> file.
                            events_for_path[path] = [
                                DirDeletedEvent(path),
                                FileCreatedEvent(path),
                            ]
                        elif events[-1].is_directory:
                            # Type change file -> folder.
                            events_for_path[path] = [
                                FileDeletedEvent(path),
                                DirCreatedEvent(path),
                            ]
                    else:
                        # Item was likely only temporary. We still trigger a rescan of
                        # the path because some atomic modifications may be reported as
                        # out-of-order created and deleted events on macOS.
                        del events_for_path[path]
                        self.rescan(path)

        # Recombine moved events if we have retained both sides of event during the
        # above consolidation.
        for src_event in moved_events_to_recombine:
            src_path = src_event.src_path
            dest_path = moved_from_to[src_event].src_path

            new_event: DirMovedEvent | FileMovedEvent

            if src_event.is_directory:
                new_event = DirMovedEvent(src_path, dest_path)
            else:
                new_event = FileMovedEvent(src_path, dest_path)

            # Only recombine events if neither has an excluded path: We want to
            # treat renaming from / to an excluded path as a creation / deletion,
            # respectively.
            if not self._should_split_excluded(new_event):
                del events_for_path[src_path]
                events_for_path[dest_path] = [new_event]

        # At this point, `events_for_path` will contain a single event per path or
        # exactly two events (deleted and created) in case of a type change.

        # COMBINE MOVED AND DELETED EVENTS OF FOLDERS AND THEIR CHILDREN INTO ONE EVENT

        # Avoid nested iterations over all events here, they are on the order of O(n^2)
        # which becomes costly when the user moves or deletes folder with a large number
        # of children. Benchmark: aim to stay below 1 sec for 20,000 nested events on
        # representative laptop PCs.

        # 0) Collect all moved and deleted events in sets.

        dir_moved_paths: set[tuple[str | bytes, str | bytes]] = set()
        dir_deleted_paths: set[str | bytes] = set()

        for events in events_for_path.values():
            event = events[0]
            if isinstance(event, DirMovedEvent):
                dir_moved_paths.add((event.src_path, event.dest_path))
            elif isinstance(event, DirDeletedEvent):
                dir_deleted_paths.add(event.src_path)

        # 1) Combine moved events of folders and their children into one event.

        if len(dir_moved_paths) > 0:
            child_moved_dst_paths: set[str | bytes] = set()

            # For each event, check if it is a child of a moved event discard it if yes.
            for events in events_for_path.values():
                event = events[0]
                if is_moved(event):
                    dirnames = (
                        osp.dirname(event.src_path),
                        osp.dirname(event.dest_path),
                    )
                    if dirnames in dir_moved_paths:
                        child_moved_dst_paths.add(event.dest_path)

            for path in child_moved_dst_paths:
                del events_for_path[path]

        # 2) Combine deleted events of folders and their children to one event.

        if len(dir_deleted_paths) > 0:
            child_deleted_paths: set[str | bytes] = set()

            for events in events_for_path.values():
                event = events[0]
                if is_deleted(event):
                    dirname = osp.dirname(event.src_path)
                    if dirname in dir_deleted_paths:
                        child_deleted_paths.add(event.src_path)

            for path in child_deleted_paths:
                del events_for_path[path]

        # PREPARE RETURN VALUE AND FREE MEMORY

        cleaned_events = []

        for events in events_for_path.values():
            cleaned_events.extend(events)

        # Free memory early to prevent fragmentation.
        del events_for_path
        del moved_from_to
        del moved_events_to_recombine
        del dir_moved_paths
        del dir_deleted_paths
        gc.collect()

        return cleaned_events

    def _should_split_excluded(self, event: FileMovedEvent | DirMovedEvent) -> bool:
        dbx_src_path = self.to_dbx_path(event.src_path)
        dbx_dest_path = self.to_dbx_path(event.dest_path)

        if (
            self.is_excluded(event.src_path)
            or self.is_excluded(event.dest_path)
            or self.is_excluded_by_selective_sync(normalize(dbx_src_path))
            or self.is_excluded_by_selective_sync(normalize(dbx_dest_path))
        ):
            return True

        elif len(self.mignore_rules.patterns) == 0:
            return False
        else:
            src_is_mignore = self._is_mignore_path(dbx_src_path, event.is_directory)
            dest_is_mignore = self._is_mignore_path(dbx_dest_path, event.is_directory)

            return src_is_mignore or dest_is_mignore

    def _handle_normalization_conflict(self, event: SyncEvent) -> bool:
        """
        Checks for other items in the same directory with a different name but the same
        normalization.

        :param event: SyncEvent for local created or moved event.
        :returns: Whether a case conflict was detected and handled.
        """
        if not (event.is_added or event.is_moved):
            return False

        dirname, basename = osp.split(event.local_path)
        equivalent_paths = get_existing_equivalent_paths(basename, root=dirname)

        if len(equivalent_paths) > 1:
            # We have different file names that would map to the same normalized path!
            conflict_path = next(p for p in equivalent_paths if p != event.local_path)

            # Check if we have a case conflict or a unicode conflict.
            if normalize_case(event.local_path) == normalize_case(conflict_path):
                suffix = "case conflict"
            elif normalize_unicode(event.local_path) == normalize_unicode(
                conflict_path
            ):
                suffix = "unicode conflict"
            else:
                suffix = "normalization conflict"

            local_path_cc = self._move_local_to_unique_conflict(
                event,
                lambda: generate_cc_name(event.local_path, suffix=suffix),
                "Cannot create normalization conflict copy",
            )
            self.rescan(local_path_cc)

            self._logger.info(
                'Normalization conflict: renamed "%s" to "%s"',
                event.local_path,
                local_path_cc,
            )

            return True
        else:
            return False

    def _handle_selective_sync_conflict(self, event: SyncEvent) -> bool:
        """
        Checks for items in the local directory with same path as an item which is
        excluded by selective sync. Renames items if necessary.

        :param event: SyncEvent for local created or moved event.
        :returns: Whether a selective sync conflict was detected and handled.
        """
        if not (event.is_added or event.is_moved):
            return False

        if (
            self.selective_sync_mode == "exclude"
            and self.is_excluded_by_selective_sync(event.dbx_path_lower)
        ):
            local_path_cc = self._move_local_to_unique_conflict(
                event,
                lambda: generate_cc_name(
                    event.local_path,
                    suffix="selective sync conflict",
                ),
                "Cannot create selective sync conflict copy",
            )
            self.rescan(local_path_cc)

            self._logger.info(
                'Selective sync conflict: renamed "%s" to "%s"',
                event.local_path,
                local_path_cc,
            )
            return True
        else:
            return False

    def _create_remote_entry(self, event: SyncEvent) -> SyncEvent:
        """
        Applies a local file system event to the remote Dropbox and clears any existing
        sync errors belonging to that path. Any :exc:`maestral.exception.SyncError` will
        be caught and logged as appropriate.

        This method always uses a new copy of client and closes the network session
        afterwards.

        :param event: SyncEvent for local file event.
        :returns: SyncEvent with updated status.
        """
        if self._cancel_requested.is_set():
            raise CancelledError("Sync cancelled")

        self._slow_down()
        self.ensure_dropbox_folder_present()

        event.status = SyncStatus.Syncing

        try:
            local_paths = [event.local_path]
            if event.local_path_from:
                local_paths.append(event.local_path_from)

            symlink_path = next(
                (
                    path
                    for local_path in local_paths
                    if (
                        path := self._find_local_symlink(
                            local_path,
                            remember=self.ignore_symlinks,
                        )
                    )
                ),
                None,
            )

            if symlink_path and self.ignore_symlinks:
                status = SyncStatus.Skipped
            elif symlink_path:
                raise SymlinkError(
                    "Cannot sync symbolic link",
                    f'The local path contains the symbolic link "{symlink_path}".',
                    dbx_path=event.dbx_path,
                    local_path=event.local_path,
                )
            elif event.is_file and (event.is_added or event.is_changed):
                status = self._on_local_file_modified(event)
            elif event.is_directory and event.is_added:
                status = self._on_local_folder_created(event)
            elif event.is_moved:
                status = self._on_local_moved(event)
            elif event.is_deleted:
                status = self._on_local_deleted(event)
            else:
                status = SyncStatus.Skipped

            event.status = status

        except SyncError as err:
            self._handle_sync_error(err, direction=SyncDirection.Up)
            event.status = SyncStatus.Failed
        else:
            self.clear_sync_errors_from_event(event)
            if event.status is SyncStatus.Done:
                self._maybe_finish_recovered_local_path(event.dbx_path_lower)
            self.activity.discard(event)

        # Add events to history database.
        if event.status == SyncStatus.Done:
            with self._database_access():
                self._history_table.save(event)

        return event

    @staticmethod
    def _wait_for_creation(local_path: str) -> None:
        """
        Wait for a file at a path to be created or modified.

        :param local_path: Absolute path to file on drive.
        """
        try:
            while True:
                size1 = getsize(local_path)
                time.sleep(0.2)
                size2 = getsize(local_path)
                if size1 == size2:
                    return
        except OSError:
            return

    def _on_local_moved(self, event: SyncEvent) -> SyncStatus:
        """
        Call when a local item is moved.

        Keep in mind that we may be moving a whole tree of items. But its better deal
        with the complexity than to delete and re-uploading everything. Thankfully, in
        case of directories, we always process the top-level first. Trying to move the
        children will then be delegated to `on_create` (because the old item no longer
        lives on Dropbox) and that won't upload anything because file contents have
        remained the same.

        :param event: SyncEvent for local moved event.
        :returns: Metadata for created remote item at destination.
        :raises MaestralApiError: For any issues when syncing the item.
        """
        check_change_type(event, {ChangeType.Moved})
        check_encoding(event.local_path)

        if event.local_path_from == self.dropbox_path:
            self.ensure_dropbox_folder_present()

        if self._handle_selective_sync_conflict(event):
            return SyncStatus.Skipped
        if self._handle_normalization_conflict(event):
            return SyncStatus.Skipped

        dbx_path_from = cast(str, event.dbx_path_from)
        dbx_path_from_lower = cast(str, event.dbx_path_from_lower)
        local_path_from = cast(str, event.local_path_from)

        # Ignore changes in unicode normalization only. This avoids deleting and
        # recreating files on Dropbox which always normalizes to composed (NFC).
        if equal_but_for_unicode_norm(local_path_from, event.local_path):
            self._logger.debug(
                "Ignoring rename in unicode norm only: %s -> %s",
                local_path_from.encode(),
                event.local_path.encode(),
            )
            return SyncStatus.Skipped

        local_move_snapshot = self._snapshot_local_tree(event.local_path)
        if not local_move_snapshot:
            self.rescan(event.local_path)
            return SyncStatus.Skipped
        moved_identity = local_move_snapshot[event.local_path]
        recovered_source = self._validated_recovered_local_paths().get(
            dbx_path_from_lower
        )
        # Protect the visible destination before the first remote side effect. A
        # restart will then upload it before any remote deletion can remove it.
        self._record_recovered_local_path(
            event.local_path,
            moved_identity[:3],
        )
        if (
            dbx_path_from_lower != event.dbx_path_lower
            and recovered_source is not None
            and recovered_source["identity"] == list(moved_identity[:3])
        ):
            self._forget_recovered_local_path(
                dbx_path_from_lower,
                expected_identity=recovered_source["identity"],
                expected_source=recovered_source["source"],
            )
        source_index_entries = {
            entry.dbx_path_lower: entry
            for entry in self.iter_index()
            if is_equal_or_child(
                entry.dbx_path_lower,
                dbx_path_from_lower,
            )
        }
        source_entry = source_index_entries.get(dbx_path_from_lower)
        if source_entry is None:
            self.rescan(event.local_path)
            return SyncStatus.Skipped

        # If a file at the destination should be replaced, remove it first, but only if
        # its rev matches the rev of the local overwritten file.
        # The Dropbox API does not allow overwriting a destination file during a move.
        local_entry = self.get_index_entry(event.dbx_path_lower)

        if (
            local_entry
            and local_entry.is_file
            and event.dbx_path_from_lower != event.dbx_path_lower
        ):
            try:
                self.client.remove(
                    local_entry.dbx_path_lower,
                    parent_rev=local_entry.rev,
                    expected_provider_id=local_entry.provider_id,
                )
            except (NotFoundError, FileConflictError):
                pass
            else:
                self._logger.debug(
                    'Replacing existing file "%s" by move', local_entry.dbx_path_lower
                )

        # Perform the move.
        try:
            md_to_new = self.client.move(
                dbx_path_from,
                event.dbx_path,
                autorename=True,
                expected_provider_id=source_entry.provider_id,
            )
        except NotFoundError:
            # If not on Dropbox, e.g., because its old name was invalid,
            # create it instead of moving it.
            self._logger.debug(
                'Could not move "%s" -> "%s" on Dropbox, source does not exists. '
                'Creating "%s" instead',
                event.dbx_path_from,
                event.dbx_path,
                event.dbx_path,
            )

            self.rescan(event.local_path)
            return SyncStatus.Skipped

        moved_remote_entries = self._remote_metadata_tree(md_to_new)
        local_move_snapshot_after = self._snapshot_local_tree(event.local_path)
        move_is_verified = (
            local_move_snapshot_after == local_move_snapshot
            and self._snapshot_matches_index_tree(
                local_move_snapshot,
                event.local_path,
                dbx_path_from_lower,
                source_index_entries,
            )
            and self._remote_move_matches_index_tree(
                moved_remote_entries,
                md_to_new.path_lower,
                dbx_path_from_lower,
                source_index_entries,
            )
        )
        if not move_is_verified:
            self._queue_remote_restore(md_to_new.path_lower)
            self.remove_node_from_index(dbx_path_from_lower)
            self.rescan(event.local_path)
            self._logger.warning(
                'Moved remote item "%s" changed concurrently; preserving both sides',
                md_to_new.path_display,
            )
            return SyncStatus.Conflict

        self.remove_node_from_index(dbx_path_from_lower)

        upload_conflict = self._handle_upload_conflict(md_to_new, event)
        if upload_conflict:
            status = SyncStatus.Conflict
        else:
            status = SyncStatus.Done
            self._logger.debug(
                'Moved "%s" to "%s" on Dropbox', dbx_path_from, event.dbx_path
            )
        for md in moved_remote_entries.values():
            self.update_index_from_dbx_metadata(md)
        if upload_conflict:
            self._maybe_finish_recovered_local_path(md_to_new.path_lower)

        return status

    @staticmethod
    def _open_file_identity(file: BinaryIO) -> tuple[int, int, int, int, int, int]:
        stat_result = os.fstat(file.fileno())
        return (
            stat_result.st_dev,
            stat_result.st_ino,
            stat_result.st_mode,
            stat_result.st_size,
            stat_result.st_mtime_ns,
            stat_result.st_ctime_ns,
        )

    def _hash_open_upload_file(
        self,
        event: SyncEvent,
        file: BinaryIO,
    ) -> tuple[int, int, int, int, int, int]:
        """Hash a held upload source and update its event from the same inode."""
        initial_identity = self._open_file_identity(file)
        if not S_ISREG(initial_identity[2]):
            raise IsAFolderError(
                "Cannot upload local item",
                "The local upload source is not a regular file.",
                dbx_path=event.dbx_path,
                local_path=event.local_path,
            )

        hasher = self.client.content_hasher_factory()
        file.seek(0)
        while data := file.read(1024 * 1024):
            hasher.update(data)
        final_identity = self._open_file_identity(file)
        file.seek(0)
        if final_identity != initial_identity:
            raise DataChangedError("File was modified during hashing")

        event.content_hash = hasher.hexdigest()
        event.size = final_identity[3]
        event.change_time = get_local_change_time(os.fstat(file.fileno()))
        event.symlink_target = None
        return final_identity

    def _upload_path_matches_open_file(
        self,
        event: SyncEvent,
        expected_identity: tuple[int, ...],
    ) -> bool:
        """Return whether the rooted upload name still identifies the held file."""
        try:
            current_identity = self._snapshot_local_item(
                event.local_path,
                self.dropbox_path,
                expected_root_identity=self.confirmed_root_identity,
            )
        except OSError:
            return False
        return (
            current_identity[: len(expected_identity)] == expected_identity
            and current_identity[6] == event.content_hash
        )

    def _on_local_file_modified(self, event: SyncEvent) -> SyncStatus:
        """
        Call when a local file is created or modified.

        :param event: SyncEvent corresponding to local created event.
        :returns: Metadata for created item or None if no remote item is created.
        :raises MaestralApiError: For any issues when syncing the item.
        :raises ValueError: If the ChangeType is not Added or Modified.
        """
        check_change_type(event, {ChangeType.Added, ChangeType.Modified})
        check_encoding(event.local_path)

        if self._handle_selective_sync_conflict(event):
            return SyncStatus.Conflict
        if self._handle_normalization_conflict(event):
            return SyncStatus.Conflict

        self._wait_for_creation(event.local_path)

        self._raise_for_local_symlink_at_or_below(
            event.local_path,
            event.dbx_path,
            "Cannot upload local symbolic link",
        )

        local_entry = self.get_index_entry(event.dbx_path_lower)
        local_rev: str | None = None

        if not local_entry:
            # File is new to us, let Dropbox rename it if something is in the way.
            mode = WriteMode.Add
            event.change_type = ChangeType.Added
        elif local_entry.is_directory:
            # Try to overwrite the destination, this will fail...
            mode = WriteMode.Overwrite
        else:
            # File has been modified, update remote if matching rev,
            # create conflict otherwise.
            mode = WriteMode.Update
            event.change_type = ChangeType.Modified
            local_rev = local_entry.rev

        try:
            with self._parallel_up_semaphore:
                self.ensure_dropbox_folder_present()
                upload_file = open_rooted_file(
                    event.local_path,
                    self.dropbox_path,
                    expected_root_identity=self.confirmed_root_identity,
                )
                with upload_file:
                    source_identity = self._hash_open_upload_file(event, upload_file)
                    matching_remote = self._matching_remote_file(event)
                    if matching_remote is not None:
                        held_identity = self._open_file_identity(upload_file)
                        if held_identity != source_identity or not (
                            self._upload_path_matches_open_file(
                                event,
                                held_identity,
                            )
                        ):
                            try:
                                current_identity = self._snapshot_local_item(
                                    event.local_path,
                                    self.dropbox_path,
                                    expected_root_identity=(
                                        self.confirmed_root_identity
                                    ),
                                )
                            except (FileNotFoundError, NotADirectoryError):
                                pass
                            else:
                                self._record_recovered_local_path(
                                    event.local_path,
                                    current_identity[:3],
                                )
                            self.rescan(event.local_path)
                            return SyncStatus.Skipped

                        publication_snapshot = self._snapshot_local_tree(
                            event.local_path
                        )
                        publication_identity = publication_snapshot.get(
                            event.local_path
                        )
                        if (
                            publication_identity is None
                            or publication_identity[:6] != source_identity
                            or publication_identity[6] != event.content_hash
                        ):
                            if publication_identity is not None:
                                self._record_recovered_local_path(
                                    event.local_path,
                                    publication_identity[:3],
                                )
                            self.rescan(event.local_path)
                            return SyncStatus.Skipped
                        if not self._publish_metadata_for_local_snapshot(
                            matching_remote,
                            event.local_path,
                            publication_snapshot,
                        ):
                            return SyncStatus.Skipped
                        return SyncStatus.Skipped

                    md_new = self.client.upload(
                        upload_file,
                        event.dbx_path,
                        autorename=True,
                        write_mode=mode,
                        update_rev=local_rev,
                        sync_event=event,
                        local_path=event.local_path,
                    )
                    uploaded_identity = self._open_file_identity(upload_file)
        except (
            FileNotFoundError,
            NotADirectoryError,
            IsADirectoryError,
            NotFoundError,
            NotAFolderError,
            IsAFolderError,
        ):
            # Note: NotAFolderError can be raised when a parent in the local path
            # refers to a file instead of a folder.
            self._logger.debug(
                'Could not upload "%s": the file does not exist', event.local_path
            )
            return SyncStatus.Skipped
        except DataChangedError:
            self._logger.debug(
                'Could not upload "%s": the file was modified during upload',
                event.local_path,
            )
            return SyncStatus.Skipped
        except OSError as exc:
            if exc.errno in {errno.ENOENT, errno.ENOTDIR, errno.ESTALE, errno.ELOOP}:
                self.rescan(event.local_path)
                return SyncStatus.Skipped
            raise

        path_matches_upload = self._upload_path_matches_open_file(
            event,
            uploaded_identity,
        )
        needs_rescan = (
            uploaded_identity != source_identity
            or not path_matches_upload
            or isinstance(md_new, FileMetadata)
            and md_new.content_hash != event.content_hash
        )
        if needs_rescan:
            try:
                current_identity = self._snapshot_local_item(
                    event.local_path,
                    self.dropbox_path,
                    expected_root_identity=self.confirmed_root_identity,
                )
            except (FileNotFoundError, NotADirectoryError, OSError):
                pass
            else:
                self._record_recovered_local_path(
                    event.local_path,
                    current_identity[:3],
                )

        remote_was_renamed = (
            self.is_fs_case_sensitive and md_new.name != osp.basename(event.local_path)
        ) or (
            not self.is_fs_case_sensitive and md_new.path_lower != event.dbx_path_lower
        )
        if not path_matches_upload and remote_was_renamed:
            # The upload source no longer owns its pathname. Restore the uploaded
            # inode from its remote conflict path and rescan the replacement at the
            # original path. Do not publish an index row for either until then.
            self._queue_remote_restore(md_new.path_lower)
            self.rescan(event.local_path)
            return SyncStatus.Conflict

        upload_conflict = self._handle_upload_conflict(
            md_new,
            event,
            expected_source_identity=uploaded_identity,
        )
        if upload_conflict:
            status = SyncStatus.Conflict
        else:
            status = SyncStatus.Done
            if event.change_type is ChangeType.Added:
                self._logger.debug('Created "%s" on Dropbox', event.dbx_path)
            else:
                self._logger.debug('Uploaded modified "%s" to Dropbox', event.dbx_path)

        publication_path = (
            self.to_local_path(md_new.path_display)
            if upload_conflict
            else event.local_path
        )
        publication_snapshot = self._snapshot_local_tree(publication_path)
        publication_identity = publication_snapshot.get(publication_path)
        stable_fields = (0, 1, 2, 3, 4)
        if (
            not isinstance(md_new, FileMetadata)
            or publication_identity is None
            or any(
                publication_identity[index] != uploaded_identity[index]
                for index in stable_fields
            )
            or publication_identity[6] != md_new.content_hash
        ):
            if publication_identity is not None:
                self._record_recovered_local_path(
                    publication_path,
                    publication_identity[:3],
                )
            self.rescan(publication_path)
            return SyncStatus.Conflict
        if not self._publish_metadata_for_local_snapshot(
            md_new,
            publication_path,
            publication_snapshot,
        ):
            return SyncStatus.Conflict
        if needs_rescan:
            self.rescan(
                self.to_local_path(md_new.path_display)
                if status is SyncStatus.Conflict
                else event.local_path
            )

        return status

    def _on_local_folder_created(self, event: SyncEvent) -> SyncStatus:
        """
        Call when a local folder is created.

        :param event: SyncEvent corresponding to local created event.
        :returns: Metadata for created item or None if no remote item is created.
        :raises MaestralApiError: For any issues when syncing the item.
        """
        check_change_type(event, {ChangeType.Added})
        check_encoding(event.local_path)

        if self._handle_selective_sync_conflict(event):
            return SyncStatus.Conflict
        if self._handle_normalization_conflict(event):
            return SyncStatus.Conflict

        self._wait_for_creation(event.local_path)

        try:
            if self.client.is_team_space and event.dbx_path.count("/") == 1:
                md_new = self.client.share_dir(event.dbx_path)

                if not md_new:
                    # The remote folder disappeared before the API returned metadata.
                    # Keep the local tree and queue it again as durable recovery data.
                    self._logger.debug(
                        '"%s" on Dropbox was deleted after creation, '
                        "preserving the local copy",
                        event.dbx_path,
                    )
                    try:
                        current_identity = self._snapshot_local_item(
                            event.local_path,
                            self.dropbox_path,
                            expected_root_identity=self.confirmed_root_identity,
                        )
                    except (FileNotFoundError, NotADirectoryError, OSError):
                        pass
                    else:
                        self._record_recovered_local_path(
                            event.local_path,
                            current_identity[:3],
                        )
                    self.rescan(event.local_path)
                    return SyncStatus.Conflict
            else:
                md_new = self.client.make_dir(event.dbx_path)
        except FolderConflictError:
            self._logger.debug(
                'No conflict for "%s": the folder already exists', event.local_path
            )
            try:
                md = self.client.get_metadata(event.dbx_path)
                if isinstance(md, FolderMetadata):
                    self.update_index_from_dbx_metadata(md)
                    self._maybe_finish_recovered_local_path(md.path_lower)
            except NotFoundError:
                pass

            return SyncStatus.Skipped
        except FileConflictError:
            md_new = self.client.make_dir(event.dbx_path, autorename=True)

        upload_conflict = self._handle_upload_conflict(md_new, event)
        if upload_conflict:
            status = SyncStatus.Conflict
        else:
            status = SyncStatus.Done
            self._logger.debug('Created "%s" on Dropbox', event.dbx_path)

        self.update_index_from_dbx_metadata(md_new)
        if upload_conflict:
            self._maybe_finish_recovered_local_path(md_new.path_lower)

        return status

    def _on_local_deleted(self, event: SyncEvent) -> SyncStatus:
        """
        Call when a local item is deleted. We try not to delete remote items which have
        been modified since the last sync.

        :param event: SyncEvent for local deletion.
        :returns: Metadata for deleted item or None if no remote item is deleted.
        :raises MaestralApiError: For any issues when syncing the item.
        """
        check_change_type(event, {ChangeType.Removed})
        try:
            check_encoding(event.local_path)
        except PathError:
            # Don't raise an error here because the file cannot exist on the server.
            self._logger.debug(
                'Could not delete "%s": the item does not exist on Dropbox',
                event.dbx_path,
            )
            return SyncStatus.Skipped

        # We intercept any attempts to delete the home folder here instead of waiting
        # for an error from the Dropbox API. This allows us to provide a better error
        # message.

        home_path = self._state.get("account", "home_path")

        if event.dbx_path == home_path:
            raise SyncError(
                title="Could not delete item",
                message="Cannot delete the user's home folder",
                dbx_path=event.dbx_path,
                local_path=event.local_path,
            )

        if event.local_path == self.dropbox_path:
            self.ensure_dropbox_folder_present()

        if self.is_excluded_by_selective_sync(event.dbx_path_lower):
            self._logger.debug(
                'Not deleting "%s": is excluded by selective sync', event.dbx_path
            )
            return SyncStatus.Skipped

        md = self.client.get_metadata(event.dbx_path, include_deleted=True)

        if not md:
            self._logger.debug(
                'Could not delete "%s": the item does not exist on Dropbox',
                event.dbx_path,
            )
            return SyncStatus.Skipped

        if isinstance(md, FileMetadata):
            index_entry = self.get_index_entry(event.dbx_path_lower)
            if (
                index_entry is None
                or not index_entry.is_file
                or index_entry.rev != md.rev
                or index_entry.provider_id != md.id
            ):
                self._logger.warning(
                    'Keeping remote file "%s": its revision is not bound to the '
                    "local deletion",
                    md.path_display,
                )
                self._queue_remote_restore(event.dbx_path_lower)
                return SyncStatus.Conflict

        if event.is_directory and isinstance(md, FileMetadata):
            self._logger.warning(
                'Keeping remote file "%s": the local deletion expected a folder',
                md.path_display,
            )
            self._queue_remote_restore(event.dbx_path_lower)
            return SyncStatus.Conflict

        if event.is_file and isinstance(md, FolderMetadata):
            self._logger.warning(
                'Keeping remote folder "%s": the local deletion expected a file',
                md.path_display,
            )
            self._queue_remote_restore(event.dbx_path_lower)
            return SyncStatus.Conflict

        if event.is_directory and isinstance(md, FolderMetadata):
            # Dropbox has no compare-and-delete operation for folders. Restore the
            # remote tree instead of risking deletion of a concurrent child update.
            self._logger.warning(
                'Keeping remote folder "%s": Dropbox cannot bind a folder deletion '
                "to the indexed subtree",
                md.path_display,
            )
            self._queue_remote_restore(event.dbx_path_lower)
            return SyncStatus.Conflict

        if not isinstance(md, FileMetadata):
            self._queue_remote_restore(event.dbx_path_lower)
            return SyncStatus.Conflict

        try:
            self._snapshot_local_item(
                event.local_path,
                self.dropbox_path,
                expected_root_identity=self.confirmed_root_identity,
            )
        except (FileNotFoundError, NotADirectoryError):
            pass
        except OSError:
            self.rescan(event.local_path)
            return SyncStatus.Conflict
        else:
            self.rescan(event.local_path)
            return SyncStatus.Conflict

        try:
            # The metadata and indexed identity match, so bind the deletion to the
            # exact remote file revision.
            self.client.remove(
                event.dbx_path,
                parent_rev=md.rev,
                expected_provider_id=md.id,
            )
            status = SyncStatus.Done
        except NotFoundError:
            self._logger.debug(
                'Could not delete "%s": the item no longer exists on Dropbox',
                event.dbx_path,
            )
            status = SyncStatus.Skipped
        except PathError:
            self._logger.debug(
                'Could not delete "%s": the item has been changed since last sync',
                event.dbx_path,
            )
            status = SyncStatus.Skipped

        # Remove revision metadata.
        self.remove_node_from_index(event.dbx_path_lower)

        return status

    def _handle_upload_conflict(
        self,
        md_new: Metadata,
        event: SyncEvent,
        *,
        expected_source_identity: tuple[int, ...] | None = None,
    ) -> bool:
        """
        If a conflicting copy was created by Dropbox during the upload, we mirror the
        remote changes locally. This method can only be used for added, changed and
        moved events.

        :param md_new: Metadata of item after upload.
        :param event: Original upload sync event which triggered the upload.
        :returns: Whether a conflicting copy was created.
        """
        if event.is_deleted:
            raise ValueError("Cannot process deleted event.")

        if self.is_fs_case_sensitive:
            # Compare un-normalized file names. This ensures that we mirror locally any
            # unicode or case normalization performed by Dropbox servers.
            if md_new.name == osp.basename(event.local_path):
                # No conflicting copy was created.
                return False
        else:
            # Compare normalized paths. Mirroring any normalization changes locally is
            # not required on normalising file systems.
            if md_new.path_lower == event.dbx_path_lower:
                # No conflicting copy was created.
                return False

        # Get new local path corresponding to created entry.
        local_path_cc = self.to_local_path(md_new.path_display)
        source_snapshot = self._snapshot_local_tree(event.local_path)
        source_identity = source_snapshot.get(event.local_path)
        if source_identity is None:
            raise FileConflictError(
                "Cannot apply upload conflict rename",
                "The local upload source is missing.",
                dbx_path=event.dbx_path,
                local_path=event.local_path,
            )
        if expected_source_identity is None:
            expected_source_identity = source_identity[:6]
        elif source_identity[: len(expected_source_identity)] != (
            expected_source_identity
        ):
            raise FileConflictError(
                "Cannot apply upload conflict rename",
                "The local upload source changed after the upload.",
                dbx_path=event.dbx_path,
                local_path=event.local_path,
            )

        try:
            destination_snapshot = self._snapshot_local_tree(local_path_cc)
        except (FileNotFoundError, NotADirectoryError):
            destination_snapshot = {}
        destination_identity = destination_snapshot.get(local_path_cc)
        destination_is_source = bool(
            destination_identity and destination_identity[:3] == source_identity[:3]
        )
        if destination_identity is not None and not destination_is_source:
            raise FileConflictError(
                "Cannot apply upload conflict rename",
                "A different local item already uses the destination path.",
                dbx_path=event.dbx_path,
                local_path=local_path_cc,
            )

        # Move the local item.
        self._raise_for_local_move_symlink(
            event.local_path,
            local_path_cc,
            event.dbx_path,
            "Cannot apply upload conflict rename",
        )
        event_cls = (
            DirMovedEvent if _snapshot_is_directory(source_identity) else FileMovedEvent
        )
        source_recovery = self._validated_recovered_local_paths().get(
            event.dbx_path_lower
        )
        try:
            self._record_recovered_local_path(
                local_path_cc,
                source_identity[:3],
                source_path=event.local_path,
            )
        except FileExistsError as exc:
            raise FileConflictError(
                "Cannot apply upload conflict rename",
                "The local conflict destination is already reserved.",
                dbx_path=event.dbx_path,
                local_path=local_path_cc,
            ) from exc
        destination_lower = self.to_dbx_path_lower(local_path_cc)
        reservation_source = self.to_dbx_path(event.local_path)
        try:
            with self.fs_events.ignore(event_cls(event.local_path, local_path_cc)):
                with convert_api_errors():
                    move(
                        event.local_path,
                        local_path_cc,
                        replace=destination_is_source,
                        raise_error=True,
                        root_path=self.dropbox_path,
                        expected_root_identity=self.confirmed_root_identity,
                        expected_source_identity=expected_source_identity,
                    )
        except BaseException:
            try:
                destination_snapshot = self._snapshot_local_tree(local_path_cc)
            except OSError:
                pass
            else:
                destination_identity = destination_snapshot.get(local_path_cc)
                if destination_identity is None:
                    self._forget_recovered_local_path(
                        destination_lower,
                        expected_identity=source_identity[:3],
                        expected_source=reservation_source,
                    )
                elif list(destination_identity[:3]) == list(source_identity[:3]):
                    self._activate_recovered_local_path(
                        destination_lower,
                        expected_identity=source_identity[:3],
                        expected_source=reservation_source,
                    )
                    self.rescan(local_path_cc)
            raise

        self._activate_recovered_local_path(
            destination_lower,
            expected_identity=source_identity[:3],
            expected_source=reservation_source,
        )

        # Delete entry of old path.
        self.remove_node_from_index(event.dbx_path_lower)
        if source_recovery is not None:
            self._forget_recovered_local_path(
                event.dbx_path_lower,
                expected_identity=source_recovery["identity"],
                expected_source=source_recovery["source"],
            )
        self._logger.debug(
            'Upload conflict: renamed "%s" to "%s"',
            event.dbx_path,
            md_new.path_display,
        )

        return True

    def _matching_remote_file(self, event: SyncEvent) -> FileMetadata | None:
        """
        Return remote metadata when a local file does not need an upload.

        This checks for equal file contents. The caller publishes the matching metadata
        only after it records a durable local recovery path. Conflict resolution for
        different content remains with the server.

        :param event: Local sync event of a file creation or modification.
        :return: Matching remote metadata, or ``None`` when an upload is required.
        """
        if not (event.is_file and (event.is_changed or event.is_added)):
            raise ValueError("Can only be called with added or modified files")

        md_old = self.client.get_metadata(event.dbx_path)

        if isinstance(md_old, FileMetadata):
            if (
                event.content_hash == md_old.content_hash
                and event.symlink_target == md_old.symlink_target
            ):
                # File contents are identical, do not upload.
                change_type = "Modification" if event.is_changed else "Creation"
                self._logger.debug(
                    '%s of "%s" detected but file content is the same as on Dropbox',
                    change_type,
                    event.dbx_path,
                )
                return md_old

        return None

    # ==== Download sync ===============================================================

    def _is_unmanaged_remote_metadata(self, md: Metadata) -> bool:
        return (
            self.is_excluded_by_selective_sync(md.path_lower)
            or self.is_excluded(md.path_display)
            or (
                self.selective_sync_mode == "include"
                and isinstance(md, FileMetadata)
                and self._is_required_selective_sync_parent(md.path_lower)
            )
        )

    def _sync_event_from_remote_metadata(
        self, md: Metadata
    ) -> tuple[SyncEvent | None, bool]:
        """Convert metadata and report if an omitted item was safe to skip."""
        try:
            return SyncEvent.from_metadata(md, self), True
        except ValueError as exc:
            if isinstance(md, DeletedMetadata):
                self.remove_node_from_index(md.path_lower)
                self.clear_sync_errors_for_path(md.path_lower, recursive=True)
                return None, True

            if self._is_unmanaged_remote_metadata(md):
                self.clear_sync_errors_for_path(md.path_lower, recursive=True)
                return None, True

            local_path = osp.join(
                self.dropbox_path,
                *md.path_display.lstrip("/").split("/"),
            )
            error = PathError(
                "Cannot sync invalid path",
                str(exc),
                dbx_path=md.path_display,
                local_path=local_path,
            )
            self._handle_sync_error(error, direction=SyncDirection.Down)
            return None, False

    def get_remote_item(self, dbx_path: str) -> bool:
        """
        Downloads a remote file or folder and updates its local rev. If the remote item
        does not exist, any corresponding local items will be deleted. If ``dbx_path``
        refers to a folder, the download will be handled by :meth:`_get_remote_folder`.
        If it refers to a single file, the download will be performed by
        :meth:`_create_local_entry`.

        This method can be used to fetch individual items outside the regular sync
        cycle, for instance when including a previously excluded file or folder.

        :param dbx_path: Path relative to Dropbox folder.
        :returns: Whether download was successful.
        """
        self.ensure_dropbox_folder_present()
        self._logger.info(f"Syncing ↓ {dbx_path}")

        with self.sync_lock:
            dbx_path_lower = next(
                iter(
                    self.clean_selective_sync_paths(
                        [dbx_path],
                        validate_local=False,
                    )
                )
            )

            if dbx_path == "/":
                success = self._get_remote_folder(dbx_path)
                self._clear_caches()
                return success

            if self.is_excluded_by_selective_sync(dbx_path_lower):
                self._clear_caches()
                return True

            targeted_intent = self._targeted_download_intent(dbx_path_lower)
            if targeted_intent == "restore":
                self.remove_node_from_index(dbx_path_lower)

            index_entry = self.get_index_entry(dbx_path_lower)
            if index_entry is not None and targeted_intent in {"include", "restore"}:
                indexed_local_path = self.to_local_path_from_cased(
                    index_entry.dbx_path_cased
                )
                if not osp.lexists(indexed_local_path):
                    self.remove_node_from_index(dbx_path_lower)

            md = self.client.get_metadata(dbx_path, include_deleted=True)

            if md is None:
                # Create a fake deleted event.
                index_entry = self.get_index_entry(dbx_path_lower)
                cased_path = index_entry.dbx_path_cased if index_entry else dbx_path

                md = DeletedMetadata(
                    name=posixpath.basename(dbx_path),
                    path_lower=dbx_path_lower,
                    path_display=cased_path,
                )

            if (
                isinstance(md, FileMetadata)
                and self.selective_sync_mode == "include"
                and self._is_required_selective_sync_parent(dbx_path_lower)
            ):
                index_entry = self.get_index_entry(dbx_path_lower)
                deleted_md = DeletedMetadata(
                    name=md.name,
                    path_lower=md.path_lower,
                    path_display=(
                        index_entry.dbx_path_cased
                        if index_entry is not None
                        else md.path_display
                    ),
                )
                deleted_event = SyncEvent.from_metadata(deleted_md, self)
                deleted_results = self.apply_remote_changes([deleted_event])
                if not (
                    len(deleted_results) == 1
                    and deleted_results[0].status
                    in (SyncStatus.Done, SyncStatus.Skipped)
                ):
                    self._clear_caches()
                    return False

            event, conversion_succeeded = self._sync_event_from_remote_metadata(md)
            if event is None:
                self._clear_caches()
                return conversion_succeeded
            root_results = self.apply_remote_changes([event])
            success = (not root_results and self._is_unmanaged_remote_metadata(md)) or (
                len(root_results) == 1
                and root_results[0].status
                in (
                    SyncStatus.Done,
                    SyncStatus.Skipped,
                )
            )

            if success and event.is_directory and not event.is_deleted:
                success = self._get_remote_folder(dbx_path)

            self._clear_caches()

            return success

    def _get_remote_folder(self, dbx_path: str) -> bool:
        """
        Gets all files/folders from a Dropbox folder and writes them to the local folder
        :attr:`dropbox_path`.

        :param dbx_path: Path relative to Dropbox folder.
        :returns: Whether download was successful.
        """
        with self.sync_lock:
            try:
                idx = 0
                success = True

                # Iterate over index and download results.
                list_iter = self.client.list_folder_iterator(dbx_path, recursive=True)

                for res in list_iter:
                    idx += len(res.entries)

                    if idx > 0:
                        self._logger.info(f"Indexing {idx}...")

                    res.entries.sort(key=lambda x: x.path_lower.count("/"))

                    # Convert metadata to sync events without dropping a managed
                    # invalid path from the page result.
                    sync_events: list[SyncEvent] = []
                    page_success = True
                    for md in res.entries:
                        event, conversion_succeeded = (
                            self._sync_event_from_remote_metadata(md)
                        )
                        page_success = page_success and conversion_succeeded
                        if event is not None:
                            sync_events.append(event)
                    download_res = self.apply_remote_changes(sync_events)

                    page_success = page_success and all(
                        e.status in (SyncStatus.Done, SyncStatus.Skipped)
                        for e in download_res
                    )
                    success = success and page_success

                    if self._cancel_requested.is_set():
                        raise CancelledError("Sync cancelled")

            except SyncError as e:
                self._handle_sync_error(e, direction=SyncDirection.Down)
                return False

            return success

    def wait_for_remote_changes(
        self,
        last_cursor: str,
        timeout: int = 40,
    ) -> bool:
        """
        Blocks until changes to the remote Dropbox are available.

        :param last_cursor: Cursor form last sync.
        :param timeout: Timeout in seconds before returning even if there are no
            changes. Dropbox adds random jitter of up to 90 sec to this value.
        :returns: ``True`` if changes are available, ``False`` otherwise.
        """
        self._logger.debug("Waiting for remote changes since cursor:\n%s", last_cursor)
        has_changes = self.client.wait_for_remote_changes(last_cursor, timeout=timeout)

        # Wait for 2 sec. This delay is typically only necessary folders are shared /
        # un-shared with other Dropbox accounts.
        time.sleep(2)

        self._logger.debug("Detected remote changes: %s", has_changes)
        return has_changes

    def download_sync_cycle(self) -> None:
        """
        Performs a full download sync cycle by calling in order:

            1) :meth:`list_remote_changes_iterator`
            2) :meth:`apply_remote_changes`

        Handles updating the remote cursor and resuming interrupted syncs for you.
        Calling this method will perform a full indexing if this is the first download.
        """
        self.ensure_dropbox_folder_present()

        with self.sync_lock:
            if self.remote_cursor == "":
                self._state.set("sync", "last_reindex", time.time())
                self._state.set("sync", "did_finish_indexing", False)
                self._state.set("sync", "indexing_counter", 0)

            idx = self._state.get("sync", "indexing_counter")
            is_indexing = not self._state.get("sync", "did_finish_indexing")

            if is_indexing and idx == 0:
                self._logger.info("Indexing remote Dropbox")
            elif is_indexing:
                self._logger.info("Resuming indexing")
            else:
                self._logger.info("Fetching remote changes")

            changes_iter = self.list_remote_changes_iterator(self.remote_cursor)

            # Download changes in chunks to reduce memory usage.
            for changes, cursor in changes_iter:
                idx += len(changes)

                if idx > 0:
                    self._logger.info(f"Indexing {idx}...")

                downloaded = self.apply_remote_changes(changes)

                # Save (incremental) remote cursor.
                self.remote_cursor = cursor
                self._state.set("sync", "indexing_counter", idx)

                if self._cancel_requested.is_set():
                    raise CancelledError("Sync cancelled")

                # Free memory early to prevent fragmentation.
                del changes
                del downloaded
                gc.collect()

            self._state.set("sync", "did_finish_indexing", True)
            self._state.set("sync", "indexing_counter", 0)

            if idx > 0:
                self._logger.info(IDLE)

            self._clear_caches()

    def list_remote_changes_iterator(
        self, last_cursor: str
    ) -> Iterator[tuple[list[SyncEvent], str]]:
        """
        Get remote changes since the last download sync, as specified by
        ``last_cursor``. If the ``last_cursor`` is from paginating through a previous
        set of changes, continue where we left off. If ``last_cursor`` is an empty
        string, perform a full indexing of the Dropbox folder.

        :param last_cursor: Cursor from last download sync.
        :returns: Iterator yielding tuples with remote changes and corresponding cursor.
        """
        if last_cursor == "":
            # We are starting from the beginning, do a full indexing.
            changes_iter = self.client.list_folder_iterator("/", recursive=True)
        else:
            # Pick up where we left off. This may be an interrupted indexing /
            # pagination through changes or a completely new set of changes.
            self._logger.debug("Fetching remote changes since cursor: %s", last_cursor)
            indexed_paths = {
                entry.provider_id: entry.dbx_path_cased for entry in self.iter_index()
            }
            changes_iter = self.client.list_remote_changes_iterator(
                last_cursor,
                indexed_paths=indexed_paths,
            )

        for changes in changes_iter:
            changes = self._clean_remote_changes(changes)
            changes.entries.sort(key=lambda x: x.path_lower.count("/"))

            self._logger.debug("Remote changes:\n%s", pf_repr(changes.entries))

            sync_events: list[SyncEvent] = []
            for md in changes.entries:
                event, _ = self._sync_event_from_remote_metadata(md)
                if event is not None:
                    sync_events.append(event)

            self._logger.debug("Converted remote changes to SyncEvents")

            yield sync_events, changes.cursor

    def apply_remote_changes(
        self, sync_events: Collection[SyncEvent]
    ) -> list[SyncEvent]:
        """
        Applies remote changes to local folder. Call this on the result of
        :meth:`list_remote_changes`. The saved cursor is updated after a set of changes
        has been successfully applied. Entries in the local index are created after
        successful completion.

        :param sync_events: List of remote changes.
        :returns: List of changes that were made to local files and bool indicating if
            all download syncs were successful.
        """
        results: list[SyncEvent] = []

        if len(sync_events) == 0:
            return results

        # Sort changes into folders, files and deleted items. Discard paths outside the
        # current selection. Deleted exclusions are pruned in exclude mode.
        # Sort according to path hierarchy:
        # - Do not create sub-folder / file before parent exists.
        # - Delete parents before deleting children to save some work.

        files: list[SyncEvent] = []
        folders: defaultdict[int, list[SyncEvent]] = defaultdict(list)
        deleted: defaultdict[int, list[SyncEvent]] = defaultdict(list)

        selected_paths = self.selective_sync_paths

        for event in sync_events:
            is_required_parent_file = (
                self.selective_sync_mode == "include"
                and self._is_required_selective_sync_parent(event.dbx_path_lower)
                and event.is_file
                and not event.is_deleted
            )
            is_excluded = (
                self.is_excluded_by_selective_sync(event.dbx_path_lower)
                or self.is_excluded(event.dbx_path)
                or is_required_parent_file
            )

            if is_excluded:
                if event.is_deleted and self.selective_sync_mode == "exclude":
                    selected_paths = {
                        path
                        for path in selected_paths
                        if not is_equal_or_child(path, event.dbx_path_lower)
                    }

            elif self._ignored_symlink_for_local_path(event.local_path):
                # Keep the remote index current without touching the local overlay.
                self.update_index_from_sync_event(event, local_present=False)
                event.status = SyncStatus.Skipped
                results.append(event)

            else:
                level = event.dbx_path.count("/")

                if event.is_deleted:
                    deleted[level].append(event)
                elif event.is_file:
                    files.append(event)
                elif event.is_directory:
                    folders[level].append(event)

                # Housekeeping.
                self.activity.add(event)

        if selected_paths != self.selective_sync_paths:
            self.set_selective_sync(self.selective_sync_mode, selected_paths)

        # Apply deleted items.
        if deleted:
            self._logger.info("Applying deletions...")

        for level in sorted(deleted):
            res = do_parallel(
                self._create_local_entry,
                deleted[level],
                on_progress=lambda x, y: self._logger.info(f"Deleting {x}/{y}"),
                thread_name_prefix="maestral-download-pool",
            )
            results.extend(res)

        # Create local folders, start with top-level and work your way down.
        if folders:
            self._logger.info("Creating folders...")

        for level in sorted(folders):
            res = do_parallel(
                self._create_local_entry,
                folders[level],
                on_progress=lambda x, y: self._logger.info(f"Creating folder {x}/{y}"),
                thread_name_prefix="maestral-download-pool",
            )
            results.extend(res)

        # Apply created files.
        res = do_parallel(
            self._create_local_entry,
            files,
            on_progress=lambda x, y: self._logger.info(f"Syncing ↓ {x}/{y}"),
            thread_name_prefix="maestral-download-pool",
        )
        results.extend(res)

        self._clean_history()

        completed = [
            event for event in results if event.status is not SyncStatus.Skipped
        ]
        if self.event_callback and completed:
            self.event_callback(completed)

        return results

    def _display_name_for_account(self, dbid: str | None) -> str | None:
        """
        Returns the display name corresponding to a Dropbox ID.
        """
        if dbid is None:
            return None
        if dbid == self.client.account_info.account_id:
            # Return cached display name
            return self.client.account_info.display_name

        try:
            account_info = self.client.get_account_info(dbid)
        except InvalidDbidError:
            return None
        else:
            return account_info.display_name

    def _check_download_conflict(
        self,
        event: SyncEvent,
        local_snapshot: dict[str, TreeSnapshotIdentity],
    ) -> Conflict:
        """
        Check if a local item is conflicting with remote change. The equivalent check
        when uploading and a change will be carried out by Dropbox itself.

        Checks are carried out against our index, reflecting the latest sync state.
        We compare the following values:

        1) Local vs remote rev: 'folder' for folders, actual revision for files and None
           for deleted or not present items.
        2) Local vs remote content hash: 'folder' for folders, actual hash for files and
           None for deletions.
        3) Local change time vs last sync time: This is calculated recursively for
           folders. We use mtime on Windows and ctime on Unix.

        :param event: Download SyncEvent.
        :returns: Conflict check result.
        """
        local_rev = self.get_local_rev(event.dbx_path_lower)
        local_identity = local_snapshot.get(event.local_path)
        local_hash: str | None = None
        local_symlink_target: str | None = None
        if local_identity is not None:
            content_identity = local_identity[6]
            symlink_target = _snapshot_symlink_target(local_identity)
            if symlink_target is not None:
                local_symlink_target = symlink_target
            elif _snapshot_is_directory(local_identity):
                local_hash = "folder"
            else:
                local_hash = content_identity
        if (
            event.content_hash == local_hash
            and event.symlink_target == local_symlink_target
        ):
            # Content hashes are equal, therefore items are identical. Folders will
            # have a content hash of 'folder'.
            self._logger.debug(
                'Equal content hashes for "%s": no conflict', event.dbx_path
            )
            return Conflict.Identical

        if local_identity is not None and not self._snapshot_matches_index(
            local_snapshot,
            event.local_path,
            allow_ignored_link_overlays=event.is_deleted and self.ignore_symlinks,
            allow_selective_exclusions=event.is_deleted,
        ):
            self._logger.debug(
                'Local item "%s" differs from the sync index: conflict',
                event.dbx_path,
            )
            return (
                Conflict.LocalNewerOrIdentical
                if event.is_deleted
                else Conflict.Conflict
            )

        if osp.lexists(event.local_path) and (
            self._recovered_local_root(event.dbx_path_lower) is not None
            or self._has_recovered_local_descendant(event.dbx_path_lower)
        ):
            self._logger.debug(
                'Recovered local item at "%s": conflict',
                event.dbx_path,
            )
            return (
                Conflict.LocalNewerOrIdentical
                if event.is_deleted
                else Conflict.Conflict
            )

        targeted_intent = self._targeted_download_intent(event.dbx_path_lower)
        if targeted_intent and osp.lexists(event.local_path):
            index_entry = self.get_index_entry(event.dbx_path_lower)
            if targeted_intent == "restore" or index_entry is None:
                self._logger.debug(
                    'Existing local item at targeted path "%s": conflict',
                    event.dbx_path,
                )
                return (
                    Conflict.LocalNewerOrIdentical
                    if event.is_deleted
                    else Conflict.Conflict
                )

        if event.rev == local_rev:
            # Local change has the same rev. The local item (or deletion) must be newer
            # and not yet synced or identical to the remote state. Don't overwrite.
            self._logger.debug(
                'Equal revs for "%s": local item is the same or newer '
                "than on Dropbox",
                event.dbx_path,
            )
            return Conflict.LocalNewerOrIdentical

        elif (
            len(
                self.sync_errors_for_path(
                    event.dbx_path_lower, direction=SyncDirection.Up
                )
            )
            > 0
        ):
            # Local version could not be uploaded due to a sync error. Do not
            # over-write unsynced changes but declare a conflict.
            self._logger.debug(
                'Unresolved upload error for "%s": conflict', event.dbx_path
            )
            return Conflict.Conflict
        elif not self._change_time_newer_than_last_sync(event.local_path):
            # Last change time of local item (recursive for folders) is older than
            # the last time the item was synced. Remote must be newer.
            self._logger.debug(
                'Local item "%s" has no unsynced changes: remote item is newer',
                event.dbx_path,
            )
            return Conflict.RemoteNewer
        elif event.is_deleted:
            # Remote item was deleted but local item has been modified since then.
            self._logger.debug(
                'Local item "%s" has unsynced changes and remote was '
                "deleted: local item is newer",
                event.dbx_path,
            )
            return Conflict.LocalNewerOrIdentical
        else:
            # Both remote and local items have unsynced changes: conflict.
            self._logger.debug(
                'Local item "%s" has unsynced local changes: conflict',
                event.dbx_path,
            )
            return Conflict.Conflict

    def _change_time_newer_than_last_sync(self, local_path: str) -> bool:
        """
        Checks if a local item has any unsynced changes. This compares the platform
        change time to ``last_sync`` in our index. For folders, it checks all children.

        :param local_path: Local path of item to check.
        :returns: Whether the local item has unsynced changes.
        """
        if self.is_excluded(local_path):
            # Excluded names such as .DS_Store etc. never count as unsynced changes.
            return False

        dbx_path_lower = self.to_dbx_path_lower(local_path)
        if self.is_excluded_by_selective_sync(dbx_path_lower):
            return False
        if self._is_managed_symlink(local_path, dbx_path_lower):
            self._forget_ignored_symlink(dbx_path_lower)
        elif self._ignored_symlink_for_local_path(local_path):
            return False

        index_entry = self.get_index_entry(dbx_path_lower)
        ignored_paths: dict[str, bool] = {}

        def is_unmanaged_child(path: str) -> bool:
            if path == local_path:
                return False
            try:
                return ignored_paths[path]
            except KeyError:
                child_dbx_path_lower = self.to_dbx_path_lower(path)
                if self._is_managed_symlink(path, child_dbx_path_lower):
                    self._forget_ignored_symlink(child_dbx_path_lower)
                    ignored_paths[path] = False
                    return False
                ignored = (
                    self.is_excluded(path)
                    or self.is_excluded_by_selective_sync(child_dbx_path_lower)
                    or self._ignored_symlink_for_local_path(path) is not None
                )
                ignored_paths[path] = ignored
                return ignored

        with convert_api_errors(local_path=local_path):
            walked = tuple(
                rooted_walk(
                    local_path,
                    self.dropbox_path,
                    expected_root_identity=self.confirmed_root_identity,
                    should_recurse=lambda path, _stat: not is_unmanaged_child(path),
                    include_root=True,
                )
            )
            if not walked:
                # Do not check a timestamp for a deleted item, but confirm its prior
                # presence in the index.
                return index_entry is not None

            for path, path_stat in walked:
                if is_unmanaged_child(path):
                    continue
                path_dbx_lower = self.to_dbx_path_lower(path)
                path_entry = self.get_index_entry(path_dbx_lower)
                if S_ISDIR(path_stat.st_mode) and not is_fs_link(path_stat):
                    if path_entry is None or path_entry.is_file:
                        return True
                elif get_local_change_time(path_stat) > self.get_last_sync(
                    path_dbx_lower
                ):
                    return True

            return False

    def _get_change_time(self, local_path: str) -> float:
        """
        Returns a local item's platform change time, or -1.0 if it does not exist. For
        a directory, returns the latest child change time and ignores excluded items.

        :param local_path: Absolute path on local drive.
        :returns: Change time or -1.0.
        """
        try:
            absolute_path = osp.normpath(osp.abspath(local_path))
            walked = tuple(
                rooted_walk(
                    absolute_path,
                    self.dropbox_path,
                    expected_root_identity=self.confirmed_root_identity,
                    include_root=True,
                )
            )
            change_times = [
                get_local_change_time(path_stat)
                for path, path_stat in walked
                if path == absolute_path
                or S_ISDIR(path_stat.st_mode)
                or not self.is_excluded(path)
            ]
            return max(change_times, default=-1.0)
        except OSError as exc:
            raise os_to_maestral_error(exc, local_path=local_path)

    def _clean_remote_changes(self, changes: ListFolderResult) -> ListFolderResult:
        """
        Takes remote file events since last sync and cleans them up so that there is
        only a single event per path.

        Dropbox will sometimes report multiple changes per path. Once such instance is
        when sharing a folder: ``files/list_folder/continue`` will report the shared
        folder and its children as deleted and then created because the folder *is*
        actually deleted from the user's Dropbox and recreated as a shared folder which
        then gets mounted to the user's Dropbox. Ideally, we want to deal with this
        without re-downloading all its contents.

        :param changes: Result from Dropbox API call to retrieve remote changes.
        :returns: Cleaned up changes with a single Metadata entry per path.
        """
        # Note: we won't have to deal with modified or moved events,
        # Dropbox only reports DeletedMetadata or FileMetadata / FolderMetadata
        histories: defaultdict[str, list[Metadata]] = defaultdict(list)

        for entry in changes.entries:
            histories[entry.path_lower].append(entry)

        new_entries = []

        for h in histories.values():
            if len(h) == 1:
                new_entries.extend(h)
            else:
                last_event = h[-1]
                local_entry = self.get_index_entry(last_event.path_lower)
                was_dir = local_entry and local_entry.is_directory

                # Dropbox guarantees that applying events in the provided order will
                # reproduce the state in the cloud. We therefore keep only the last
                # event, unless there is a change in item type.
                if (
                    was_dir
                    and isinstance(last_event, FileMetadata)
                    or not was_dir
                    and isinstance(last_event, FolderMetadata)
                ):
                    deleted_event = DeletedMetadata(
                        name=last_event.name,
                        path_lower=last_event.path_lower,
                        path_display=last_event.path_display,
                    )
                    new_entries.append(deleted_event)
                    new_entries.append(last_event)
                else:
                    new_entries.append(last_event)

        changes.entries = new_entries

        return changes

    def _create_local_entry(self, event: SyncEvent) -> SyncEvent:
        """
        Applies a file / folder change from Dropbox servers to the local Dropbox folder.
        Any :exc:`maestral.exception.MaestralApiError` will be caught and logged as
        appropriate. Entries in the local index are created after successful completion.

        :param event: Dropbox metadata.
        :returns: Copy of the Dropbox metadata if the change was applied successfully,
            ``True`` if the change already existed, ``False`` in case of a sync error
            and ``None`` if cancelled.
        """
        if self._cancel_requested.is_set():
            raise CancelledError("Sync cancelled")

        self.ensure_dropbox_folder_present()
        self._slow_down()

        event.status = SyncStatus.Syncing

        try:
            managed_symlink = self._is_managed_symlink_at_event_path(event)
            if managed_symlink:
                self._forget_ignored_symlink(event.dbx_path_lower)
                symlink_path = None
            else:
                symlink_path = self._find_local_symlink(
                    event.local_path,
                    remember=self.ignore_symlinks,
                )
            if symlink_path:
                if self.ignore_symlinks:
                    self.update_index_from_sync_event(event, local_present=False)
                    status = SyncStatus.Skipped
                else:
                    raise SymlinkError(
                        "Cannot sync through symbolic link",
                        f'The local path contains the symbolic link "{symlink_path}".',
                        dbx_path=event.dbx_path,
                        local_path=event.local_path,
                    )
            elif event.is_deleted:
                status = self._on_remote_deleted(event)
            elif event.is_file:
                status = self._on_remote_file(event)
            elif event.is_directory:
                status = self._on_remote_folder(event)
            else:
                status = SyncStatus.Skipped

            event.status = status

        except SyncError as e:
            self._handle_sync_error(e, direction=SyncDirection.Down)
            event.status = SyncStatus.Failed
        else:
            if event.status not in {SyncStatus.Failed, SyncStatus.Conflict}:
                self.clear_sync_errors_from_event(event)
                self.activity.discard(event)

        # Add events to history database.
        if event.status == SyncStatus.Done:
            with self._database_access():
                self._history_table.save(event)

        return event

    def _ensure_parent(self, event: SyncEvent) -> None:
        """
        Ensures that all parent folders for a sync event exist locally. This is used to
        prevent children from being downloaded before their parents. In the most cases,
        we will automatically sync parents before their children but this is not always
        guaranteed. See https://github.com/SamSchott/maestral/issues/452.

        :param event: SyncEvent for target file.
        """
        dbx_path_lower_dirname = posixpath.dirname(event.dbx_path_lower)

        if dbx_path_lower_dirname == "/":
            return

        with self._tree_traversal:
            if not self.get_index_entry(dbx_path_lower_dirname):
                self._logger.debug(
                    f"Parent folder {dbx_path_lower_dirname} is not in index. "
                    f"Syncing it now before syncing any children."
                )

                parent_md = self.client.get_metadata(dbx_path_lower_dirname)

                if parent_md:  # If the parent no longer exists, we don't do anything.
                    parent_event = SyncEvent.from_metadata(parent_md, self)
                    self._on_remote_folder(parent_event)

    def _local_cc_filename(
        self,
        local_path: str,
        dbid: str | None,
        reservation: int = 0,
    ) -> str:
        change_owner = self._display_name_for_account(dbid)
        date = datetime.now().strftime("%Y-%m-%d")
        if change_owner:
            suffix = f"{change_owner}'s conflicted copy {date}"
        else:
            suffix = f"conflicted copy {date}"
        if reservation:
            suffix = f"{suffix} {reservation}"
        return generate_cc_name(local_path, suffix)

    def _move_local_to_unique_conflict(
        self,
        event: SyncEvent,
        destination_factory: Callable[[], str],
        title: str,
        *,
        source_path: str | None = None,
    ) -> str:
        """Move a local item to a conflict path without replacing another item."""
        source = source_path or event.local_path
        source_snapshot = self._snapshot_local_tree(source)
        if source not in source_snapshot:
            raise FileConflictError(
                title,
                "The local item disappeared before Maestral could preserve it.",
                dbx_path=event.dbx_path,
                local_path=source,
            )
        if self._snapshot_has_unmanaged_symlink(source_snapshot):
            raise SymlinkError(
                title,
                "The local item contains an unmanaged symbolic link.",
                dbx_path=event.dbx_path,
                local_path=source,
            )
        expected_source_identity = source_snapshot[source][:6]
        source_lower = self.to_dbx_path_lower(source)
        source_recovery = self._validated_recovered_local_paths().get(source_lower)
        source_is_recovery = bool(
            source_recovery
            and source_recovery["identity"] == list(expected_source_identity[:3])
        )
        for reservation in range(100):
            destination = destination_factory()
            if reservation:
                stem, extension = osp.splitext(destination)
                destination = f"{stem} (local recovery {reservation}){extension}"
            destination_lower = self.to_dbx_path_lower(destination)
            if (
                self.get_index_entry(destination_lower) is not None
                or self._reserved_recovered_local_root(destination_lower) is not None
            ):
                continue
            self._raise_for_local_ancestor_symlink(
                osp.dirname(source),
                event.dbx_path,
                title,
            )
            self._raise_for_local_ancestor_symlink(
                osp.dirname(destination),
                event.dbx_path,
                title,
            )
            try:
                self._record_recovered_local_path(
                    destination,
                    expected_source_identity[:3],
                    source_path=source,
                )
            except FileExistsError:
                continue
            reservation_source = self.to_dbx_path(source)
            event_cls = (
                DirMovedEvent
                if _snapshot_is_directory(source_snapshot[source])
                else FileMovedEvent
            )
            try:
                with self.fs_events.ignore(event_cls(source, destination)):
                    move(
                        source,
                        destination,
                        replace=False,
                        raise_error=True,
                        root_path=self.dropbox_path,
                        expected_root_identity=self.confirmed_root_identity,
                        expected_source_identity=expected_source_identity,
                    )
            except FileExistsError:
                self._forget_recovered_local_path(
                    destination_lower,
                    expected_identity=expected_source_identity[:3],
                    expected_source=reservation_source,
                )
                continue
            except OSError as err:
                try:
                    destination_snapshot = self._snapshot_local_tree(destination)
                except OSError:
                    pass
                else:
                    destination_identity = destination_snapshot.get(destination)
                    if destination_identity is None:
                        self._forget_recovered_local_path(
                            destination_lower,
                            expected_identity=expected_source_identity[:3],
                            expected_source=reservation_source,
                        )
                    elif list(destination_identity[:3]) == list(
                        expected_source_identity[:3]
                    ):
                        self._activate_recovered_local_path(
                            destination_lower,
                            expected_identity=expected_source_identity[:3],
                            expected_source=reservation_source,
                        )
                        self.rescan(destination)
                raise os_to_maestral_error(
                    err,
                    dbx_path=event.dbx_path,
                    local_path=source,
                ) from err
            self._activate_recovered_local_path(
                destination_lower,
                expected_identity=expected_source_identity[:3],
                expected_source=reservation_source,
            )
            if source_is_recovery and source_recovery is not None:
                self._forget_recovered_local_path(
                    source_lower,
                    expected_identity=source_recovery["identity"],
                    expected_source=source_recovery["source"],
                )
            return destination

        raise FileConflictError(
            title,
            "Could not reserve a unique conflict-copy path.",
            dbx_path=event.dbx_path,
            local_path=source,
        )

    def _evacuate_local_item(
        self,
        event: SyncEvent,
        *,
        local_path: str | None = None,
        dbx_path: str | None = None,
    ) -> tuple[str, str]:
        """Journal and atomically move a local item into the internal cache."""
        source_path = local_path or event.local_path
        source_dbx_path = dbx_path or event.dbx_path
        token, backup_path = self._record_local_evacuation(
            source_path,
            source_dbx_path,
            event.change_dbid,
        )
        expected_source_identity = tuple(
            self._validated_local_evacuations()[token]["identity"]
        )
        try:
            self._raise_for_local_ancestor_symlink(
                osp.dirname(source_path),
                source_dbx_path,
                "Cannot preserve local item before remote change",
            )
            self._raise_for_local_ancestor_symlink(
                osp.dirname(backup_path),
                source_dbx_path,
                "Cannot preserve local item before remote change",
            )
            event_cls = (
                DirMovedEvent
                if S_ISDIR(expected_source_identity[2])
                else FileMovedEvent
            )
            with self.fs_events.ignore(event_cls(source_path, backup_path)):
                move(
                    source_path,
                    backup_path,
                    replace=False,
                    raise_error=True,
                    root_path=self.dropbox_path,
                    expected_root_identity=self.confirmed_root_identity,
                    expected_source_identity=expected_source_identity,
                )
        except BaseException:
            backup_snapshot = self._snapshot_local_tree(backup_path)
            if backup_path in backup_snapshot:
                self._bind_local_evacuation(token, backup_path)
            else:
                self._clear_local_evacuation(token)
            raise
        self._bind_local_evacuation(token, backup_path)
        return token, backup_path

    def _evacuated_item_matches_snapshot(
        self,
        event: SyncEvent,
        backup_path: str,
        expected_snapshot: dict[str, TreeSnapshotIdentity],
        *,
        original_path: str | None = None,
        actual_snapshot: dict[str, TreeSnapshotIdentity] | None = None,
    ) -> bool:
        """Return whether an evacuated item still matches its stable snapshot."""
        source_path = original_path or event.local_path
        if actual_snapshot is None:
            actual_snapshot = self._snapshot_local_tree(backup_path)
        rebased_snapshot = {}
        for path, identity in expected_snapshot.items():
            relative_path = osp.relpath(path, source_path)
            rebased_path = (
                backup_path
                if relative_path == osp.curdir
                else osp.join(backup_path, relative_path)
            )
            rebased_snapshot[rebased_path] = identity

        if actual_snapshot.keys() != rebased_snapshot.keys():
            return False

        for path, expected_identity in rebased_snapshot.items():
            actual_identity = actual_snapshot[path]
            if path == backup_path:
                # A rename changes ctime for the moved root item. Compare every
                # other stat field and the full content identity.
                if (
                    actual_identity[:5] != expected_identity[:5]
                    or actual_identity[6:] != expected_identity[6:]
                ):
                    return False
            elif actual_identity != expected_identity:
                return False
        return True

    def _preserve_evacuated_conflict(
        self,
        event: SyncEvent,
        token: str,
        backup_path: str,
        *,
        original_path: str | None = None,
        original_dbx_path: str | None = None,
    ) -> str:
        """Move an internal backup to a visible conflict-copy path."""
        local_path = original_path or event.local_path
        dbx_path = original_dbx_path or event.dbx_path
        for reservation in range(100):
            conflict_path = self._local_cc_filename(
                local_path,
                event.change_dbid,
                reservation,
            )
            conflict_dbx_path = self.to_dbx_path_lower(conflict_path)
            if (
                self.get_index_entry(conflict_dbx_path) is not None
                or self._reserved_recovered_local_root(conflict_dbx_path) is not None
            ):
                continue
            self._raise_for_local_symlink_at_or_below(
                conflict_path,
                dbx_path,
                "Cannot preserve local change",
            )
            expected_identity = self._validated_local_evacuations()[token]["identity"]
            try:
                self._record_recovered_local_path(
                    conflict_path,
                    expected_identity,
                    source_path=backup_path,
                )
            except FileExistsError:
                continue
            previous_phase = self._reserve_local_evacuation_visible_path(
                token,
                conflict_path,
            )
            try:
                backup_stat = os.lstat(backup_path)
                event_cls = (
                    DirMovedEvent
                    if S_ISDIR(backup_stat.st_mode) and not is_fs_link(backup_stat)
                    else FileMovedEvent
                )
                with self.fs_events.ignore(event_cls(backup_path, conflict_path)):
                    move(
                        backup_path,
                        conflict_path,
                        replace=False,
                        raise_error=True,
                        root_path=self.dropbox_path,
                        expected_root_identity=self.confirmed_root_identity,
                        expected_source_identity=tuple(
                            self._validated_local_evacuations()[token]["identity"]
                        ),
                    )
            except FileExistsError:
                self._cancel_local_evacuation_visible_path(token, previous_phase)
                self._forget_recovered_local_path(
                    conflict_dbx_path,
                    expected_identity=expected_identity,
                    expected_source=self.to_dbx_path(backup_path),
                )
                continue
            except BaseException:
                try:
                    conflict_snapshot = self._snapshot_local_tree(conflict_path)
                except OSError:
                    pass
                else:
                    conflict_identity = conflict_snapshot.get(conflict_path)
                    if conflict_identity is None:
                        self._forget_recovered_local_path(
                            conflict_dbx_path,
                            expected_identity=expected_identity,
                            expected_source=self.to_dbx_path(backup_path),
                        )
                    elif list(conflict_identity[:3]) == expected_identity:
                        self._activate_recovered_local_path(
                            conflict_dbx_path,
                            expected_identity=expected_identity,
                            expected_source=self.to_dbx_path(backup_path),
                        )
                        self.rescan(conflict_path)
                raise
            visible_snapshot = self._snapshot_local_tree(conflict_path)
            visible_identity = visible_snapshot.get(conflict_path)
            expected_identity = self._validated_local_evacuations()[token]["identity"]
            if (
                visible_identity is None
                or list(visible_identity[:3]) != expected_identity
            ):
                raise CacheDirError(
                    "Cannot preserve local item",
                    "The visible recovery item does not match its journal.",
                )
            self._activate_recovered_local_path(
                conflict_dbx_path,
                expected_identity=expected_identity,
                expected_source=self.to_dbx_path(backup_path),
            )
            break
        else:
            raise FileConflictError(
                "Cannot preserve local change",
                "Could not reserve a unique conflict-copy path.",
                dbx_path=dbx_path,
                local_path=local_path,
            )

        self._clear_local_evacuation(token)
        self.rescan(conflict_path)
        return conflict_path

    def _on_remote_file(self, event: SyncEvent) -> SyncStatus:
        """
        Applies a remote file change or creation locally.

        :param event: SyncEvent for file download.
        :returns: SyncEvent corresponding to local item or None if no local changes
            are made.
        """
        self._apply_case_change(event)

        self._raise_for_remote_event_symlink(event, "Cannot replace folder with file")

        # Store the new entry at the given path in your local state. If the required
        # parent folders don’t exist yet, create them. If there’s already something else
        # at the given path, replace it and remove all its children.

        conflict_check, expected_snapshot = self._stable_download_conflict(event)

        if conflict_check is Conflict.Identical:
            if self._publish_sync_event_for_local_snapshot(event, expected_snapshot):
                return SyncStatus.Skipped
            return SyncStatus.Conflict
        elif conflict_check is Conflict.LocalNewerOrIdentical:
            return SyncStatus.Skipped

        # Ensure that parent folders are synced.
        self._ensure_parent(event)

        tmp_file = self._new_tmp_file()
        tmp_fname = tmp_file.path

        if event.symlink_target is not None:
            # Don't download but reproduce the symlink at a temporary path. It can then
            # atomically replace an existing local item like a regular downloaded file.
            with convert_api_errors(dbx_path=event.dbx_path):
                tmp_file.close()
                rooted_unlink(
                    tmp_fname,
                    root_path=self.dropbox_path,
                    expected_root_identity=self.confirmed_root_identity,
                    expected_target_identity=tmp_file.identity,
                )
                rooted_symlink(
                    event.symlink_target,
                    tmp_fname,
                    root_path=self.dropbox_path,
                    expected_root_identity=self.confirmed_root_identity,
                )
                symlink_snapshot = self._snapshot_local_tree(tmp_fname)
                symlink_identity = symlink_snapshot.get(tmp_fname)
                if (
                    symlink_identity is None
                    or symlink_identity[6] != f"symlink:{event.symlink_target}"
                ):
                    raise FileConflictError(
                        "Cannot stage remote symbolic link",
                        "The temporary link changed after Maestral created it.",
                        dbx_path=event.dbx_path,
                        local_path=tmp_fname,
                    )
                tmp_file.identity = symlink_identity[:3]
        else:
            # We download to a temporary file first (this may take some time).
            try:
                with self._parallel_down_semaphore:
                    with tmp_file.open("wb") as download_file:
                        md = self.client.download(
                            event.dbx_path,
                            download_file,
                            sync_event=event,
                            rev=event.rev,
                            provider_id=event.dbx_id,
                        )
                event = SyncEvent.from_metadata(md, self)
            except SyncError as err:
                self._discard_tmp_file(tmp_file)
                # Replace rev number with path.
                err.dbx_path = event.dbx_path
                raise err

        try:
            self._raise_for_remote_event_symlink(
                event, "Cannot replace folder with file"
            )
        except BaseException:
            self._discard_tmp_file(tmp_file)
            raise

        # Re-check against one stable snapshot after preparing the temporary item.
        conflict_check, expected_snapshot = self._stable_download_conflict(event)
        if conflict_check is Conflict.Identical:
            self._discard_tmp_file(tmp_file)
            if self._publish_sync_event_for_local_snapshot(event, expected_snapshot):
                return SyncStatus.Skipped
            return SyncStatus.Conflict
        if conflict_check is Conflict.LocalNewerOrIdentical:
            self._discard_tmp_file(tmp_file)
            return SyncStatus.Skipped

        if (
            conflict_check is Conflict.RemoteNewer
            and self._snapshot_has_selectively_excluded_descendant(
                expected_snapshot,
                event.local_path,
            )
        ):
            conflict_check = Conflict.Conflict

        if conflict_check is Conflict.Conflict:
            cc_local_path = self._move_local_to_unique_conflict(
                event,
                lambda: self._local_cc_filename(event.local_path, event.change_dbid),
                "Cannot create download conflict copy",
            )

            self._logger.debug(
                'Download conflict: renamed "%s" to "%s"',
                event.local_path,
                cc_local_path,
            )
            self.rescan(cc_local_path)
            status = SyncStatus.Conflict
            expected_snapshot = {}
        else:
            status = SyncStatus.Done

        evacuated_token: str | None = None
        evacuated_path: str | None = None
        evacuated_snapshot = expected_snapshot
        if isdir(event.local_path):
            self._raise_for_remote_event_symlink(
                event, "Cannot replace folder with file"
            )
            if self._snapshot_local_tree(event.local_path) != expected_snapshot:
                self._discard_tmp_file(tmp_file)
                raise FileConflictError(
                    "Cannot replace folder with file",
                    "The local folder changed before it could be replaced.",
                    dbx_path=event.dbx_path,
                    local_path=event.local_path,
                )
            evacuated_token, evacuated_path = self._evacuate_local_item(event)
            expected_snapshot = {}

        ignore_events: list[FileSystemEvent] = [
            FileMovedEvent(tmp_fname, event.local_path)
        ]

        # Preserve permissions of the destination file if we are only syncing an
        # update to the file content (Dropbox ID of the file remains the same). Do not
        # apply this to symlinks because chmod may follow the link target.
        old_entry = self.get_index_entry(event.dbx_path_lower)
        preserve_metadata = bool(
            event.symlink_target is None
            and old_entry
            and event.dbx_id == old_entry.provider_id
        )

        if isfile(event.local_path):
            # Ignore FileDeletedEvent when replacing old file.
            ignore_events.append(FileDeletedEvent(event.local_path))

        if evacuated_path is None and expected_snapshot:
            if self._snapshot_local_tree(event.local_path) != expected_snapshot:
                self._discard_tmp_file(tmp_file)
                raise FileConflictError(
                    "Cannot replace local file",
                    "The local file changed before it could be replaced.",
                    dbx_path=event.dbx_path,
                    local_path=event.local_path,
                )
            evacuated_token, evacuated_path = self._evacuate_local_item(event)
            evacuated_snapshot = expected_snapshot

        metadata_source_path = evacuated_path if preserve_metadata else None

        # Move the downloaded file or symlink to its destination.
        installed = False
        installed_identity: TreeSnapshotIdentity | None = None
        install_reservation_identity: tuple[int, ...] | None = None
        install_reservation_source = ""
        try:
            with self.fs_events.ignore(*ignore_events, recursive=False):
                with convert_api_errors(
                    dbx_path=event.dbx_path, local_path=event.local_path
                ):
                    self._raise_for_local_symlink_at_or_below(
                        event.local_path,
                        event.dbx_path,
                        "Cannot replace local symbolic link",
                    )
                    expected_tmp_content: str | None
                    if not tmp_file.closed:
                        with tmp_file.open("rb") as staged_file:
                            initial_tmp_identity = self._open_file_identity(staged_file)
                            hasher = self.client.content_hasher_factory()
                            while data := staged_file.read(1024 * 1024):
                                hasher.update(data)
                            expected_tmp_identity = self._open_file_identity(
                                staged_file
                            )
                        if expected_tmp_identity != initial_tmp_identity:
                            raise DataChangedError(
                                "Downloaded file changed during verification"
                            )
                        expected_tmp_content = hasher.hexdigest()
                        if expected_tmp_content != event.content_hash:
                            raise DataChangedError(
                                "Downloaded file changed before installation"
                            )
                    else:
                        tmp_identity = self._snapshot_local_item(
                            tmp_fname,
                            self.dropbox_path,
                            expected_root_identity=self.confirmed_root_identity,
                        )
                        expected_tmp_identity = tmp_identity[:6]
                        expected_tmp_content = tmp_identity[6]
                    install_reservation_identity = tuple(expected_tmp_identity[:3])
                    install_reservation_source = self.to_dbx_path(tmp_fname)
                    self._record_recovered_local_path(
                        event.local_path,
                        install_reservation_identity,
                        source_path=tmp_fname,
                    )
                    tmp_file.close()
                    move(
                        tmp_fname,
                        event.local_path,
                        keep_target_permissions=preserve_metadata,
                        keep_target_xattrs=preserve_metadata,
                        replace=False,
                        metadata_source_path=metadata_source_path,
                        raise_error=True,
                        root_path=self.dropbox_path,
                        expected_root_identity=self.confirmed_root_identity,
                        expected_source_identity=expected_tmp_identity,
                    )
                    installed = True
                    if not self._activate_recovered_local_path(
                        event.dbx_path_lower,
                        expected_identity=install_reservation_identity,
                        expected_source=install_reservation_source,
                    ):
                        raise CacheDirError(
                            "Cannot track downloaded file",
                            "The local publication reservation changed.",
                        )
                    installed_identity = self._snapshot_local_item(
                        event.local_path,
                        self.dropbox_path,
                        expected_root_identity=self.confirmed_root_identity,
                    )
                    stable_fields = (0, 1, 3, 4)
                    if (
                        any(
                            installed_identity[index] != expected_tmp_identity[index]
                            for index in stable_fields
                        )
                        or installed_identity[6] != expected_tmp_content
                    ):
                        raise DataChangedError(
                            "Downloaded file changed during installation"
                        )
                    tmp_inode = installed_identity[1]
                    tmp_mtime = installed_identity[4] / 1_000_000_000
        except BaseException:
            if not installed and install_reservation_identity is not None:
                try:
                    installed_snapshot = self._snapshot_local_tree(event.local_path)
                except OSError:
                    pass
                else:
                    installed_candidate = installed_snapshot.get(event.local_path)
                    if installed_candidate is not None and list(
                        installed_candidate[:3]
                    ) == list(install_reservation_identity):
                        installed = True
                        installed_identity = installed_candidate
                        self._activate_recovered_local_path(
                            event.dbx_path_lower,
                            expected_identity=install_reservation_identity,
                            expected_source=install_reservation_source,
                        )
            if installed:
                if installed_identity is not None:
                    self._record_recovered_local_path(
                        event.local_path,
                        installed_identity[:3],
                    )
                self.fs_events.queue_event(FileModifiedEvent(event.local_path))
            elif install_reservation_identity is not None:
                self._forget_recovered_local_path(
                    event.dbx_path_lower,
                    expected_identity=install_reservation_identity,
                    expected_source=install_reservation_source,
                )
            if evacuated_token and evacuated_path and osp.lexists(evacuated_path):
                self._preserve_evacuated_conflict(
                    event,
                    evacuated_token,
                    evacuated_path,
                )
            raise

        if evacuated_token and evacuated_path:
            if self._retain_local_evacuation(
                event,
                evacuated_token,
                evacuated_path,
                evacuated_snapshot,
            ):
                pass
            else:
                self._preserve_evacuated_conflict(
                    event,
                    evacuated_token,
                    evacuated_path,
                )
                status = SyncStatus.Conflict

        if installed_identity is None:
            raise CacheDirError(
                "Cannot publish downloaded file",
                "The installed local file has no proved identity.",
            )
        installed_snapshot = {event.local_path: installed_identity}
        if not self._publish_sync_event_for_local_snapshot(
            event,
            installed_snapshot,
        ):
            return SyncStatus.Conflict
        self._save_local_hash(
            tmp_inode,
            event.local_path,
            event.content_hash,
            tmp_mtime,
        )

        self._logger.debug('Created local file "%s"', event.dbx_path)

        return status

    def _on_remote_folder(self, event: SyncEvent) -> SyncStatus:
        """
        Applies a remote folder creation locally.

        :param event: SyncEvent for folder download.
        :returns: SyncEvent corresponding to local item or None if no local changes
            are made.
        """
        self._apply_case_change(event)

        # Store the new entry at the given path in your local state. If the required
        # parent folders don’t exist yet, create them. If there’s already something else
        # at the given path, replace it but leave the children as they are.

        conflict_check, expected_snapshot = self._stable_download_conflict(event)

        if conflict_check is Conflict.Identical:
            if self._publish_sync_event_for_local_snapshot(event, expected_snapshot):
                return SyncStatus.Skipped
            return SyncStatus.Conflict
        elif conflict_check is Conflict.LocalNewerOrIdentical:
            return SyncStatus.Skipped

        if conflict_check == Conflict.Conflict:
            cc_local_path = self._move_local_to_unique_conflict(
                event,
                lambda: self._local_cc_filename(event.local_path, event.change_dbid),
                "Cannot create download conflict copy",
            )

            self._logger.debug(
                'Download conflict: renamed "%s" to "%s"',
                event.local_path,
                cc_local_path,
            )
            self.rescan(cc_local_path)
            status = SyncStatus.Conflict
            expected_snapshot = {}
        else:
            status = SyncStatus.Done

        # Ensure that parent folders are synced.
        self._ensure_parent(event)

        evacuated_token: str | None = None
        evacuated_path: str | None = None
        evacuated_snapshot = expected_snapshot
        if isfile(event.local_path):
            self._raise_for_remote_event_symlink(
                event, "Cannot replace file with folder"
            )
            if self._snapshot_local_tree(event.local_path) != expected_snapshot:
                raise FileConflictError(
                    "Cannot replace file with folder",
                    "The local file changed before it could be replaced.",
                    dbx_path=event.dbx_path,
                    local_path=event.local_path,
                )
            evacuated_token, evacuated_path = self._evacuate_local_item(event)
            expected_snapshot = {}

        try:
            try:
                self.ensure_dropbox_folder_present()
                self._raise_for_local_symlink_at_or_below(
                    event.local_path,
                    event.dbx_path,
                    "Cannot create local folder",
                )
                with self.fs_events.ignore(
                    DirCreatedEvent(event.local_path), recursive=False
                ):
                    rooted_mkdir(
                        event.local_path,
                        root_path=self.dropbox_path,
                        expected_root_identity=self.confirmed_root_identity,
                    )
            except FileExistsError:
                self._raise_for_local_symlink_at_or_below(
                    event.local_path,
                    event.dbx_path,
                    "Cannot create local folder",
                )
                try:
                    local_stat = os.lstat(event.local_path)
                except (FileNotFoundError, NotADirectoryError) as err:
                    raise FileConflictError(
                        "Cannot create local folder",
                        "The local item changed while the folder was created.",
                        dbx_path=event.dbx_path,
                        local_path=event.local_path,
                    ) from err
                if not S_ISDIR(local_stat.st_mode) or is_fs_link(local_stat):
                    raise FileConflictError(
                        "Cannot create local folder",
                        "A local file appeared where the folder must be created.",
                        dbx_path=event.dbx_path,
                        local_path=event.local_path,
                    )
                if expected_snapshot == {}:
                    raise FileConflictError(
                        "Cannot create local folder",
                        "A local folder appeared while the remote change was applied.",
                        dbx_path=event.dbx_path,
                        local_path=event.local_path,
                    )
                status = SyncStatus.Skipped
            except OSError as err:
                self.ensure_dropbox_folder_present()
                raise os_to_maestral_error(err, dbx_path=event.dbx_path)
        except BaseException:
            if evacuated_token and evacuated_path and osp.lexists(evacuated_path):
                self._preserve_evacuated_conflict(
                    event,
                    evacuated_token,
                    evacuated_path,
                )
            raise

        if evacuated_token and evacuated_path:
            if self._retain_local_evacuation(
                event,
                evacuated_token,
                evacuated_path,
                evacuated_snapshot,
            ):
                pass
            else:
                self._preserve_evacuated_conflict(
                    event,
                    evacuated_token,
                    evacuated_path,
                )
                status = SyncStatus.Conflict

        installed_snapshot = self._snapshot_local_tree(event.local_path)
        installed_identity = installed_snapshot.get(event.local_path)
        if installed_identity is None or not _snapshot_is_directory(installed_identity):
            if installed_identity is not None:
                self._record_recovered_local_path(
                    event.local_path,
                    installed_identity[:3],
                )
            self.rescan(event.local_path)
            return SyncStatus.Conflict
        if not self._publish_sync_event_for_local_snapshot(
            event,
            installed_snapshot,
        ):
            return SyncStatus.Conflict

        self._logger.debug('Created local folder "%s"', event.dbx_path)

        return status

    def _on_remote_required_parent_deleted(self, event: SyncEvent) -> SyncStatus:
        """Delete selected descendants but preserve unmanaged local siblings."""
        selected_children = {
            path
            for path in self.selective_sync_paths
            if is_child(path, event.dbx_path_lower)
        }
        child_results: list[SyncEvent] = []
        required_parents = {event.dbx_path_lower: event.dbx_path}

        for selected_path in selected_children:
            index_entry = self.get_index_entry(selected_path)
            cased_path = index_entry.dbx_path_cased if index_entry else selected_path
            deleted_md = DeletedMetadata(
                name=posixpath.basename(cased_path),
                path_lower=selected_path,
                path_display=cased_path,
            )
            child_results.append(
                self._create_local_entry(SyncEvent.from_metadata(deleted_md, self))
            )

            parent_lower = posixpath.dirname(selected_path)
            parent_cased = posixpath.dirname(cased_path)
            while is_equal_or_child(parent_lower, event.dbx_path_lower):
                required_parents.setdefault(parent_lower, parent_cased)
                if parent_lower == event.dbx_path_lower:
                    break
                parent_lower = posixpath.dirname(parent_lower)
                parent_cased = posixpath.dirname(parent_cased)

        if any(result.status is SyncStatus.Failed for result in child_results):
            if self.download_callback:
                self.download_callback(event.dbx_path_lower)
            return SyncStatus.Failed

        for parent_lower in required_parents:
            self.remove_index_entry(parent_lower)

        for parent_lower, parent_cased in sorted(
            required_parents.items(),
            key=lambda item: item[0].count("/"),
            reverse=True,
        ):
            if parent_lower == "/":
                continue
            local_parent = self.to_local_path_from_cased(parent_cased)
            self.ensure_dropbox_folder_present()
            if self._find_local_symlink(
                local_parent,
                remember=self.ignore_symlinks,
            ):
                continue
            try:
                local_stat = os.lstat(local_parent)
                if is_fs_link(local_stat):
                    continue
                rooted_rmdir(
                    local_parent,
                    root_path=self.dropbox_path,
                    expected_root_identity=self.confirmed_root_identity,
                    expected_target_identity=self._local_stat_identity(local_stat)[:6],
                )
            except OSError as exc:
                if exc.errno not in {
                    errno.ENOENT,
                    errno.ENOTDIR,
                    errno.ENOTEMPTY,
                    errno.EEXIST,
                }:
                    raise

        rescan_children = {
            result.dbx_path_lower
            for result in child_results
            if result.status in {SyncStatus.Conflict, SyncStatus.Skipped}
            and not self._stored_ignored_symlink_for_path(result.dbx_path_lower)
        }
        surviving_children: set[str] = set()
        for selected_path in rescan_children:
            index_entry = self.get_index_entry(selected_path)
            cased_path = index_entry.dbx_path_cased if index_entry else selected_path
            local_path = self.to_local_path_from_cased(cased_path)
            if get_existing_equivalent_paths(local_path, root=self.dropbox_path):
                surviving_children.add(selected_path)

        if surviving_children:
            local_parents = get_existing_equivalent_paths(
                event.local_path,
                root=self.dropbox_path,
            )
            for local_parent in local_parents:
                self.rescan(local_parent)

        if any(result.status is SyncStatus.Conflict for result in child_results):
            return SyncStatus.Conflict
        if any(result.status is SyncStatus.Skipped for result in child_results):
            return SyncStatus.Skipped
        return SyncStatus.Done

    def _on_remote_deleted(self, event: SyncEvent) -> SyncStatus:
        """
        Applies a remote deletion locally.

        :param event: Dropbox deleted metadata.
        :returns: SyncEvent corresponding to local deletion or None if no local changes
            are made.
        """
        self._apply_case_change(event)

        if self._is_required_selective_sync_parent(event.dbx_path_lower):
            return self._on_remote_required_parent_deleted(event)

        # If your local state has something at the given path, remove it and all its
        # children. If there’s nothing at the given path, ignore this entry.

        conflict_check, expected_snapshot = self._stable_download_conflict(event)

        if self._snapshot_has_unmanaged_symlink(expected_snapshot) and not (
            self.ignore_symlinks
        ):
            raise SymlinkError(
                "Cannot apply remote deletion",
                "The local item contains an unmanaged symbolic link.",
                dbx_path=event.dbx_path,
                local_path=event.local_path,
            )

        if conflict_check is Conflict.Identical:
            self.update_index_from_sync_event(event)
            return SyncStatus.Skipped
        elif conflict_check is Conflict.LocalNewerOrIdentical:
            return SyncStatus.Skipped
        elif conflict_check is Conflict.Conflict:
            self.rescan(event.local_path)
            return SyncStatus.Conflict

        if self._snapshot_has_selectively_excluded_descendant(
            expected_snapshot,
            event.local_path,
        ):
            conflict_path = self._move_local_to_unique_conflict(
                event,
                lambda: self._local_cc_filename(
                    event.local_path,
                    event.change_dbid,
                ),
                "Cannot preserve selectively excluded local data",
            )
            self.update_index_from_sync_event(event)
            self.rescan(conflict_path)
            return SyncStatus.Conflict

        if self._snapshot_has_unmanaged_symlink(expected_snapshot):
            index_entries = {
                entry.dbx_path_lower: entry
                for entry in self.iter_index()
                if is_equal_or_child(entry.dbx_path_lower, event.dbx_path_lower)
            }
            result = self._delete_local_path_preserving_ignored_symlinks(
                event.local_path,
                expected_snapshot,
                change_dbid=event.change_dbid,
                index_entries=index_entries,
            )
            if result.error is not None:
                if result.changed:
                    self.update_index_from_sync_event(event)
                    self.rescan(event.local_path)
                if isinstance(result.error, OSError):
                    raise os_to_maestral_error(
                        result.error,
                        dbx_path=event.dbx_path,
                        local_path=event.local_path,
                    ) from result.error
                raise result.error
            if not result.preserved:
                raise OSError(
                    errno.ESTALE,
                    "The ignored symbolic link changed before deletion",
                    event.local_path,
                )
            self.update_index_from_sync_event(event)
            if result.conflict:
                self.rescan(event.local_path)
            self._logger.debug(
                'Kept ignored links below "%s" after its remote deletion',
                event.local_path,
            )
            return SyncStatus.Conflict if result.conflict else SyncStatus.Done

        root_identity = expected_snapshot.get(event.local_path)
        if root_identity is None:
            self.update_index_from_sync_event(event)
            return SyncStatus.Skipped

        event_cls = (
            DirDeletedEvent
            if _snapshot_is_directory(root_identity)
            else FileDeletedEvent
        )
        self.ensure_dropbox_folder_present()
        with self.fs_events.ignore(event_cls(event.local_path)):
            token, backup_path = self._evacuate_local_item(event)

        if self._retain_local_evacuation(
            event,
            token,
            backup_path,
            expected_snapshot,
        ):
            status = SyncStatus.Done
        else:
            self._preserve_evacuated_conflict(event, token, backup_path)
            status = SyncStatus.Conflict

        self.update_index_from_sync_event(event)
        self._logger.debug('Deleted local item "%s"', event.local_path)
        return status

    def _validated_case_changes(self) -> dict[str, dict[str, Any]]:
        """Return a validated copy of the durable case-change journal."""
        journal = self._state.get("recovery", "case_changes")
        if not isinstance(journal, dict):
            raise FileConflictError(
                "Cannot recover local case change",
                "The saved case-change journal has an invalid format.",
            )

        validated: dict[str, dict[str, Any]] = {}
        for dbx_path_lower, item in journal.items():
            if not isinstance(dbx_path_lower, str) or not isinstance(item, dict):
                raise FileConflictError(
                    "Cannot recover local case change",
                    "The saved case-change journal has an invalid entry.",
                )
            old_path = item.get("old_path")
            new_path = item.get("new_path")
            stage_name = item.get("stage_name")
            identity = item.get("identity")
            phase = item.get("phase")
            stage_token = (
                stage_name.removeprefix(CASE_CHANGE_TEMP_PREFIX)
                if isinstance(stage_name, str)
                else ""
            )
            if (
                normalize(dbx_path_lower) != dbx_path_lower
                or not isinstance(old_path, str)
                or not isinstance(new_path, str)
                or normalize(old_path) != dbx_path_lower
                or normalize(new_path) != dbx_path_lower
                or old_path == new_path
                or not isinstance(stage_name, str)
                or osp.basename(stage_name) != stage_name
                or not stage_name.startswith(CASE_CHANGE_TEMP_PREFIX)
                or len(stage_token) != 32
                or any(char not in "0123456789abcdef" for char in stage_token)
                or not isinstance(identity, list)
                or len(identity) != 3
                or any(not isinstance(value, int) for value in identity)
                or phase not in {"planned", "staged", "moved"}
            ):
                raise FileConflictError(
                    "Cannot recover local case change",
                    "The saved case-change journal has an unsafe entry.",
                    dbx_path=new_path if isinstance(new_path, str) else None,
                )
            try:
                self.to_local_path_from_cased(old_path)
                self.to_local_path_from_cased(new_path)
            except ValueError as exc:
                raise FileConflictError(
                    "Cannot recover local case change",
                    "The saved case-change path is invalid.",
                    dbx_path=new_path,
                ) from exc
            validated[dbx_path_lower] = {
                "old_path": old_path,
                "new_path": new_path,
                "stage_name": stage_name,
                "identity": identity,
                "phase": phase,
            }
        return validated

    def _record_case_change(
        self,
        dbx_path_lower: str,
        old_path: str,
        new_path: str,
        source_stat: os.stat_result,
    ) -> str:
        """Persist a case-only move before its file-system rename."""
        with self._local_mutation_lock:
            self._ensure_unlink_not_pending()
            journal = self._validated_case_changes()
            existing = journal.get(dbx_path_lower)
            identity = self._journal_item_identity(source_stat)
            if existing is not None:
                if (
                    existing["old_path"] != old_path
                    or existing["new_path"] != new_path
                    or existing["identity"] != identity
                ):
                    raise FileConflictError(
                        "Cannot apply remote case change",
                        "A different case change is already pending for this path.",
                        dbx_path=new_path,
                    )
                return osp.join(
                    osp.dirname(self.to_local_path_from_cased(old_path)),
                    existing["stage_name"],
                )

            old_local_path = self.to_local_path_from_cased(old_path)
            for _ in range(100):
                stage_name = f"{CASE_CHANGE_TEMP_PREFIX}{uuid4().hex}"
                stage_path = osp.join(osp.dirname(old_local_path), stage_name)
                if not self._snapshot_local_tree(stage_path):
                    break
            else:
                raise FileExistsError("Could not reserve a case-change stage path")

            proposed = {
                "old_path": old_path,
                "new_path": new_path,
                "stage_name": stage_name,
                "identity": identity,
                "phase": "planned",
            }
            journal[dbx_path_lower] = proposed
            self._state.set("recovery", "case_changes", journal)
            return stage_path

    def _mark_case_change_staged(self, dbx_path_lower: str) -> None:
        """Record that the old spelling moved to its private stage path."""
        self._set_case_change_phase(dbx_path_lower, "staged")

    def _mark_case_change_moved(self, dbx_path_lower: str) -> None:
        """Record that a journalled case-only file-system move completed."""
        self._set_case_change_phase(dbx_path_lower, "moved")

    def _set_case_change_phase(self, dbx_path_lower: str, phase: str) -> None:
        """Persist one proved case-change file-system phase."""
        with self._local_mutation_lock:
            journal = self._validated_case_changes()
            item = journal.get(dbx_path_lower)
            if item is None:
                raise FileConflictError(
                    "Cannot apply remote case change",
                    "The case-change journal entry is missing.",
                    dbx_path=dbx_path_lower,
                )
            item["phase"] = phase
            journal[dbx_path_lower] = item
            self._state.set("recovery", "case_changes", journal)

    def _clear_case_change(self, dbx_path_lower: str) -> None:
        """Remove one completed case-only move from the durable journal."""
        with self._local_mutation_lock:
            journal = self._validated_case_changes()
            if dbx_path_lower in journal:
                journal.pop(dbx_path_lower)
                self._state.set("recovery", "case_changes", journal)

    def _update_index_case_subtree(
        self,
        dbx_path_lower: str,
        new_path: str,
    ) -> None:
        """Rewrite one indexed subtree to a new cased path in one transaction."""
        with self._database_access():
            subtree_entries = self._index_table.select(
                PathTreeQuery(IndexEntry.dbx_path_lower, dbx_path_lower)
            )
            root_depth = len(dbx_path_lower.lstrip("/").split("/"))
            for subtree_entry in subtree_entries:
                path_parts = subtree_entry.dbx_path_cased.lstrip("/").split("/")
                child_parts = path_parts[root_depth:]
                subtree_entry.dbx_path_cased = posixpath.join(
                    new_path,
                    *child_parts,
                )
            self._index_table.update_many(subtree_entries)

    def _exact_case_change_identity(self, local_path: str) -> list[int] | None:
        """Return an identity only when a rooted path has the requested spelling."""
        if not rooted_name_has_exact_case(
            local_path,
            self.dropbox_path,
            expected_root_identity=self.confirmed_root_identity,
        ):
            return None
        try:
            return list(
                self._snapshot_local_item(
                    local_path,
                    self.dropbox_path,
                    expected_root_identity=self.confirmed_root_identity,
                )[:3]
            )
        except (FileNotFoundError, NotADirectoryError):
            return None

    def _resume_case_change_filesystem(
        self,
        dbx_path_lower: str,
        item: dict[str, Any],
    ) -> str:
        """Complete a durable two-step case rename and return its new local path."""
        old_local_path = self.to_local_path_from_cased(item["old_path"])
        new_local_path = self.to_local_path_from_cased(item["new_path"])
        stage_path = osp.join(osp.dirname(old_local_path), item["stage_name"])
        expected_identity = item["identity"]

        old_identity = self._exact_case_change_identity(old_local_path)
        new_identity = self._exact_case_change_identity(new_local_path)
        stage_identity = self._exact_case_change_identity(stage_path)

        if (
            new_identity == expected_identity
            and old_identity is None
            and stage_identity is None
        ):
            return new_local_path

        if (
            stage_identity == expected_identity
            and old_identity is None
            and new_identity is None
        ):
            move(
                stage_path,
                new_local_path,
                replace=False,
                raise_error=True,
                root_path=self.dropbox_path,
                expected_root_identity=self.confirmed_root_identity,
                expected_source_identity=tuple(expected_identity),
            )
            self._mark_case_change_moved(dbx_path_lower)
            return new_local_path

        if (
            item["phase"] == "planned"
            and old_identity == expected_identity
            and new_identity is None
            and stage_identity is None
        ):
            move(
                old_local_path,
                stage_path,
                replace=False,
                raise_error=True,
                root_path=self.dropbox_path,
                expected_root_identity=self.confirmed_root_identity,
                expected_source_identity=tuple(expected_identity),
            )
            self._mark_case_change_staged(dbx_path_lower)
            move(
                stage_path,
                new_local_path,
                replace=False,
                raise_error=True,
                root_path=self.dropbox_path,
                expected_root_identity=self.confirmed_root_identity,
                expected_source_identity=tuple(expected_identity),
            )
            self._mark_case_change_moved(dbx_path_lower)
            return new_local_path

        raise FileConflictError(
            "Cannot recover local case change",
            "The local paths no longer match the case-change journal.",
            dbx_path=item["new_path"],
            local_path=new_local_path,
        )

    def _recover_pending_case_changes(self) -> None:
        """Complete any journalled case-only move and its index update."""
        for dbx_path_lower, item in tuple(self._validated_case_changes().items()):
            entry = self.get_index_entry(dbx_path_lower)
            if entry is not None and entry.dbx_path_cased not in {
                item["old_path"],
                item["new_path"],
            }:
                raise FileConflictError(
                    "Cannot recover local case change",
                    "The indexed path no longer matches the case-change journal.",
                    dbx_path=item["new_path"],
                )

            new_local_path = self._resume_case_change_filesystem(
                dbx_path_lower,
                item,
            )
            if entry is not None and entry.dbx_path_cased == item["old_path"]:
                self._update_index_case_subtree(
                    dbx_path_lower,
                    item["new_path"],
                )
            self._clear_case_change(dbx_path_lower)
            if entry is None:
                self.rescan(new_local_path)

    def _apply_case_change(self, event: SyncEvent) -> None:
        """
        Applies any changes in casing of the remote item locally. This should be called
        before any system calls using ``local_path`` because the actual path on the file
        system may have a different casing on case-sensitive file systems. On case-
        insensitive file systems, this causes only a cosmetic change.

        :param event: Download SyncEvent.
        """
        self._recover_pending_case_changes()
        entry = self.get_index_entry(event.dbx_path_lower)

        if entry and entry.dbx_path_cased != event.dbx_path:
            local_path_old = self.to_local_path_from_cased(entry.dbx_path_cased)
            destination_is_source = False
            source_stat = os.lstat(local_path_old)

            if osp.lexists(event.local_path):
                try:
                    destination_is_source = osp.samefile(
                        local_path_old, event.local_path
                    )
                except (FileNotFoundError, NotADirectoryError, OSError):
                    destination_is_source = False

                if not destination_is_source:
                    raise FileConflictError(
                        "Cannot apply remote case change",
                        "A different local item already uses the destination path.",
                        dbx_path=event.dbx_path,
                        local_path=event.local_path,
                    )

            source_snapshot = self._snapshot_local_tree(local_path_old)
            unmanaged_symlink = None
            for path, identity in source_snapshot.items():
                target = _snapshot_symlink_target(identity)
                if target is None:
                    continue
                dbx_path_lower = self.to_dbx_path_lower(path)
                link_entry = self.get_index_entry(dbx_path_lower)
                if link_entry and target == link_entry.symlink_target:
                    self._forget_ignored_symlink(dbx_path_lower)
                    continue
                unmanaged_symlink = dbx_path_lower
                if self.ignore_symlinks:
                    self._remember_ignored_symlink(dbx_path_lower)
                break

            if unmanaged_symlink:
                raise SymlinkError(
                    "Cannot apply remote case change",
                    "The old local path contains the symbolic link "
                    f'"{unmanaged_symlink}".',
                    dbx_path=event.dbx_path,
                    local_path=local_path_old,
                )

            self._raise_for_local_ancestor_symlink(
                osp.dirname(local_path_old),
                event.dbx_path,
                "Cannot apply remote case change",
            )
            self._raise_for_local_ancestor_symlink(
                osp.dirname(event.local_path),
                event.dbx_path,
                "Cannot apply remote case change",
            )

            stage_path = self._record_case_change(
                event.dbx_path_lower,
                entry.dbx_path_cased,
                event.dbx_path,
                source_stat,
            )

            event_cls = DirMovedEvent if isdir(local_path_old) else FileMovedEvent
            with self.fs_events.ignore(
                event_cls(local_path_old, stage_path),
                event_cls(stage_path, event.local_path),
            ):
                with convert_api_errors(
                    dbx_path=event.dbx_path, local_path=local_path_old
                ):
                    self._resume_case_change_filesystem(
                        event.dbx_path_lower,
                        self._validated_case_changes()[event.dbx_path_lower],
                    )

            self._update_index_case_subtree(
                event.dbx_path_lower,
                event.dbx_path,
            )
            self._clear_case_change(event.dbx_path_lower)

            self._logger.debug('Renamed "%s" to "%s"', local_path_old, event.local_path)

    def rescan(self, local_path: str | bytes) -> None:
        """
        Forces a rescan of a local path: schedules created events for every folder,
        modified events for every file and deleted events for every deleted item
        (compared to our index).

        :param local_path: Path to rescan.
        """
        local_path = os.fsdecode(local_path)
        self._logger.debug('Rescanning "%s"', local_path)

        try:
            local_identity = self._snapshot_local_item(
                local_path,
                self.dropbox_path,
                expected_root_identity=self.confirmed_root_identity,
            )
        except (FileNotFoundError, NotADirectoryError):
            local_identity = None

        if local_identity is not None and not _snapshot_is_directory(local_identity):
            self.fs_events.queue_event(FileModifiedEvent(local_path))

        elif local_identity is not None:
            self.fs_events.queue_event(DirCreatedEvent(local_path))

            # Add created and modified events for children as appropriate.

            for path, stat in rooted_walk(
                local_path,
                self.dropbox_path,
                expected_root_identity=self.confirmed_root_identity,
                should_recurse=self._include_rooted_walk_entry,
            ):
                if not self._include_rooted_walk_entry(path, stat):
                    continue
                if S_ISDIR(stat.st_mode) and not is_fs_link(stat):
                    self.fs_events.queue_event(DirCreatedEvent(path))
                else:
                    self.fs_events.queue_event(FileModifiedEvent(path))

            # Add deleted events for children.

            dbx_path_lower = self.to_dbx_path_lower(local_path)

            with self._database_access():
                query = PathTreeQuery(IndexEntry.dbx_path_lower, dbx_path_lower)
                entries = self._index_table.select(query)

            for entry in entries:
                child_path = self.to_local_path_from_cased(entry.dbx_path_cased)
                try:
                    self._snapshot_local_item(
                        child_path,
                        self.dropbox_path,
                        expected_root_identity=self.confirmed_root_identity,
                    )
                except (FileNotFoundError, NotADirectoryError):
                    if entry.is_directory:
                        self.fs_events.queue_event(DirDeletedEvent(child_path))
                    else:
                        self.fs_events.queue_event(FileDeletedEvent(child_path))

        else:
            dbx_path_lower = self.to_dbx_path_lower(local_path)
            local_entry = self.get_index_entry(dbx_path_lower)

            if local_entry:
                if local_entry.is_directory:
                    self.fs_events.queue_event(DirDeletedEvent(local_path))
                else:
                    self.fs_events.queue_event(FileDeletedEvent(local_path))

    def rescan_dbx_path(self, dbx_path: str) -> bool:
        """Queue unindexed local items below a canonical Dropbox path."""
        try:
            self.ensure_dropbox_folder_present()
            if dbx_path in {"", "/"}:
                for child_path, child_stat in rooted_walk(
                    self.dropbox_path,
                    self.dropbox_path,
                    expected_root_identity=self.confirmed_root_identity,
                    should_recurse=self._include_rooted_walk_entry,
                ):
                    if not self._include_rooted_walk_entry(child_path, child_stat):
                        continue
                    if self.get_index_entry_for_local_path(child_path) is not None:
                        continue
                    if S_ISDIR(child_stat.st_mode) and not is_fs_link(child_stat):
                        self.fs_events.queue_event(DirCreatedEvent(child_path))
                    else:
                        self.fs_events.queue_event(FileModifiedEvent(child_path))
                return True

            local_path = self.to_local_path_from_cased(dbx_path)
            matching_paths = get_existing_equivalent_paths(
                local_path,
                root=self.dropbox_path,
            )

            for matching_path in matching_paths:
                self._rescan_unindexed(matching_path)
        except OSError as exc:
            if exc.errno == errno.ESTALE:
                return False
            raise
        return True

    def _rescan_unindexed(self, local_path: str) -> None:
        """Queue existing local items which have no sync-index entry."""
        if self._ignored_symlink_for_local_path(local_path):
            return

        try:
            identity = self._snapshot_local_item(
                local_path,
                self.dropbox_path,
                expected_root_identity=self.confirmed_root_identity,
            )
        except (FileNotFoundError, NotADirectoryError):
            return

        if self.get_index_entry_for_local_path(local_path) is None:
            if _snapshot_is_directory(identity):
                self.fs_events.queue_event(DirCreatedEvent(local_path))
            else:
                self.fs_events.queue_event(FileModifiedEvent(local_path))

        if _snapshot_is_directory(identity):
            for child_path, child_stat in rooted_walk(
                local_path,
                self.dropbox_path,
                expected_root_identity=self.confirmed_root_identity,
                should_recurse=self._include_rooted_walk_entry,
            ):
                if not self._include_rooted_walk_entry(child_path, child_stat):
                    continue
                if self.get_index_entry_for_local_path(child_path) is not None:
                    continue
                if S_ISDIR(child_stat.st_mode) and not is_fs_link(child_stat):
                    self.fs_events.queue_event(DirCreatedEvent(child_path))
                else:
                    self.fs_events.queue_event(FileModifiedEvent(child_path))

    def _clean_history(self) -> None:
        """Commits new events and removes all events older than ``_keep_history`` from
        history."""
        with self._database_access():
            # Drop all entries older than keep_history.
            now = time.time()
            keep_history = self._conf.get("sync", "keep_history")

            self._db.execute(
                "DELETE FROM history WHERE IFNULL(change_time, sync_time) < ?",
                now - keep_history,
            )
            self._history_table.clear_cache()

    def _include_rooted_walk_entry(
        self,
        local_path: str,
        stat_result: os.stat_result,
    ) -> bool:
        """Apply sync exclusions to one securely walked item."""
        dbx_path = self.to_dbx_path(local_path)
        is_link = is_fs_link(stat_result)
        is_dir = S_ISDIR(stat_result.st_mode) and not is_link
        if self.ignore_symlinks and is_link:
            self._remember_ignored_symlink(dbx_path)
            return False
        if (
            (self.selective_sync_mode == "include" or "/" in self.selective_sync_paths)
            and self.is_excluded_by_selective_sync(normalize(dbx_path))
            and self._tracked_recovered_local_root(normalize(dbx_path)) is None
        ):
            return False
        current_path = local_path
        while current_path != self.dropbox_path:
            current_dbx_path = self.to_dbx_path(current_path)
            if self.is_excluded(current_path) or self._is_mignore_path(
                current_dbx_path,
                is_dir if current_path == local_path else True,
            ):
                return False
            current_path = osp.dirname(current_path)
        return True

    def _scandir_with_ignore(
        self, path: str | os.PathLike[str]
    ) -> Iterator[os.DirEntry[str]]:
        with os.scandir(path) as it:
            for entry in it:
                dbx_path = self.to_dbx_path(entry.path)
                stat = os.lstat(entry.path)
                is_link = is_fs_link(stat)
                is_dir = S_ISDIR(stat.st_mode) and not is_link
                if self.ignore_symlinks and is_link:
                    self._remember_ignored_symlink(dbx_path)
                    continue

                if (
                    (
                        self.selective_sync_mode == "include"
                        or "/" in self.selective_sync_paths
                    )
                    and self.is_excluded_by_selective_sync(normalize(dbx_path))
                    and self._tracked_recovered_local_root(normalize(dbx_path)) is None
                ):
                    continue

                if not self.is_excluded(entry.path) and not self._is_mignore_path(
                    dbx_path, is_dir
                ):
                    yield entry


# ======================================================================================
# Helper functions
# ======================================================================================


def do_parallel(
    func: Callable[P, T],
    *iterables: list[Any],
    thread_name_prefix: str = "",
    on_progress: Callable[[int, int], Any] | None = None,
) -> Iterable[T]:
    """
    Similar to ``ThreadPoolExecutor.map()`` but yields results as they become available.

    :param func: A callable that will take as many arguments as there are passed iterables.
    :param iterables: Arguments to pass to ``func``. All iterables must have the same size.
    :param thread_name_prefix: Used for internal ThreadPoolExecutor.
    :param on_progress: Callback when each task is completed. Takes the number of
        completed items and the total number of items as arguments.
    """
    with ThreadPoolExecutor(
        max_workers=NUM_THREADS, thread_name_prefix=thread_name_prefix
    ) as thread_pool_executor:
        futures = [
            thread_pool_executor.submit(func, *args)  # type: ignore[call-arg]
            for args in zip(*iterables)
        ]

        n_done = 0
        for future in as_completed(futures):
            n_done += 1
            if on_progress:
                on_progress(n_done, len(iterables[0]))
            yield future.result()


def is_moved(event: FileSystemEvent) -> TypeGuard[FileMovedEvent | DirMovedEvent]:
    return event.event_type == EVENT_TYPE_MOVED


def is_deleted(event: FileSystemEvent) -> TypeGuard[FileDeletedEvent | DirDeletedEvent]:
    return event.event_type == EVENT_TYPE_DELETED


def is_created(event: FileSystemEvent) -> TypeGuard[FileCreatedEvent | DirCreatedEvent]:
    return event.event_type == EVENT_TYPE_CREATED


@overload
def split_moved_event(
    event: FileMovedEvent,
) -> tuple[FileDeletedEvent, FileCreatedEvent]: ...


@overload
def split_moved_event(
    event: DirMovedEvent,
) -> tuple[DirDeletedEvent, DirCreatedEvent]: ...


def split_moved_event(
    event: FileMovedEvent | DirMovedEvent,
) -> tuple[FileSystemEvent, FileSystemEvent]:
    """
    Splits a FileMovedEvent or DirMovedEvent into "deleted" and "created" events of the
    same type. A new attribute ``move_id`` is added to both instances.

    :param event: Original event.
    :returns: Tuple of deleted and created events.
    """
    created_event_cls: Type[FileSystemEvent]
    deleted_event_cls: Type[FileSystemEvent]
    if event.is_directory:
        created_event_cls = DirCreatedEvent
        deleted_event_cls = DirDeletedEvent
    else:
        created_event_cls = FileCreatedEvent
        deleted_event_cls = FileDeletedEvent

    deleted_event = deleted_event_cls(event.src_path)
    created_event = created_event_cls(event.dest_path)

    return deleted_event, created_event


class pf_repr:
    """
    Class that wraps an object and creates a pretty formatted representation for it.
    This can be used to get pretty formatting in log messages while deferring the actual
    formatting until the message is created.

    :param obj: Object to wrap.
    """

    def __init__(self, obj: Any) -> None:
        self.obj = obj

    def __repr__(self) -> str:
        return pformat(self.obj)


def check_encoding(local_path: str) -> None:
    """
    Validate that the path contains only characters in the reported file system
    encoding. On Unix, paths are fundamentally bytes and some platforms do not enforce
    a uniform encoding of file names despite reporting a file system encoding. Such
    paths will be handed to us in Python with "surrogate escapes" in place of the
    unknown characters.

    Since the Dropbox API and our database both require utf-8 encoded paths, we use this
    method to check and fail early on unknown characters.

    :param local_path: Path to check.
    :raises PathError: if the path contains characters with an unknown encoding.
    """
    try:
        local_path.encode()
    except UnicodeEncodeError:
        fs_encoding = sys.getfilesystemencoding()
        error = PathError(
            "Could not upload item",
            f"The file name contains characters outside the "
            f"{fs_encoding} encoding of your file system",
        )
        error.local_path = local_path
        raise error


def check_change_type(event: SyncEvent, allowed_change_types: set[ChangeType]) -> None:
    """
    Check if the sync event has one of passed allowed_change_types.

    :param event: SyncEvent to check.
    :param allowed_change_types: Set of allowed change types.
    :raises ValueError: If the change type is not in the allow list.
    """
    if event.change_type not in allowed_change_types:
        raise ValueError(
            f"Require change_type in {allowed_change_types} but "
            f"found {event.change_type}"
        )
