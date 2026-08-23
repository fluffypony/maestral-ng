"""This module contains the classes to coordinate sync threads."""

from __future__ import annotations

import ctypes
import errno
import gc
import hashlib
import json

# system imports
import os
import os.path as osp
import time
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
from queue import Empty, Queue
from stat import S_ISDIR, S_ISREG
from threading import Condition, Event, RLock, Thread, current_thread
from typing import Any, Callable, Generic, Iterator, TypeVar
from uuid import uuid4

from typing_extensions import Concatenate, ParamSpec

# local imports
from . import __url__
from .client import API_HOST
from .config import MaestralConfig, MaestralState, PersistentMutableSet
from .config.user import UserConfig
from .constants import (
    CONNECTED,
    CONNECTING,
    DISCONNECTED,
    FILE_CACHE,
    IDLE,
    IS_LINUX,
    MIGNORE_FILE,
    OLD_REV_FILE,
    PATH_ROOT_MIGRATION_PREFIX,
    PATH_ROOT_RECOVERY_PREFIX,
    PAUSED,
    ROOT_MARKER_FILE,
    SYNCING,
)
from .core import TeamRootInfo, UserRootInfo
from .errorhandling import convert_api_errors
from .exceptions import (
    CancelledError,
    DropboxConnectionError,
    DropboxServerError,
    InotifyError,
    MaestralApiError,
    NoDropboxDirError,
    PathRootError,
    SymlinkError,
)
from .fsevents import Observer, ObserverType
from .logging import scoped_logger
from .sync import SyncEngine
from .utils import removeprefix
from .utils.hashing import DropboxContentHasher
from .utils.integration import check_connection, get_inotify_limits
from .utils.path import (
    create_rooted_tempfile,
    delete,
    is_equal_or_child,
    is_fs_link,
)
from .utils.path import mkdir as rooted_mkdir
from .utils.path import (
    move,
    normalize,
)
from .utils.path import rmdir as rooted_rmdir
from .utils.path import (
    rooted_item_snapshot,
)
from .utils.path import unlink as rooted_unlink

__all__ = ["SyncManager"]

DROPBOX_API_HOSTNAME = "https://" + API_HOST


P = ParamSpec("P")
T = TypeVar("T")

malloc_trim: Callable[[int], None]

try:
    libc = ctypes.CDLL("libc.so.6")
    malloc_trim = libc.malloc_trim
except (OSError, AttributeError):

    def malloc_trim(pad: int) -> None:
        pass


def _free_memory() -> None:
    """Give back memory"""
    gc.collect()
    malloc_trim(0)


@dataclass(frozen=True)
class _StopState:
    generation: int
    was_running: bool
    was_autostarting: bool
    internal_operation: int | None = None


class PersistentQueue(Generic[T]):
    def __init__(self, conf: UserConfig, section: str, option: str) -> None:
        self._lock = RLock()
        self._queue: Queue[T] = Queue()
        self._persistent: PersistentMutableSet[T] = PersistentMutableSet(
            conf, section, option
        )
        self._queued: set[T] = set()
        self._in_flight: set[T] = set()
        self._rerun: set[T] = set()

        for item in self._persistent:
            self._queue.put(item)
            self._queued.add(item)

    def qsize(self) -> int:
        return self._queue.qsize()

    def has_pending(self) -> bool:
        return not self._queue.empty()

    def _put_persisted_locked(self, item: T) -> None:
        """Queue one item which is already present in persistent storage."""
        if item in self._queued:
            with self._queue.mutex:
                if item in self._queue.queue:
                    return
            self._queued.discard(item)
            self._in_flight.add(item)
            self._rerun.add(item)
            return
        if item in self._in_flight:
            self._rerun.add(item)
            return
        self._queue.put(item)
        self._queued.add(item)

    def put(self, item: T) -> None:
        with self._lock:
            if item not in self._persistent:
                try:
                    self._persistent.add(item)
                except BaseException:
                    if item in self._persistent:
                        self._put_persisted_locked(item)
                    raise
            self._put_persisted_locked(item)

    def put_persisted(self, item: T) -> None:
        """Queue an item after its caller atomically persisted the queue record."""
        with self._lock:
            if item not in self._persistent:
                raise ValueError("The queue item is not persistent")
            self._put_persisted_locked(item)

    def get(self, block: bool = True, timeout: int | None = None) -> T:
        item = self._queue.get(block, timeout)
        with self._lock:
            self._queued.discard(item)
            self._in_flight.add(item)
        return item

    def join(self) -> None:
        self._queue.join()

    def task_done(
        self,
        item: T,
        persist_completion: Callable[[T], None] | None = None,
    ) -> None:
        with self._lock:
            if item in self._rerun:
                self._rerun.discard(item)
                self._in_flight.discard(item)
                self._queue.put(item)
                self._queued.add(item)
                self._queue.task_done()
                return

            try:
                if persist_completion is None:
                    self._persistent.discard(item)
                else:
                    persist_completion(item)
            except BaseException:
                self._in_flight.discard(item)
                if item in self._persistent:
                    self._queue.put(item)
                    self._queued.add(item)
                self._queue.task_done()
                raise
            else:
                self._in_flight.discard(item)
                self._queue.task_done()

    def requeue(self, item: T) -> None:
        """Return a dequeued item to the queue without changing persistent state."""
        with self._lock:
            if item in self._persistent:
                self._rerun.discard(item)
                self._in_flight.discard(item)
                self._queue.put(item)
                self._queued.add(item)
            else:
                self._in_flight.discard(item)
            self._queue.task_done()

    def clear(self) -> None:
        """Remove queued and persisted items while preserving in-flight accounting."""
        with self._lock:
            self._persistent.clear()
            self._queued.clear()
            self._rerun.clear()

            with self._queue.mutex:
                removed = len(self._queue.queue)
                self._queue.queue.clear()
                self._queue.unfinished_tasks -= removed

                if self._queue.unfinished_tasks == 0:
                    self._queue.all_tasks_done.notify_all()

                self._queue.not_full.notify_all()

    def reload_persisted(self) -> None:
        """Rebuild the live queue from its durable records."""
        with self._lock:
            if self._in_flight:
                raise RuntimeError("Cannot reload a queue with in-flight items")
            self._queue = Queue()
            self._queued.clear()
            self._rerun.clear()
            for item in self._persistent:
                self._queue.put(item)
                self._queued.add(item)

    def __contains__(self, entry: Any) -> bool:
        return entry in self._persistent


class SyncManager:
    """Class to manage sync threads

    :param sync: The SyncEngine.
    """

    download_queue: PersistentQueue[str]
    """Queue of remote paths which have been newly included in syncing."""

    def __init__(self, sync: SyncEngine) -> None:
        self.sync = sync
        self._conf = MaestralConfig(self.sync.config_name)
        self._state = MaestralState(self.sync.config_name)
        self._logger = scoped_logger(__name__, self.sync.config_name)

        self._lock = RLock()
        self._stop_condition = Condition(self._lock)
        self._stopping = False
        self._stop_generation = 0
        self._internal_operation_generation = 0
        self._active_internal_operation: int | None = None

        self.running = Event()
        self.startup_completed = Event()
        self.autostart = Event()

        self.download_queue = PersistentQueue(self._state, "sync", "pending_downloads")
        self.sync.download_callback = self._queue_download
        self.sync.targeted_download_callback = self._queue_targeted_download
        for dbx_path in self.sync.targeted_download_paths:
            self.download_queue.put(dbx_path)
        self.sync.rescan_recovered_local_paths()
        self.sync._finish_sync_reset_queue()

        self._startup_time = -1.0

        self.connection_check_interval = 10
        self.download_retry_interval = 40
        self.connected = False
        self._connection_helper_stop = Event()
        self._shutdown_requested = False
        self.connection_helper = Thread(
            target=self.connection_monitor,
            name="maestral-connection-helper",
            daemon=True,
        )
        self.connection_helper.start()

        self.local_observer_thread: ObserverType | None = None

    def _queue_download(self, dbx_path: str) -> None:
        """Queue ordinary work only while the current account remains active."""
        with self.download_queue._lock, self.sync._local_mutation_lock:
            self.sync._ensure_unlink_not_pending()
            self.download_queue.put(dbx_path)

    def _queue_targeted_download(self, dbx_path: str, intent: str) -> None:
        """Persist one targeted intent and its queue record as one state update."""
        with (
            self.download_queue._lock,
            self.sync._local_mutation_lock,
            self._state._lock,
        ):
            self.sync._ensure_unlink_not_pending()
            previous_intents = self.sync._validated_download_intents()
            previous_pending = list(self._state.get("sync", "pending_downloads"))
            intents = previous_intents.copy()
            if intents.get(dbx_path) != "restore":
                intents[dbx_path] = intent
            pending = list(dict.fromkeys([*previous_pending, dbx_path]))
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
                else:
                    self.download_queue.put_persisted(dbx_path)
                raise
            self.download_queue.put_persisted(dbx_path)

    def _with_lock(  # type: ignore[misc]
        fn: Callable[Concatenate[SyncManager, P], T],
    ) -> Callable[Concatenate[SyncManager, P], T]:
        @wraps(fn)
        def wrapper(__self: SyncManager, *args: P.args, **kwargs: P.kwargs) -> T:
            with __self._lock:
                return fn(__self, *args, **kwargs)

        return wrapper

    @contextmanager
    def _sync_gate(self, title: str) -> Iterator[None]:
        """Hold the sync transaction gate without a manager-lock wait cycle."""
        if not self.sync.sync_lock.acquire(blocking=False):
            raise MaestralApiError(title, "Please try again when idle.")
        try:
            yield
        finally:
            self.sync.sync_lock.release()

    def _wait_for_stop_completion(self) -> None:
        """Wait for a stop unless this worker is one of its join targets."""
        while self._stopping:
            worker_names = (
                "startup_thread",
                "upload_thread",
                "download_thread",
                "download_thread_added_folder",
            )
            if any(
                current_thread() is getattr(self, worker_name, None)
                for worker_name in worker_names
            ):
                raise CancelledError("Sync shutdown is already in progress")
            self._stop_condition.wait()

    # ---- config and state ------------------------------------------------------------

    @property
    def idle_time(self) -> float:
        """
        Returns the idle time in seconds since the last file change or since startup if
        there haven't been any changes in our current session.
        """
        now = time.time()
        time_since_startup = now - self._startup_time
        time_since_last_sync = now - self.sync.last_change

        return min(time_since_startup, time_since_last_sync)

    # ---- control methods -------------------------------------------------------------

    @_with_lock
    def begin_unlink_reset(
        self,
        stop_state: _StopState,
        *,
        account_id: str,
        keyring: str,
        root_path: str,
        root_marker_id: str,
    ) -> None:
        """Reserve a stopped manager generation for an account unlink."""
        if self._active_internal_operation != stop_state.internal_operation:
            raise MaestralApiError(
                "Cannot unlink Dropbox account",
                "The unlink reservation is no longer active.",
            )
        if self.running.is_set():
            raise MaestralApiError(
                "Cannot unlink Dropbox account",
                "Sync started again before the unlink could begin.",
            )
        self.sync._begin_sync_reset(
            "unlink",
            account_id=account_id,
            keyring=keyring,
            root_path=root_path,
            root_marker_id=root_marker_id,
        )

    @_with_lock
    def begin_root_move(self, journal: dict[str, Any]) -> None:
        """Reserve a stopped manager generation for a Dropbox-root move."""
        while self._stopping:
            self._stop_condition.wait()
        if self.running.is_set():
            raise MaestralApiError(
                "Cannot move Dropbox folder",
                "Sync started again before the folder move could begin.",
            )
        with self.sync._local_mutation_lock, self._state._lock:
            if self._state.get("recovery", "sync_reset"):
                raise MaestralApiError(
                    "Cannot move Dropbox folder",
                    "A sync reset started during the folder move.",
                )
            self._state.set("recovery", "root_move", journal)

    @_with_lock
    def start(self) -> None:
        """Creates observer threads and starts syncing."""
        while self._stopping:
            self._stop_condition.wait()
        if self._active_internal_operation is not None:
            raise MaestralApiError(
                "Cannot start sync during an internal operation",
                "Wait for the current operation to finish.",
            )
        if self._shutdown_requested:
            return
        if self.running.is_set():
            return

        if self._state.get("recovery", "root_move"):
            raise MaestralApiError(
                "Cannot start sync during folder recovery",
                "Finish the pending Dropbox folder move first.",
            )

        reset = self._state.get("recovery", "sync_reset")
        if reset:
            with self._sync_gate("Cannot recover sync state"):
                if isinstance(reset, dict) and reset.get("kind") == "sync":
                    self.sync._complete_pending_sync_reset()
                    self.download_queue.reload_persisted()
                    self.sync._finish_sync_reset_queue()
            if self._state.get("recovery", "sync_reset"):
                raise MaestralApiError(
                    "Cannot start sync during state recovery",
                    "Finish the pending reset before you start sync.",
                )

        self.sync.ensure_dropbox_folder_present()

        if not check_connection(DROPBOX_API_HOSTNAME, logger=self._logger):
            # Schedule autostart when connection becomes available.
            self.autostart.set()
            self._logger.info(CONNECTING)
            return

        # create a new set of events to let old threads die down
        self.running = Event()
        self.startup_completed = Event()

        self.startup_thread = Thread(
            target=self.startup_worker,
            daemon=True,
            args=(
                self.running,
                self.startup_completed,
                self.autostart,
            ),
            name="maestral-sync-startup",
        )

        if self._conf.get("sync", "download"):
            self.download_thread = Thread(
                target=self.download_worker,
                daemon=True,
                args=(
                    self.running,
                    self.startup_completed,
                    self.autostart,
                ),
                name="maestral-download",
            )
            self.download_thread_added_folder = Thread(
                target=self.download_worker_added_item,
                daemon=True,
                args=(
                    self.running,
                    self.startup_completed,
                    self.autostart,
                ),
                name="maestral-folder-download",
            )

        enable_upload = self._conf.get("sync", "upload")
        enable_download = self._conf.get("sync", "download")

        if enable_upload:
            self.upload_thread = Thread(
                target=self.upload_worker,
                daemon=True,
                args=(
                    self.running,
                    self.startup_completed,
                    self.autostart,
                ),
                name="maestral-upload",
            )

            if not self.local_observer_thread:
                try:
                    self.local_observer_thread = self._create_observer()
                except MaestralApiError as exc:
                    self._logger.error(exc.title, exc_info=True)
                    return

        self.running.set()
        self.autostart.set()

        if enable_upload:
            self.sync.fs_events.enable()
            self.upload_thread.start()

        if enable_download:
            self.download_thread.start()
            self.download_thread_added_folder.start()

        self.startup_thread.start()
        self._startup_time = time.time()

    def _create_observer(self) -> ObserverType:
        local_observer_thread = Observer(timeout=40)
        local_observer_thread.name = "maestral-fsobserver"
        local_observer_thread.schedule(
            self.sync.fs_events, self.sync.dropbox_path, recursive=True
        )

        for emitter in local_observer_thread.emitters:
            # there should be only a single emitter thread
            emitter.name = "maestral-fsemitter"

        try:
            local_observer_thread.start()
        except OSError as exc:
            if IS_LINUX and exc.errno in (errno.ENOSPC, errno.EMFILE):
                try:
                    max_user_watches, max_user_instances, _ = get_inotify_limits()
                except OSError:
                    max_user_watches, max_user_instances = 2**18, 2**9

                url = f"{__url__}/docs/inotify-limits"

                if exc.errno == errno.ENOSPC:
                    n_new = max(2**19, 2 * max_user_watches)

                    raise InotifyError(
                        "Inotify limit reached",
                        "Changes to your Dropbox folder cannot be monitored because it "
                        "contains too many items. Please increase "
                        f"fs.inotify.max_user_watches to {n_new}. See {url} for more "
                        "information.",
                    )

                else:
                    n_new = max(2**10, 2 * max_user_instances)

                    raise InotifyError(
                        "Inotify limit reached",
                        "Changes to your Dropbox folder cannot be monitored because "
                        "there are too many activity inotify instances. Please "
                        f"increase fs.inotify.max_user_instances to {n_new}. See "
                        f"{url} for more information.",
                    )

            elif exc.errno in (errno.EPERM, errno.EACCES):
                error_cls = InotifyError if IS_LINUX else MaestralApiError
                raise error_cls(
                    "Insufficient permissions to monitor local changes",
                    "Please check the permissions for your local Dropbox folder",
                )

            elif exc.errno in (errno.ENOENT, errno.ENOTDIR):
                raise NoDropboxDirError(
                    "Dropbox folder missing",
                    "Please move the Dropbox folder back to its original location "
                    "or restart Maestral to set up a new folder.",
                )
            else:
                raise MaestralApiError(
                    "Could not start watch of local directory",
                    exc.strerror or "Unknown error",
                )

        return local_observer_thread

    def stop(
        self,
        *,
        clear_autostart: bool = True,
        _record_user_stop: bool = True,
    ) -> _StopState:
        """Stops syncing and destroys worker threads."""
        stop_state = self._stop(
            clear_autostart=clear_autostart,
            record_user_stop=_record_user_stop,
            require_connection_restart=False,
        )
        assert stop_state is not None
        return stop_state

    def _stop_for_connection_restart(self) -> bool:
        """Stop stale worker state only if reconnect still requires a restart."""
        return (
            self._stop(
                clear_autostart=False,
                record_user_stop=False,
                require_connection_restart=True,
            )
            is not None
        )

    def _stop(
        self,
        *,
        clear_autostart: bool,
        record_user_stop: bool,
        require_connection_restart: bool,
    ) -> _StopState | None:
        """Stop one worker generation after an optional reconnect recheck."""
        stopping_started = False
        observer: ObserverType | None = None
        startup_completed: Event | None = None
        workers: dict[str, Thread | None] = {}
        try:
            with self._lock:
                self._wait_for_stop_completion()
                if require_connection_restart and (
                    self._connection_helper_stop.is_set()
                    or self.running.is_set()
                    or not self.autostart.is_set()
                    or self._active_internal_operation is not None
                ):
                    return None
                self._stopping = True
                stopping_started = True
                was_running = self.running.is_set()
                was_autostarting = self.autostart.is_set()
                if clear_autostart and record_user_stop:
                    self._stop_generation += 1
                stop_state = _StopState(
                    generation=self._stop_generation,
                    was_running=was_running,
                    was_autostarting=was_autostarting,
                )
                observer = self.local_observer_thread
                startup_completed = self.startup_completed
                worker_names = (
                    "startup_thread",
                    "upload_thread",
                    "download_thread",
                    "download_thread_added_folder",
                )
                workers = {
                    worker_name: getattr(self, worker_name, None)
                    for worker_name in worker_names
                }

                if self.running.is_set():
                    self._logger.info("Shutting down threads...")
                self.sync.fs_events.disable()
                self.running.clear()
                self.startup_completed.set()
                if clear_autostart:
                    self.autostart.clear()
                self.sync.request_cancel()
                if observer:
                    observer.stop()

            self.sync.cancel_sync()

            for worker in workers.values():
                if worker and worker.is_alive() and current_thread() is not worker:
                    worker.join()

            if observer and observer.is_alive() and current_thread() is not observer:
                observer.join()
        finally:
            if stopping_started:
                with self._lock:
                    self._stopping = False
                    self._stop_condition.notify_all()
                    if self.local_observer_thread is observer:
                        self.local_observer_thread = None
                    for worker_name, worker in workers.items():
                        if getattr(self, worker_name, None) is worker:
                            setattr(self, worker_name, None)
                    if self.startup_completed is startup_completed:
                        self.startup_completed.clear()

        self._logger.info(PAUSED)
        return stop_state

    def pause_for_internal_operation(self) -> _StopState:
        """Pause sync and capture prior intent without recording a user stop."""
        with self._lock:
            self._wait_for_stop_completion()
            if self._active_internal_operation is not None:
                raise MaestralApiError(
                    "Cannot start another internal operation",
                    "Wait for the current operation to finish.",
                )
            self._internal_operation_generation += 1
            internal_operation = self._internal_operation_generation
            self._active_internal_operation = internal_operation

        try:
            stop_state = self.stop(
                clear_autostart=True,
                _record_user_stop=False,
            )
        except BaseException:
            with self._lock:
                if self._active_internal_operation == internal_operation:
                    self._active_internal_operation = None
                    self._stop_condition.notify_all()
            raise

        return _StopState(
            generation=stop_state.generation,
            was_running=stop_state.was_running,
            was_autostarting=stop_state.was_autostarting,
            internal_operation=internal_operation,
        )

    @_with_lock
    def _finish_internal_operation(self, stop_state: _StopState) -> bool:
        """Release an internal-operation reservation without restoring sync."""
        internal_operation = stop_state.internal_operation
        if (
            internal_operation is None
            or self._active_internal_operation != internal_operation
        ):
            return False
        self._active_internal_operation = None
        self._stop_condition.notify_all()
        return True

    @_with_lock
    def resume_after_internal_stop(
        self,
        stop_state: _StopState,
    ) -> bool:
        """Restore prior sync intent unless a later user stop replaced it."""
        if not self._finish_internal_operation(stop_state):
            return False
        if stop_state.generation != self._stop_generation:
            return False
        if stop_state.was_running:
            self.start()
        elif stop_state.was_autostarting:
            self.autostart.set()
        return True

    def _signal_worker_stop(
        self,
        running: Event,
        autostart: Event,
        *,
        restart: bool,
    ) -> None:
        """Signal one worker generation to stop without joining its peers."""
        stopping_started = False
        try:
            with self._lock:
                running.clear()
                if self.running is not running:
                    return
                if self._stopping:
                    return
                self._stopping = True
                stopping_started = True
                self.sync.fs_events.disable()
                self.startup_completed.set()
                if restart:
                    autostart.set()
                else:
                    autostart.clear()
                self.sync.request_cancel()
                if self.local_observer_thread:
                    self.local_observer_thread.stop()

            self.sync.cancel_sync()
        finally:
            if stopping_started:
                with self._lock:
                    self._stopping = False
                    self._stop_condition.notify_all()

    @_with_lock
    def reset_sync_state(self) -> None:
        """Resets all saved sync state. Settings are not affected."""
        if self.running.is_set():
            raise MaestralApiError(
                "Cannot reset sync state while syncing", "Please try again when idle."
            )
        was_autostarting = self.autostart.is_set()
        self.autostart.clear()
        try:
            with self._sync_gate("Cannot reset sync state"):
                self.sync.reset_sync_state()
                self.download_queue.reload_persisted()
                self.sync._finish_sync_reset_queue()
        except BaseException:
            if was_autostarting and not self._state.get("recovery", "sync_reset"):
                self.autostart.set()
            raise
        else:
            if was_autostarting:
                self.autostart.set()

    @_with_lock
    def reload_download_queue(self) -> None:
        """Load durable download work before a reset marker is cleared."""
        if self.running.is_set():
            raise MaestralApiError(
                "Cannot reload download work while syncing",
                "Please try again when idle.",
            )
        with self._sync_gate("Cannot reload download work"):
            self.download_queue.reload_persisted()
            self.sync._finish_sync_reset_queue()

    def rebuild_index(self) -> None:
        """
        Rebuilds the rev file by comparing remote with local files and updating rev
        numbers from the Dropbox server. Files are compared by their content hashes and
        conflicting copies are created if the contents differ. File changes during the
        rebuild process will be queued and uploaded once rebuilding has completed.

        Rebuilding will be performed asynchronously.
        """
        self._logger.info("Rebuilding index...")

        stop_state = self.pause_for_internal_operation()
        try:
            self.reset_sync_state()
        except BaseException:
            self._finish_internal_operation(stop_state)
            raise

        self.resume_after_internal_stop(stop_state)

    # ---- path root management --------------------------------------------------------

    def check_and_update_path_root(self) -> bool:
        """
        Checks if the user's root namespace corresponds to the currently configured
        path root. Updates the root namespace if required and migrates the local
        folder structure. Syncing will be paused during the migration.

        :returns: Whether the path root was updated.
        """
        self.sync.ensure_dropbox_folder_present()

        migration = self._state.get("account", "path_root_migration")
        if migration or self._needs_path_root_update():
            stop_state = self.pause_for_internal_operation()
            try:
                self._update_path_root()
            except BaseException:
                self._finish_internal_operation(stop_state)
                raise

            resumed = self.resume_after_internal_stop(stop_state)
            if stop_state.was_running and resumed:
                self._logger.info("Restarting sync")

            return True
        else:
            self._logger.debug("Path root is up to date")
            return False

    def _needs_path_root_update(self) -> bool:
        """
        Checks if the user's root namespace corresponds to the currently configured
        path root.

        :returns: Whether the configured root namespace needs to be updated.
        """
        self._logger.debug("Checking path root...")

        account_info = self.sync.client.get_account_info()
        return self.sync.client.namespace_id != account_info.root_info.root_namespace_id

    def _preflight_path_root_entries(self, entries: list[os.DirEntry[str]]) -> None:
        """Stop a root migration before it moves or deletes a symbolic link."""
        for entry in entries:
            with convert_api_errors(local_path=entry.path):
                symlink_path = self.sync._find_local_symlink(
                    entry.path,
                    remember=self.sync.ignore_symlinks,
                ) or self.sync._find_local_symlink_below(entry.path)

            if symlink_path:
                raise SymlinkError(
                    "Cannot migrate folder structure",
                    "A path which the migration must change contains the symbolic "
                    f'link "{symlink_path}".',
                    dbx_path=symlink_path,
                    local_path=entry.path,
                )

    @staticmethod
    def _migration_error(message: str) -> MaestralApiError:
        return MaestralApiError("Cannot migrate folder structure", message)

    def _save_path_root_migration(self, migration: dict[str, Any]) -> None:
        with self.sync._local_mutation_lock, self._state._lock:
            reset = self._state.get("recovery", "sync_reset")
            if migration and isinstance(reset, dict) and reset.get("kind") == "unlink":
                raise self._migration_error(
                    "An account unlink started during the folder migration."
                )
            self._state.set("account", "path_root_migration", migration)

    def _set_path_root_migration_phase(
        self, migration: dict[str, Any], phase: str
    ) -> None:
        migration["phase"] = phase
        self._save_path_root_migration(migration)

    def _record_path_root_migration_step(
        self,
        migration: dict[str, Any],
        field: str,
        step: str,
    ) -> None:
        completed = list(migration.get(field, []))
        if step not in completed:
            completed.append(step)
            migration[field] = completed
            self._save_path_root_migration(migration)

    def _path_root_migration_stage(self, migration: dict[str, Any]) -> str:
        stage_path = migration.get("staging_path")
        token = migration.get("staging_token")
        if (
            not isinstance(stage_path, str)
            or not isinstance(token, str)
            or len(token) != 32
            or any(char not in "0123456789abcdef" for char in token)
        ):
            raise self._migration_error("The migration journal has no staging path.")

        dropbox_path = osp.abspath(self.sync.dropbox_path)
        expected_parent = dropbox_path
        expected_name = f"{PATH_ROOT_MIGRATION_PREFIX}{token}"
        expected_path = osp.join(expected_parent, expected_name)
        if (
            stage_path != expected_path
            or osp.normpath(stage_path) != stage_path
            or osp.realpath(osp.dirname(stage_path)) != osp.realpath(expected_parent)
        ):
            raise self._migration_error(
                "The migration journal contains an unsafe staging path."
            )

        return stage_path

    def _path_root_migration_proof_path(self, migration: dict[str, Any]) -> str:
        return self._path_root_migration_stage(migration) + ".owner"

    def _path_root_migration_recovery(self, migration: dict[str, Any]) -> str:
        recovery_path = migration.get("recovery_path")
        token = migration.get("staging_token")
        expected_path = osp.join(
            osp.abspath(self.sync.dropbox_path),
            f"{PATH_ROOT_RECOVERY_PREFIX}{token}",
        )
        if (
            not isinstance(recovery_path, str)
            or recovery_path != expected_path
            or osp.normpath(recovery_path) != recovery_path
            or osp.realpath(osp.dirname(recovery_path))
            != osp.realpath(self.sync.dropbox_path)
        ):
            raise self._migration_error(
                "The migration journal contains an unsafe recovery path."
            )
        return recovery_path

    @staticmethod
    def _path_root_migration_proof(migration: dict[str, Any]) -> str:
        immutable_fields = (
            "version",
            "transition",
            "old_root_nsid",
            "old_root_type",
            "old_home_path",
            "new_root_nsid",
            "new_home_nsid",
            "new_root_type",
            "new_home_path",
            "old_selection_mode",
            "old_selection_paths",
            "new_selection_mode",
            "new_selection_paths",
            "old_recovery_local_paths",
            "new_recovery_local_paths",
            "old_download_intents",
            "new_download_intents",
            "staging_path",
            "staging_token",
            "recovery_path",
            "top_level_names",
            "current_home_name",
            "new_home_name",
            "home_child_names",
            "final_root_names",
            "discard_names",
            "empty_names",
        )
        plan = {field: migration.get(field) for field in immutable_fields}
        payload = json.dumps(
            plan,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        digest = hashlib.sha256(payload).hexdigest()
        return f'{migration.get("staging_token")}:{digest}'

    def _path_root_stage_has_entries(self, stage_path: str) -> bool:
        snapshot = self.sync._snapshot_local_tree(stage_path)
        stage_identity = snapshot.get(stage_path)
        if stage_identity is None or not S_ISDIR(stage_identity[2]):
            raise SyncManager._migration_error(
                "The migration staging path is not a safe directory."
            )
        return len(snapshot) > 1

    def _ensure_path_root_migration_proof(
        self, migration: dict[str, Any], *, create: bool = False
    ) -> None:
        """Verify the owner proof for a staging directory."""
        stage_path = self._path_root_migration_stage(migration)
        proof_path = self._path_root_migration_proof_path(migration)
        expected_proof = self._path_root_migration_proof(migration)

        stage_snapshot = self.sync._snapshot_local_tree(stage_path)
        stage_identity = stage_snapshot.get(stage_path)
        if stage_identity is not None:
            if not S_ISDIR(stage_identity[2]):
                raise self._migration_error(
                    "The migration staging path is not a safe directory."
                )

        proof_snapshot = self.sync._snapshot_local_tree(proof_path)
        proof_identity = proof_snapshot.get(proof_path)
        if proof_identity is None:
            if not create:
                raise self._migration_error(
                    "The migration staging directory has no owner proof."
                )
            if stage_identity is not None and self._path_root_stage_has_entries(
                stage_path
            ):
                raise self._migration_error(
                    "An unproved migration staging directory is not empty."
                )
            temp_file = create_rooted_tempfile(
                self.sync.dropbox_path,
                self.sync.dropbox_path,
                expected_root_identity=self.sync.confirmed_root_identity,
                prefix=".~maestral-migration-proof-",
                mode=0o600,
            )
            try:
                with temp_file.open("wb") as proof_file:
                    proof_file.write(expected_proof.encode("ascii"))
                    proof_file.flush()
                    os.fsync(proof_file.fileno())
                temp_file.close()
                temp_identity = rooted_item_snapshot(
                    temp_file.path,
                    self.sync.dropbox_path,
                    expected_root_identity=self.sync.confirmed_root_identity,
                )[:6]
                move(
                    temp_file.path,
                    proof_path,
                    replace=False,
                    raise_error=True,
                    root_path=self.sync.dropbox_path,
                    expected_root_identity=self.sync.confirmed_root_identity,
                    expected_source_identity=temp_identity,
                )
            finally:
                temp_file.close()
                delete(
                    temp_file.path,
                    root_path=self.sync.dropbox_path,
                    expected_root_identity=self.sync.confirmed_root_identity,
                    expected_target_identity=temp_file.identity,
                )
            return

        if not S_ISREG(proof_identity[2]):
            raise self._migration_error("The migration owner proof is unsafe.")
        proof_hasher = DropboxContentHasher()
        proof_hasher.update(expected_proof.encode("ascii"))
        if (
            proof_identity[3] != len(expected_proof)
            or proof_identity[6] != proof_hasher.hexdigest()
        ):
            raise self._migration_error("The migration owner proof does not match.")

    def _remove_path_root_migration_proof(self, migration: dict[str, Any]) -> None:
        proof_path = self._path_root_migration_proof_path(migration)
        proof_snapshot = self.sync._snapshot_local_tree(proof_path)
        proof_identity = proof_snapshot.get(proof_path)
        if proof_identity is None:
            return
        self._ensure_path_root_migration_proof(migration)
        rooted_unlink(
            proof_path,
            root_path=self.sync.dropbox_path,
            expected_root_identity=self.sync.confirmed_root_identity,
            expected_target_identity=proof_identity[:6],
        )

    def _preflight_path_root_path(self, path: str) -> None:
        """Reject a migration path which is or contains a symbolic link."""
        snapshot = self.sync._snapshot_local_tree(path)
        self._preflight_path_root_snapshot(path, snapshot)

    def _preflight_path_root_snapshot(
        self,
        root_path: str,
        snapshot: dict[
            str,
            tuple[int, int, int, int, int, int, str | None],
        ],
    ) -> None:
        """Reject a rooted migration snapshot which contains a link."""
        for child_path, identity in snapshot.items():
            content_identity = identity[6]
            if isinstance(content_identity, str) and content_identity.startswith(
                "symlink:"
            ):
                description = (
                    "is the symbolic link"
                    if child_path == root_path
                    else "contains the symbolic link"
                )
                raise SymlinkError(
                    "Cannot migrate folder structure",
                    f'The migration path {description} "{child_path}".',
                    local_path=root_path,
                )

    def _validate_path_root_migration_name(self, name: Any) -> str:
        if (
            not isinstance(name, str)
            or name in {"", ".", ".."}
            or osp.basename(name) != name
        ):
            raise self._migration_error(
                "The migration journal contains an unsafe file name."
            )
        return name

    @staticmethod
    def _path_root_tree_digest(
        snapshot: dict[str, tuple[int, int, int, int, int, int, str | None]],
        root_path: str,
    ) -> str:
        """Return a rename-stable digest for a complete migration tree."""
        rows: list[tuple[str, list[int | str | None]]] = []
        for path, identity in sorted(snapshot.items()):
            relative_path = osp.relpath(path, root_path)
            fields: list[int | str | None] = list(identity)
            if relative_path == osp.curdir:
                fields[5] = None
            rows.append((relative_path, fields))
        payload = json.dumps(
            rows,
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")
        return hashlib.sha256(payload).hexdigest()

    def _path_root_move_proof(
        self,
        migration: dict[str, Any],
        source: str,
        destination: str,
        step: str,
    ) -> dict[str, Any]:
        """Create or validate the proof for one journalled move."""
        root_path = self.sync.dropbox_path
        source_relative = osp.relpath(source, root_path)
        destination_relative = osp.relpath(destination, root_path)
        proofs = dict(migration.get("move_proofs", {}))
        proof = proofs.get(step)
        if proof is None:
            snapshot = self.sync._snapshot_local_tree(source)
            self._preflight_path_root_snapshot(source, snapshot)
            identity = snapshot.get(source)
            if identity is None:
                raise self._migration_error(
                    f'The migration source is missing: "{source}".'
                )
            proof = {
                "source": source_relative,
                "destination": destination_relative,
                "identity": list(identity[:6]),
                "digest": self._path_root_tree_digest(snapshot, source),
            }
            proofs[step] = proof
            migration["move_proofs"] = proofs
            self._save_path_root_migration(migration)
        elif (
            not isinstance(proof, dict)
            or proof.get("source") != source_relative
            or proof.get("destination") != destination_relative
        ):
            raise self._migration_error(
                f'The migration proof for "{step}" does not match its paths.'
            )
        return proof

    def _verify_path_root_move_proof(
        self,
        path: str,
        proof: dict[str, Any],
    ) -> None:
        """Verify one moved tree against its journalled identity and digest."""
        snapshot = self.sync._snapshot_local_tree(path)
        self._preflight_path_root_snapshot(path, snapshot)
        identity = snapshot.get(path)
        if (
            identity is None
            or list(identity[:5]) != proof.get("identity", [])[:5]
            or self._path_root_tree_digest(snapshot, path) != proof.get("digest")
        ):
            raise self._migration_error(
                f'The migration item at "{path}" does not match its journal.'
            )

    def _rename_path_root_item(
        self,
        migration: dict[str, Any],
        source: str,
        destination: str,
        step: str,
        *,
        progress_field: str = "completed_moves",
    ) -> None:
        """Apply one journalled rename, or confirm that it already completed."""
        if step in migration.get(progress_field, []):
            proof = self._path_root_move_proof(migration, source, destination, step)
            completed_steps = set(migration.get(progress_field, []))
            move_proofs = migration.get("move_proofs", {})
            destination_was_consumed = any(
                completed_step != step
                and isinstance(move_proofs.get(completed_step), dict)
                and is_equal_or_child(
                    osp.join(
                        self.sync.dropbox_path,
                        move_proofs[completed_step]["source"],
                    ),
                    destination,
                )
                for completed_step in completed_steps
            )
            if not destination_was_consumed:
                self._verify_path_root_move_proof(destination, proof)
                self._preflight_path_root_path(destination)
            return

        source_exists = osp.lexists(source)
        destination_exists = osp.lexists(destination)

        if source_exists and destination_exists:
            raise self._migration_error(
                f'Both migration paths exist: "{source}" and "{destination}".'
            )
        if not source_exists and not destination_exists:
            raise self._migration_error(
                f'Neither migration path exists: "{source}" nor "{destination}".'
            )

        self._preflight_path_root_path(source)
        self._preflight_path_root_path(destination)

        proof = self._path_root_move_proof(
            migration,
            source,
            destination,
            step,
        )
        if source_exists:
            self._verify_path_root_move_proof(source, proof)
            move(
                source,
                destination,
                replace=False,
                raise_error=True,
                root_path=self.sync.dropbox_path,
                expected_root_identity=self.sync.confirmed_root_identity,
                expected_source_identity=tuple(proof["identity"]),
            )

        self._verify_path_root_move_proof(destination, proof)

        self._record_path_root_migration_step(migration, progress_field, step)

    def _new_path_root_migration(self, root_info: Any) -> dict[str, Any]:
        """Build and persist an exact, preflighted root-layout move plan."""
        self.sync.ensure_dropbox_folder_present()
        if self.sync._validated_local_evacuations():
            raise self._migration_error(
                "A retained local recovery item must finish before migration."
            )
        if self.sync._validated_case_changes():
            raise self._migration_error(
                "A pending case-only change must finish before migration."
            )
        current_root_type = self._state.get("account", "path_root_type")
        current_root_nsid = self._state.get("account", "path_root_nsid")
        current_home_path = self._state.get("account", "home_path")
        current_home_path_lower = normalize(current_home_path)
        current_home_name_lower = normalize(current_home_path.lstrip("/"))

        if current_root_type == "team" and current_home_path == "":
            raise self._migration_error("Inconsistent namespace information found.")

        if isinstance(root_info, UserRootInfo):
            new_root_type = "user"
            new_home_path = ""
        elif isinstance(root_info, TeamRootInfo):
            new_root_type = "team"
            new_home_path = root_info.home_path
        else:
            raise self._migration_error(
                f"Got {root_info!r} but expected UserRootInfo or TeamRootInfo."
            )

        def direct_home_name(path: str) -> str:
            name = path.lstrip("/")
            if not name or "/" in name or "\\" in name:
                raise self._migration_error(
                    f"The team home path is not a direct child: {path!r}."
                )
            return name

        if current_root_type == "team":
            direct_home_name(current_home_path)

        new_home_name = direct_home_name(new_home_path) if new_home_path else ""
        for home_path in (current_home_path, new_home_path):
            if not home_path:
                continue
            try:
                self.sync.to_local_path_from_cased(home_path)
            except ValueError as exc:
                raise self._migration_error(
                    f"The team home path is invalid: {home_path!r}."
                ) from exc
        home_path_changed = current_home_path != new_home_path
        transition = f"{current_root_type}_to_{new_root_type}"
        maestral_file_names = {
            normalize(name)
            for name in (FILE_CACHE, MIGNORE_FILE, OLD_REV_FILE, ROOT_MARKER_FILE)
        }

        def is_maestral_entry(entry: os.DirEntry[str]) -> bool:
            return normalize(
                entry.name
            ) in maestral_file_names or entry.name.startswith(PATH_ROOT_RECOVERY_PREFIX)

        try:
            root_entries = list(os.scandir(self.sync.dropbox_path))
        except (FileNotFoundError, NotADirectoryError):
            raise NoDropboxDirError(
                "Dropbox folder missing",
                "Please move the Dropbox folder back to its original location or "
                "restart Maestral to set up a new folder.",
            )

        if any(
            entry.name.startswith(PATH_ROOT_MIGRATION_PREFIX) for entry in root_entries
        ):
            raise self._migration_error(
                "An unjournalled migration staging item exists in the Dropbox root."
            )

        current_home_matches = [
            entry
            for entry in root_entries
            if normalize(entry.name) == current_home_name_lower
        ]
        if len(current_home_matches) > 1:
            raise self._migration_error(
                "Multiple local folders match the current team home path."
            )
        current_home_entry = next(
            (
                entry
                for entry in current_home_matches
                if entry.name == current_home_path.lstrip("/")
            ),
            current_home_matches[0] if current_home_matches else None,
        )

        if transition in {"user_to_team", "team_to_user"}:
            mutable_entries = [
                entry for entry in root_entries if not is_maestral_entry(entry)
            ]
        elif transition == "team_to_team":
            mutable_entries = [
                entry
                for entry in root_entries
                if (home_path_changed or entry is not current_home_entry)
                and not is_maestral_entry(entry)
            ]
        else:
            mutable_entries = []

        self._preflight_path_root_entries(mutable_entries)

        top_level_names = [entry.name for entry in mutable_entries]
        current_home_name = current_home_entry.name if current_home_entry else ""
        home_child_names: list[str] = []
        if transition == "team_to_user" and current_home_entry:
            try:
                home_entries = list(os.scandir(current_home_entry.path))
            except (FileNotFoundError, NotADirectoryError):
                raise self._migration_error(
                    "The current team home path is not a directory."
                )
            home_child_names = [entry.name for entry in home_entries]

        mutable_name_set = set(top_level_names)
        for child_name in home_child_names:
            conflicts = [
                entry.name
                for entry in root_entries
                if normalize(entry.name) == normalize(child_name)
                and entry.name not in mutable_name_set
            ]
            if conflicts:
                raise self._migration_error(
                    f'The personal item "{child_name}" conflicts with "{conflicts[0]}".'
                )

        if new_home_name and (transition == "user_to_team" or home_path_changed):
            conflicts = [
                entry.name
                for entry in root_entries
                if normalize(entry.name) == normalize(new_home_name)
                and entry.name not in mutable_name_set
            ]
            if conflicts:
                raise self._migration_error(
                    f'The new home "{new_home_name}" conflicts with "{conflicts[0]}".'
                )

        selected_mode = self.sync.selective_sync_mode
        selected_paths = self.sync.selective_sync_paths
        selects_root = "/" in selected_paths

        old_recovery_local_paths = self.sync._validated_recovered_local_paths()
        old_download_intents = self.sync._validated_download_intents()

        def personal_suffix(path: str) -> str:
            parts = path.lstrip("/").split("/", 1)
            return f"/{parts[1]}" if len(parts) == 2 else ""

        def rewrite_personal_path(path: str) -> str | None:
            path_lower = normalize(path)
            if transition == "user_to_team":
                suffix = "" if path_lower == "/" else path
                return new_home_path + suffix
            if transition == "team_to_user":
                if not is_equal_or_child(path_lower, current_home_path_lower):
                    return None
                suffix = personal_suffix(path)
                return suffix or "/"
            if transition == "team_to_team" and is_equal_or_child(
                path_lower,
                current_home_path_lower,
            ):
                suffix = personal_suffix(path)
                return new_home_path + suffix
            return path

        new_recovery_local_paths: dict[str, dict[str, Any]] = {}
        for path_lower, recovery_entry in old_recovery_local_paths.items():
            if transition in {"team_to_user", "team_to_team"} and not is_equal_or_child(
                path_lower,
                current_home_path_lower,
            ):
                continue
            rewritten_cased = rewrite_personal_path(recovery_entry["path"])
            if rewritten_cased is None:
                continue
            rewritten_source = (
                rewrite_personal_path(recovery_entry["source"])
                if recovery_entry["source"]
                else ""
            )
            if recovery_entry["phase"] == "reserved" and not rewritten_source:
                raise self._migration_error(
                    "A pending recovery move cannot cross the path-root change."
                )
            rewritten_lower = normalize(rewritten_cased)
            new_recovery_local_paths[rewritten_lower] = {
                "path": rewritten_cased,
                "identity": list(recovery_entry["identity"]),
                "phase": recovery_entry["phase"],
                "source": rewritten_source,
            }

        new_download_intents: dict[str, str] = {}
        for path_lower, intent in old_download_intents.items():
            rewritten = rewrite_personal_path(path_lower)
            if rewritten is not None:
                new_download_intents[normalize(rewritten)] = intent

        if transition == "user_to_team":
            new_selected_paths = (
                {"/"}
                if selects_root
                else {normalize(new_home_path) + path for path in selected_paths}
            )
        elif transition == "team_to_user":
            new_selected_paths = (
                {"/"}
                if selects_root
                else {
                    removeprefix(path, current_home_path_lower) or "/"
                    for path in selected_paths
                    if is_equal_or_child(path, current_home_path_lower)
                }
            )
        elif transition == "team_to_team":
            new_selected_paths = (
                {"/"}
                if selects_root
                else {
                    normalize(new_home_path)
                    + removeprefix(path, current_home_path_lower)
                    for path in selected_paths
                    if is_equal_or_child(path, current_home_path_lower)
                }
            )
        else:
            new_selected_paths = selected_paths

        dropbox_path = osp.abspath(self.sync.dropbox_path)
        staging_token = uuid4().hex
        stage_name = f"{PATH_ROOT_MIGRATION_PREFIX}{staging_token}"
        staging_path = osp.join(dropbox_path, stage_name)
        recovery_path = osp.join(
            dropbox_path,
            f"{PATH_ROOT_RECOVERY_PREFIX}{staging_token}",
        )

        if transition == "team_to_user":
            discard_names = [
                name for name in top_level_names if name != current_home_name
            ]
            empty_names = [current_home_name] if current_home_name else []
        elif transition == "team_to_team":
            if home_path_changed and current_home_name:
                discard_names = [
                    name for name in top_level_names if name != current_home_name
                ]
            else:
                discard_names = top_level_names.copy()
            empty_names = []
        else:
            discard_names = []
            empty_names = []

        if transition == "user_to_user":
            final_root_names = [
                entry.name for entry in root_entries if not is_maestral_entry(entry)
            ]
        elif transition == "user_to_team":
            final_root_names = [new_home_name]
        elif transition == "team_to_user":
            final_root_names = home_child_names.copy()
        elif home_path_changed:
            final_root_names = [new_home_name]
        else:
            final_root_names = [current_home_name] if current_home_name else []

        migration: dict[str, Any] = {
            "version": 1,
            "phase": "PLANNED",
            "transition": transition,
            "old_root_nsid": current_root_nsid,
            "old_root_type": current_root_type,
            "old_home_path": current_home_path,
            "new_root_nsid": root_info.root_namespace_id,
            "new_home_nsid": root_info.home_namespace_id,
            "new_root_type": new_root_type,
            "new_home_path": new_home_path,
            "old_selection_mode": selected_mode,
            "old_selection_paths": sorted(selected_paths),
            "new_selection_mode": selected_mode,
            "new_selection_paths": sorted(new_selected_paths),
            "old_recovery_local_paths": old_recovery_local_paths,
            "new_recovery_local_paths": new_recovery_local_paths,
            "old_download_intents": old_download_intents,
            "new_download_intents": new_download_intents,
            "staging_path": staging_path,
            "staging_token": staging_token,
            "recovery_path": recovery_path,
            "top_level_names": top_level_names,
            "current_home_name": current_home_name,
            "new_home_name": new_home_name,
            "home_child_names": home_child_names,
            "final_root_names": final_root_names,
            "discard_names": discard_names,
            "empty_names": empty_names,
            "completed_moves": [],
            "completed_rollbacks": [],
            "completed_cleanup": [],
            "preserved_names": [],
            "move_proofs": {},
        }
        self._validate_path_root_migration(migration)
        self._save_path_root_migration(migration)
        prepared_save_generation: int | None = None
        try:
            self._ensure_path_root_migration_proof(migration, create=True)
            prepared_save_generation = self._state.save_generation
            self._set_path_root_migration_phase(migration, "PREPARED")
        except BaseException:
            committed = self._state.get("account", "path_root_migration")
            if (
                prepared_save_generation is not None
                and self._state.save_committed_since(prepared_save_generation)
                and isinstance(committed, dict)
                and committed.get("staging_token") == migration["staging_token"]
                and committed.get("phase") == "PREPARED"
            ):
                raise
            self._remove_path_root_migration_proof(migration)
            self._save_path_root_migration({})
            raise
        return migration

    def _validate_path_root_migration_target(
        self, migration: dict[str, Any], root_info: Any
    ) -> None:
        if isinstance(root_info, UserRootInfo):
            root_type = "user"
            home_path = ""
        elif isinstance(root_info, TeamRootInfo):
            root_type = "team"
            home_path = root_info.home_path
        else:
            raise self._migration_error("Dropbox returned an unknown root type.")

        if (
            migration.get("new_root_nsid") != root_info.root_namespace_id
            or migration.get("new_root_type") != root_type
            or migration.get("new_home_path") != home_path
        ):
            raise self._migration_error(
                "Dropbox changed the target root during an unfinished migration."
            )

    def _validate_path_root_migration(self, migration: dict[str, Any]) -> None:
        """Validate every journal field before a resume or rollback action."""
        phases = {
            "PLANNED",
            "PREPARED",
            "FILES_MOVED",
            "SELECTION_SAVED",
            "ROOT_UPDATED",
            "SYNC_RESET",
            "FILES_CLEANED",
            "ROLLING_BACK",
            "ROLLED_BACK",
            "CLEANED",
        }
        if migration.get("version") != 1 or migration.get("phase") not in phases:
            raise self._migration_error("The migration journal version is invalid.")

        old_root_type = migration.get("old_root_type")
        new_root_type = migration.get("new_root_type")
        transition = migration.get("transition")
        if (
            old_root_type not in {"user", "team"}
            or new_root_type not in {"user", "team"}
            or transition != f"{old_root_type}_to_{new_root_type}"
        ):
            raise self._migration_error("The migration root types are inconsistent.")

        for key in (
            "old_root_nsid",
            "new_root_nsid",
            "new_home_nsid",
            "old_home_path",
            "new_home_path",
        ):
            if not isinstance(migration.get(key), str):
                raise self._migration_error(
                    f"The migration journal field {key!r} is invalid."
                )

        move_proofs = migration.get("move_proofs")
        if not isinstance(move_proofs, dict):
            raise self._migration_error("The migration move proofs are invalid.")

        if (old_root_type == "user") != (migration["old_home_path"] == ""):
            raise self._migration_error("The old home path is inconsistent.")
        if (new_root_type == "user") != (migration["new_home_path"] == ""):
            raise self._migration_error("The new home path is inconsistent.")
        for home_path in (
            migration["old_home_path"],
            migration["new_home_path"],
        ):
            if not home_path:
                continue
            try:
                self.sync.to_local_path_from_cased(home_path)
            except ValueError as exc:
                raise self._migration_error(
                    f"The migration team home path is invalid: {home_path!r}."
                ) from exc

        for key in (
            "top_level_names",
            "home_child_names",
            "final_root_names",
            "discard_names",
            "empty_names",
            "completed_moves",
            "completed_rollbacks",
            "completed_cleanup",
            "preserved_names",
            "old_selection_paths",
            "new_selection_paths",
        ):
            if not isinstance(migration.get(key), list):
                raise self._migration_error(
                    f"The migration journal field {key!r} is invalid."
                )

        top_level_names = [
            self._validate_path_root_migration_name(name)
            for name in migration["top_level_names"]
        ]
        home_child_names = [
            self._validate_path_root_migration_name(name)
            for name in migration["home_child_names"]
        ]
        final_root_names = [
            self._validate_path_root_migration_name(name)
            for name in migration["final_root_names"]
        ]
        discard_names = [
            self._validate_path_root_migration_name(name)
            for name in migration["discard_names"]
        ]
        empty_names = [
            self._validate_path_root_migration_name(name)
            for name in migration["empty_names"]
        ]
        preserved_names = [
            self._validate_path_root_migration_name(name)
            for name in migration["preserved_names"]
        ]
        for names in (top_level_names, home_child_names, final_root_names):
            if len(names) != len(set(names)) or len(names) != len(
                {normalize(name) for name in names}
            ):
                raise self._migration_error(
                    "The migration move plan contains duplicate names."
                )

        current_home_name = migration.get("current_home_name")
        new_home_name = migration.get("new_home_name")
        if not isinstance(current_home_name, str) or not isinstance(new_home_name, str):
            raise self._migration_error("The migration home names are invalid.")
        if current_home_name:
            self._validate_path_root_migration_name(current_home_name)
            if normalize(current_home_name) != normalize(
                migration["old_home_path"].lstrip("/")
            ):
                raise self._migration_error("The current home name is inconsistent.")
        if new_home_name:
            self._validate_path_root_migration_name(new_home_name)
            if new_home_name != migration["new_home_path"].lstrip("/"):
                raise self._migration_error("The new home name is inconsistent.")

        top_level_set = set(top_level_names)
        if not set(discard_names).issubset(top_level_set) or not set(
            empty_names
        ).issubset(top_level_set):
            raise self._migration_error(
                "The cleanup plan is not a subset of the move plan."
            )
        if set(discard_names) & set(empty_names):
            raise self._migration_error("The cleanup plan overlaps itself.")
        if not set(preserved_names).issubset(set(discard_names)):
            raise self._migration_error("The recovery plan is inconsistent.")

        if transition == "team_to_user":
            expected_discard = top_level_set - (
                {current_home_name} if current_home_name else set()
            )
            expected_empty = {current_home_name} if current_home_name else set()
        elif transition == "team_to_team":
            if (
                migration["old_home_path"] != migration["new_home_path"]
                and current_home_name
            ):
                expected_discard = top_level_set - {current_home_name}
            else:
                expected_discard = top_level_set
            expected_empty = set()
        else:
            expected_discard = set()
            expected_empty = set()
        if set(discard_names) != expected_discard or set(empty_names) != expected_empty:
            raise self._migration_error("The cleanup plan is inconsistent.")

        if transition == "user_to_team":
            expected_final_names = {new_home_name}
        elif transition == "team_to_user":
            expected_final_names = set(home_child_names)
        elif transition == "team_to_team":
            expected_home_name = (
                new_home_name
                if migration["old_home_path"] != migration["new_home_path"]
                else current_home_name
            )
            expected_final_names = {expected_home_name} if expected_home_name else set()
        else:
            expected_final_names = set(final_root_names)
        if set(final_root_names) != expected_final_names:
            raise self._migration_error("The final root plan is inconsistent.")

        for mode_key in ("old_selection_mode", "new_selection_mode"):
            if migration.get(mode_key) not in {"exclude", "include"}:
                raise self._migration_error("The migration selection mode is invalid.")
        for paths_key in ("old_selection_paths", "new_selection_paths"):
            paths = migration[paths_key]
            if not isinstance(paths, list) or not all(
                isinstance(path, str) for path in paths
            ):
                raise self._migration_error(
                    "The migration selection paths are invalid."
                )
            cleaned = self.sync.clean_selective_sync_paths(
                paths,
                validate_local=False,
            )
            if cleaned != set(paths) or len(paths) != len(cleaned):
                raise self._migration_error(
                    "The migration selection paths are invalid."
                )

        for paths_key in (
            "old_recovery_local_paths",
            "new_recovery_local_paths",
        ):
            paths = migration.get(paths_key)
            if not isinstance(paths, dict) or not all(
                isinstance(path_lower, str)
                and normalize(path_lower) == path_lower
                and isinstance(entry, dict)
                and isinstance(entry.get("path"), str)
                and self.sync._is_valid_cased_dbx_path(entry["path"])
                and normalize(entry["path"]) == path_lower
                and isinstance(entry.get("identity"), list)
                and len(entry["identity"]) == 3
                and all(isinstance(value, int) for value in entry["identity"])
                and entry.get("phase") in {"reserved", "tracked"}
                and isinstance(entry.get("source"), str)
                and (
                    not entry["source"]
                    or self.sync._is_valid_cased_dbx_path(entry["source"])
                )
                and (entry["phase"] == "reserved") == bool(entry["source"])
                for path_lower, entry in paths.items()
            ):
                raise self._migration_error("The migration recovery paths are invalid.")

        for intents_key in ("old_download_intents", "new_download_intents"):
            intents = migration.get(intents_key)
            if not isinstance(intents, dict) or not all(
                isinstance(path_lower, str)
                and normalize(path_lower) == path_lower
                and intent in {"include", "restore"}
                for path_lower, intent in intents.items()
            ):
                raise self._migration_error(
                    "The migration download intents are invalid."
                )
        allowed_moves = {"create:staging"}
        allowed_moves.update(f"stage:{name}" for name in top_level_names)
        if transition == "user_to_team":
            allowed_moves.add("final:user-home")
        elif transition == "team_to_user":
            allowed_moves.update(f"final:personal:{name}" for name in home_child_names)
        elif (
            transition == "team_to_team"
            and migration["old_home_path"] != migration["new_home_path"]
            and current_home_name
        ):
            allowed_moves.add("final:team-home")
        if not set(migration["completed_moves"]).issubset(allowed_moves):
            raise self._migration_error("The completed move list is invalid.")

        allowed_rollbacks = {f"rollback:stage:{name}" for name in top_level_names}
        allowed_rollbacks.update(
            f"rollback:personal:{name}" for name in home_child_names
        )
        allowed_rollbacks.update({"rollback:user-home", "rollback:team-home"})
        if not set(migration["completed_rollbacks"]).issubset(allowed_rollbacks):
            raise self._migration_error("The rollback list is invalid.")

        allowed_cleanup = {f"discard:{name}" for name in discard_names}
        allowed_cleanup.update(f"empty:{name}" for name in empty_names)
        allowed_cleanup.add("final:recovery")
        if not set(migration["completed_cleanup"]).issubset(allowed_cleanup):
            raise self._migration_error("The cleanup progress is invalid.")

        allowed_proofs = allowed_moves | allowed_rollbacks | allowed_cleanup
        if not set(move_proofs).issubset(allowed_proofs):
            raise self._migration_error("The migration move proofs are invalid.")
        for step, proof in move_proofs.items():
            if not isinstance(step, str) or not isinstance(proof, dict):
                raise self._migration_error("The migration move proof is invalid.")
            source = proof.get("source")
            destination = proof.get("destination")
            identity = proof.get("identity")
            digest = proof.get("digest")
            if (
                not isinstance(source, str)
                or not isinstance(destination, str)
                or any(
                    osp.isabs(path)
                    or osp.normpath(path) != path
                    or path == osp.pardir
                    or path.startswith(osp.pardir + osp.sep)
                    for path in (source, destination)
                )
                or not isinstance(identity, list)
                or len(identity) != 6
                or any(not isinstance(value, int) for value in identity)
                or not isinstance(digest, str)
                or len(digest) != 64
                or any(char not in "0123456789abcdef" for char in digest)
            ):
                raise self._migration_error("The migration move proof is invalid.")

        self._path_root_migration_stage(migration)
        self._path_root_migration_recovery(migration)

    def _path_root_info_from_migration(
        self, migration: dict[str, Any]
    ) -> UserRootInfo | TeamRootInfo:
        if migration["new_root_type"] == "user":
            return UserRootInfo(
                root_namespace_id=migration["new_root_nsid"],
                home_namespace_id=migration["new_home_nsid"],
            )
        return TeamRootInfo(
            root_namespace_id=migration["new_root_nsid"],
            home_namespace_id=migration["new_home_nsid"],
            home_path=migration["new_home_path"],
        )

    def _path_root_state_matches_migration(self, migration: dict[str, Any]) -> bool:
        return (
            self._state.get("account", "path_root_nsid") == migration["new_root_nsid"]
            and self._state.get("account", "path_root_type")
            == migration["new_root_type"]
            and self._state.get("account", "home_path") == migration["new_home_path"]
        )

    def _set_path_root_recovery_state(
        self,
        migration: dict[str, Any],
        *,
        use_new: bool,
    ) -> None:
        """Atomically restore or apply recovery paths and targeted downloads."""
        prefix = "new" if use_new else "old"
        local_paths = migration[f"{prefix}_recovery_local_paths"]
        intents = migration[f"{prefix}_download_intents"]
        with self._state._lock:
            previous_paths = self._state.get("recovery", "local_paths")
            previous_intents = self._state.get("recovery", "download_intents")
            save_generation = self._state.save_generation
            try:
                self._state.set(
                    "recovery",
                    "local_paths",
                    local_paths,
                    save=False,
                )
                self._state.set(
                    "recovery",
                    "download_intents",
                    intents,
                    save=False,
                )
                self._state.save()
            except BaseException:
                if not self._state.save_committed_since(save_generation):
                    self._state.set(
                        "recovery",
                        "local_paths",
                        previous_paths,
                        save=False,
                    )
                    self._state.set(
                        "recovery",
                        "download_intents",
                        previous_intents,
                        save=False,
                    )
                raise
        self.sync.reload_cached_config()

    def _reconcile_path_root_recovery_state(self) -> None:
        """Queue durable work after a same-process migration reset."""
        for dbx_path in self.sync.targeted_download_paths:
            self.download_queue.put(dbx_path)
        self.sync.rescan_recovered_local_paths()

    def _path_root_user_to_team_is_nested(self, migration: dict[str, Any]) -> bool:
        stage_path = self._path_root_migration_stage(migration)
        new_home_name = self._validate_path_root_migration_name(
            migration.get("new_home_name")
        )
        final_home = osp.join(self.sync.dropbox_path, new_home_name)
        names = [
            self._validate_path_root_migration_name(name)
            for name in migration.get("top_level_names", [])
        ]
        completed_moves = set(migration.get("completed_moves", []))
        has_move_evidence = "final:user-home" in completed_moves or all(
            f"stage:{name}" in completed_moves for name in names
        )
        proof_path = self._path_root_migration_proof_path(migration)
        if not has_move_evidence:
            return False
        if not osp.lexists(proof_path):
            return False
        self._ensure_path_root_migration_proof(migration)

        is_nested = (
            not osp.lexists(stage_path)
            and osp.lexists(final_home)
            and S_ISDIR(os.lstat(final_home).st_mode)
            and not is_fs_link(os.lstat(final_home))
            and all(osp.lexists(osp.join(final_home, name)) for name in names)
            and all(
                name == new_home_name
                or not osp.lexists(osp.join(self.sync.dropbox_path, name))
                for name in names
            )
        )
        if is_nested:
            self._preflight_path_root_path(final_home)
            for name in names:
                self._preflight_path_root_path(osp.join(final_home, name))
        return is_nested

    def _apply_path_root_file_moves(self, migration: dict[str, Any]) -> None:
        self.sync.ensure_dropbox_folder_present()
        self._ensure_path_root_migration_proof(migration)
        transition = migration.get("transition")
        if transition not in {
            "user_to_user",
            "user_to_team",
            "team_to_user",
            "team_to_team",
        }:
            raise self._migration_error("The migration journal has an unknown type.")

        if transition == "user_to_user":
            return

        stage_path = self._path_root_migration_stage(migration)
        top_level_names = [
            self._validate_path_root_migration_name(name)
            for name in migration.get("top_level_names", [])
        ]

        if transition == "user_to_team" and self._path_root_user_to_team_is_nested(
            migration
        ):
            for name in top_level_names:
                self._record_path_root_migration_step(
                    migration, "completed_moves", f"stage:{name}"
                )
            self._record_path_root_migration_step(
                migration, "completed_moves", "final:user-home"
            )
            return

        if not osp.lexists(stage_path):
            rooted_mkdir(
                stage_path,
                mode=0o700,
                root_path=self.sync.dropbox_path,
                expected_root_identity=self.sync.confirmed_root_identity,
            )
            self._record_path_root_migration_step(
                migration, "completed_moves", "create:staging"
            )
        else:
            stage_stat = os.lstat(stage_path)
            if not S_ISDIR(stage_stat.st_mode) or is_fs_link(stage_stat):
                raise self._migration_error(
                    "The migration staging path is not a safe directory."
                )
            self._ensure_path_root_migration_proof(migration)

        for name in top_level_names:
            if (
                transition == "team_to_team"
                and migration.get("old_home_path") != migration.get("new_home_path")
                and name == migration.get("current_home_name")
                and not osp.lexists(osp.join(self.sync.dropbox_path, name))
                and not osp.lexists(osp.join(stage_path, name))
                and osp.lexists(
                    osp.join(
                        self.sync.dropbox_path,
                        self._validate_path_root_migration_name(
                            migration.get("new_home_name")
                        ),
                    )
                )
            ):
                self._record_path_root_migration_step(
                    migration, "completed_moves", f"stage:{name}"
                )
                continue
            self._rename_path_root_item(
                migration,
                osp.join(self.sync.dropbox_path, name),
                osp.join(stage_path, name),
                f"stage:{name}",
            )

        if transition == "user_to_team":
            new_home_name = self._validate_path_root_migration_name(
                migration.get("new_home_name")
            )
            self._rename_path_root_item(
                migration,
                stage_path,
                osp.join(self.sync.dropbox_path, new_home_name),
                "final:user-home",
            )
        elif transition == "team_to_user":
            current_home_name = migration.get("current_home_name")
            if current_home_name:
                current_home_name = self._validate_path_root_migration_name(
                    current_home_name
                )
                for child_name_value in migration.get("home_child_names", []):
                    child_name = self._validate_path_root_migration_name(
                        child_name_value
                    )
                    self._rename_path_root_item(
                        migration,
                        osp.join(stage_path, current_home_name, child_name),
                        osp.join(self.sync.dropbox_path, child_name),
                        f"final:personal:{child_name}",
                    )
        elif transition == "team_to_team" and (
            migration.get("old_home_path") != migration.get("new_home_path")
            and migration.get("current_home_name")
        ):
            current_home_name = self._validate_path_root_migration_name(
                migration.get("current_home_name")
            )
            new_home_name = self._validate_path_root_migration_name(
                migration.get("new_home_name")
            )
            self._rename_path_root_item(
                migration,
                osp.join(stage_path, current_home_name),
                osp.join(self.sync.dropbox_path, new_home_name),
                "final:team-home",
            )

        self._verify_path_root_layout(migration)

    def _verify_path_root_layout(self, migration: dict[str, Any]) -> None:
        transition = migration.get("transition")
        if transition == "user_to_team":
            if not self._path_root_user_to_team_is_nested(migration):
                raise self._migration_error("The new team home layout is incomplete.")
        elif transition == "team_to_user":
            for child_name_value in migration.get("home_child_names", []):
                child_name = self._validate_path_root_migration_name(child_name_value)
                final_path = osp.join(self.sync.dropbox_path, child_name)
                if not osp.lexists(final_path):
                    raise self._migration_error(
                        f'The personal item "{child_name}" is not in its final path.'
                    )
                self._preflight_path_root_path(final_path)
        elif transition == "team_to_team" and migration.get("current_home_name"):
            home_name_value = (
                migration.get("new_home_name")
                if migration.get("old_home_path") != migration.get("new_home_path")
                else migration.get("current_home_name")
            )
            home_name = self._validate_path_root_migration_name(home_name_value)
            home_path = osp.join(self.sync.dropbox_path, home_name)
            if not osp.lexists(home_path):
                raise self._migration_error("The personal team home is missing.")
            home_stat = os.lstat(home_path)
            if not S_ISDIR(home_stat.st_mode) or is_fs_link(home_stat):
                raise self._migration_error(
                    "The personal team home is not a safe directory."
                )
            self._preflight_path_root_path(home_path)

    def _validate_live_path_root_plan(self, migration: dict[str, Any]) -> None:
        """Stop if an unplanned root item appeared before the root update."""
        expected_names = {
            self._validate_path_root_migration_name(name)
            for name in migration.get("final_root_names", [])
        }
        internal_names = {
            normalize(name)
            for name in (FILE_CACHE, MIGNORE_FILE, OLD_REV_FILE, ROOT_MARKER_FILE)
        }
        stage_name = osp.basename(self._path_root_migration_stage(migration))
        proof_name = osp.basename(self._path_root_migration_proof_path(migration))

        try:
            root_entries = list(os.scandir(self.sync.dropbox_path))
        except (FileNotFoundError, NotADirectoryError) as exc:
            raise self._migration_error(
                "The Dropbox root disappeared during migration."
            ) from exc

        live_names: list[str] = []
        for entry in root_entries:
            if (
                normalize(entry.name) in internal_names
                or entry.name in {stage_name, proof_name}
                or entry.name.startswith(PATH_ROOT_RECOVERY_PREFIX)
            ):
                continue
            live_names.append(entry.name)
            self._preflight_path_root_path(entry.path)

        if len(live_names) != len(set(live_names)) or set(live_names) != expected_names:
            unexpected = sorted(set(live_names) - expected_names)
            missing = sorted(expected_names - set(live_names))
            details = []
            if unexpected:
                details.append(f"unexpected items: {unexpected!r}")
            if missing:
                details.append(f"missing items: {missing!r}")
            raise self._migration_error(
                "The Dropbox root changed after the migration plan ("
                + "; ".join(details)
                + ")."
            )

    def _rollback_path_root_file_moves(self, migration: dict[str, Any]) -> None:
        self.sync.ensure_dropbox_folder_present()
        transition = migration.get("transition")
        if transition == "user_to_user":
            self._set_path_root_migration_phase(migration, "ROLLED_BACK")
            self._remove_path_root_migration_proof(migration)
            self._save_path_root_migration({})
            return

        stage_path = self._path_root_migration_stage(migration)
        if osp.lexists(stage_path) and self._path_root_stage_has_entries(stage_path):
            self._ensure_path_root_migration_proof(migration)

        def roll_back_item(
            source: str,
            destination: str,
            forward_step: str,
            rollback_step: str,
        ) -> None:
            if rollback_step in migration["completed_rollbacks"]:
                return

            source_exists = osp.lexists(source)
            forward_completed = forward_step in migration["completed_moves"]

            if not forward_completed and not source_exists:
                return

            self._rename_path_root_item(
                migration,
                source,
                destination,
                rollback_step,
                progress_field="completed_rollbacks",
            )

        staging_completed = all(
            f"stage:{name}" in migration["completed_moves"]
            for name in migration["top_level_names"]
        )

        if transition == "user_to_team":
            new_home_name = self._validate_path_root_migration_name(
                migration.get("new_home_name")
            )
            final_home = osp.join(self.sync.dropbox_path, new_home_name)
            if "final:user-home" in migration["completed_moves"] or staging_completed:
                roll_back_item(
                    final_home,
                    stage_path,
                    "final:user-home",
                    "rollback:user-home",
                )
        elif transition == "team_to_user":
            current_home_name = migration.get("current_home_name")
            if current_home_name:
                current_home_name = self._validate_path_root_migration_name(
                    current_home_name
                )
                for child_name_value in reversed(migration.get("home_child_names", [])):
                    child_name = self._validate_path_root_migration_name(
                        child_name_value
                    )
                    source = osp.join(self.sync.dropbox_path, child_name)
                    destination = osp.join(stage_path, current_home_name, child_name)
                    forward_step = f"final:personal:{child_name}"
                    if (
                        forward_step in migration["completed_moves"]
                        or staging_completed
                    ):
                        roll_back_item(
                            source,
                            destination,
                            forward_step,
                            f"rollback:personal:{child_name}",
                        )
        elif transition == "team_to_team" and (
            migration.get("old_home_path") != migration.get("new_home_path")
            and migration.get("current_home_name")
        ):
            current_home_name = self._validate_path_root_migration_name(
                migration.get("current_home_name")
            )
            new_home_name = self._validate_path_root_migration_name(
                migration.get("new_home_name")
            )
            source = osp.join(self.sync.dropbox_path, new_home_name)
            destination = osp.join(stage_path, current_home_name)
            if "final:team-home" in migration["completed_moves"] or staging_completed:
                roll_back_item(
                    source,
                    destination,
                    "final:team-home",
                    "rollback:team-home",
                )

        for name_value in reversed(migration.get("top_level_names", [])):
            name = self._validate_path_root_migration_name(name_value)
            source = osp.join(stage_path, name)
            destination = osp.join(self.sync.dropbox_path, name)
            roll_back_item(
                source,
                destination,
                f"stage:{name}",
                f"rollback:stage:{name}",
            )

        if osp.lexists(stage_path):
            stage_snapshot = self.sync._snapshot_local_tree(stage_path)
            stage_identity = stage_snapshot[stage_path]
            rooted_rmdir(
                stage_path,
                root_path=self.sync.dropbox_path,
                expected_root_identity=self.sync.confirmed_root_identity,
                expected_target_identity=stage_identity[:6],
            )
        self._set_path_root_migration_phase(migration, "ROLLED_BACK")
        self._remove_path_root_migration_proof(migration)
        self._save_path_root_migration({})

    def _cleanup_path_root_migration_files(self, migration: dict[str, Any]) -> None:
        self.sync.ensure_dropbox_folder_present()
        self._verify_path_root_layout(migration)
        transition = migration.get("transition")
        stage_path = self._path_root_migration_stage(migration)
        recovery_path = self._path_root_migration_recovery(migration)

        if transition == "user_to_user" or (
            transition == "user_to_team"
            and self._path_root_user_to_team_is_nested(migration)
        ):
            self._ensure_path_root_migration_proof(migration)
            self._set_path_root_migration_phase(migration, "FILES_CLEANED")
            return

        self._ensure_path_root_migration_proof(migration)
        for name_value in migration.get("empty_names", []):
            name = self._validate_path_root_migration_name(name_value)
            path = osp.join(stage_path, name)
            if osp.lexists(path):
                self._preflight_path_root_path(path)
                path_snapshot = self.sync._snapshot_local_tree(path)
                path_identity = path_snapshot[path]
                rooted_rmdir(
                    path,
                    root_path=self.sync.dropbox_path,
                    expected_root_identity=self.sync.confirmed_root_identity,
                    expected_target_identity=path_identity[:6],
                )
            self._record_path_root_migration_step(
                migration, "completed_cleanup", f"empty:{name}"
            )

        for name_value in migration.get("discard_names", []):
            name = self._validate_path_root_migration_name(name_value)
            path = osp.join(stage_path, name)
            if name in migration["preserved_names"]:
                continue
            if osp.lexists(path):
                self._preflight_path_root_path(path)
                self._record_path_root_migration_step(
                    migration,
                    "preserved_names",
                    name,
                )
                continue
            self._record_path_root_migration_step(
                migration, "completed_cleanup", f"discard:{name}"
            )

        if migration["preserved_names"]:
            if osp.lexists(stage_path):
                remaining_names = {entry.name for entry in os.scandir(stage_path)}
                if not remaining_names.issubset(set(migration["preserved_names"])):
                    raise self._migration_error(
                        "The migration staging directory has unexpected items."
                    )
            self._rename_path_root_item(
                migration,
                stage_path,
                recovery_path,
                "final:recovery",
                progress_field="completed_cleanup",
            )
            self._logger.warning(
                "Preserved files from the old Dropbox root at %r",
                recovery_path,
            )
        elif osp.lexists(stage_path):
            stage_snapshot = self.sync._snapshot_local_tree(stage_path)
            stage_identity = stage_snapshot[stage_path]
            rooted_rmdir(
                stage_path,
                root_path=self.sync.dropbox_path,
                expected_root_identity=self.sync.confirmed_root_identity,
                expected_target_identity=stage_identity[:6],
            )

        self._set_path_root_migration_phase(migration, "FILES_CLEANED")

    def _finalize_path_root_migration(self, migration: dict[str, Any]) -> None:
        self._set_path_root_migration_phase(migration, "CLEANED")
        self._remove_path_root_migration_proof(migration)
        self._save_path_root_migration({})

    def _resume_path_root_migration(
        self, migration: dict[str, Any], root_info: Any
    ) -> str:
        self._validate_path_root_migration(migration)
        phase = migration.get("phase")

        if phase in {"ROLLED_BACK", "CLEANED"}:
            stage_path = self._path_root_migration_stage(migration)
            if osp.lexists(stage_path):
                raise self._migration_error(
                    "A completed migration still has a staging directory."
                )
            proof_path = self._path_root_migration_proof_path(migration)
            if osp.lexists(proof_path):
                self._ensure_path_root_migration_proof(migration)
                self._remove_path_root_migration_proof(migration)
            self._save_path_root_migration({})
            return "rolled_back" if phase == "ROLLED_BACK" else "completed"

        if phase == "PLANNED":
            proof_path = self._path_root_migration_proof_path(migration)
            if not osp.lexists(proof_path):
                self._save_path_root_migration({})
                return "rolled_back"
            self._ensure_path_root_migration_proof(migration)
            self._set_path_root_migration_phase(migration, "PREPARED")
            phase = "PREPARED"

        self._ensure_path_root_migration_proof(migration)

        if phase == "ROLLING_BACK":
            self._rollback_path_root_file_moves(migration)
            return "rolled_back"

        try:
            self._validate_path_root_migration_target(migration, root_info)
        except MaestralApiError:
            root_was_updated = phase in {
                "ROOT_UPDATED",
                "FILES_CLEANED",
                "SYNC_RESET",
            } or (
                phase == "SELECTION_SAVED"
                and self._path_root_state_matches_migration(migration)
            )
            if root_was_updated:
                saved_root_info = self._path_root_info_from_migration(migration)
                self.sync.client.update_path_root(saved_root_info)
                if phase == "SELECTION_SAVED":
                    self._set_path_root_migration_phase(migration, "ROOT_UPDATED")
                    phase = "ROOT_UPDATED"
                if phase == "ROOT_UPDATED":
                    self._cleanup_path_root_migration_files(migration)
                    phase = "FILES_CLEANED"
                if phase == "FILES_CLEANED":
                    self.reset_sync_state()
                    self._set_path_root_recovery_state(
                        migration,
                        use_new=True,
                    )
                    self._reconcile_path_root_recovery_state()
                    self._set_path_root_migration_phase(migration, "SYNC_RESET")
                    phase = "SYNC_RESET"
                self._finalize_path_root_migration(migration)
                return "target_changed"

            self._set_path_root_recovery_state(migration, use_new=False)
            self.sync.set_selective_sync(
                migration["old_selection_mode"],
                migration["old_selection_paths"],
            )
            self._set_path_root_migration_phase(migration, "ROLLING_BACK")
            self._rollback_path_root_file_moves(migration)
            return "rolled_back"

        if phase == "PREPARED":
            try:
                self._apply_path_root_file_moves(migration)
                self._set_path_root_migration_phase(migration, "FILES_MOVED")
            except Exception as move_error:
                try:
                    self._set_path_root_migration_phase(migration, "ROLLING_BACK")
                    self._rollback_path_root_file_moves(migration)
                except Exception as rollback_error:
                    raise self._migration_error(
                        "The move failed and rollback did not finish. Personal data "
                        f'remains in "{self._path_root_migration_stage(migration)}".'
                    ) from rollback_error
                raise move_error
            phase = "FILES_MOVED"

        if phase == "FILES_MOVED":
            self._set_path_root_recovery_state(migration, use_new=True)
            self.sync.set_selective_sync(
                migration["new_selection_mode"],
                migration["new_selection_paths"],
            )
            self._set_path_root_migration_phase(migration, "SELECTION_SAVED")
            phase = "SELECTION_SAVED"

        if phase == "SELECTION_SAVED":
            self._validate_live_path_root_plan(migration)
            self.sync.client.update_path_root(root_info)
            self._validate_live_path_root_plan(migration)
            self._set_path_root_migration_phase(migration, "ROOT_UPDATED")
            phase = "ROOT_UPDATED"

        if phase == "ROOT_UPDATED":
            self._cleanup_path_root_migration_files(migration)
            phase = "FILES_CLEANED"

        if phase == "FILES_CLEANED":
            self.reset_sync_state()
            self._set_path_root_recovery_state(migration, use_new=True)
            self._reconcile_path_root_recovery_state()
            self._set_path_root_migration_phase(migration, "SYNC_RESET")
            phase = "SYNC_RESET"

        if phase == "SYNC_RESET":
            self._finalize_path_root_migration(migration)
            return "completed"

        raise self._migration_error(
            f"The migration journal has an unknown phase: {phase!r}."
        )

    def _update_path_root(self) -> None:
        """Resume or start a durable Dropbox root-layout migration."""
        root_info = self.sync.client.account_info.root_info

        with self.sync.sync_lock:
            migration = self._state.get("account", "path_root_migration")
            if migration:
                if not isinstance(migration, dict):
                    raise self._migration_error(
                        "The migration journal has an invalid format."
                    )
                outcome = self._resume_path_root_migration(migration, root_info)
                if outcome == "completed":
                    return

            migration = self._new_path_root_migration(root_info)
            self._resume_path_root_migration(migration, root_info)

    # ---- thread methods --------------------------------------------------------------

    def connection_monitor(self) -> None:
        """
        Monitors the connection to Dropbox servers. Pauses syncing when the connection
        is lost and resumes syncing when reconnected and syncing has not been paused by
        the user.
        """
        while not self._connection_helper_stop.is_set():
            connected = check_connection(DROPBOX_API_HOSTNAME)

            if connected != self.connected:
                # Log the status change.
                self._logger.info(CONNECTED if connected else CONNECTING)

            if connected:
                with self._lock:
                    should_restart = (
                        not self._connection_helper_stop.is_set()
                        and not self.running.is_set()
                        and self.autostart.is_set()
                        and self._active_internal_operation is None
                    )
                if should_restart:
                    if self._stop_for_connection_restart():
                        with self._lock:
                            should_restart = (
                                not self._connection_helper_stop.is_set()
                                and not self.running.is_set()
                                and self.autostart.is_set()
                                and self._active_internal_operation is None
                            )
                            if should_restart:
                                self.start()

            self.connected = connected

            self._connection_helper_stop.wait(self.connection_check_interval)

    def shutdown(self) -> None:
        """Stop syncing and shut down the connection monitor."""
        with self._lock:
            self._shutdown_requested = True
            self._connection_helper_stop.set()
        self.stop()

        if (
            self.connection_helper.is_alive()
            and current_thread() is not self.connection_helper
        ):
            self.connection_helper.join()

    def download_worker(
        self,
        running: Event,
        startup_completed: Event,
        autostart: Event,
    ) -> None:
        """
        Worker to sync changes of remote Dropbox with local folder.

        :param running: Event to shut down local file event handler and worker threads.
        :param startup_completed: Set when startup sync is completed.
        :param autostart: Set when syncing should automatically resume on connection.
        """
        startup_completed.wait()

        while running.is_set():
            with self._handle_sync_thread_errors(running, autostart):
                has_changes = self.sync.wait_for_remote_changes(self.sync.remote_cursor)

                # Check for root namespace updates. Don't apply any remote
                # changes in case of a changed root path.
                if self.check_and_update_path_root():
                    return

                if not running.is_set():
                    return

                if has_changes:
                    with self.sync.sync_lock:
                        if not running.is_set():
                            return
                        self.sync.ensure_dropbox_folder_present()
                        self._logger.info(SYNCING)
                        self.sync.download_sync_cycle()
                        self._logger.info(IDLE)

                        if running.is_set():
                            self.sync.client.get_space_usage()

        _free_memory()

    def download_worker_added_item(
        self,
        running: Event,
        startup_completed: Event,
        autostart: Event,
    ) -> None:
        """
        Worker to download items which have been newly included in sync.

        :param running: Event to shut down local file event handler and worker threads.
        :param startup_completed: Set when startup sync is completed.
        :param autostart: Set when syncing should automatically resume on connection.
        """
        startup_completed.wait()

        while running.is_set():
            with self._handle_sync_thread_errors(running, autostart):
                try:
                    dbx_path_lower = self.download_queue.get(timeout=40)
                except Empty:
                    continue

                if not running.is_set():
                    self.download_queue.requeue(dbx_path_lower)
                    return

                try:
                    with self.sync.sync_lock:
                        if not running.is_set():
                            self.download_queue.requeue(dbx_path_lower)
                            return
                        success = self.sync.get_remote_item(dbx_path_lower)
                        if success and running.is_set():
                            success = self.sync.rescan_dbx_path(dbx_path_lower)
                except BaseException:
                    self.download_queue.requeue(dbx_path_lower)
                    raise
                else:
                    if success:
                        self.download_queue.task_done(
                            dbx_path_lower,
                            self.sync.finish_targeted_download,
                        )
                        self._logger.info(IDLE)
                    else:
                        self.download_queue.requeue(dbx_path_lower)
                        if not self._wait_for_download_retry(running):
                            break

        _free_memory()

    def _wait_for_download_retry(self, running: Event) -> bool:
        """Wait before a targeted download retry while syncing remains active."""
        deadline = time.monotonic() + self.download_retry_interval

        while running.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return True
            time.sleep(min(remaining, 0.25))

        return False

    def upload_worker(
        self,
        running: Event,
        startup_completed: Event,
        autostart: Event,
    ) -> None:
        """
        Worker to sync local changes to remote Dropbox.

        :param running: Event to shut down local file event handler and worker threads.
        :param startup_completed: Set when startup sync is completed.
        :param autostart: Set when syncing should automatically resume on connection.
        """
        startup_completed.wait()

        while running.is_set():
            with self._handle_sync_thread_errors(running, autostart):
                has_changes = self.sync.wait_for_local_changes()

                if not running.is_set():
                    return

                if has_changes:
                    with self.sync.sync_lock:
                        if not running.is_set():
                            return
                        self.sync.ensure_dropbox_folder_present()
                        self._logger.info(SYNCING)
                        self.sync.upload_sync_cycle()
                        self._logger.info(IDLE)

        _free_memory()

    def startup_worker(
        self,
        running: Event,
        startup_completed: Event,
        autostart: Event,
    ) -> None:
        """
        Worker to sync local changes to remote Dropbox.

        :param running: Event to shut down local file event handler and worker threads.
        :param startup_completed: Set when startup sync is completed.
        :param autostart: Set when syncing should automatically resume on connection.
        """
        with self._handle_sync_thread_errors(running, autostart):
            # Fail early if Dropbox folder disappeared.
            self.sync.ensure_dropbox_folder_present()

            # Reload mignore rules.
            self.sync.load_mignore_file()

            self.sync.client.get_space_usage()

            # Update path root and migrate local folders. This is required when a user
            # joins or leaves a team and their root namespace changes.
            self.check_and_update_path_root()

            if not running.is_set():
                startup_completed.set()
                return

            # Retry failed downloads.
            if len(self.sync.download_errors) > 0:
                self._logger.info("Retrying failed syncs...")

            for error in list(self.sync.download_errors):
                if not running.is_set():
                    startup_completed.set()
                    return
                with self.sync.sync_lock:
                    if not running.is_set():
                        startup_completed.set()
                        return
                    self.sync.get_remote_item(error.dbx_path_lower)

            # Resume interrupted downloads.
            if self.download_queue.qsize() > 0:
                self._logger.info("Resuming interrupted syncs...")

            while self.download_queue.has_pending():
                if not running.is_set():
                    startup_completed.set()
                    return
                dbx_path = self.download_queue.get()
                if not running.is_set():
                    self.download_queue.requeue(dbx_path)
                    startup_completed.set()
                    return
                try:
                    with self.sync.sync_lock:
                        if not running.is_set():
                            self.download_queue.requeue(dbx_path)
                            startup_completed.set()
                            return
                        success = self.sync.get_remote_item(dbx_path)
                        if success and running.is_set():
                            success = self.sync.rescan_dbx_path(dbx_path)
                except BaseException:
                    self.download_queue.requeue(dbx_path)
                    raise
                else:
                    if success:
                        self.download_queue.task_done(
                            dbx_path,
                            self.sync.finish_targeted_download,
                        )
                    else:
                        self.download_queue.requeue(dbx_path)
                        break

            if not running.is_set():
                startup_completed.set()
                return

            self.sync.download_sync_cycle()

            if not running.is_set():
                startup_completed.set()
                return

            if self._conf.get("sync", "upload"):
                self.sync.upload_local_changes_while_inactive()

            self._logger.info(IDLE)

        startup_completed.set()
        _free_memory()

    # ---- utilities -------------------------------------------------------------------

    @contextmanager
    def _handle_sync_thread_errors(
        self, running: Event, autostart: Event
    ) -> Iterator[None]:
        try:
            yield
        except CancelledError:
            # Shutdown will be handled externally.
            running.clear()
        except (DropboxConnectionError, DropboxServerError):
            self._logger.debug("Connection error", exc_info=True)
            self._logger.info(DISCONNECTED)
            self._signal_worker_stop(running, autostart, restart=True)
            self._logger.info(CONNECTING)
        except PathRootError:
            self._logger.debug("API call failed due to path root error", exc_info=True)
            self._signal_worker_stop(running, autostart, restart=True)
        except Exception as err:
            title = getattr(err, "title", "Unexpected error")
            self._logger.error(title, exc_info=True)
            self._signal_worker_stop(running, autostart, restart=False)

    def __del__(self) -> None:
        try:
            self.shutdown()
        except Exception:
            pass
