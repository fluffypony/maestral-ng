"""This module defines the main API which is exposed to the CLI or GUI."""

from __future__ import annotations

import asyncio
import difflib
import errno
import gc
import logging
import mimetypes

# system imports
import os
import os.path as osp
import random
import sqlite3
import tempfile
import threading
import time
from asyncio import AbstractEventLoop, Future
from collections import deque
from datetime import datetime, timezone
from stat import S_ISDIR
from typing import Any, Collection, Iterator, Sequence

# external imports
import requests
from packaging.version import Version

try:
    from systemd import journal
except ImportError:
    journal = None

# local imports
from . import __version__
from .client import DropboxClient
from .config import MaestralConfig, MaestralState, validate_config_name
from .constants import (
    CONNECTING,
    DEFAULT_CONFIG_NAME,
    GITHUB_RELEASES_API,
    IDLE,
    IS_LINUX,
    IS_MACOS,
    IS_WINDOWS,
    PAUSED,
    ROOT_MARKER_FILE,
    FileStatus,
)
from .core import (
    FileMetadata,
    FullAccount,
    LinkAccessLevel,
    LinkAudience,
    Metadata,
    PersonalSpaceUsage,
    SharedLinkMetadata,
    UpdateCheckResult,
)
from .database.core import Database
from .errorhandling import CONNECTION_ERRORS, convert_api_errors
from .exceptions import (
    BusyError,
    KeyringAccessError,
    MaestralApiError,
    NoDropboxDirError,
    NotFoundError,
    NotLinkedError,
    SymlinkError,
    UnsupportedFileTypeForDiff,
    UpdateCheckError,
)
from .keyring import CredentialStorage
from .logging import (
    LOG_FMT_SHORT,
    AwaitableHandler,
    CachedHandler,
    scoped_logger,
    setup_logging,
)
from .manager import SyncManager
from .models import SyncErrorEntry, SyncEvent, SyncStatus
from .sync import SyncDirection, SyncEngine
from .utils import exc_info_tuple, get_newer_version
from .utils.appdirs import get_cache_path, get_data_path
from .utils.path import (
    create_rooted_tempfile,
    is_fs_link,
)
from .utils.path import makedirs as rooted_makedirs
from .utils.path import mkdir as rooted_mkdir
from .utils.path import (
    move,
    rooted_tree_snapshot,
)
from .utils.path import unlink as rooted_unlink

__all__ = ["Maestral"]


def _sql_add_column(db: Database, table: str, column: str, affinity: str) -> None:
    try:
        db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {affinity};")
    except sqlite3.OperationalError:
        # column already exists
        pass


def _sql_drop_table(db: Database, table: str) -> None:
    try:
        db.execute(f"DROP TABLE {table};")
    except sqlite3.OperationalError:
        # table does not exist
        pass


# ======================================================================================
# Main API
# ======================================================================================


class Maestral:
    """The public API

    All methods and properties return objects or raise exceptions which the JSON-RPC
    transport can serialize safely.

    Sync errors and fatal errors which occur in the sync threads can be read with the
    properties :attr:`sync_errors` and :attr:`fatal_errors`, respectively.

    :Example:

        First create an instance with a new config_name. In this example, we choose
        "private" to sync a private Dropbox account. Then link the created config to an
        existing Dropbox account and set up the local Dropbox folder. If successful,
        invoke :meth:`start_sync` to start syncing.

        >>> from maestral.main import Maestral
        >>> m = Maestral(config_name='private')
        >>> url = m.get_auth_url()  # get token from Dropbox website
        >>> print(f'Please go to {url} to retrieve a Dropbox authorization token.')
        >>> token = input('Enter auth token: ')
        >>> res = m.link(token)
        >>> if res == 0:
        ...     m.create_dropbox_directory('~/Dropbox (Private)')
        ...     m.start_sync()

    :param config_name: Name of maestral configuration to run. Must not contain any
        whitespace. If the given config file does exist, it will be created.
    :param log_to_stderr: If ``True``, Maestral will print log messages to stderr.
        When started as a systemd services, this can result in duplicate log messages
        in the systemd journal. Defaults to ``False``.
    :param event_loop: Event loop used by ``shutdown_future``.
    :param shutdown_future: Feature to set a result when shutdown is complete. Used to
        inform the caller if an API client calls :method:`shutdown_daemon`. The event
        loop associated with the Future must be the same as ``event_loop``.
    """

    _external_log_handlers: Sequence[logging.Handler]
    _log_handler_status_longpoll: AwaitableHandler
    _log_handler_info_cache: CachedHandler
    _log_handler_error_cache: CachedHandler

    def __init__(
        self,
        config_name: str = DEFAULT_CONFIG_NAME,
        log_to_stderr: bool = False,
        event_loop: AbstractEventLoop | None = None,
        shutdown_future: Future[bool] | None = None,
    ) -> None:
        # Check system compatibility.
        self._check_system_compatibility()

        self._loop = event_loop
        self._config_name = validate_config_name(config_name)
        self._conf = MaestralConfig(self.config_name)
        self._state = MaestralState(self.config_name)
        self._logger = scoped_logger(__name__, self.config_name)
        self.cred_storage = CredentialStorage(self.config_name)
        self._resume_pending_unlink_credentials()
        self._notification_snooze_until = 0.0

        # Set up logging.
        self._log_to_stderr = log_to_stderr
        self._root_logger = scoped_logger("maestral", self.config_name)
        self._root_logger.setLevel(min(self.log_level, logging.INFO))
        self._root_logger.handlers.clear()
        self._setup_logging_external()
        self._setup_logging_internal()

        # Run update scripts after init of loggers and config / state.
        self._check_and_run_post_update_scripts()

        # Set up sync infrastructure.
        self.client = DropboxClient(
            self.config_name,
            self.cred_storage,
            bandwidth_limit_up=self.bandwidth_limit_up,
            bandwidth_limit_down=self.bandwidth_limit_down,
        )
        self._sync_event_condition = threading.Condition()
        self._sync_event_cursor = 0
        self._sync_event_batches: deque[tuple[int, tuple[SyncEvent, ...]]] = deque(
            maxlen=100
        )
        self._sync_event_stream_closed = False
        self._recover_pending_dropbox_root_move()
        self.sync = SyncEngine(self.client, event_callback=self._publish_sync_events)
        self.manager = SyncManager(self.sync)

        # Create a future which will return once `shutdown_daemon` is called.
        # This can be used by an event loop to wait until maestral has been stopped.
        if shutdown_future and not shutdown_future.get_loop() is self._loop:
            raise RuntimeError("'shutdown_future' must use the passed event loop.")

        self.shutdown_future = shutdown_future

    @staticmethod
    def _check_system_compatibility() -> None:
        if not (IS_MACOS or IS_LINUX or IS_WINDOWS):
            raise RuntimeError("Only macOS, Linux, and Windows are supported")

    def _setup_logging_external(self) -> None:
        """
        Sets up logging to external channels:
          * Log files.
          * The systemd journal, if started by systemd.
          * The systemd notify status, if started by systemd.
          * Stderr, if requested.
        """
        self._external_log_handlers = setup_logging(
            self.config_name, stderr=self._log_to_stderr
        )

    def _setup_logging_internal(self) -> None:
        """Sets up logging to internal info and error caches."""
        # Log to cached handlers for status and error APIs.
        self._log_handler_info_cache = CachedHandler(maxlen=1)
        self._log_handler_info_cache.setFormatter(LOG_FMT_SHORT)
        self._log_handler_info_cache.setLevel(logging.INFO)
        self._root_logger.addHandler(self._log_handler_info_cache)

        self._log_handler_error_cache = CachedHandler()
        self._log_handler_error_cache.setFormatter(LOG_FMT_SHORT)
        self._log_handler_error_cache.setLevel(logging.ERROR)
        self._root_logger.addHandler(self._log_handler_error_cache)

        self._log_handler_status_longpoll = AwaitableHandler(max_unblock_per_second=1)
        self._log_handler_status_longpoll.setFormatter(LOG_FMT_SHORT)
        self._log_handler_status_longpoll.setLevel(logging.INFO)
        self._root_logger.addHandler(self._log_handler_status_longpoll)

    @property
    def version(self) -> str:
        """Returns the current Maestral version."""
        return __version__

    def get_auth_url(self) -> str:
        """
        Returns a URL to authorize access to a Dropbox account. To link a Dropbox
        account, retrieve an authorization code from the URL and link Maestral by
        calling :meth:`link` with the provided code.

        :returns: URL to retrieve an authorization code.
        """
        return self.client.get_auth_url()

    def link(
        self,
        code: str | None = None,
        refresh_token: str | None = None,
        access_token: str | None = None,
        allow_plaintext_keyring: bool = False,
    ) -> int:
        """
        Links Maestral with a Dropbox account using the given authorization code. The
        code will be exchanged for an access token and a refresh token with Dropbox
        servers. The refresh token will be stored for future usage as documented in the
        :mod:`oauth` module. Supported keyring backends are, in order of preference:

            * macOS Keychain
            * Any keyring implementing the SecretService Dbus specification
            * KWallet
            * Gnome Keyring
            * Plain text storage, with explicit permission from the caller

        For testing, it is also possible to directly provide a long-lived refresh token
        or a short-lived access token. Note that the tokens must be issued for Maestral,
        with the required scopes, and will be validated with Dropbox servers as part of
        this call.

        :param code: Authorization code.
        :param refresh_token: Optionally, instead of an authorization code, directly
            provide a refresh token.
        :param access_token: Optionally, instead of an authorization code or a refresh
            token, directly provide an access token. Note that access tokens are
            short-lived.
        :param allow_plaintext_keyring: Whether to allow storage of a refresh token in
            plain text when no secure keyring is available.
        :returns: 0 on success, 1 for an invalid token and 2 for connection errors.
        """
        with self.sync.sync_lock:
            reset = self._state.get("recovery", "sync_reset")
            if isinstance(reset, dict) and reset.get("kind") == "unlink":
                pending = self._root_bound_recovery_names()
                if pending:
                    raise MaestralApiError(
                        "Cannot link Dropbox account",
                        "Finish the old account recovery first: " + ", ".join(pending),
                    )
                self._resume_pending_unlink_credentials()
                self.sync._complete_pending_sync_reset()
                self.manager.reload_download_queue()
                if self._state.get("recovery", "sync_reset"):
                    raise MaestralApiError(
                        "Cannot link Dropbox account",
                        "Unlock the old credential store and try again.",
                    )
            return self.client.link(
                code=code,
                refresh_token=refresh_token,
                access_token=access_token,
                allow_plaintext_keyring=allow_plaintext_keyring,
            )

    def unlink(self) -> None:
        """
        Unlinks the configured Dropbox account but leaves all downloaded files in place.
        All syncing metadata will be removed as well. Connection and API errors will be
        handled silently but the Dropbox access key will always be removed from the
        user's PC.

        :raises NotLinkedError: if no Dropbox account is linked.
        """
        self._check_linked()
        self.sync._ensure_unlink_recovery_clear()
        stop_state = self.manager.pause_for_internal_operation()
        try:
            with self.sync.sync_lock:
                self.sync._ensure_unlink_recovery_clear()
                self.manager.begin_unlink_reset(
                    stop_state,
                    account_id=self._conf.get("auth", "account_id"),
                    keyring=self._conf.get("auth", "keyring"),
                    root_path=self._conf.get("sync", "path"),
                    root_marker_id=self._conf.get("sync", "root_marker_id"),
                )

                try:
                    self.client.unlink()
                except (ConnectionError, MaestralApiError):
                    self._logger.debug(
                        "Could not invalidate token with Dropbox", exc_info=True
                    )
                except KeyringAccessError:
                    self._logger.debug(
                        "Could not remove token from keyring", exc_info=True
                    )

                self._resume_pending_unlink_credentials()

                self.sync._complete_pending_sync_reset()
                self.manager.reload_download_queue()
        except BaseException:
            if not self._state.get("recovery", "sync_reset"):
                self.manager.resume_after_internal_stop(stop_state)
            else:
                self.manager._finish_internal_operation(stop_state)
            raise

        self.manager._finish_internal_operation(stop_state)
        self._logger.info("Unlinked Dropbox account.")

    def _root_bound_recovery_names(self) -> tuple[str, ...]:
        """Return saved work which belongs to the configured account root."""
        values = {
            "local evacuations": self._state.get("recovery", "local_evacuations"),
            "local paths": self._state.get("recovery", "local_paths"),
            "case changes": self._state.get("recovery", "case_changes"),
            "root move": self._state.get("recovery", "root_move"),
            "path-root migration": self._state.get("account", "path_root_migration"),
        }
        return tuple(name for name, value in values.items() if value != {})

    def _resume_pending_unlink_credentials(self) -> bool:
        """Retry credential deletion recorded before an interrupted unlink."""
        journal = self._state.get("recovery", "sync_reset")
        if not isinstance(journal, dict) or journal.get("kind") != "unlink":
            return True
        if self._root_bound_recovery_names():
            return False
        if journal.get("credentials_deleted") is True:
            return True
        account_id = journal.get("account_id")
        keyring_name = journal.get("keyring")
        if not isinstance(account_id, str) or not isinstance(keyring_name, str):
            return False

        try:
            self.cred_storage.delete_creds(account_id, keyring_name)
        except KeyringAccessError:
            self._logger.debug("Could not remove token from keyring", exc_info=True)
            return False

        with self._state._lock:
            current = self._state.get("recovery", "sync_reset")
            if current != journal:
                return False
            journal = journal.copy()
            journal["credentials_deleted"] = True
            self._state.set("recovery", "sync_reset", journal)
        return True

    # ==== Methods to access config and saved state ====================================

    @property
    def config_name(self) -> str:
        """The selected configuration."""
        return self._config_name

    def set_conf(self, section: str, name: str, value: Any) -> None:
        """
        Sets a configuration option.

        :param section: Name of section in config file.
        :param name: Name of config option.
        :param value: Config value. May be any type accepted by :obj:`ast.literal_eval`.
        """
        normalised_section = section.lower()
        normalised_name = name.lower()

        if (
            normalised_section == "sync"
            and normalised_name
            in {
                "selective_sync_mode",
                "selective_sync_paths",
                "ignore_symlinks",
            }
        ) or (
            normalised_section in {"main", "sync"}
            and normalised_name == "excluded_items"
        ):
            raise ValueError("Use the selective-sync or symlink API for this setting")
        if normalised_section == "sync" and normalised_name in {
            "path",
            "root_marker_id",
        }:
            raise ValueError("Use the Dropbox root API for this setting")
        self._conf.set(section, name, value)

    def get_conf(self, section: str, name: str) -> Any:
        """
        Gets a configuration option.

        :param section: Name of section in config file.
        :param name: Name of config option.
        :returns: Config value. May be any type accepted by :obj:`ast.literal_eval`.
        """
        return self._conf.get(section, name)

    def set_state(self, section: str, name: str, value: Any) -> None:
        """
        Sets a state value.

        :param section: Name of section in state file.
        :param name: Name of state variable.
        :param value: State value. May be any type accepted by :obj:`ast.literal_eval`.
        """
        self._state.set(section, name, value)

    def get_state(self, section: str, name: str) -> Any:
        """
        Gets a state value.

        :param section: Name of section in state file.
        :param name: Name of state variable.
        :returns: State value. May be any type accepted by :obj:`ast.literal_eval`.
        """
        return self._state.get(section, name)

    # ==== Getters / setters for config with side effects ==============================

    @property
    def dropbox_path(self) -> str:
        """
        Returns the path to the local Dropbox folder (read only). This will be an empty
        string if not Dropbox folder has been set up yet. Use
        :meth:`create_dropbox_directory` or :meth:`move_dropbox_directory` to set or
        change the Dropbox directory location instead.

        :raises NotLinkedError: if no Dropbox account is linked.
        """
        if self.pending_link:
            return ""
        else:
            return self.sync.dropbox_path

    @property
    def selective_sync_mode(self) -> str:
        """Selective-sync mode, either ``exclude`` or ``include`` (read only)."""
        return self.sync.selective_sync_mode

    @property
    def selective_sync_paths(self) -> set[str]:
        """Paths selected by the current selective-sync mode (read only)."""
        return self.sync.selective_sync_paths

    @property
    def ignore_symlinks(self) -> bool:
        """Whether local symbolic links remain unmanaged."""
        return self.sync.ignore_symlinks

    @ignore_symlinks.setter
    def ignore_symlinks(self, ignore: bool) -> None:
        """Set whether local symbolic links remain unmanaged."""
        if self.sync.sync_lock.acquire(blocking=False):
            try:
                self.sync.ignore_symlinks = ignore
            finally:
                self.sync.sync_lock.release()
        else:
            raise BusyError(
                "Cannot change symlink policy", "Please try again when idle."
            )

    @property
    def log_level(self) -> int:
        """Log level for log files, stderr and the systemd journal."""
        return self._conf.get("app", "log_level")

    @log_level.setter
    def log_level(self, level: int) -> None:
        """Setter: log_level."""
        self._root_logger.setLevel(min(level, logging.INFO))
        for handler in self._external_log_handlers:
            handler.setLevel(level)
        self._conf.set("app", "log_level", level)

    @property
    def notification_snooze(self) -> float:
        """Remaining notification snooze time in minutes."""
        return max(0.0, (self._notification_snooze_until - time.time()) / 60.0)

    @notification_snooze.setter
    def notification_snooze(self, minutes: float) -> None:
        """Setter: notification_snooze."""
        self._notification_snooze_until = time.time() + max(0.0, minutes) * 60.0

    @property
    def notification_level(self) -> int:
        """Notification level used by the desktop app."""
        return self._conf.get("app", "notification_level")

    @notification_level.setter
    def notification_level(self, level: int) -> None:
        """Setter: notification_level."""
        self._conf.set("app", "notification_level", level)

    @property
    def bandwidth_limit_down(self) -> float:
        """Maximum download bandwidth to use in bytes per second."""
        return self._conf.get("app", "bandwidth_limit_down")

    @bandwidth_limit_down.setter
    def bandwidth_limit_down(self, value: float) -> None:
        """Setter: bandwidth_limit_down."""
        self.client.bandwidth_limit_down = value
        self._conf.set("app", "bandwidth_limit_down", value)

    @property
    def bandwidth_limit_up(self) -> float:
        """Maximum download bandwidth to use in bytes per second."""
        return self._conf.get("app", "bandwidth_limit_up")

    @bandwidth_limit_up.setter
    def bandwidth_limit_up(self, value: float) -> None:
        """Setter: bandwidth_limit_up."""
        self.client.bandwidth_limit_up = value
        self._conf.set("app", "bandwidth_limit_up", value)

    # ==== State information  ==========================================================

    def status_change_longpoll(self, timeout: float | None = 60) -> bool:
        """
        Blocks until there is a change in status or until a timeout occurs.

        This method can be used by frontends to wait for status changes without constant
        polling. Status changes are for example transitions from syncing to idle or
        vice-versa, new errors, or connection status changes.

        Will unblock at most once per second.

        :param timeout: Maximum time to block before returning, even if there is no
            status change.
        :returns: Whether there was a status change within the timeout.

        .. versionadded:: 1.3.0
        """
        return self._log_handler_status_longpoll.wait_for_emit(timeout)

    def get_app_snapshot(self) -> dict[str, Any]:
        """Return the state needed to render a frontend without repeated RPC calls."""
        account = {
            "email": self.get_state("account", "email"),
            "display_name": self.get_state("account", "display_name"),
            "type": self.get_state("account", "type"),
        }
        space_usage = {
            "used": self.get_state("account", "usage_used"),
            "allocated": self.get_state("account", "usage_allocated"),
        }
        active_events = sorted(
            self.sync.activity.get_events(),
            key=lambda event: (
                event.dbx_path_lower,
                event.direction.value,
                event.change_type.value,
                event.sync_time,
            ),
        )
        recent_events = list(reversed(self.sync.get_history()[-100:]))
        activity: list[SyncEvent] = []
        seen_events: set[tuple[Any, ...]] = set()

        for event in active_events + recent_events:
            event_key: tuple[Any, ...]
            if event.id is not None:
                event_key = ("id", event.id)
            else:
                event_key = (
                    "event",
                    event.direction,
                    event.dbx_path_lower,
                    event.change_type,
                    event.sync_time,
                )
            if event_key not in seen_events:
                seen_events.add(event_key)
                activity.append(event)

        return {
            "config_name": self.config_name,
            "version": self.version,
            "status": self.status,
            "pending_link": self.pending_link,
            "pending_dropbox_folder": self.pending_dropbox_folder,
            "pending_first_download": self.pending_first_download,
            "paused": self.paused,
            "running": self.running,
            "connected": self.connected,
            "sync_errors": self.sync_errors,
            "fatal_errors": self.fatal_errors,
            "activity": activity,
            "account": account,
            "space_usage": space_usage,
            "selective_sync_mode": self.selective_sync_mode,
            "selective_sync_paths": self.selective_sync_paths,
            "ignore_symlinks": self.ignore_symlinks,
            "dropbox_path": self.dropbox_path,
            "notification_snooze": self.notification_snooze,
            "notification_level": self.notification_level,
            "bandwidth_limit_down": self.bandwidth_limit_down,
            "bandwidth_limit_up": self.bandwidth_limit_up,
        }

    def _publish_sync_events(self, sync_events: Sequence[SyncEvent]) -> None:
        events = tuple(
            event for event in sync_events if event.status is not SyncStatus.Skipped
        )
        if not events:
            return

        with self._sync_event_condition:
            self._sync_event_cursor += 1
            self._sync_event_batches.append((self._sync_event_cursor, events))
            self._sync_event_condition.notify_all()

    def wait_for_sync_events(
        self, cursor: int | None = None, timeout: float | None = 60
    ) -> dict[str, Any]:
        """Wait for completed sync events after ``cursor``.

        A ``None`` cursor starts at the current position and waits only for new events.
        """
        if cursor is not None and (
            not isinstance(cursor, int) or isinstance(cursor, bool) or cursor < 0
        ):
            raise ValueError("cursor must be a non-negative integer")
        if timeout is not None and (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or timeout < 0
        ):
            raise ValueError("timeout must be non-negative or None")

        with self._sync_event_condition:
            if cursor is None:
                cursor = self._sync_event_cursor

            if cursor == self._sync_event_cursor and not self._sync_event_stream_closed:
                self._sync_event_condition.wait_for(
                    lambda: cursor != self._sync_event_cursor
                    or self._sync_event_stream_closed,
                    timeout,
                )

            current_cursor = self._sync_event_cursor
            if cursor > current_cursor:
                events: list[SyncEvent] = []
            else:
                events = [
                    event
                    for batch_cursor, batch in self._sync_event_batches
                    if batch_cursor > cursor
                    for event in batch
                ]

            return {"cursor": current_cursor, "events": events}

    @property
    def pending_link(self) -> bool:
        """Whether Maestral is linked to a Dropbox account (read only). This will block
        until the user's keyring is unlocked to load the saved auth token."""
        return not self.client.linked

    @property
    def pending_dropbox_folder(self) -> bool:
        """Whether a local Dropbox directory has been configured (read only). This will
        not check if the configured directory actually exists, starting the sync may
        still raise a :exc:`maestral.exceptions.NoDropboxDirError`."""
        return not self.sync.dropbox_path

    @property
    def pending_first_download(self) -> bool:
        """Whether the initial download has already started (read only)."""
        return self.sync.local_cursor == 0 or self.sync.remote_cursor == ""

    @property
    def paused(self) -> bool:
        """Whether syncing is paused by the user (read only). Use :meth:`start_sync` and
        :meth:`stop_sync` to start and stop syncing, respectively."""
        return not self.manager.autostart.is_set() and not self.sync.busy()

    @property
    def running(self) -> bool:
        """Whether sync threads are running (read only). This is similar to
        :attr:`paused` but also returns False if syncing is paused because we cannot
        connect to Dropbox servers.."""
        return self.manager.running.is_set() or self.sync.busy()

    @property
    def connected(self) -> bool:
        """Whether Dropbox servers can be reached (read only)."""
        if self.pending_link:
            return False
        else:
            return self.manager.connected

    @property
    def status(self) -> str:
        """The last status message (read only). This can be displayed as information to
        the user but should not be relied on otherwise."""
        if self.paused:
            return PAUSED
        elif not self.connected:
            return CONNECTING
        else:
            return self._log_handler_info_cache.get_last_message()

    @property
    def sync_errors(self) -> list[SyncErrorEntry]:
        """
        A list of current sync errors as dicts (read only). This list is populated by
        the sync threads. The following keys will always be present but may contain
        empty values: "type", "inherits", "title", "traceback", "title", "message",
        "local_path", "dbx_path".

        :raises NotLinkedError: if no Dropbox account is linked.
        """
        return self.sync.sync_errors

    @property
    def fatal_errors(self) -> list[MaestralApiError]:
        """
        Returns a list of fatal errors as dicts (read only). This does not include lost
        internet connections or file sync errors which only emit warnings and are
        tracked and cleared separately. Errors listed here must be acted upon for
        Maestral to continue syncing.

        The following keys will always be present but may contain empty values: "type",
        "inherits", "title", "traceback", "title", and "message".s

        This list is populated from all log messages with level ERROR or higher that
        have ``exc_info`` attached.
        """
        errors: list[MaestralApiError] = []

        for r in self._log_handler_error_cache.cached_records:
            if r.exc_info:
                err = r.exc_info[1]
                if isinstance(err, MaestralApiError):
                    errors.append(err)

        return errors

    def clear_fatal_errors(self) -> None:
        """
        Manually clears all fatal errors. This should be used after they have been
        resolved by the user through the GUI or CLI.
        """
        self._log_handler_error_cache.clear()

    @property
    def account_profile_pic_path(self) -> str:
        """
        The path of the current account's profile picture (read only). There may not be
        an actual file at that path if the user did not set a profile picture or the
        picture has not yet been downloaded.
        """
        return get_cache_path("maestral", f"{self._config_name}_profile_pic.jpeg")

    def get_file_status(self, local_path: str) -> str:
        """
        Returns the sync status of a file or folder. The returned status is recursive
        for folders.

        * "uploading" if any file inside the folder is being uploaded.
        * "downloading" if any file inside the folder is being downloaded.
        * "error" if any item inside the folder failed to sync and none are currently
          being uploaded or downloaded.
        * "up to date" if all items are successfully synced.
        * "unwatched" if syncing is paused or for items outside the Dropbox directory.

        .. versionadded:: 1.4.4
           Recursive behavior. Previous versions would return "up to date" for a folder,
           even if some contained files would be syncing.

        :param local_path: Path to file on the local drive. May be relative to the
            current working directory.
        :returns: String indicating the sync status. Can be 'uploading', 'downloading',
            'up to date', 'error', or 'unwatched'.
        """
        if not self.running:
            return FileStatus.Unwatched.value

        local_path = osp.realpath(local_path)

        try:
            dbx_path_cased = self.sync.to_dbx_path(local_path)
        except ValueError:
            return FileStatus.Unwatched.value

        # Find any sync activity for the local path.
        sync_events = self.sync.activity.get_events(dbx_path_cased)

        if not sync_events:
            # Always return synced for the root folder in the absense of sync activity.
            if dbx_path_cased == "/":
                return FileStatus.Synced.value

            # Check if the path is in our index. If yes, it is fully synced, otherwise
            # it is unwatched.
            if self.sync.get_index_entry_for_local_path(local_path):
                return FileStatus.Synced.value

            return FileStatus.Unwatched.value

        # Return effective status of item and its children. Syncing items take
        # precedence over Failed which take precedence over Synced. Note that Up and
        # Down are mutually exclusive because they are performed in alternating cycles.
        file_status = FileStatus.Synced

        for event in sync_events:
            if event.status is SyncStatus.Syncing:
                if event.direction is SyncDirection.Up:
                    return FileStatus.Uploading.value
                elif event.direction is SyncDirection.Down:
                    return FileStatus.Downloading.value
            elif event.status is SyncStatus.Failed:
                file_status = FileStatus.Error

        return file_status.value

    def get_activity(self, limit: int | None = 100) -> list[SyncEvent]:
        """
        Returns the current upload / download activity.

        :param limit: Maximum number of items to return. If None, all entries will be
            returned.
        :returns: A lists of all sync events currently queued for or being uploaded or
            downloaded with the events the furthest up in the queue coming first.
        :raises NotLinkedError: if no Dropbox account is linked.
        """
        self._check_linked()
        return list(self.sync.activity.get_events())[:limit]

    def get_history(
        self, dbx_path: str | None = None, limit: int | None = 100
    ) -> list[SyncEvent]:
        """
        Returns the historic upload / download activity. Up to 1,000 sync events are
        kept in the database. Any events which occurred before the interval specified by
        the ``keep_history`` config value are discarded.

        :param dbx_path: If given, show sync history for the specified Dropbox path only.
        :param limit: Maximum number of items to return. If None, all entries will be
            returned.
        :returns: A lists of all sync events since ``keep_history`` sorted by time with
            the oldest event first.
        :raises NotLinkedError: if no Dropbox account is linked.
        """
        self._check_linked()
        events = self.sync.get_history(dbx_path=dbx_path)
        return events[-limit:] if limit else events

    def get_account_info(self) -> FullAccount:
        """
        Returns the account information from Dropbox and returns it as a dictionary.

        :returns: Dropbox account information.
        :raises DropboxAuthError: in case of an invalid access token.
        :raises DropboxServerError: for internal Dropbox errors.
        :raises ConnectionError: if the connection to Dropbox fails.
        :raises NotLinkedError: if no Dropbox account is linked.
        """
        self._check_linked()
        return self.client.get_account_info()

    def get_space_usage(self) -> PersonalSpaceUsage:
        """
        Gets the space usage from Dropbox and returns it as a dictionary.

        :returns: Dropbox space usage information.
        :raises DropboxAuthError: in case of an invalid access token.
        :raises DropboxServerError: for internal Dropbox errors.
        :raises ConnectionError: if the connection to Dropbox fails.
        :raises NotLinkedError: if no Dropbox account is linked.
        """
        self._check_linked()
        return self.client.get_space_usage()

    # ==== Control methods for front ends ==============================================

    def get_profile_pic(self) -> str | None:
        """
        Attempts to download the user's profile picture from Dropbox. The picture is
        saved in Maestral's cache directory for retrieval when there is no internet
        connection. Check :attr:`account_profile_pic_path` for cached profile pics.

        :returns: Path to saved profile picture or ``None`` if no profile picture was
            downloaded.
        :raises DropboxAuthError: in case of an invalid access token.
        :raises DropboxServerError: for internal Dropbox errors.
        :raises ConnectionError: if the connection to Dropbox fails.
        :raises NotLinkedError: if no Dropbox account is linked.
        """
        self._check_linked()

        account_info = self.client.get_account_info()

        if account_info.profile_photo_url:
            with convert_api_errors():
                res = requests.get(account_info.profile_photo_url)
                cache = self._profile_pic_cache_dir()
                if cache is None:
                    raise SymlinkError(
                        "Cannot cache profile picture",
                        "The profile picture cache is not a safe directory.",
                        local_path=get_cache_path("maestral"),
                    )
                cache_path, cache_stat = cache
                profile_path = osp.join(
                    cache_path,
                    f"{self._config_name}_profile_pic.jpeg",
                )
                cache_identity = (
                    cache_stat.st_dev,
                    cache_stat.st_ino,
                    cache_stat.st_mode,
                )
                temporary_file = create_rooted_tempfile(
                    cache_path,
                    cache_path,
                    expected_root_identity=cache_identity,
                    prefix=f".{self._config_name}_profile_pic-",
                )
                try:
                    with temporary_file.open("wb") as file:
                        file.write(res.content)
                        file.flush()
                        os.fsync(file.fileno())
                    temporary_file.close()

                    try:
                        target_snapshot = rooted_tree_snapshot(
                            profile_path,
                            cache_path,
                            expected_root_identity=cache_identity,
                        )
                    except (FileNotFoundError, NotADirectoryError):
                        target_snapshot = {}
                    target_identity = target_snapshot.get(profile_path)
                    if target_identity is not None:
                        try:
                            rooted_unlink(
                                profile_path,
                                root_path=cache_path,
                                expected_root_identity=cache_identity,
                                expected_target_identity=target_identity[:6],
                            )
                        except FileNotFoundError:
                            pass

                    move(
                        temporary_file.path,
                        profile_path,
                        replace=False,
                        raise_error=True,
                        root_path=cache_path,
                        expected_root_identity=cache_identity,
                        expected_source_identity=temporary_file.identity,
                    )
                finally:
                    temporary_file.close()
                    try:
                        rooted_unlink(
                            temporary_file.path,
                            root_path=cache_path,
                            expected_root_identity=cache_identity,
                            expected_target_identity=temporary_file.identity,
                        )
                    except (FileNotFoundError, NotADirectoryError):
                        pass
            return profile_path
        else:
            self._delete_old_profile_pics()
            return None

    def get_metadata(self, dbx_path: str) -> Metadata | None:
        """
        Returns metadata for a file or folder on Dropbox.

        :param dbx_path: Path to file or folder on Dropbox.
        :returns: Dropbox item metadata as dict. See :class:`dropbox.files.Metadata` for
            keys and values.
        :raises NotFoundError: if there is nothing at the given path.
        :raises DropboxAuthError: in case of an invalid access token.
        :raises DropboxServerError: for internal Dropbox errors.
        :raises ConnectionError: if the connection to Dropbox fails.
        :raises NotLinkedError: if no Dropbox account is linked.
        """
        self._check_linked()
        return self.client.get_metadata(dbx_path)

    def list_folder(
        self,
        dbx_path: str,
        recursive: bool = False,
        include_deleted: bool = False,
        include_mounted_folders: bool = True,
        include_non_downloadable_files: bool = False,
    ) -> list[Metadata]:
        """
        List all items inside the folder given by ``dbx_path``. Keyword arguments are
        passed on the Dropbox API call :meth:`client.DropboxClient.list_folder`.

        :param dbx_path: Path to folder on Dropbox.
        :param recursive: If true, the list folder operation will be applied recursively
            to all subfolders and the response will contain contents of all subfolders.
        :param include_deleted: If true, the results will include entries for files and
            folders that used to exist but were deleted.
        :param bool include_mounted_folders: If true, the results will include
            entries under mounted folders which includes app folder, shared
            folder and team folder.
        :param bool include_non_downloadable_files: If true, include files that
            are not downloadable, i.e. Google Docs.
        :raises NotFoundError: if there is nothing at the given path.
        :raises NotAFolderError: if the given path refers to a file.
        :raises DropboxAuthError: in case of an invalid access token.
        :raises DropboxServerError: for internal Dropbox errors.
        :raises ConnectionError: if the connection to Dropbox fails.
        :raises NotLinkedError: if no Dropbox account is linked.
        """
        self._check_linked()

        res = self.client.list_folder(
            dbx_path,
            recursive=recursive,
            include_deleted=include_deleted,
            include_mounted_folders=include_mounted_folders,
            include_non_downloadable_files=include_non_downloadable_files,
        )
        return res.entries

    def list_folder_iterator(
        self,
        dbx_path: str,
        recursive: bool = False,
        include_deleted: bool = False,
        include_mounted_folders: bool = True,
        limit: int | None = None,
        include_non_downloadable_files: bool = False,
    ) -> Iterator[list[Metadata]]:
        """
        Returns an iterator over items inside the folder given by ``dbx_path``. Keyword
        arguments are passed on the client call
        :meth:`client.DropboxClient.list_folder_iterator`. Each iteration will yield a
        list of approximately 500 entries, depending on the number of entries returned
        by an individual API call.

        :param dbx_path: Path to folder on Dropbox.
        :param recursive: If true, the list folder operation will be applied recursively
            to all subfolders and the response will contain contents of all subfolders.
        :param include_deleted: If true, the results will include entries for files and
            folders that used to exist but were deleted.
        :param bool include_mounted_folders: If true, the results will include
            entries under mounted folders which includes app folder, shared
            folder and team folder.
        :param Nullable[int] limit: The maximum number of results to return per
            request. Note: This is an approximate number and there can be
            slightly more entries returned in some cases.
        :param bool include_non_downloadable_files: If true, include files that
            are not downloadable, i.e. Google Docs.
        :returns: Iterator over list of Dropbox item metadata as dicts. See
            :class:`dropbox.files.Metadata` for keys and values.
        :raises NotFoundError: if there is nothing at the given path.
        :raises NotAFolderError: if the given path refers to a file.
        :raises DropboxAuthError: in case of an invalid access token.
        :raises DropboxServerError: for internal Dropbox errors.
        :raises ConnectionError: if the connection to Dropbox fails.
        :raises NotLinkedError: if no Dropbox account is linked.
        """
        self._check_linked()

        res_iter = self.client.list_folder_iterator(
            dbx_path,
            recursive=recursive,
            include_deleted=include_deleted,
            include_mounted_folders=include_mounted_folders,
            limit=limit,
            include_non_downloadable_files=include_non_downloadable_files,
        )

        for res in res_iter:
            yield res.entries
            del res
            gc.collect()

    def list_revisions(self, dbx_path: str, limit: int = 10) -> list[FileMetadata]:
        """
        List revisions of old files at the given path ``dbx_path``. This will also
        return revisions if the file has already been deleted.

        :param dbx_path: Path to file on Dropbox.
        :param limit: Maximum number of revisions to list.
        :returns: List of Dropbox file metadata as dicts. See
            :class:`dropbox.files.Metadata` for keys and values.
        :raises NotFoundError: if there never was a file at the given path.
        :raises IsAFolderError: if the given path refers to a folder
        :raises DropboxAuthError: in case of an invalid access token.
        :raises DropboxServerError: for internal Dropbox errors.
        :raises ConnectionError: if the connection to Dropbox fails.
        """
        self._check_linked()
        return self.client.list_revisions(dbx_path, limit=limit)

    def get_file_diff(self, old_rev: str, new_rev: str | None = None) -> list[str]:
        """
        Compare to revisions of a text file using Python's difflib. The versions will be
        downloaded to temporary files. If new_rev is None, the old revision will be
        compared to the corresponding local file, if any.

        :param old_rev: Identifier of old revision.
        :param new_rev: Identifier of new revision.
        :returns: Diff as a list of strings (lines).
        :raises UnsupportedFileTypeForDiff: if file type is not supported.
        :raises UnsupportedFileTypeForDiff: if file content could not be decoded.
        :raises MaestralApiError: if file could not be read for any other reason.
        """

        def str_from_date(d: datetime) -> str:
            """Convert 'client_modified' metadata to string in local timezone"""
            tz_date = d.replace(tzinfo=timezone.utc).astimezone()
            return tz_date.strftime("%d %b %Y at %H:%M")

        def download_rev(rev: str) -> tuple[list[str], FileMetadata]:
            """
            Download a rev to a tmp file, read it and return the content + metadata.
            """
            with tempfile.NamedTemporaryFile(mode="w+") as f:
                md = self.client.download(f"rev:{rev}", f.name)

                # Read from the file.
                try:
                    with convert_api_errors(md.path_display, f.name):
                        content = f.readlines()
                except UnicodeDecodeError:
                    raise UnsupportedFileTypeForDiff(
                        "Failed to decode the file",
                        "Only UTF-8 plain text files are currently supported.",
                    )

            return content, md

        # Get the metadata for old_rev before attempting to download. This is used
        # to guess the file type and fail early for unsupported files.

        md_old = self.client.get_metadata(f"rev:{old_rev}", include_deleted=True)

        if md_old is None:
            raise NotFoundError(
                f"Could not a file with revision {old_rev}",
                "Use 'list_revisions' to list past revisions of a file.",
            )

        dbx_path = self.sync.correct_case(md_old.path_display)
        local_path = self.sync.to_local_path(md_old.path_display)

        # Check if a diff is possible.
        # If mime is None, proceed because most files without
        # an extension are just text files.
        mime, _ = mimetypes.guess_type(dbx_path)
        if mime is not None and not mime.startswith("text/"):
            raise UnsupportedFileTypeForDiff(
                f"Bad file type: '{mime}'", "Only files of type 'text/*' are supported."
            )

        if new_rev:
            content_new, md_new = download_rev(new_rev)
            date_str_new = str_from_date(md_new.client_modified)
        else:
            # Use the local file if new_rev is None.
            new_rev = "local version"
            try:
                with convert_api_errors(dbx_path=dbx_path, local_path=local_path):
                    mtime = time.localtime(osp.getmtime(local_path))
                    date_str_new = time.strftime("%d %b %Y at %H:%M", mtime)

                    with open(local_path) as f:
                        content_new = f.readlines()

            except UnicodeDecodeError:
                raise UnsupportedFileTypeForDiff(
                    "Failed to decode the file",
                    "Only UTF-8 plain text files are currently supported.",
                )

        content_old, md_old = download_rev(old_rev)
        date_str_old = str_from_date(md_old.client_modified)

        return list(
            difflib.unified_diff(
                content_old,
                content_new,
                fromfile=f"{dbx_path} ({old_rev})",
                tofile=f"{dbx_path} ({new_rev})",
                fromfiledate=date_str_old,
                tofiledate=date_str_new,
            )
        )

    def restore(self, dbx_path: str, rev: str) -> FileMetadata:
        """
        Restore an old revision of a file.

        :param dbx_path: The path to save the restored file.
        :param rev: The revision to restore. Old revisions can be listed with
            :meth:`list_revisions`.
        :returns: Metadata of the returned file. See :class:`dropbox.files.FileMetadata`
            for keys and values.
        :raises DropboxAuthError: in case of an invalid access token.
        :raises DropboxServerError: for internal Dropbox errors.
        :raises ConnectionError: if the connection to Dropbox fails.
        """
        self._check_linked()
        self._logger.info(f"Restoring '{dbx_path} to {rev}'")
        res = self.client.restore(dbx_path, rev)
        self._logger.info(f"Restored '{dbx_path} to {rev}'")
        self._logger.info(IDLE)
        return res

    def _profile_pic_cache_dir(self) -> tuple[str, os.stat_result] | None:
        cache_path = get_cache_path("maestral")
        try:
            cache_stat = os.lstat(cache_path)
        except (FileNotFoundError, NotADirectoryError):
            return None
        if is_fs_link(cache_stat) or not S_ISDIR(cache_stat.st_mode):
            return None
        return cache_path, cache_stat

    def _delete_old_profile_pics(self) -> None:
        cache = self._profile_pic_cache_dir()
        if cache is None:
            return
        cache_path, cache_stat = cache
        cache_identity = (
            cache_stat.st_dev,
            cache_stat.st_ino,
            cache_stat.st_mode,
        )

        for entry in os.scandir(cache_path):
            if entry.name.startswith(f"{self._config_name}_profile_pic"):
                try:
                    entry_stat = entry.stat(follow_symlinks=False)
                    rooted_unlink(
                        entry.path,
                        root_path=cache_path,
                        expected_root_identity=cache_identity,
                        expected_target_identity=(
                            entry_stat.st_dev,
                            entry_stat.st_ino,
                            entry_stat.st_mode,
                        ),
                    )
                except OSError:
                    pass

    def rebuild_index(self) -> None:
        """
        Rebuilds the rev file by comparing remote with local files and updating rev
        numbers from the Dropbox server. Files are compared by their content hashes and
        conflicting copies are created if the contents differ. File changes during the
        rebuild process will be queued and uploaded once rebuilding has completed.

        Rebuilding will be performed asynchronously and errors can be accessed through
        :attr:`sync_errors` or :attr:`maestral_errors`.

        :raises NotLinkedError: if no Dropbox account is linked.
        :raises NoDropboxDirError: if local Dropbox folder is not set up.
        """
        self._check_linked()
        self._check_dropbox_dir()

        self.manager.rebuild_index()

    def start_sync(self) -> None:
        """
        Creates syncing threads and starts syncing.

        :raises NotLinkedError: if no Dropbox account is linked.
        :raises NoDropboxDirError: if local Dropbox folder is not set up.
        """
        self._check_linked()
        self._check_dropbox_dir()

        self.manager.start()

    def stop_sync(self) -> None:
        """
        Stops all syncing threads if running. Call :meth:`start_sync` to restart
        syncing.
        """
        self.manager.stop()

    def reset_sync_state(self) -> None:
        """
        Resets the sync index and state. Only call this to clean up leftover state
        information if a Dropbox was improperly unlinked (e.g., auth token has been
        manually deleted). Otherwise, leave state management to Maestral.

        :raises NotLinkedError: if no Dropbox account is linked.
        """
        self._check_linked()
        self.manager.reset_sync_state()

    def set_selective_sync(self, mode: str, dbx_paths: Collection[str]) -> None:
        """
        Atomically replace the selective-sync mode and selected paths.

        ``exclude`` syncs everything except the selected paths. ``include`` syncs only
        the selected paths and retains the parent folders needed to reach them.

        :param mode: Either ``exclude`` or ``include``.
        :param dbx_paths: Dropbox paths selected by the mode.
        :raises NotLinkedError: if no Dropbox account is linked.
        :raises NoDropboxDirError: if the local Dropbox folder is not set up.
        """
        self._check_linked()
        self._check_dropbox_dir()

        selected_paths = self.sync.clean_selective_sync_paths(
            dbx_paths,
            validate_local=mode != "exclude",
        )

        if not self.sync.sync_lock.acquire(blocking=False):
            raise BusyError(
                "Cannot change selective sync", "Please try again when idle."
            )

        try:
            self._check_linked()
            self.sync._ensure_unlink_not_pending()
            old_mode = self.selective_sync_mode
            old_paths = self.selective_sync_paths

            if mode == old_mode and selected_paths == old_paths:
                return

            indexed_entries = list(self.sync.iter_index())
            newly_excluded_entries = [
                entry
                for entry in indexed_entries
                if not self.sync._is_path_excluded(
                    entry.dbx_path_lower, old_mode, old_paths
                )
                and self.sync._is_path_excluded(
                    entry.dbx_path_lower, mode, selected_paths
                )
            ]
            newly_excluded_paths = {
                entry.dbx_path_lower for entry in newly_excluded_entries
            }
            paths_to_unindex = self.sync.clean_selective_sync_paths(
                newly_excluded_paths,
                validate_local=False,
            )
            locally_mappable_paths: set[str] = set()
            for entry in newly_excluded_entries:
                try:
                    self.sync.to_local_path_from_cased(entry.dbx_path_cased)
                except ValueError:
                    continue
                locally_mappable_paths.add(entry.dbx_path_lower)
            paths_to_remove = self.sync.clean_selective_sync_paths(
                {
                    path
                    for path in newly_excluded_paths
                    if path != "/"
                    and path in locally_mappable_paths
                    and self.sync._is_path_managed(path, old_mode, old_paths)
                }
            )
            cased_paths_to_remove = {
                entry.dbx_path_lower: entry.dbx_path_cased
                for entry in newly_excluded_entries
                if entry.dbx_path_lower in paths_to_remove
            }

            paths_to_download = selected_paths if mode == "include" else {"/"}
            recovery_paths = paths_to_download | paths_to_unindex
            for dbx_path_lower in recovery_paths:
                if mode == "include" and dbx_path_lower in paths_to_download:
                    self.sync.queue_targeted_download(dbx_path_lower, "include")
                else:
                    self.manager.download_queue.put(dbx_path_lower)

            for dbx_path_lower, dbx_path_cased in cased_paths_to_remove.items():
                self._remove_local_after_selective_sync(dbx_path_cased)
                self.sync.clear_sync_errors_for_path(
                    dbx_path_lower,
                    recursive=True,
                )
                self._logger.info("Excluded %s", dbx_path_lower)

            for dbx_path_lower in paths_to_unindex - cased_paths_to_remove.keys():
                self.sync.remove_node_from_index(dbx_path_lower)
                self.sync.clear_sync_errors_for_path(dbx_path_lower, recursive=True)

            self.sync.set_selective_sync(mode, selected_paths)

            self._logger.info(IDLE)
        finally:
            self.sync.sync_lock.release()

    def _remove_local_after_selective_sync(self, dbx_path_cased: str) -> None:
        self.sync.remove_local_after_selective_sync(dbx_path_cased)

    def selective_sync_status(self, dbx_path: str) -> str:
        """
        Return ``included``, ``partially included``, or ``excluded``.

        :param dbx_path: Path to item on Dropbox.
        :returns: Selective-sync status.
        :raises NotLinkedError: if no Dropbox account is linked.
        """
        self._check_linked()
        return self.sync.selective_sync_status(dbx_path)

    def _validated_dropbox_root_move(self) -> dict[str, Any]:
        """Return the validated durable Dropbox-root move journal."""
        journal = self._state.get("recovery", "root_move")
        if journal == {}:
            return {}
        if not isinstance(journal, dict):
            raise MaestralApiError(
                "Cannot recover Dropbox folder move",
                "The saved root-move journal is invalid.",
            )
        old_path = journal.get("old_path")
        new_path = journal.get("new_path")
        identity = journal.get("identity")
        phase = journal.get("phase")
        if (
            not isinstance(old_path, str)
            or not osp.isabs(old_path)
            or osp.normpath(old_path) != old_path
            or not isinstance(new_path, str)
            or not osp.isabs(new_path)
            or osp.normpath(new_path) != new_path
            or old_path == new_path
            or not isinstance(identity, list)
            or len(identity) != 3
            or any(not isinstance(value, int) for value in identity)
            or phase not in {"planned", "moved"}
        ):
            raise MaestralApiError(
                "Cannot recover Dropbox folder move",
                "The saved root-move journal has an unsafe entry.",
            )
        return {
            "old_path": old_path,
            "new_path": new_path,
            "identity": identity,
            "phase": phase,
        }

    def _confirmed_root_identity_at(
        self,
        path: str,
    ) -> tuple[int, int, int] | None:
        """Return a marker-validated identity for a possible Dropbox root."""
        try:
            root_stat = os.lstat(path)
        except (FileNotFoundError, NotADirectoryError):
            return None
        if not S_ISDIR(root_stat.st_mode) or is_fs_link(root_stat):
            return None
        identity = (root_stat.st_dev, root_stat.st_ino, root_stat.st_mode)
        marker_id = self._conf.get("sync", "root_marker_id")
        if not (
            isinstance(marker_id, str)
            and len(marker_id) == 32
            and all(char in "0123456789abcdef" for char in marker_id)
        ):
            return None
        marker_content = f"maestral-root-v1:{marker_id}\n".encode("ascii")
        if not SyncEngine._root_marker_matches(
            osp.join(path, ROOT_MARKER_FILE),
            marker_content,
            root_path=path,
            expected_root_identity=identity,
        ):
            return None
        return identity

    def _recover_pending_dropbox_root_move(self) -> None:
        """Finish a root move which completed before its config save."""
        journal = self._validated_dropbox_root_move()
        if not journal:
            return
        expected_identity = tuple(journal["identity"])
        old_identity = self._confirmed_root_identity_at(journal["old_path"])
        new_identity = self._confirmed_root_identity_at(journal["new_path"])
        old_matches = old_identity == expected_identity
        new_matches = new_identity == expected_identity

        if old_matches and new_identity is None and journal["phase"] == "planned":
            self._state.set("recovery", "root_move", {})
            return
        if new_matches and old_identity is None:
            journal["phase"] = "moved"
            self._state.set("recovery", "root_move", journal)
            sync = getattr(self, "sync", None)
            if sync is None:
                self._conf.set("sync", "path", journal["new_path"])
            else:
                sync.dropbox_path = journal["new_path"]
                sync.ensure_dropbox_folder_present()
            self._state.set("recovery", "root_move", {})
            return
        raise MaestralApiError(
            "Cannot recover Dropbox folder move",
            "The old and new Dropbox roots do not match the move journal.",
        )

    def move_dropbox_directory(self, new_path: str) -> None:
        """
        Sets the local Dropbox directory. This moves all local files to the new location
        and resumes syncing afterwards.

        :param new_path: Full path to local Dropbox folder. "~" will be expanded to the
            user's home directory.
        :raises OSError: if moving the directory fails.
        :raises NotLinkedError: if no Dropbox account is linked.
        :raises NoDropboxDirError: if local Dropbox folder is not set up.
        """
        self._check_linked()
        self._check_dropbox_dir()
        if self._state.get("recovery", "sync_reset"):
            raise MaestralApiError(
                "Cannot move Dropbox folder",
                "Finish the pending sync reset first.",
            )
        if self._state.get("account", "path_root_migration"):
            raise MaestralApiError(
                "Cannot move Dropbox folder",
                "Finish the pending Dropbox path-root migration first.",
            )

        self._logger.info("Moving Dropbox folder...")

        old_path = self.sync.dropbox_path
        new_path = osp.realpath(osp.expanduser(new_path))

        try:
            if osp.samefile(old_path, new_path):
                self._logger.info(f'Dropbox folder moved to "{new_path}"')
                return
        except FileNotFoundError:
            pass

        try:
            common_root = osp.commonpath((old_path, new_path))
        except ValueError as exc:
            raise OSError(
                errno.EXDEV,
                "The Dropbox root move must stay on one file system.",
            ) from exc
        if common_root in {old_path, new_path}:
            raise ValueError("The old and new Dropbox paths cannot contain each other.")
        if common_root == osp.abspath(osp.sep):
            raise OSError(
                errno.EXDEV,
                "The Dropbox root move must stay below one safe parent.",
            )
        common_stat = os.lstat(common_root)
        if not S_ISDIR(common_stat.st_mode) or is_fs_link(common_stat):
            raise OSError("The common path root is not a safe directory.")
        common_identity = (
            common_stat.st_dev,
            common_stat.st_ino,
            common_stat.st_mode,
        )

        # Pause syncing.
        stop_state = self.manager.pause_for_internal_operation()
        try:
            if self._state.get("recovery", "sync_reset"):
                raise MaestralApiError(
                    "Cannot move Dropbox folder",
                    "Finish the pending sync reset first.",
                )
            if self._state.get("account", "path_root_migration"):
                raise MaestralApiError(
                    "Cannot move Dropbox folder",
                    "Finish the pending Dropbox path-root migration first.",
                )
            self.sync.ensure_dropbox_folder_present()
            root_identity = self.sync.confirmed_root_identity
            if not S_ISDIR(root_identity[2]):
                raise NoDropboxDirError(
                    "Dropbox folder missing",
                    "The configured Dropbox root is not a directory.",
                )

            root_snapshot = rooted_tree_snapshot(
                old_path,
                common_root,
                expected_root_identity=common_identity,
            )
            if root_snapshot[old_path][:3] != root_identity:
                raise OSError(
                    errno.ESTALE,
                    "The Dropbox root changed before the move.",
                    old_path,
                )
            symlink_path = next(
                (
                    self.sync.to_dbx_path_lower(path)
                    for path, identity in root_snapshot.items()
                    if isinstance(identity[6], str)
                    and identity[6].startswith("symlink:")
                ),
                None,
            )
            if symlink_path:
                raise SymlinkError(
                    "Cannot move Dropbox folder",
                    "The Dropbox folder contains the symbolic link "
                    f'"{symlink_path}".',
                    dbx_path=symlink_path,
                    local_path=old_path,
                )

            destination_parent = osp.dirname(new_path)
            relative_parent = osp.relpath(destination_parent, common_root)
            current_parent = common_root
            if relative_parent != osp.curdir:
                for name in relative_parent.split(osp.sep):
                    current_parent = osp.join(current_parent, name)
                    try:
                        rooted_mkdir(
                            current_parent,
                            root_path=common_root,
                            expected_root_identity=common_identity,
                        )
                    except FileExistsError:
                        pass

            if osp.lexists(new_path):
                raise FileExistsError(f'Path "{new_path}" already exists.')
            journal = {
                "old_path": old_path,
                "new_path": new_path,
                "identity": list(root_identity),
                "phase": "planned",
            }
            self.manager.begin_root_move(journal)

            move(
                old_path,
                new_path,
                replace=False,
                raise_error=True,
                root_path=common_root,
                expected_root_identity=common_identity,
                expected_source_identity=root_identity,
            )
            journal["phase"] = "moved"
            self._state.set("recovery", "root_move", journal)

            self.sync.dropbox_path = new_path
            self.sync.ensure_dropbox_folder_present()
            self._state.set("recovery", "root_move", {})
        except BaseException:
            try:
                self._recover_pending_dropbox_root_move()
            except BaseException:
                self.manager._finish_internal_operation(stop_state)
                raise
            self.manager.resume_after_internal_stop(stop_state)
            raise
        else:
            self.manager.resume_after_internal_stop(stop_state)
            self._logger.info(f'Dropbox folder moved to "{new_path}"')

    def create_dropbox_directory(self, path: str) -> None:
        """
        Creates a new Dropbox directory. Only call this during setup.

        :param path: Full path to local Dropbox folder. "~" will be expanded to the
            user's home directory.
        :raises OSError: if creation fails.
        :raises NotLinkedError: if no Dropbox account is linked.
        """
        self._check_linked()
        if self._state.get("recovery", "sync_reset"):
            raise MaestralApiError(
                "Cannot create Dropbox folder",
                "Finish the pending sync reset first.",
            )
        if self._state.get("account", "path_root_migration"):
            raise MaestralApiError(
                "Cannot create Dropbox folder",
                "Finish the pending Dropbox path-root migration first.",
            )

        stop_state = None

        try:
            stop_state = self.manager.pause_for_internal_operation()
            if self._state.get("recovery", "sync_reset"):
                raise MaestralApiError(
                    "Cannot create Dropbox folder",
                    "Finish the pending sync reset first.",
                )
            if self._state.get("account", "path_root_migration"):
                raise MaestralApiError(
                    "Cannot create Dropbox folder",
                    "Finish the pending Dropbox path-root migration first.",
                )

            path = osp.normpath(osp.abspath(osp.expanduser(path)))
            root_identity = rooted_makedirs(path, exist_ok=True)
            self.manager.reset_sync_state()
            self.sync.set_dropbox_path(
                path,
                expected_root_identity=root_identity,
            )
            self.sync.create_root_marker()
        except BaseException:
            replay_error: BaseException | None = None
            try:
                reset_pending = bool(self._state.get("recovery", "sync_reset"))
                if reset_pending:
                    self.sync._complete_pending_sync_reset()
                    self.manager.reload_download_queue()
                reset_pending = bool(self._state.get("recovery", "sync_reset"))
            except BaseException as error:
                replay_error = error
                try:
                    reset_pending = bool(self._state.get("recovery", "sync_reset"))
                except BaseException:
                    if stop_state is not None:
                        self.manager._finish_internal_operation(stop_state)
                    raise

            if stop_state is not None:
                if reset_pending:
                    self.manager._finish_internal_operation(stop_state)
                else:
                    self.manager.resume_after_internal_stop(stop_state)
            if replay_error is not None:
                self._logger.debug(
                    "Could not finish the interrupted sync reset",
                    exc_info=exc_info_tuple(replay_error),
                )
            raise
        else:
            assert stop_state is not None
            self.manager.resume_after_internal_stop(stop_state)

    def confirm_dropbox_directory(self) -> None:
        """Confirms the configured Dropbox folder as the intended sync root.

        This creates Maestral's root marker. Existing installations must call this
        method once before they can resume syncing. Check that the configured drive or
        network mount is available before confirmation.

        :raises NotLinkedError: if no Dropbox account is linked.
        :raises NoDropboxDirError: if no Dropbox folder is configured or it is missing.
        :raises MaestralApiError: if the root marker cannot be created.
        """
        self._check_linked()

        if self.pending_dropbox_folder:
            raise NoDropboxDirError(
                "No local Dropbox directory",
                "Please set up a local Dropbox directory using the GUI or CLI.",
            )

        try:
            self.sync.create_root_marker()
        except OSError as exc:
            raise MaestralApiError(
                "Could not confirm Dropbox folder",
                exc.strerror or str(exc),
            ) from exc

    def create_shared_link(
        self,
        dbx_path: str,
        visibility: LinkAudience = LinkAudience.Public,
        access_level: LinkAccessLevel = LinkAccessLevel.Viewer,
        allow_download: bool | None = None,
        password: str | None = None,
        expires: datetime | None = None,
    ) -> SharedLinkMetadata:
        """
        Creates a shared link for the given ``dbx_path``. Returns a dictionary with
        information regarding the link, including the URL, access permissions, expiry
        time, etc. The shared link will grant read / download access only. Note that
        basic accounts do not support password protection or expiry times.

        :param dbx_path: Dropbox path to file or folder to share.
        :param visibility: The visibility of the shared link. Can be public, team-only,
            or no-one. In case of the latter, the link merely points the user to the
            content and does not grant additional rights to the user. Users of this link
            can only access the content with their pre-existing access rights.
        :param access_level: The level of access granted with the link. Can be viewer,
            editor, or max for maximum possible access level.
        :param allow_download: Whether to allow download capabilities for the link.
        :param password: If given, enables password protection for the link.
        :param expires: Expiry time for shared link. If no timezone is given, assume
            UTC. May not be supported for all account types.
        :returns: Metadata for shared link.
        """
        self._check_linked()
        return self.client.create_shared_link(
            dbx_path=dbx_path,
            visibility=visibility,
            password=password,
            access_level=access_level,
            allow_download=allow_download,
            expires=expires,
        )

    def revoke_shared_link(self, url: str) -> None:
        """
        Revokes the given shared link. Note that any other links to the same file or
        folder will remain valid.

        :param url: URL of shared link to revoke.
        :raises DropboxAuthError: in case of an invalid access token.
        :raises DropboxServerError: for internal Dropbox errors.
        :raises ConnectionError: if the connection to Dropbox fails.
        :raises NotLinkedError: if no Dropbox account is linked.
        """
        self._check_linked()
        self.client.revoke_shared_link(url)

    def list_shared_links(
        self, dbx_path: str | None = None, *, direct_only: bool = False
    ) -> list[SharedLinkMetadata]:
        """
        Returns a list of all shared links for the given Dropbox path. If no path is
        given, return all shared links for the account, up to a maximum of 1,000 links.

        :param dbx_path: Path to item on Dropbox.
        :param direct_only: Only return links which point directly to ``dbx_path``.
        :returns: List of shared link information as dictionaries. See
            :class:`dropbox.sharing.SharedLinkMetadata` for keys and values.
        :raises DropboxAuthError: in case of an invalid access token.
        :raises DropboxServerError: for internal Dropbox errors.
        :raises ConnectionError: if the connection to Dropbox fails.
        :raises NotLinkedError: if no Dropbox account is linked.
        """
        self._check_linked()
        return self.client.list_shared_links(dbx_path, direct_only=direct_only)

    # ==== Utility methods for front ends ==============================================

    def to_local_path(self, dbx_path: str) -> str:
        """
        Converts a path relative to the Dropbox folder to a correctly cased local file
        system path.

        :param dbx_path: Path relative to Dropbox root.
        :returns: Corresponding path on local hard drive.
        :raises NotLinkedError: if no Dropbox account is linked.
        :raises NoDropboxDirError: if local Dropbox folder is not set up.
        """

        self._check_linked()
        self._check_dropbox_dir()

        return self.sync.to_local_path(dbx_path)

    def check_for_updates(self) -> UpdateCheckResult:
        """
        Checks if an update is available.

        :returns: A dictionary with information about the latest release with the fields
            'update_available' (bool), 'latest_release' (str), 'release_notes' (str)
            and 'error' (str or None).
        :raises UpdateCheckError: if checking for an update fails.
        """
        current_version = __version__.lstrip("v")
        update_release_notes = ""

        try:
            resp = requests.get(GITHUB_RELEASES_API)
            resp.raise_for_status()

            data = resp.json()

            releases = []
            release_notes = []

            # Remove? The GitHub API already returns sorted entries.
            data.sort(key=lambda x: Version(x["tag_name"]), reverse=True)

            for item in data:
                v = item["tag_name"].lstrip("v")
                if not Version(v).is_prerelease:
                    releases.append(v)
                    release_notes.append("### {tag_name}\n\n{body}".format(**item))

            new_version = get_newer_version(current_version, releases)

            if new_version:
                # closest_release == current_version if current_version appears in the
                # release list. Otherwise closest_release < current_version
                closest_release = next(
                    v for v in releases if Version(v) <= Version(current_version)
                )
                closest_release_idx = releases.index(closest_release)

                update_release_notes_list = release_notes[0:closest_release_idx]
                update_release_notes = "\n".join(update_release_notes_list)
        except CONNECTION_ERRORS:
            raise UpdateCheckError(
                "Could not check for updates",
                "No internet connection. Please try again later.",
            )
        except Exception as e:
            raise UpdateCheckError(
                "Could not check for updates",
                f"Unable to retrieve information: {e}",
            )

        return UpdateCheckResult(
            update_available=bool(new_version),
            latest_release=new_version or current_version,
            release_notes=update_release_notes,
        )

    def shutdown_daemon(self) -> None:
        """
        Stop syncing and notify anyone monitoring ``shutdown_future`` that we are done.
        """
        self.manager.shutdown()

        with self._sync_event_condition:
            self._sync_event_stream_closed = True
            self._sync_event_condition.notify_all()

        if self.shutdown_future and self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self.shutdown_future.set_result, True)

        self._logger.info("Shutting down")

    # ==== Verifiers ===================================================================

    def _check_linked(self) -> None:
        if not self.client.linked:
            raise NotLinkedError(
                "No Dropbox account linked",
                "Please link an account using the GUI or CLI.",
            )

    def _check_dropbox_dir(self) -> None:
        if self.pending_dropbox_folder:
            raise NoDropboxDirError(
                "No local Dropbox directory",
                "Please set up a local Dropbox directory using the GUI or CLI.",
            )

        self.sync.ensure_dropbox_folder_present()

    # ==== Housekeeping on update  =====================================================

    def _check_and_run_post_update_scripts(self) -> None:
        """
        Runs post-update scripts if necessary.
        """
        updated_from = self._state.get("app", "updated_scripts_completed")

        if Version(updated_from) < Version("1.2.1"):
            self._update_from_pre_v1_2_1()
        if Version(updated_from) < Version("1.3.2"):
            self._update_from_pre_v1_3_2()
        if Version(updated_from) < Version("1.4.8"):
            self._update_from_pre_v1_4_8()
        if Version(updated_from) < Version("1.6.0.dev0"):
            self._update_from_pre_v1_6_0()

        self._state.set("app", "updated_scripts_completed", __version__)

        self._conf.remove_deprecated_options()
        self._state.remove_deprecated_options()

    def _update_from_pre_v1_2_1(self) -> None:
        raise RuntimeError("Cannot upgrade from version before v1.2.1")

    def _update_from_pre_v1_3_2(self) -> None:
        if self._conf.get("app", "keyring") == "keyring.backends.OS_X.Keyring":
            self._logger.info("Migrating keyring after update from pre v1.3.2")
            self._conf.set("app", "keyring", "keyring.backends.macOS.Keyring")

    def _update_from_pre_v1_4_8(self) -> None:
        # Migrate config and state keys to new sections.
        self._logger.info("Migrating config after update from pre v1.4.8")

        mapping = {
            "path": {"old": "main", "new": "sync"},
            "keyring": {"old": "app", "new": "auth"},
            "account_id": {"old": "account", "new": "auth"},
        }

        for key, sections in mapping.items():
            if self._conf.has_option(sections["old"], key):
                value = self._conf.get(sections["old"], key)
                self._conf.set(sections["new"], key, value)

        self._logger.info("Migrating state after update from pre v1.4.8")

        mapping = {
            "token_access_type": {"old": "account", "new": "auth"},
        }

        for key, sections in mapping.items():
            if self._state.has_option(sections["old"], key):
                value = self._state.get(sections["old"], key)
                self._state.set(sections["new"], key, value)

    def _update_from_pre_v1_6_0(self) -> None:
        self._logger.info("Scheduling reindex after update from pre v1.6.0")

        db_path = get_data_path("maestral", f"{self.config_name}.db")
        connection = sqlite3.connect(db_path, check_same_thread=False)
        db = Database(connection)

        _sql_drop_table(db, "hash_cache")
        _sql_drop_table(db, "'index'")
        _sql_drop_table(db, "'history'")

        self._state.reset_to_defaults("sync")

        db.close()

    # ==== Periodic async jobs =========================================================

    def __repr__(self) -> str:
        email = self._state.get("account", "email")
        account_type = self._state.get("account", "type")

        return (
            f"<{self.__class__.__name__}(config={self._config_name!r}, "
            f"account=({email!r}, {account_type!r}))>"
        )


async def sleep_rand(target: float, jitter: float = 60) -> None:
    await asyncio.sleep(target + random.random() * jitter)
