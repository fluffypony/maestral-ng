"""Full-duplex client for Maestral's native virtual-file adapters."""

from __future__ import annotations

# system imports
import concurrent.futures
import errno
import json
import math
import os
import platform
import shutil
import stat
import subprocess
import sys
import threading
import time
import unicodedata
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Final, NoReturn, cast

# local imports
from .config import MaestralConfig, MaestralState, validate_config_name
from .constants import ROOT_MARKER_FILE
from .exceptions import (
    MaestralApiError,
    VirtualFileBusyError,
    VirtualFileNotFoundError,
    VirtualFileRevisionError,
    VirtualFilesUnsupportedError,
)
from .utils.appdirs import get_cache_path
from .utils.path import is_fs_link
from .utils.path import makedirs as rooted_makedirs
from .utils.path import rooted_item_snapshot
from .virtual_files import (
    HydrationRequest,
    NativeFileRecord,
    NativeFileState,
    UnsupportedVirtualFileBackend,
    VirtualFileBackend,
    VirtualFileDescriptor,
    VirtualFileIdentity,
    VirtualFileRootBinding,
    VirtualFileRootIdentity,
)

__all__ = [
    "NativeProcessVirtualFileBackend",
    "NativeVirtualFileError",
    "NativeVirtualFileProcessError",
    "NativeVirtualFileProtocolError",
    "NativeVirtualFileRequestError",
    "NativeVirtualFileTimeoutError",
    "create_process_virtual_file_backend",
    "resolve_native_virtual_file_executable",
]


PROTOCOL_VERSION: Final = 1
MAX_LINE_BYTES: Final = 1024 * 1024
MAX_IDENTIFIER_BYTES: Final = 4096
MAX_PATH_BYTES: Final = 32 * 1024
MAX_ERROR_BYTES: Final = 4096
RECOVERY_PAGE_LIMIT: Final = 8
MAX_RECOVERY_ITEMS: Final = 1_000_000
MAX_RECOVERY_PAGES: Final = 65_536
MAX_STDERR_BYTES: Final = 64 * 1024
MAX_REQUEST_TIMEOUT: Final = 300.0
MAX_INBOUND_REQUESTS: Final = 8
MAX_ROOT_NUMBER_BYTES: Final = 32
MAX_ROOT_MODE: Final = 0xFFFFFFFF

CAPABILITIES: Final = (
    "placeholders",
    "hydration",
    "dehydration",
    "pinning",
    "recovery",
)
ERROR_CODES: Final = frozenset(
    {
        "INVALID_REQUEST",
        "NOT_FOUND",
        "REVISION_MISMATCH",
        "BUSY",
        "UNSUPPORTED",
        "IO_ERROR",
        "INTERNAL",
    }
)
PLATFORM_ADAPTERS: Final = {
    "Darwin": ("maestral-smart-sync-macos", "macos-file-provider"),
    "Windows": ("MaestralSmartSync.exe", "windows-cfapi"),
    "Linux": ("maestral-smart-sync-fuse", "linux-fuse"),
}

_ROOT_BINDING_KEYS: Final = {
    "sourceRootPath",
    "sourceRootIdentity",
    "sourceCachePath",
    "sourceCacheIdentity",
    "visibleRootPath",
    "cachePath",
    "rootIdentity",
    "cacheIdentity",
    "state",
    "detachedRootPath",
}


@dataclass(frozen=True)
class _NativeRootBinding:
    root_marker_id: str
    source_root_path: str
    source_root_identity: dict[str, object]
    source_cache_path: str
    source_cache_identity: dict[str, object]
    visible_root_path: str
    cache_path: str
    root_identity: dict[str, object]
    cache_identity: dict[str, object]
    state: str
    detached_root_path: str | None


class NativeVirtualFileError(MaestralApiError):
    """Base class for native virtual-file process failures."""


class NativeVirtualFileProcessError(NativeVirtualFileError):
    """Raised when the native adapter cannot start or exits."""


class NativeVirtualFileProtocolError(NativeVirtualFileError):
    """Raised when the native adapter violates the protocol."""


class NativeVirtualFileTimeoutError(VirtualFileBusyError):
    """Raised when the native adapter does not meet a bounded deadline."""


class NativeVirtualFileRequestError(NativeVirtualFileError):
    """Raised when an adapter reports a non-operational failure."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__("Native virtual-file request failed", f"{code}: {message}")


class _InvalidMessage(ValueError):
    """Internal validation error for one untrusted protocol message."""


class NativeProcessVirtualFileBackend:
    """One process-backed native virtual root.

    The process owns the platform integration. This client owns request routing,
    deadlines, process lifetime, and validation at the trust boundary. Both peers may
    issue requests while another request is in progress.
    """

    supported = True

    def __init__(
        self,
        config_name: str,
        executable: str | os.PathLike[str],
        *,
        cache_path: str | os.PathLike[str] | None = None,
        request_timeout: float = 30.0,
        executable_arguments: tuple[str, ...] = (),
        platform_name: str | None = None,
    ) -> None:
        request_timeout = _validate_timeout(request_timeout)
        self.config_name = validate_config_name(config_name)
        self._platform_name = (
            platform.system() if platform_name is None else platform_name
        )
        try:
            _executable_name, self.backend_id = PLATFORM_ADAPTERS[self._platform_name]
        except KeyError:
            raise NativeVirtualFileProcessError(
                "Native virtual files are unavailable",
                f"{self._platform_name} has no native virtual-file adapter.",
            ) from None
        self.supported = self._platform_name != "Windows"

        self._executable = _require_executable(executable, self._platform_name)
        self._executable_arguments = executable_arguments
        self._request_timeout = request_timeout
        self._root_id = ""
        self._partition_default_cache = cache_path is None
        self._requested_cache_path = Path(
            cache_path
            if cache_path is not None
            else get_cache_path(
                "maestral", f"{self.config_name}.virtual-cache", create=True
            )
        ).expanduser()
        self._state = MaestralState(self.config_name)

        self._lifecycle_lock = threading.RLock()
        self._state_lock = threading.RLock()
        self._write_lock = threading.Lock()
        self._stderr_lock = threading.Lock()
        self._pending: dict[str, concurrent.futures.Future[dict[str, object]]] = {}
        self._process: subprocess.Popen[bytes] | None = None
        self._stdout_thread: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None
        self._session = 0
        self._closing = False
        self._started = False
        self._fatal_error: MaestralApiError | None = None
        self._request_hydration: HydrationRequest | None = None
        self._inbound_ids: set[str] = set()
        self._inbound_slots = threading.BoundedSemaphore(MAX_INBOUND_REQUESTS)
        self._content_condition = threading.Condition(self._state_lock)
        self._active_content_operations = 0
        self._request_context = threading.local()
        self._stderr_tail = bytearray()
        self._root_path = ""
        self._cache_path = ""
        self._active_binding: _NativeRootBinding | None = None
        self._adapter_capabilities: tuple[str, ...] = ()

    @property
    def root_id(self) -> str:
        """Return the stable native root identity for this profile."""
        return self._root_id

    @property
    def root_path(self) -> str:
        """Return the active platform root path."""
        return self._root_path

    @property
    def cache_path(self) -> str:
        """Return the active private adapter cache path."""
        return self._cache_path

    def cache_path_for_root(self, root_marker_id: str) -> str:
        """Return the private native cache path for one durable root identity."""
        marker_id = _validate_root_marker_id(root_marker_id)
        binding = self._load_root_bindings().get(marker_id)
        if binding is not None:
            if binding.state != "detached":
                self._verify_binding_caches(binding)
            return binding.cache_path
        cache_path = self._cache_path_for_marker(marker_id)
        prepared, _identity = _prepare_private_cache_path(
            cache_path, self._platform_name
        )
        return str(prepared)

    def binding_for_root(self, root_marker_id: str) -> VirtualFileRootBinding | None:
        """Return one saved platform binding without starting its adapter."""
        marker_id = _validate_root_marker_id(root_marker_id)
        binding = self._load_root_bindings().get(marker_id)
        return self._public_root_binding(binding) if binding is not None else None

    def registration_committed(self, root_marker_id: str) -> bool:
        """Return whether one native root has a confirmed live registration."""
        marker_id = _validate_root_marker_id(root_marker_id)
        binding = self._load_root_bindings().get(marker_id)
        return binding is not None and binding.state in {"registered", "accepted"}

    def binding_accepted(self, root_marker_id: str) -> bool:
        """Return whether core accepted one confirmed native binding."""
        marker_id = _validate_root_marker_id(root_marker_id)
        binding = self._load_root_bindings().get(marker_id)
        return binding is not None and binding.state == "accepted"

    def binding_detached(self, root_marker_id: str) -> bool:
        """Return whether one saved native binding is already detached."""
        marker_id = _validate_root_marker_id(root_marker_id)
        binding = self._load_root_bindings().get(marker_id)
        return binding is not None and binding.state == "detached"

    def accept_binding(self, root_marker_id: str) -> None:
        """Persist that core validated and adopted one native binding."""
        marker_id = _validate_root_marker_id(root_marker_id)
        binding = self._saved_root_binding(marker_id, require_registered=True)
        if binding.state == "accepted":
            return
        self._save_root_binding(replace(binding, state="accepted"))

    @property
    def capabilities(self) -> tuple[str, ...]:
        """Return the capabilities accepted during the start handshake."""
        return self._adapter_capabilities

    @property
    def stderr_tail(self) -> str:
        """Return a bounded, printable tail of adapter diagnostics."""
        with self._stderr_lock:
            text = bytes(self._stderr_tail).decode("utf-8", errors="replace")
        return "".join(
            character if character.isprintable() else " " for character in text
        )

    def _cache_path_for_marker(self, root_marker_id: str) -> Path:
        if self._partition_default_cache:
            return self._requested_cache_path / root_marker_id
        return self._requested_cache_path

    def _saved_root_marker_id(self) -> str:
        marker_id = MaestralConfig(self.config_name).get("sync", "root_marker_id")
        return _validate_root_marker_id(marker_id)

    def _load_root_bindings(self) -> dict[str, _NativeRootBinding]:
        value = self._state.get("virtual_files", "root_bindings")
        if value == {}:
            return {}
        if not isinstance(value, dict):
            raise _invalid_saved_root_binding()

        bindings: dict[str, _NativeRootBinding] = {}
        try:
            for marker_value, binding_value in value.items():
                marker_id = _validate_root_marker_id(marker_value)
                if (
                    not isinstance(binding_value, dict)
                    or set(binding_value) != _ROOT_BINDING_KEYS
                ):
                    raise ValueError("root binding is invalid")
                stored = cast(dict[str, object], binding_value)
                source_root_path = _validate_stored_native_path(
                    stored["sourceRootPath"], "sourceRootPath", self._platform_name
                )
                source_root_identity = _validate_directory_identity_object(
                    stored["sourceRootIdentity"]
                )
                source_cache_path = _validate_stored_native_path(
                    stored["sourceCachePath"], "sourceCachePath", self._platform_name
                )
                visible_root_path = _validate_stored_native_path(
                    stored["visibleRootPath"], "visibleRootPath", self._platform_name
                )
                cache_path = _validate_stored_native_path(
                    stored["cachePath"], "cachePath", self._platform_name
                )
                root_identity = _validate_directory_identity_object(
                    stored["rootIdentity"]
                )
                source_cache_identity = _validate_directory_identity_object(
                    stored["sourceCacheIdentity"]
                )
                cache_identity = _validate_directory_identity_object(
                    stored["cacheIdentity"]
                )
                binding_state = stored["state"]
                detached_root_value = stored["detachedRootPath"]
                if binding_state not in {
                    "pending",
                    "registered",
                    "accepted",
                    "detached",
                }:
                    raise ValueError("root binding is invalid")
                if detached_root_value is None:
                    detached_root_path = None
                else:
                    detached_root_path = _validate_stored_native_path(
                        detached_root_value,
                        "detachedRootPath",
                        self._platform_name,
                    )
                if binding_state == "pending" and (
                    visible_root_path != source_root_path
                    or cache_path != source_cache_path
                    or cache_identity != source_cache_identity
                    or root_identity != source_root_identity
                ):
                    raise ValueError("pending root binding is invalid")
                if (binding_state == "detached") != (detached_root_path is not None):
                    raise ValueError("detached root binding is invalid")
                bindings[marker_id] = _NativeRootBinding(
                    root_marker_id=marker_id,
                    source_root_path=source_root_path,
                    source_root_identity=source_root_identity,
                    source_cache_path=source_cache_path,
                    source_cache_identity=source_cache_identity,
                    visible_root_path=visible_root_path,
                    cache_path=cache_path,
                    root_identity=root_identity,
                    cache_identity=cache_identity,
                    state=binding_state,
                    detached_root_path=detached_root_path,
                )
        except (NativeVirtualFileProcessError, TypeError, ValueError):
            raise _invalid_saved_root_binding() from None
        return bindings

    def _save_root_binding(self, binding: _NativeRootBinding) -> None:
        bindings = self._load_root_bindings()
        bindings[binding.root_marker_id] = binding
        self._state.set(
            "virtual_files",
            "root_bindings",
            {
                marker_id: {
                    "sourceRootPath": saved.source_root_path,
                    "sourceRootIdentity": saved.source_root_identity,
                    "sourceCachePath": saved.source_cache_path,
                    "sourceCacheIdentity": saved.source_cache_identity,
                    "visibleRootPath": saved.visible_root_path,
                    "cachePath": saved.cache_path,
                    "rootIdentity": saved.root_identity,
                    "cacheIdentity": saved.cache_identity,
                    "state": saved.state,
                    "detachedRootPath": saved.detached_root_path,
                }
                for marker_id, saved in bindings.items()
            },
        )

    def _saved_root_binding(
        self, root_marker_id: str, *, require_registered: bool
    ) -> _NativeRootBinding:
        try:
            binding = self._load_root_bindings()[root_marker_id]
        except KeyError:
            raise NativeVirtualFileProcessError(
                "Native virtual root is unavailable",
                "The saved native root binding is missing.",
            ) from None
        if require_registered and binding.state not in {"registered", "accepted"}:
            raise NativeVirtualFileProcessError(
                "Native virtual root is unavailable",
                "The saved native root registration is not active.",
            )
        return binding

    def _prepare_start_binding(
        self, root_path: str, root_marker_id: str
    ) -> _NativeRootBinding:
        requested_root = _canonical_path_spelling(
            _require_native_path(
                root_path, "rootPath", self._platform_name, absolute=True
            )
        )
        bindings = self._load_root_bindings()
        existing = bindings.get(root_marker_id)
        if existing is not None:
            if existing.state == "detached":
                if requested_root not in {
                    existing.source_root_path,
                    existing.detached_root_path,
                }:
                    raise NativeVirtualFileProcessError(
                        "Native virtual root path changed",
                        "The configured path does not match the detached native root.",
                    )
                source_path, source_identity = _snapshot_real_directory(
                    Path(existing.source_root_path), "detached native root"
                )
                if (
                    str(source_path) != existing.source_root_path
                    or _root_identity_object(source_identity)
                    != existing.source_root_identity
                ):
                    raise NativeVirtualFileProcessError(
                        "Native virtual root identity changed",
                        "The detached native root was replaced.",
                    )
                self._verify_binding_caches(existing)
                pending = replace(
                    existing,
                    visible_root_path=existing.source_root_path,
                    cache_path=existing.source_cache_path,
                    root_identity=existing.source_root_identity,
                    cache_identity=existing.source_cache_identity,
                    state="pending",
                    detached_root_path=None,
                )
                self._save_root_binding(pending)
                return pending
            if requested_root not in {
                existing.source_root_path,
                existing.visible_root_path,
            }:
                raise NativeVirtualFileProcessError(
                    "Native virtual root path changed",
                    "The configured path does not match the saved native root.",
                )
            if self._platform_name != "Linux":
                _saved_source_path, current_source_identity = _snapshot_real_directory(
                    Path(existing.source_root_path), "native root"
                )
                if (
                    _root_identity_object(current_source_identity)
                    != existing.source_root_identity
                ):
                    raise NativeVirtualFileProcessError(
                        "Native virtual root identity changed",
                        "The saved native root source was replaced.",
                    )
            self._verify_binding_caches(existing)
            return existing

        if any(saved.state != "detached" for saved in bindings.values()):
            raise VirtualFileBusyError(
                "Cannot register a new native root",
                "Detach the prior native root before its marker identity changes.",
            )

        source_root, root_identity = _snapshot_real_directory(
            Path(requested_root), "native root"
        )
        source_cache_candidate = Path(
            _canonical_path_spelling(str(self._cache_path_for_marker(root_marker_id)))
        )
        _require_disjoint_paths(source_root, source_cache_candidate)
        source_cache, source_cache_identity = _prepare_private_cache_path(
            source_cache_candidate, self._platform_name
        )
        _require_disjoint_paths(source_root, source_cache)
        binding = _NativeRootBinding(
            root_marker_id=root_marker_id,
            source_root_path=str(source_root),
            source_root_identity=_root_identity_object(root_identity),
            source_cache_path=str(source_cache),
            source_cache_identity=source_cache_identity,
            visible_root_path=str(source_root),
            cache_path=str(source_cache),
            root_identity=_root_identity_object(root_identity),
            cache_identity=source_cache_identity,
            state="pending",
            detached_root_path=None,
        )
        self._save_root_binding(binding)
        return binding

    def _verify_binding_caches(self, binding: _NativeRootBinding) -> None:
        _require_existing_private_cache_path(
            Path(binding.source_cache_path),
            binding.source_cache_identity,
            self._platform_name,
        )
        if binding.cache_path != binding.source_cache_path:
            _require_existing_private_cache_path(
                Path(binding.cache_path),
                binding.cache_identity,
                self._platform_name,
            )
        _require_disjoint_paths(
            Path(binding.source_root_path), Path(binding.source_cache_path)
        )
        _require_disjoint_paths(
            Path(binding.visible_root_path), Path(binding.cache_path)
        )
        _require_disjoint_paths(
            Path(binding.source_root_path), Path(binding.cache_path)
        )
        _require_disjoint_paths(
            Path(binding.visible_root_path), Path(binding.source_cache_path)
        )

    def _verify_visible_root(self, binding: _NativeRootBinding) -> None:
        """Bind the saved visible root to the current no-follow directory."""
        if self._platform_name == "Linux":
            # A stale FUSE mount can reject lstat before its adapter can recover it.
            return
        visible_path, visible_identity = _snapshot_real_directory(
            Path(binding.visible_root_path), "native visible root"
        )
        if (
            str(visible_path) != binding.visible_root_path
            or _root_identity_object(visible_identity) != binding.root_identity
        ):
            raise NativeVirtualFileProcessError(
                "Native virtual root identity changed",
                "The saved visible native root was replaced.",
            )

    @staticmethod
    def _public_root_binding(binding: _NativeRootBinding) -> VirtualFileRootBinding:
        identity = binding.root_identity
        source_identity = binding.source_root_identity
        return VirtualFileRootBinding(
            source_root_path=binding.source_root_path,
            root_path=binding.visible_root_path,
            cache_path=binding.cache_path,
            source_root_identity=VirtualFileRootIdentity(
                device=cast(str, source_identity["device"]),
                inode=cast(str, source_identity["inode"]),
                mode=cast(int, source_identity["mode"]),
            ),
            root_identity=VirtualFileRootIdentity(
                device=cast(str, identity["device"]),
                inode=cast(str, identity["inode"]),
                mode=cast(int, identity["mode"]),
            ),
        )

    def start(
        self, root_path: str, request_hydration: HydrationRequest
    ) -> VirtualFileRootBinding:
        """Start the adapter and register one native virtual root."""
        if not self.supported:
            raise UnsupportedVirtualFileBackend()._unsupported()
        with self._lifecycle_lock:
            return self._start_locked(root_path, request_hydration)

    def _start_locked(
        self, root_path: str, request_hydration: HydrationRequest
    ) -> VirtualFileRootBinding:
        """Start one native root while holding the lifecycle lock."""
        if self._started:
            if self._active_binding is None:
                raise self._process_error(
                    "The active native root binding is unavailable."
                )
            return self._public_root_binding(self._active_binding)
        root_marker_id = self._saved_root_marker_id()
        root_id = _root_id(self.config_name, root_marker_id)
        binding = self._prepare_start_binding(root_path, root_marker_id)

        if self._process is not None:
            self._stop_process()
        self._launch_process()
        self._request_hydration = request_hydration
        try:
            result = self._request(
                "start",
                {
                    "rootId": root_id,
                    "rootPath": binding.source_root_path,
                    "cachePath": binding.source_cache_path,
                    "rootIdentity": binding.source_root_identity,
                    "rootMarker": _root_marker_object(root_marker_id),
                },
            )
            completed_binding, capabilities = self._validate_start_result(
                result, binding
            )
            self._save_root_binding(completed_binding)
            with self._state_lock:
                if self._fatal_error is not None:
                    raise self._fatal_error
                process = self._process
                if process is None or process.poll() is not None:
                    raise self._process_error(
                        "The native virtual-file adapter exited during start."
                    )
                self._root_path = completed_binding.visible_root_path
                self._cache_path = completed_binding.cache_path
                self._root_id = root_id
                self._active_binding = completed_binding
                self._adapter_capabilities = capabilities
                self._started = True
            return self._public_root_binding(completed_binding)
        except BaseException:
            self._request_hydration = None
            self._stop_process()
            raise

    def stop(self) -> None:
        """Stop the native root and release all process resources."""
        with self._lifecycle_lock:
            self._stop_locked()

    def _stop_locked(self) -> None:
        """Stop one native root while holding the lifecycle lock."""
        with self._state_lock:
            self._closing = True

        self._wait_for_content_operations()

        with self._state_lock:
            process = self._process
            fatal_error = self._fatal_error

        if process is not None and process.poll() is None and fatal_error is None:
            try:
                result = self._request(
                    "stop",
                    {},
                    timeout=self._request_timeout,
                    terminate_on_timeout=False,
                )
                _require_exact_keys(self, result, set(), "stop result")
            except Exception:
                with self._state_lock:
                    process_is_active = (
                        process is self._process
                        and process.poll() is None
                        and self._fatal_error is None
                    )
                if process_is_active:
                    raise

        with self._state_lock:
            self._started = False
            self._request_hydration = None
        self._stop_process()
        self._root_path = ""
        self._cache_path = ""
        self._root_id = ""
        self._active_binding = None
        self._adapter_capabilities = ()

    def validate_root(self, root_path: str, root_marker_id: str) -> None:
        """Validate an active native root without reading its mounted namespace."""
        if not self.supported:
            raise UnsupportedVirtualFileBackend()._unsupported()
        requested_root = _canonical_path_spelling(
            _require_native_path(
                root_path, "rootPath", self._platform_name, absolute=True
            )
        )
        marker_id = _validate_root_marker_id(root_marker_id)
        root_id = _root_id(self.config_name, marker_id)
        binding = self._saved_root_binding(marker_id, require_registered=True)
        if requested_root != binding.visible_root_path:
            raise NativeVirtualFileProcessError(
                "Native virtual root path changed",
                "The configured path does not match the saved visible root.",
            )
        self._verify_binding_caches(binding)
        self._verify_visible_root(binding)

        with self._lifecycle_lock:
            with self._state_lock:
                process = self._process
                fatal_error = self._fatal_error
                active_root_id = self._root_id
            if active_root_id and active_root_id != root_id:
                raise VirtualFileBusyError(
                    "Cannot validate this virtual root",
                    "A different native root is active in this adapter process.",
                )
            launched_fresh = (
                process is None or process.poll() is not None or fatal_error is not None
            )
            if launched_fresh and self._platform_name == "Linux":

                def reject_hydration(
                    _provider_id: str, _expected_revision: str
                ) -> dict[str, object]:
                    raise VirtualFileBusyError(
                        "The virtual root is under validation",
                        "Try to open this item after validation finishes.",
                    )

                try:
                    self._start_locked(requested_root, reject_hydration)
                finally:
                    if self._started:
                        self._stop_locked()
                return
            if launched_fresh:
                with self._state_lock:
                    self._closing = True
                self._wait_for_content_operations()
                with self._state_lock:
                    process = self._process
                if process is not None:
                    self._stop_process()
                self._launch_process()

            try:
                result = self._request(
                    "validate_root",
                    {
                        "rootId": root_id,
                        "rootPath": binding.visible_root_path,
                        "cachePath": binding.cache_path,
                        "rootIdentity": binding.root_identity,
                    },
                )
                _require_exact_keys(
                    self, result, {"rootIdentity"}, "validate_root result"
                )
                try:
                    returned_identity = _validate_directory_identity_object(
                        result["rootIdentity"]
                    )
                except (TypeError, ValueError):
                    self._protocol_result_failure(
                        "The adapter returned an invalid native root identity."
                    )
                if returned_identity != binding.root_identity:
                    self._protocol_result_failure(
                        "The adapter returned a different native root identity."
                    )
            finally:
                if launched_fresh:
                    self._stop_process()

    def detach(self, root_path: str, root_marker_id: str) -> str:
        """Preserve hydrated bytes and remove the native virtual-root registration."""
        if not self.supported:
            raise UnsupportedVirtualFileBackend()._unsupported()
        requested_root = _canonical_path_spelling(
            _require_native_path(
                root_path, "rootPath", self._platform_name, absolute=True
            )
        )
        marker_id = _validate_root_marker_id(root_marker_id)
        root_id = _root_id(self.config_name, marker_id)
        binding = self._saved_root_binding(marker_id, require_registered=False)
        if binding.state == "detached":
            if binding.detached_root_path is None:
                raise _invalid_saved_root_binding()
            if requested_root not in {
                binding.visible_root_path,
                binding.detached_root_path,
            }:
                raise NativeVirtualFileProcessError(
                    "Native virtual root path changed",
                    "The requested path does not match the detached native root.",
                )
            detached_path, detached_identity = _snapshot_real_directory(
                Path(binding.detached_root_path), "detached native root"
            )
            if (
                str(detached_path) != binding.detached_root_path
                or _root_identity_object(detached_identity)
                != binding.source_root_identity
            ):
                raise NativeVirtualFileProcessError(
                    "Native virtual root identity changed",
                    "The detached native root was replaced.",
                )
            return binding.detached_root_path
        if binding.state not in {"registered", "accepted"}:
            raise NativeVirtualFileProcessError(
                "Native virtual root is unavailable",
                "The native root registration did not complete.",
            )
        if requested_root != binding.visible_root_path:
            raise NativeVirtualFileProcessError(
                "Native virtual root path changed",
                "The configured path does not match the saved visible root.",
            )
        self._verify_binding_caches(binding)
        self._verify_visible_root(binding)

        with self._lifecycle_lock:
            with self._state_lock:
                process = self._process
                fatal_error = self._fatal_error
                active_root_id = self._root_id
            if (
                process is not None
                and process.poll() is None
                and fatal_error is None
                and active_root_id
                and active_root_id != root_id
            ):
                raise VirtualFileBusyError(
                    "Cannot detach this virtual root",
                    "A different native root is active in this adapter process.",
                )

            with self._state_lock:
                self._closing = True
            self._wait_for_content_operations()

            with self._state_lock:
                process = self._process
                fatal_error = self._fatal_error
            if process is None or process.poll() is not None or fatal_error is not None:
                if process is not None:
                    self._stop_process()
                self._launch_process()

            with self._state_lock:
                self._closing = True
                self._started = False
                self._request_hydration = None

            detach_error: BaseException | None = None
            preserved_root = binding.visible_root_path
            try:
                result = self._request(
                    "detach",
                    {
                        "rootId": root_id,
                        "rootPath": binding.visible_root_path,
                        "cachePath": binding.cache_path,
                        "rootIdentity": binding.root_identity,
                    },
                )
                _require_exact_keys(self, result, {"rootPath"}, "detach result")
                try:
                    preserved_root = _canonical_path_spelling(
                        _require_native_path(
                            _require_string(self, result, "rootPath"),
                            "rootPath",
                            self._platform_name,
                            absolute=True,
                        )
                    )
                except ValueError:
                    self._protocol_result_failure(
                        "The adapter returned an invalid detached root path."
                    )
                if preserved_root != binding.source_root_path:
                    self._protocol_result_failure(
                        "The adapter returned a different detached source root."
                    )
                try:
                    detached_path, _detached_identity = _snapshot_real_directory(
                        Path(preserved_root), "detached native root"
                    )
                    if (
                        _root_identity_object(_detached_identity)
                        != binding.source_root_identity
                    ):
                        raise NativeVirtualFileProcessError(
                            "Native virtual root identity changed",
                            "The detached source root was replaced.",
                        )
                    _require_disjoint_paths(detached_path, Path(binding.cache_path))
                    _require_disjoint_paths(
                        detached_path, Path(binding.source_cache_path)
                    )
                except NativeVirtualFileProcessError:
                    self._protocol_result_failure(
                        "The adapter returned an unsafe detached root path."
                    )
                self._save_root_binding(
                    replace(
                        binding,
                        state="detached",
                        detached_root_path=preserved_root,
                    )
                )
            except BaseException as exc:
                detach_error = exc
            finally:
                self._stop_process()
                self._root_path = ""
                self._cache_path = ""
                self._root_id = ""
                self._active_binding = None
                self._adapter_capabilities = ()

            if detach_error is not None:
                raise detach_error
            return preserved_root

    def upsert(
        self,
        item: VirtualFileDescriptor,
        *,
        expected_identity: VirtualFileIdentity | None,
    ) -> None:
        """Create or atomically update one placeholder."""
        item_object = _item_object(item)
        expected_object = (
            _identity_object(expected_identity)
            if expected_identity is not None
            else None
        )
        if (
            expected_identity is not None
            and expected_identity.provider_id != item.provider_id
        ):
            raise ValueError("The expected provider identity does not match the item")
        result = self._call_started(
            "upsert",
            {"item": item_object, "expectedIdentity": expected_object},
        )
        _require_exact_keys(self, result, {"operation"}, "upsert result")
        operation = _require_string(self, result, "operation")
        if operation not in {"created", "updated", "moved", "unchanged"}:
            self._protocol_result_failure(
                "The adapter returned an invalid upsert result."
            )
        allowed_operations = (
            {"created", "unchanged"}
            if expected_identity is None
            else (
                {"moved", "unchanged"}
                if expected_identity.path != item.path
                else {"updated", "unchanged"}
            )
        )
        if operation not in allowed_operations:
            self._protocol_result_failure(
                "The adapter returned an inconsistent upsert operation."
            )

    def remove(self, expected_identity: VirtualFileIdentity) -> None:
        """Remove one native item by stable identity."""
        expected_object = _identity_object(expected_identity)
        try:
            result = self._call_started(
                "remove",
                {"expectedIdentity": expected_object},
            )
        except VirtualFileNotFoundError:
            self._protocol_result_failure(
                "The adapter returned NOT_FOUND for an idempotent remove."
            )
        _require_empty_result(self, result, "remove")

    def set_pinned(self, provider_id: str, pinned: bool) -> None:
        """Set the platform pin state for one native item."""
        _validate_identifier(provider_id, "providerId")
        if not isinstance(pinned, bool):
            raise ValueError("pinned must be a boolean")
        result = self._call_started(
            "set_pinned", {"providerId": provider_id, "pinned": pinned}
        )
        _require_empty_result(self, result, "set_pinned")

    def materialize(
        self,
        item: VirtualFileDescriptor,
        staged_path: str,
        *,
        expected_revision: str,
    ) -> None:
        """Copy one exact staged revision into the native cache."""
        item_object = _item_object(item)
        _validate_identifier(expected_revision, "expectedRevision")
        if expected_revision != item.revision:
            raise VirtualFileRevisionError(
                "Could not materialise this file",
                "The requested revision does not match the virtual item.",
            )
        stage, stage_status = _require_private_stage_file(
            staged_path, self._platform_name
        )
        if stage_status.st_size != item.size:
            raise VirtualFileRevisionError(
                "Could not materialise this file",
                "The staged file size does not match the virtual item.",
            )
        operation_session = self._begin_content_operation()
        try:
            result = self._request(
                "materialize",
                {
                    "item": item_object,
                    "stagedPath": str(stage),
                    "stagedIdentity": _staged_identity_object(stage_status),
                    "expectedRevision": expected_revision,
                },
                operation_session=operation_session,
            )
            _require_empty_result(self, result, "materialize")
        finally:
            self._finish_content_operation(operation_session)

    def evict(self, provider_id: str, *, expected_revision: str) -> None:
        """Evict one exact clean and unpinned native revision."""
        _validate_identifier(provider_id, "providerId")
        _validate_identifier(expected_revision, "expectedRevision")
        operation_session = self._begin_content_operation()
        try:
            result = self._request(
                "evict",
                {"providerId": provider_id, "expectedRevision": expected_revision},
                operation_session=operation_session,
            )
            _require_empty_result(self, result, "evict")
        finally:
            self._finish_content_operation(operation_session)

    def inspect(self, provider_id: str) -> NativeFileState | None:
        """Return native state for one stable identity."""
        _validate_identifier(provider_id, "providerId")
        try:
            result = self._call_started("inspect", {"providerId": provider_id})
        except VirtualFileNotFoundError:
            return None
        record = self._native_record(result, "inspect result")
        if record.identity.provider_id != provider_id:
            self._protocol_result_failure(
                "The adapter returned state for a different provider identity."
            )
        return NativeFileState(
            identity=record.identity,
            hydrated_revision=record.hydrated_revision,
            pinned=record.pinned,
            dirty=record.dirty,
            open_count=record.open_count,
        )

    def recover(self) -> Sequence[NativeFileRecord]:
        """Return one complete, immutable native recovery snapshot."""
        cursor: str | None = None
        seen_cursors: set[str] = set()
        seen_provider_ids: set[str] = set()
        seen_paths: set[str] = set()
        records: list[NativeFileRecord] = []
        page_count = 0

        while True:
            page_count += 1
            if page_count > MAX_RECOVERY_PAGES:
                self._protocol_result_failure(
                    "The adapter returned too many recovery pages."
                )
            result = self._call_started(
                "recover", {"cursor": cursor, "limit": RECOVERY_PAGE_LIMIT}
            )
            _require_exact_keys(
                self, result, {"items", "cursor", "hasMore"}, "recover result"
            )
            items = result["items"]
            page_cursor = result["cursor"]
            has_more = result["hasMore"]
            if (
                not isinstance(items, list)
                or len(items) > RECOVERY_PAGE_LIMIT
                or not isinstance(page_cursor, str)
                or not page_cursor
                or not isinstance(has_more, bool)
            ):
                self._protocol_result_failure(
                    "The adapter returned an invalid recovery page."
                )
            try:
                _validate_identifier(page_cursor, "cursor")
            except (TypeError, ValueError):
                self._protocol_result_failure(
                    "The adapter returned an invalid recovery cursor."
                )

            page = [
                self._native_record(item, "recovery item")
                for item in cast(list[object], items)
            ]
            if has_more and not page:
                self._protocol_result_failure(
                    "The adapter returned an empty intermediate recovery page."
                )
            if len(records) + len(page) > MAX_RECOVERY_ITEMS:
                self._protocol_result_failure(
                    "The adapter returned too many recovery items."
                )
            for record in page:
                provider_id = record.identity.provider_id
                path = record.identity.path
                if provider_id in seen_provider_ids or path in seen_paths:
                    self._protocol_result_failure(
                        "The adapter returned duplicate recovery identities."
                    )
                seen_provider_ids.add(provider_id)
                seen_paths.add(path)
            records.extend(page)

            if not has_more:
                return records
            if page_cursor == cursor or page_cursor in seen_cursors:
                self._protocol_result_failure(
                    "The adapter returned a non-advancing recovery cursor."
                )
            seen_cursors.add(page_cursor)
            cursor = page_cursor

    def _launch_process(self) -> None:
        command = [str(self._executable), *self._executable_arguments]
        with self._stderr_lock:
            self._stderr_tail.clear()
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
                close_fds=True,
                shell=False,
            )
        except OSError:
            raise self._process_error(
                "The native virtual-file adapter could not start."
            ) from None

        with self._state_lock:
            self._session += 1
            session = self._session
            self._process = process
            self._pending.clear()
            self._fatal_error = None
            self._closing = False
            self._started = False
            self._inbound_ids.clear()
            self._inbound_slots = threading.BoundedSemaphore(MAX_INBOUND_REQUESTS)
            self._active_content_operations = 0
        self._stdout_thread = threading.Thread(
            target=self._read_stdout,
            args=(process, session),
            name=f"maestral-smart-sync-{self.config_name}-stdout",
            daemon=True,
        )
        self._stderr_thread = threading.Thread(
            target=self._read_stderr,
            args=(process,),
            name=f"maestral-smart-sync-{self.config_name}-stderr",
            daemon=True,
        )
        self._stdout_thread.start()
        self._stderr_thread.start()

    def _call_started(
        self, method: str, params: dict[str, object]
    ) -> dict[str, object]:
        with self._state_lock:
            if not self._started:
                if self._fatal_error is not None:
                    raise self._fatal_error
                raise VirtualFileBusyError(
                    "The virtual root is stopped",
                    "Start the virtual root before this operation.",
                )
        return self._request(method, params)

    def _begin_content_operation(self) -> int:
        with self._content_condition:
            context_session = getattr(self._request_context, "session", None)
            continues_inbound_request = context_session == self._session
            if not self._started or (self._closing and not continues_inbound_request):
                if self._fatal_error is not None:
                    raise self._fatal_error
                raise VirtualFileBusyError(
                    "The virtual root is stopping",
                    "Try the content operation again after the root starts.",
                )
            if context_session is not None and context_session != self._session:
                raise VirtualFileBusyError(
                    "The native hydration request expired",
                    "Open the file again after the virtual root starts.",
                )
            self._active_content_operations += 1
            return self._session

    def _finish_content_operation(self, session: int) -> None:
        with self._content_condition:
            if session == self._session:
                self._active_content_operations -= 1
            self._content_condition.notify_all()

    def _wait_for_content_operations(self) -> None:
        with self._content_condition:
            while self._active_content_operations:
                self._content_condition.wait()

    def _request(
        self,
        method: str,
        params: dict[str, object],
        *,
        timeout: float | None = None,
        operation_session: int | None = None,
        terminate_on_timeout: bool = True,
    ) -> dict[str, object]:
        request_timeout = _validate_timeout(
            self._request_timeout if timeout is None else timeout
        )
        deadline = time.monotonic() + request_timeout
        request_id = f"core-{uuid.uuid4()}"
        future: concurrent.futures.Future[dict[str, object]] = (
            concurrent.futures.Future()
        )
        with self._state_lock:
            if self._fatal_error is not None:
                raise self._fatal_error
            context_session = getattr(self._request_context, "session", None)
            if context_session is not None and context_session != self._session:
                raise VirtualFileBusyError(
                    "The native hydration request expired",
                    "Open the file again after the virtual root starts.",
                )
            if operation_session is not None and operation_session != self._session:
                raise VirtualFileBusyError(
                    "The native content request expired",
                    "Try the content operation again after the virtual root starts.",
                )
            process = self._process
            if process is None or process.poll() is not None:
                raise self._process_error(
                    "The native virtual-file adapter is not active."
                )
            continues_content_request = (
                context_session == self._session or operation_session == self._session
            )
            if (
                self._closing
                and not continues_content_request
                and method not in {"stop", "detach"}
            ):
                raise VirtualFileBusyError(
                    "The virtual root is stopping", "Try the operation again later."
                )
            session = self._session
            self._pending[request_id] = future

        message = {
            "protocolVersion": PROTOCOL_VERSION,
            "requestId": request_id,
            "method": method,
            "params": params,
        }
        try:
            payload = _encode_message(message)
        except NativeVirtualFileProtocolError:
            self._discard_request(request_id, future, session)
            raise

        try:
            self._write_payload(
                process,
                payload,
                timeout=max(0.0, deadline - time.monotonic()),
            )
        except NativeVirtualFileTimeoutError as exc:
            if terminate_on_timeout:
                self._fail_session(exc, session, terminate=True)
            raise
        except (BrokenPipeError, OSError, ValueError):
            process_error = self._process_error(
                "The native virtual-file adapter input pipe closed."
            )
            self._fail_session(process_error, session, terminate=True)
            raise process_error from None

        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise concurrent.futures.TimeoutError
            return future.result(timeout=remaining)
        except concurrent.futures.TimeoutError:
            timeout_error = NativeVirtualFileTimeoutError(
                "The native virtual-file adapter timed out",
                (
                    "The adapter process was stopped before the operation returned."
                    if terminate_on_timeout
                    else "The operation is still active and can be retried."
                ),
            )
            if terminate_on_timeout:
                self._fail_session(timeout_error, session, terminate=True)
            raise timeout_error from None
        except BaseException:
            if not future.done():
                self._discard_request(request_id, future, session)
                interruption_error = self._process_error(
                    "The native virtual-file request was interrupted."
                )
                self._fail_session(interruption_error, session, terminate=True)
            raise

    def _write_payload(
        self,
        process: subprocess.Popen[bytes],
        payload: bytes,
        *,
        timeout: float,
    ) -> None:
        completed = threading.Event()
        errors: list[BaseException] = []

        def write() -> None:
            try:
                self._write_payload_blocking(process, payload)
            except BaseException as exc:
                errors.append(exc)
            finally:
                completed.set()

        writer = threading.Thread(
            target=write,
            name=f"maestral-smart-sync-{self.config_name}-write",
            daemon=True,
        )
        try:
            writer.start()
        except RuntimeError as exc:
            raise OSError("The protocol writer could not start") from exc
        if not completed.wait(timeout):
            raise NativeVirtualFileTimeoutError(
                "The native virtual-file adapter timed out",
                "The adapter process stopped reading protocol requests.",
            )
        if errors:
            raise errors[0]

    def _write_payload_blocking(
        self, process: subprocess.Popen[bytes], payload: bytes
    ) -> None:
        with self._write_lock:
            stream = process.stdin
            if stream is None:
                raise BrokenPipeError
            view = memoryview(payload)
            while view:
                written = stream.write(view)
                if not written:
                    raise BrokenPipeError
                view = view[written:]
            stream.flush()

    def _read_stdout(self, process: subprocess.Popen[bytes], session: int) -> None:
        stream = process.stdout
        if stream is None:
            self._fail_session(
                self._process_error(
                    "The native virtual-file adapter output pipe is unavailable."
                ),
                session,
                terminate=True,
            )
            return

        while True:
            try:
                line = stream.readline(MAX_LINE_BYTES + 1)
            except OSError:
                line = b""
            if not line:
                with self._state_lock:
                    clean_exit = session == self._session and self._closing
                if not clean_exit:
                    self._fail_session(
                        self._process_error(
                            "The native virtual-file adapter exited unexpectedly."
                        ),
                        session,
                        terminate=True,
                    )
                return
            if (
                len(line) > MAX_LINE_BYTES
                or not line.endswith(b"\n")
                or b"\n" in line[:-1]
                or b"\r" in line[:-1]
            ):
                self._protocol_failure(
                    "The adapter returned an oversized or multiline message.", session
                )
                return
            try:
                value = _decode_message(line[:-1])
                self._dispatch_message(value, process, session)
            except MaestralApiError as exc:
                self._fail_session(exc, session, terminate=True)
                return
            except Exception:
                self._protocol_failure(
                    "The adapter returned invalid protocol data.", session
                )
                return

    def _read_stderr(self, process: subprocess.Popen[bytes]) -> None:
        stream = process.stderr
        if stream is None:
            return
        while True:
            try:
                chunk = stream.read(4096)
            except OSError:
                return
            if not chunk:
                return
            with self._stderr_lock:
                self._stderr_tail.extend(chunk)
                excess = len(self._stderr_tail) - MAX_STDERR_BYTES
                if excess > 0:
                    del self._stderr_tail[:excess]

    def _dispatch_message(
        self,
        value: object,
        process: subprocess.Popen[bytes],
        session: int,
    ) -> None:
        if not isinstance(value, dict):
            raise self._protocol_error("The adapter message is not an object.")
        message = cast(dict[str, object], value)
        if "method" in message:
            self._dispatch_adapter_request(message, process, session)
        else:
            self._dispatch_response(message, session)

    def _dispatch_response(self, response: dict[str, object], session: int) -> None:
        if (
            response.get("protocolVersion") != PROTOCOL_VERSION
            or type(response.get("protocolVersion")) is not int
        ):
            raise self._protocol_error("The adapter response version is invalid.")
        request_id = response.get("requestId")
        try:
            _validate_identifier(request_id, "requestId")
        except (TypeError, ValueError):
            raise self._protocol_error("The adapter response ID is invalid.") from None
        request_id = cast(str, request_id)

        has_result = "result" in response
        has_error = "error" in response
        expected_keys = (
            {"protocolVersion", "requestId", "result"}
            if has_result
            else {"protocolVersion", "requestId", "error"}
        )
        if has_result == has_error or set(response) != expected_keys:
            raise self._protocol_error("The adapter response envelope is invalid.")

        if has_result:
            result = response["result"]
            if not isinstance(result, dict):
                raise self._protocol_error("The adapter result is not an object.")
            response_result = cast(dict[str, object], result)
            response_error: BaseException | None = None
        else:
            error = response["error"]
            if not isinstance(error, dict) or set(error) != {"code", "message"}:
                raise self._protocol_error("The adapter error envelope is invalid.")
            error_object = cast(dict[str, object], error)
            code = error_object.get("code")
            message = error_object.get("message")
            if (
                not isinstance(code, str)
                or code not in ERROR_CODES
                or not isinstance(message, str)
                or not message
                or len(message.encode("utf-8")) > MAX_ERROR_BYTES
                or _has_control_character(message)
            ):
                raise self._protocol_error("The adapter error fields are invalid.")
            response_result = {}
            response_error = _map_adapter_error(code, message)

        with self._state_lock:
            if session != self._session:
                return
            future = self._pending.pop(request_id, None)
        if future is None:
            raise self._protocol_error("The adapter response ID is unknown.")
        if response_error is None:
            future.set_result(response_result)
        else:
            future.set_exception(response_error)

    def _dispatch_adapter_request(
        self,
        request: dict[str, object],
        process: subprocess.Popen[bytes],
        session: int,
    ) -> None:
        request_id = request.get("requestId")
        try:
            _validate_identifier(request_id, "requestId")
        except (TypeError, ValueError):
            raise self._protocol_error("The adapter request ID is invalid.") from None
        request_id = cast(str, request_id)

        try:
            method, params = _validate_adapter_request(request, self._platform_name)
        except (_InvalidMessage, KeyError, TypeError, ValueError):
            self._send_adapter_error(
                process,
                session,
                request_id,
                "INVALID_REQUEST",
                "The adapter request is invalid",
            )
            return

        if method in {"enumerate", "dehydrate", "pin_changed", "local_change"}:
            self._send_adapter_error(
                process,
                session,
                request_id,
                "UNSUPPORTED",
                "This core version does not accept this native event",
            )
            return
        if method != "hydrate":
            self._send_adapter_error(
                process,
                session,
                request_id,
                "INVALID_REQUEST",
                "The adapter cannot call this daemon method",
            )
            return

        with self._state_lock:
            if request_id in self._inbound_ids:
                duplicate = True
            else:
                duplicate = False
                self._inbound_ids.add(request_id)
            slots = self._inbound_slots
        if duplicate:
            self._send_adapter_error(
                process,
                session,
                request_id,
                "INVALID_REQUEST",
                "The adapter request ID is already active",
            )
            return
        if not slots.acquire(blocking=False):
            with self._state_lock:
                self._inbound_ids.discard(request_id)
            self._send_adapter_error(
                process,
                session,
                request_id,
                "BUSY",
                "The core hydration request limit was reached",
            )
            return

        try:
            operation_session = self._begin_content_operation()
        except VirtualFileBusyError:
            with self._state_lock:
                self._inbound_ids.discard(request_id)
            slots.release()
            self._send_adapter_error(
                process,
                session,
                request_id,
                "BUSY",
                "The virtual root is stopping",
            )
            return

        thread = threading.Thread(
            target=self._serve_hydration_request,
            args=(
                process,
                session,
                request_id,
                params,
                slots,
                operation_session,
            ),
            name=f"maestral-smart-sync-{self.config_name}-hydrate",
            daemon=True,
        )
        try:
            thread.start()
        except RuntimeError:
            with self._state_lock:
                self._inbound_ids.discard(request_id)
            slots.release()
            self._finish_content_operation(operation_session)
            self._send_adapter_error(
                process,
                session,
                request_id,
                "BUSY",
                "The core could not start a hydration worker",
            )

    def _serve_hydration_request(
        self,
        process: subprocess.Popen[bytes],
        session: int,
        request_id: str,
        params: dict[str, object],
        slots: threading.BoundedSemaphore,
        operation_session: int,
    ) -> None:
        self._request_context.session = session
        try:
            provider_id = cast(str, params["providerId"])
            expected_revision = cast(str, params["expectedRevision"])
            callback = self._request_hydration
            if callback is None:
                raise VirtualFileBusyError(
                    "The virtual root is not ready", "Try to open the file again."
                )
            status = callback(provider_id, expected_revision)
            if not isinstance(status, dict) or status.get("provider_id") != provider_id:
                raise NativeVirtualFileProtocolError(
                    "Native hydration failed",
                    "The core returned a different provider identity.",
                )
            if status.get("revision") != expected_revision:
                raise VirtualFileRevisionError(
                    "Could not hydrate this file",
                    "The remote revision changed during hydration.",
                )
            if status.get("hydration_state") != "hydrated":
                raise NativeVirtualFileProtocolError(
                    "Native hydration failed",
                    "The core did not confirm materialised content.",
                )
            native = self.inspect(provider_id)
            if native is None:
                raise VirtualFileNotFoundError(
                    "Could not hydrate this file",
                    "The native placeholder no longer exists.",
                )
            if native.hydrated_revision != expected_revision:
                raise VirtualFileRevisionError(
                    "Could not hydrate this file",
                    "The native adapter did not materialise the requested revision.",
                )
            if native.dirty:
                raise VirtualFileBusyError(
                    "Could not hydrate this file",
                    "The native placeholder has local changes.",
                )
            self._send_adapter_result(process, session, request_id, {})
        except BaseException as exc:
            code, message = _adapter_error_for_exception(exc)
            self._send_adapter_error(
                process, session, request_id, code, message, fail_process=False
            )
        finally:
            try:
                del self._request_context.session
            except AttributeError:
                pass
            with self._state_lock:
                if session == self._session:
                    self._inbound_ids.discard(request_id)
            slots.release()
            self._finish_content_operation(operation_session)

    def _send_adapter_result(
        self,
        process: subprocess.Popen[bytes],
        session: int,
        request_id: str,
        result: dict[str, object],
    ) -> None:
        self._send_adapter_message(
            process,
            session,
            {
                "protocolVersion": PROTOCOL_VERSION,
                "requestId": request_id,
                "result": result,
            },
        )

    def _send_adapter_error(
        self,
        process: subprocess.Popen[bytes],
        session: int,
        request_id: str,
        code: str,
        message: str,
        *,
        fail_process: bool = True,
    ) -> None:
        try:
            self._send_adapter_message(
                process,
                session,
                {
                    "protocolVersion": PROTOCOL_VERSION,
                    "requestId": request_id,
                    "error": {"code": code, "message": message},
                },
            )
        except MaestralApiError:
            if fail_process:
                raise

    def _send_adapter_message(
        self,
        process: subprocess.Popen[bytes],
        session: int,
        message: dict[str, object],
    ) -> None:
        with self._state_lock:
            if session != self._session or process is not self._process:
                return
        payload = _encode_message(message)
        try:
            self._write_payload(
                process,
                payload,
                timeout=self._request_timeout,
            )
        except NativeVirtualFileTimeoutError as error:
            self._fail_session(error, session, terminate=True)
            raise
        except (BrokenPipeError, OSError, ValueError):
            process_error = self._process_error(
                "The native virtual-file adapter input pipe closed."
            )
            self._fail_session(process_error, session, terminate=True)
            raise process_error from None

    def _validate_start_result(
        self,
        result: dict[str, object],
        binding: _NativeRootBinding,
    ) -> tuple[_NativeRootBinding, tuple[str, ...]]:
        _require_exact_keys(
            self,
            result,
            {"adapter", "capabilities", "rootPath", "cachePath", "rootIdentity"},
            "start result",
        )
        adapter = _require_string(self, result, "adapter")
        if adapter != self.backend_id:
            self._protocol_result_failure(
                "The executable returned the wrong native adapter identity."
            )
        capabilities_value = result["capabilities"]
        if not isinstance(capabilities_value, list) or not all(
            isinstance(capability, str) for capability in capabilities_value
        ):
            self._protocol_result_failure(
                "The adapter returned invalid capability information."
            )
        capabilities = tuple(cast(list[str], capabilities_value))
        if capabilities != CAPABILITIES:
            self._protocol_result_failure(
                "The adapter does not provide the required capability set."
            )

        try:
            actual_root = _canonical_path_spelling(
                _require_native_path(
                    _require_string(self, result, "rootPath"),
                    "rootPath",
                    self._platform_name,
                    absolute=True,
                )
            )
            actual_cache = _canonical_path_spelling(
                _require_native_path(
                    _require_string(self, result, "cachePath"),
                    "cachePath",
                    self._platform_name,
                    absolute=True,
                )
            )
            actual_identity = _validate_directory_identity_object(
                result["rootIdentity"]
            )
            actual_root_path, local_root_identity = _snapshot_real_directory(
                Path(actual_root), "adopted native root"
            )
            if (
                str(actual_root_path) != actual_root
                or _root_identity_object(local_root_identity) != actual_identity
            ):
                raise NativeVirtualFileProcessError(
                    "Native virtual root identity changed",
                    "The adapter returned a different adopted root identity.",
                )
            _require_disjoint_paths(Path(actual_root), Path(actual_cache))
            _require_disjoint_paths(Path(binding.source_root_path), Path(actual_cache))
            _require_disjoint_paths(Path(actual_root), Path(binding.source_cache_path))
            _actual_cache_path, actual_cache_identity = (
                _require_existing_private_cache_path(
                    Path(actual_cache), None, self._platform_name
                )
            )
        except (NativeVirtualFileProcessError, TypeError, ValueError):
            self._protocol_result_failure(
                "The adapter returned an invalid native root binding."
            )

        completed = _NativeRootBinding(
            root_marker_id=binding.root_marker_id,
            source_root_path=binding.source_root_path,
            source_root_identity=binding.source_root_identity,
            source_cache_path=binding.source_cache_path,
            source_cache_identity=binding.source_cache_identity,
            visible_root_path=actual_root,
            cache_path=actual_cache,
            root_identity=actual_identity,
            cache_identity=actual_cache_identity,
            state=("accepted" if binding.state == "accepted" else "registered"),
            detached_root_path=None,
        )
        if binding.state in {"registered", "accepted"}:
            comparable = (
                replace(completed, root_identity=binding.root_identity)
                if self._platform_name == "Linux"
                else completed
            )
            if comparable != binding:
                self._protocol_result_failure(
                    "The adapter returned a different saved native root binding."
                )
        return completed, capabilities

    def _native_record(self, value: object, context: str) -> NativeFileRecord:
        if not isinstance(value, dict):
            self._protocol_result_failure(f"The adapter returned an invalid {context}.")
        record = cast(dict[str, object], value)
        _require_exact_keys(
            self,
            record,
            {
                "identity",
                "hydratedRevision",
                "pinned",
                "dirty",
                "openCount",
            },
            context,
        )
        identity_value = record["identity"]
        if not isinstance(identity_value, dict):
            self._protocol_result_failure(
                "The adapter returned an invalid native identity."
            )
        identity_object = cast(dict[str, object], identity_value)
        _require_exact_keys(
            self,
            identity_object,
            {"providerId", "path", "isDirectory", "revision"},
            "native identity",
        )
        provider_id = _require_string(self, identity_object, "providerId")
        revision = _require_string(self, identity_object, "revision")
        path = _require_string(self, identity_object, "path")
        is_directory = _require_boolean(identity_object, "isDirectory", self)
        hydrated_revision_value = record["hydratedRevision"]
        if hydrated_revision_value is not None and not isinstance(
            hydrated_revision_value, str
        ):
            self._protocol_result_failure(
                "The adapter returned an invalid hydrated revision."
            )
        try:
            _validate_identifier(provider_id, "providerId")
            _validate_identifier(revision, "revision")
            _validate_provider_path(path)
            if isinstance(hydrated_revision_value, str):
                _validate_identifier(hydrated_revision_value, "hydratedRevision")
        except (TypeError, ValueError):
            self._protocol_result_failure(
                "The adapter returned invalid native file identity data."
            )
        pinned = _require_boolean(record, "pinned", self)
        dirty = _require_boolean(record, "dirty", self)
        open_count = record["openCount"]
        if (
            isinstance(open_count, bool)
            or not isinstance(open_count, int)
            or not 0 <= open_count <= 0xFFFFFFFF
        ):
            self._protocol_result_failure(
                "The adapter returned an invalid native open count."
            )
        return NativeFileRecord(
            identity=VirtualFileIdentity(
                provider_id=provider_id,
                path=path,
                is_directory=is_directory,
                revision=revision,
            ),
            hydrated_revision=hydrated_revision_value,
            pinned=pinned,
            dirty=dirty,
            open_count=open_count,
        )

    def _discard_request(
        self,
        request_id: str,
        future: concurrent.futures.Future[dict[str, object]],
        session: int,
    ) -> None:
        with self._state_lock:
            if session == self._session and self._pending.get(request_id) is future:
                self._pending.pop(request_id)
                future.cancel()

    def _fail_session(
        self,
        error: BaseException,
        session: int,
        *,
        terminate: bool,
    ) -> None:
        with self._state_lock:
            if session != self._session:
                return
            if isinstance(error, MaestralApiError) and self._fatal_error is None:
                self._fatal_error = error
            self._started = False
            pending = list(self._pending.values())
            self._pending.clear()
            process = self._process
        for future in pending:
            if not future.done():
                future.set_exception(error)
        if terminate and process is not None:
            _terminate_process(process)

    def _stop_process(self) -> None:
        with self._state_lock:
            process = self._process
            session = self._session
            self._closing = True
        if process is None:
            with self._state_lock:
                self._closing = False
                self._started = False
            return

        stream = process.stdin
        if stream is not None:
            try:
                stream.close()
            except OSError:
                pass
        _terminate_process(process, wait_first=True)

        self._fail_session(
            self._process_error("The native virtual-file adapter was stopped."),
            session,
            terminate=False,
        )
        current = threading.current_thread()
        for thread in (self._stdout_thread, self._stderr_thread):
            if thread is not None and thread is not current:
                thread.join(timeout=1.0)
        with self._state_lock:
            if process is self._process:
                self._process = None
                self._stdout_thread = None
                self._stderr_thread = None
                self._pending.clear()
                self._inbound_ids.clear()
                self._fatal_error = None
                self._closing = False
                self._started = False

    def _process_error(self, message: str) -> NativeVirtualFileProcessError:
        tail = self.stderr_tail.strip()
        detail = message if not tail else f"{message} Adapter diagnostics: {tail}"
        return NativeVirtualFileProcessError(
            "Native virtual-file adapter failed", detail
        )

    def _protocol_error(self, message: str) -> NativeVirtualFileProtocolError:
        return NativeVirtualFileProtocolError(
            "Native virtual-file protocol failed", message
        )

    def _protocol_failure(self, message: str, session: int) -> None:
        self._fail_session(self._protocol_error(message), session, terminate=True)

    def _protocol_result_failure(self, message: str) -> NoReturn:
        with self._state_lock:
            session = self._session
        error = self._protocol_error(message)
        self._fail_session(error, session, terminate=True)
        raise error


def create_process_virtual_file_backend(config_name: str) -> VirtualFileBackend:
    """Return the native process backend, or an inactive backend when unavailable."""
    if platform.system() == "Windows":
        return UnsupportedVirtualFileBackend(config_name)
    try:
        executable = resolve_native_virtual_file_executable()
        return NativeProcessVirtualFileBackend(config_name, executable)
    except NativeVirtualFileProcessError:
        return UnsupportedVirtualFileBackend(config_name)


def resolve_native_virtual_file_executable(
    configured_path: str | os.PathLike[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    platform_name: str | None = None,
) -> Path:
    """Resolve only the exact adapter executable for the current platform."""
    selected_platform = platform.system() if platform_name is None else platform_name
    try:
        executable_name, _adapter_id = PLATFORM_ADAPTERS[selected_platform]
    except KeyError:
        raise NativeVirtualFileProcessError(
            "Native virtual files are unavailable",
            f"{selected_platform} has no native virtual-file adapter.",
        ) from None

    if configured_path is not None and os.fspath(configured_path).strip():
        return _require_executable(
            configured_path,
            selected_platform,
            expected_name=executable_name,
        )

    environment = os.environ if environ is None else environ
    environment_path = environment.get("MAESTRAL_SMART_SYNC_ADAPTER", "").strip()
    if environment_path:
        return _require_executable(
            environment_path,
            selected_platform,
            expected_name=executable_name,
        )

    executable_dir = Path(sys.executable).resolve().parent
    package_bases = [
        executable_dir,
        executable_dir.parent / "Resources",
        Path(__file__).resolve().parent / "resources",
    ]
    frozen_root = getattr(sys, "_MEIPASS", None)
    if isinstance(frozen_root, str):
        package_bases.append(Path(frozen_root))

    seen: set[Path] = set()
    for base in package_bases:
        for candidate in (
            base / "smart-sync" / executable_name,
            base / executable_name,
        ):
            if candidate in seen:
                continue
            seen.add(candidate)
            if candidate.is_file():
                return _require_executable(
                    candidate,
                    selected_platform,
                    expected_name=executable_name,
                )

    found = shutil.which(executable_name, path=environment.get("PATH", ""))
    if found and os.path.isabs(found):
        return _require_executable(
            found,
            selected_platform,
            expected_name=executable_name,
        )

    raise NativeVirtualFileProcessError(
        "Native virtual files are unavailable",
        f"The {executable_name} adapter is not installed.",
    )


def _require_executable(
    path_value: str | os.PathLike[str],
    platform_name: str,
    *,
    expected_name: str | None = None,
) -> Path:
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        raise NativeVirtualFileProcessError(
            "Native virtual files are unavailable",
            "The native adapter path must be absolute.",
        )
    if expected_name is not None:
        actual_name = path.name
        names_match = (
            actual_name.casefold() == expected_name.casefold()
            if platform_name == "Windows"
            else actual_name == expected_name
        )
        if not names_match:
            raise NativeVirtualFileProcessError(
                "Native virtual files are unavailable",
                f"The native adapter must be named {expected_name}.",
            )
    try:
        resolved = path.resolve(strict=True)
    except OSError:
        raise NativeVirtualFileProcessError(
            "Native virtual files are unavailable",
            "The native adapter executable does not exist.",
        ) from None
    if not resolved.is_file():
        raise NativeVirtualFileProcessError(
            "Native virtual files are unavailable",
            "The native adapter path is not a file.",
        )
    if platform_name != "Windows" and not os.access(resolved, os.X_OK):
        raise NativeVirtualFileProcessError(
            "Native virtual files are unavailable",
            "The native adapter file is not executable.",
        )
    return resolved


def _invalid_saved_root_binding() -> NativeVirtualFileProcessError:
    return NativeVirtualFileProcessError(
        "Native virtual root is unavailable",
        "The saved native root binding is missing or invalid.",
    )


def _validate_stored_native_path(value: object, name: str, platform_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} is invalid")
    path = _require_native_path(value, name, platform_name, absolute=True)
    canonical = _canonical_path_spelling(path)
    if value != canonical:
        raise ValueError(f"{name} is not canonical")
    return canonical


def _require_existing_private_cache_path(
    path: Path,
    expected_identity: dict[str, object] | None,
    platform_name: str,
) -> tuple[Path, dict[str, object]]:
    canonical_input = Path(_canonical_path_spelling(str(path.expanduser())))
    try:
        canonical_path, identity = _snapshot_real_directory(
            canonical_input, "native cache"
        )
        status = os.lstat(canonical_path)
    except NativeVirtualFileProcessError:
        raise NativeVirtualFileProcessError(
            "Native virtual-file cache is unavailable",
            "The saved private native cache is missing or unsafe.",
        ) from None
    if (
        not stat.S_ISDIR(status.st_mode)
        or is_fs_link(status)
        or (hasattr(os, "getuid") and status.st_uid != os.getuid())
        or (platform_name != "Windows" and stat.S_IMODE(status.st_mode) != 0o700)
    ):
        raise NativeVirtualFileProcessError(
            "Native virtual-file cache is unsafe",
            "The saved private native cache has unsafe access or permissions.",
        )
    actual_identity = _root_identity_object(identity)
    if expected_identity is not None and actual_identity != expected_identity:
        raise NativeVirtualFileProcessError(
            "Native virtual-file cache identity changed",
            "The saved private native cache was replaced.",
        )
    return canonical_path, actual_identity


def _prepare_private_cache_path(
    path: Path, platform_name: str
) -> tuple[Path, dict[str, object]]:
    path = Path(_canonical_path_spelling(str(path.expanduser())))
    try:
        created_identity = rooted_makedirs(str(path), mode=0o700, exist_ok=True)
        canonical_path, snapshot_identity = _snapshot_real_directory(
            path, "native cache"
        )
        if snapshot_identity != created_identity:
            raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), str(path))
        status = os.lstat(canonical_path)
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.ENOTDIR, errno.ESTALE}:
            raise NativeVirtualFileProcessError(
                "Native virtual-file cache is unsafe",
                "The native cache must be a real directory with no linked ancestor.",
            ) from None
        raise NativeVirtualFileProcessError(
            "Native virtual-file cache is unavailable",
            "The private native cache directory could not be created.",
        ) from None
    if not stat.S_ISDIR(status.st_mode):
        raise NativeVirtualFileProcessError(
            "Native virtual-file cache is unsafe",
            "The native cache path is not a directory.",
        )
    if hasattr(os, "getuid") and status.st_uid != os.getuid():
        raise NativeVirtualFileProcessError(
            "Native virtual-file cache is unsafe",
            "The current user does not own the native cache directory.",
        )
    if platform_name != "Windows":
        try:
            flags = (
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0)
            )
            descriptor = os.open(canonical_path, flags)
            try:
                opened = os.fstat(descriptor)
                if (opened.st_dev, opened.st_ino, opened.st_mode) != snapshot_identity:
                    raise OSError(
                        errno.ESTALE, os.strerror(errno.ESTALE), str(canonical_path)
                    )
                os.fchmod(descriptor, 0o700)
                status = os.fstat(descriptor)
            finally:
                os.close(descriptor)
            final_path, final_identity = _snapshot_real_directory(
                canonical_path, "native cache"
            )
            if final_path != canonical_path or final_identity != (
                status.st_dev,
                status.st_ino,
                status.st_mode,
            ):
                raise OSError(
                    errno.ESTALE, os.strerror(errno.ESTALE), str(canonical_path)
                )
            mode = stat.S_IMODE(status.st_mode)
        except OSError:
            raise NativeVirtualFileProcessError(
                "Native virtual-file cache is unsafe",
                "The native cache permissions could not be restricted.",
            ) from None
        if mode & 0o077:
            raise NativeVirtualFileProcessError(
                "Native virtual-file cache is unsafe",
                "The native cache grants access to another user.",
            )
    return canonical_path, _root_identity_object(
        (status.st_dev, status.st_ino, status.st_mode)
    )


def _canonical_path_spelling(value: str) -> str:
    return os.path.normpath(os.path.abspath(os.path.expanduser(value)))


def _snapshot_real_directory(
    path: Path, label: str
) -> tuple[Path, tuple[int, int, int]]:
    canonical_path = Path(_canonical_path_spelling(str(path)))
    absolute_path = str(canonical_path)
    drive, _tail = os.path.splitdrive(absolute_path)
    volume_root = drive + os.path.sep if drive else os.path.abspath(os.path.sep)
    try:
        volume_stat = os.lstat(volume_root)
        if not stat.S_ISDIR(volume_stat.st_mode) or is_fs_link(volume_stat):
            raise NotADirectoryError(volume_root)
        snapshot = rooted_item_snapshot(
            absolute_path,
            volume_root,
            expected_root_identity=(
                volume_stat.st_dev,
                volume_stat.st_ino,
                volume_stat.st_mode,
            ),
        )
    except (FileNotFoundError, NotADirectoryError, OSError):
        raise NativeVirtualFileProcessError(
            "Native virtual-file path is unsafe",
            f"The {label} must be a real directory with no linked ancestor.",
        ) from None
    if not stat.S_ISDIR(snapshot[2]) or snapshot[6] is not None:
        raise NativeVirtualFileProcessError(
            "Native virtual-file path is unsafe",
            f"The {label} must be a real directory with no linked ancestor.",
        )
    return canonical_path, snapshot[:3]


def _require_disjoint_paths(first: Path, second: Path) -> None:
    first_value = os.path.normcase(_canonical_path_spelling(str(first)))
    second_value = os.path.normcase(_canonical_path_spelling(str(second)))
    try:
        common = os.path.commonpath((first_value, second_value))
    except ValueError:
        return
    if common in {first_value, second_value}:
        raise NativeVirtualFileProcessError(
            "Native virtual-file paths overlap",
            "The visible root and private native cache must be disjoint.",
        )


def _validate_root_marker_id(value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 32
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise NativeVirtualFileProcessError(
            "Native virtual root is unavailable",
            "The saved root marker identity is invalid.",
        )
    return value


def _root_id(config_name: str, root_marker_id: str) -> str:
    return f"maestral:{config_name}:{root_marker_id}"


def _root_marker_object(root_marker_id: str) -> dict[str, object]:
    return {
        "name": ROOT_MARKER_FILE,
        "content": f"maestral-root-v1:{root_marker_id}\n",
    }


def _root_identity_object(identity: tuple[int, int, int]) -> dict[str, object]:
    device, inode, mode = identity
    value = {"device": str(device), "inode": str(inode), "mode": mode}
    return _validate_root_identity_object(value)


def _validate_root_identity_object(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != {"device", "inode", "mode"}:
        raise ValueError("rootIdentity is invalid")
    identity = cast(dict[str, object], value)
    for key in ("device", "inode"):
        number = identity[key]
        if (
            not isinstance(number, str)
            or not number
            or len(number.encode("ascii", errors="ignore")) > MAX_ROOT_NUMBER_BYTES
            or not number.isascii()
            or not number.isdecimal()
            or (len(number) > 1 and number.startswith("0"))
        ):
            raise ValueError("rootIdentity is invalid")
    mode = identity["mode"]
    if (
        isinstance(mode, bool)
        or not isinstance(mode, int)
        or mode < 0
        or mode > MAX_ROOT_MODE
    ):
        raise ValueError("rootIdentity is invalid")
    return {"device": identity["device"], "inode": identity["inode"], "mode": mode}


def _validate_directory_identity_object(value: object) -> dict[str, object]:
    identity = _validate_root_identity_object(value)
    if not stat.S_ISDIR(cast(int, identity["mode"])):
        raise ValueError("rootIdentity is not a directory")
    return identity


def _require_private_stage_file(
    path_value: str, platform_name: str
) -> tuple[Path, os.stat_result]:
    value = _require_native_path(path_value, "stagedPath", platform_name, absolute=True)
    canonical = _canonical_path_spelling(value)
    if value != canonical:
        raise NativeVirtualFileProtocolError(
            "Could not materialise this file",
            "The staged path is not canonical.",
        )
    path = Path(canonical)
    try:
        status = os.lstat(path)
    except OSError:
        raise VirtualFileNotFoundError(
            "Could not materialise this file", "The private staged file is missing."
        ) from None
    if is_fs_link(status) or not stat.S_ISREG(status.st_mode):
        raise NativeVirtualFileProtocolError(
            "Could not materialise this file",
            "The staged path is not a regular file.",
        )
    if hasattr(os, "getuid") and status.st_uid != os.getuid():
        raise NativeVirtualFileProtocolError(
            "Could not materialise this file",
            "The current user does not own the staged file.",
        )
    if platform_name != "Windows" and stat.S_IMODE(status.st_mode) != 0o600:
        raise NativeVirtualFileProtocolError(
            "Could not materialise this file", "The staged file mode must be 0600."
        )
    return path, status


def _staged_identity_object(status: os.stat_result) -> dict[str, object]:
    if not 0 <= status.st_size < 2**64:
        raise NativeVirtualFileProtocolError(
            "Could not materialise this file", "The staged file size is invalid."
        )
    identity = _root_identity_object((status.st_dev, status.st_ino, status.st_mode))
    return {
        "device": identity["device"],
        "inode": identity["inode"],
        "size": status.st_size,
        "mode": identity["mode"],
    }


def _item_object(item: VirtualFileDescriptor) -> dict[str, object]:
    if not isinstance(item, VirtualFileDescriptor):
        raise TypeError("item must be a VirtualFileDescriptor")
    _validate_identifier(item.provider_id, "providerId")
    _validate_provider_path(item.path)
    _validate_identifier(item.revision, "revision")
    if item.content_hash is not None:
        _validate_identifier(item.content_hash, "contentHash")
    if item.symlink_target is not None:
        if (
            not isinstance(item.symlink_target, str)
            or not item.symlink_target
            or len(item.symlink_target.encode("utf-8")) > MAX_PATH_BYTES
            or _has_control_character(item.symlink_target)
        ):
            raise ValueError("symlinkTarget is invalid")
    if not isinstance(item.is_directory, bool) or not isinstance(item.pinned, bool):
        raise TypeError("Virtual-file flags must be boolean values")
    if (
        isinstance(item.size, bool)
        or not isinstance(item.size, int)
        or not 0 <= item.size < 2**64
    ):
        raise ValueError("The virtual-file size is invalid")
    if item.is_directory and (
        item.size != 0
        or item.content_hash is not None
        or item.symlink_target is not None
    ):
        raise ValueError("A virtual directory has invalid content metadata")
    if item.symlink_target is not None and (
        item.size != 0 or item.content_hash is not None
    ):
        raise ValueError("A virtual symbolic link has invalid content metadata")
    return {
        "providerId": item.provider_id,
        "path": item.path,
        "isDirectory": item.is_directory,
        "revision": item.revision,
        "contentHash": item.content_hash,
        "size": item.size,
        "symlinkTarget": item.symlink_target,
        "pinned": item.pinned,
    }


def _identity_object(identity: VirtualFileIdentity) -> dict[str, object]:
    if not isinstance(identity, VirtualFileIdentity):
        raise TypeError("expected_identity must be a VirtualFileIdentity")
    _validate_identifier(identity.provider_id, "providerId")
    _validate_provider_path(identity.path)
    if not isinstance(identity.is_directory, bool):
        raise TypeError("isDirectory must be a boolean value")
    _validate_identifier(identity.revision, "revision")
    return {
        "providerId": identity.provider_id,
        "path": identity.path,
        "isDirectory": identity.is_directory,
        "revision": identity.revision,
    }


def _validate_adapter_request(
    request: dict[str, object],
    platform_name: str,
) -> tuple[str, dict[str, object]]:
    if set(request) != {"protocolVersion", "requestId", "method", "params"}:
        raise _InvalidMessage
    if type(request["protocolVersion"]) is not int or request["protocolVersion"] != 1:
        raise _InvalidMessage
    method = request["method"]
    params = request["params"]
    if not isinstance(method, str) or not isinstance(params, dict):
        raise _InvalidMessage
    params = cast(dict[str, object], params)

    if method == "hydrate":
        _require_request_keys(params, {"providerId", "expectedRevision"})
        _validate_identifier(params["providerId"], "providerId")
        _validate_identifier(params["expectedRevision"], "expectedRevision")
    elif method == "enumerate":
        _require_request_keys(params, {"parentProviderId", "cursor", "limit"})
        _validate_identifier(params["parentProviderId"], "parentProviderId")
        cursor = params["cursor"]
        if cursor is not None:
            _validate_identifier(cursor, "cursor")
        limit = params["limit"]
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 4096
        ):
            raise _InvalidMessage
    elif method == "dehydrate":
        _require_request_keys(params, {"providerId"})
        _validate_identifier(params["providerId"], "providerId")
    elif method == "pin_changed":
        _require_request_keys(params, {"providerId", "pinned"})
        _validate_identifier(params["providerId"], "providerId")
        if not isinstance(params["pinned"], bool):
            raise _InvalidMessage
    elif method == "local_change":
        _validate_local_change(params, platform_name)
    elif method in {
        "start",
        "stop",
        "detach",
        "upsert",
        "remove",
        "set_pinned",
        "materialize",
        "evict",
        "inspect",
        "recover",
    }:
        pass
    else:
        raise _InvalidMessage
    return method, params


def _validate_local_change(params: dict[str, object], platform_name: str) -> None:
    _require_request_keys(
        params, {"kind", "providerId", "path", "previousPath", "stagedPath"}
    )
    kind = params["kind"]
    if kind not in {"created", "modified", "deleted", "moved"}:
        raise _InvalidMessage
    provider_id = params["providerId"]
    if provider_id is not None:
        _validate_identifier(provider_id, "providerId")
    if kind != "created" and provider_id is None:
        raise _InvalidMessage
    path = params["path"]
    if not isinstance(path, str):
        raise _InvalidMessage
    _validate_provider_path(path)
    previous_path = params["previousPath"]
    if previous_path is not None:
        if not isinstance(previous_path, str):
            raise _InvalidMessage
        _validate_provider_path(previous_path)
    if (kind == "moved") != (previous_path is not None):
        raise _InvalidMessage
    staged_path = params["stagedPath"]
    if staged_path is not None:
        if not isinstance(staged_path, str) or kind not in {"created", "modified"}:
            raise _InvalidMessage
        _require_native_path(staged_path, "stagedPath", platform_name, absolute=True)


def _require_request_keys(params: dict[str, object], keys: set[str]) -> None:
    if set(params) != keys:
        raise _InvalidMessage


def _encode_message(message: dict[str, object]) -> bytes:
    try:
        payload = (
            json.dumps(
                message,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
        )
    except (TypeError, ValueError, UnicodeError):
        raise NativeVirtualFileProtocolError(
            "Native virtual-file protocol failed",
            "The core produced invalid protocol data.",
        ) from None
    if len(payload) > MAX_LINE_BYTES:
        raise NativeVirtualFileProtocolError(
            "Native virtual-file protocol failed",
            "The core produced an oversized protocol message.",
        )
    return payload


def _decode_message(payload: bytes) -> object:
    def reject_constant(value: str) -> None:
        raise ValueError(f"Invalid JSON constant: {value}")

    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result

    try:
        text = payload.decode("utf-8")
        return cast(
            object,
            json.loads(
                text,
                parse_constant=reject_constant,
                object_pairs_hook=reject_duplicate_keys,
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError):
        raise NativeVirtualFileProtocolError(
            "Native virtual-file protocol failed",
            "The adapter returned invalid JSON protocol data.",
        ) from None


def _validate_identifier(value: object, name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > MAX_IDENTIFIER_BYTES
        or _has_control_character(value)
    ):
        raise ValueError(f"{name} is invalid")


def _validate_timeout(value: object) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
        or value > MAX_REQUEST_TIMEOUT
    ):
        raise ValueError(
            f"The request timeout must be between zero and {MAX_REQUEST_TIMEOUT}"
        )
    return float(value)


def _require_native_path(
    value: object,
    name: str,
    platform_name: str,
    *,
    absolute: bool,
) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > MAX_PATH_BYTES
        or _has_control_character(value)
    ):
        raise ValueError(f"{name} is invalid")
    if absolute:
        pure_path = (
            PureWindowsPath(value)
            if platform_name == "Windows"
            else PurePosixPath(value)
        )
        if not pure_path.is_absolute():
            raise ValueError(f"{name} must be absolute")
    return value


def _validate_provider_path(value: str) -> None:
    if value == "/":
        return
    parts = value.split("/")[1:]
    if (
        not value
        or len(value.encode("utf-8")) > MAX_PATH_BYTES
        or not value.startswith("/")
        or value.endswith("/")
        or _has_control_character(value)
        or any(part in {"", ".", ".."} for part in parts)
        or (
            bool(parts)
            and (
                parts[0] == ROOT_MARKER_FILE or parts[0].startswith(".~maestral-root-")
            )
        )
    ):
        raise ValueError("The provider path is not canonical")


def _has_control_character(value: str) -> bool:
    return any(unicodedata.category(character) == "Cc" for character in value)


def _require_exact_keys(
    backend: NativeProcessVirtualFileBackend,
    value: dict[str, object],
    keys: set[str],
    context: str,
) -> None:
    if set(value) != keys:
        backend._protocol_result_failure(
            f"The adapter returned an invalid {context}.",
        )


def _require_string(
    backend: NativeProcessVirtualFileBackend,
    value: dict[str, object],
    key: str,
) -> str:
    item = value.get(key)
    if not isinstance(item, str):
        backend._protocol_result_failure(
            "The adapter returned an invalid string field.",
        )
    return item


def _require_boolean(
    value: dict[str, object], key: str, backend: NativeProcessVirtualFileBackend
) -> bool:
    item = value.get(key)
    if not isinstance(item, bool):
        backend._protocol_result_failure(
            "The adapter returned an invalid boolean field."
        )
    return item


def _require_empty_result(
    backend: NativeProcessVirtualFileBackend,
    result: dict[str, object],
    method: str,
) -> None:
    if result:
        backend._protocol_result_failure(
            f"The adapter returned an invalid {method} result."
        )


def _map_adapter_error(code: str, message: str) -> MaestralApiError:
    if code == "NOT_FOUND":
        return VirtualFileNotFoundError("Virtual file not found", message)
    if code == "REVISION_MISMATCH":
        return VirtualFileRevisionError("Virtual file revision changed", message)
    if code == "BUSY":
        return VirtualFileBusyError("Virtual file is busy", message)
    if code == "UNSUPPORTED":
        return VirtualFilesUnsupportedError(
            "Native virtual-file operation is unavailable", message
        )
    return NativeVirtualFileRequestError(code, message)


def _adapter_error_for_exception(exc: BaseException) -> tuple[str, str]:
    if isinstance(exc, VirtualFileNotFoundError):
        return "NOT_FOUND", "The requested provider identity does not exist"
    if isinstance(exc, VirtualFileRevisionError):
        return "REVISION_MISMATCH", "The requested remote revision changed"
    if isinstance(exc, VirtualFileBusyError):
        return "BUSY", "The requested virtual file is busy"
    if isinstance(exc, VirtualFilesUnsupportedError):
        return "UNSUPPORTED", "The requested core operation is unavailable"
    if isinstance(exc, OSError):
        return "IO_ERROR", "The core could not read the requested remote content"
    return "INTERNAL", "The core could not hydrate the requested file"


def _terminate_process(
    process: subprocess.Popen[bytes], *, wait_first: bool = False
) -> None:
    if process.poll() is not None:
        return
    if wait_first:
        try:
            process.wait(timeout=1.0)
            return
        except subprocess.TimeoutExpired:
            pass
    try:
        process.terminate()
        process.wait(timeout=1.0)
        return
    except (OSError, subprocess.TimeoutExpired):
        pass
    try:
        process.kill()
    except OSError:
        return
    try:
        process.wait(timeout=1.0)
    except (OSError, subprocess.TimeoutExpired):
        pass
