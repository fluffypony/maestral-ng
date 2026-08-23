"""Client for Maestral's isolated Cryptomator sidecar process."""

from __future__ import annotations

# system imports
import base64
import binascii
import builtins
import concurrent.futures
import json
import os
import shutil
import stat as stat_module
import subprocess
import sys
import tempfile
import threading
import uuid
from dataclasses import dataclass
from functools import wraps
from pathlib import Path, PurePosixPath
from types import TracebackType
from typing import (
    Any,
    Callable,
    Concatenate,
    Literal,
    Mapping,
    ParamSpec,
    TypeAlias,
    TypeVar,
    cast,
)

__all__ = [
    "CryptomatorClient",
    "CryptomatorError",
    "CryptomatorProcessError",
    "CryptomatorProtocolError",
    "CryptomatorRequestError",
    "CryptomatorTimeoutError",
    "SidecarHello",
    "StorageMapEntry",
    "VaultEntry",
    "VaultInfo",
    "resolve_sidecar_path",
]


PROTOCOL_VERSION = 1
SIDECAR_VERSION = "0.1.0"
CRYPTOFS_VERSION = "2.10.0"
CRYPTOLIB_VERSION = "2.2.2"
VAULT_FORMAT = 8
MAX_INLINE_BYTES = 1024 * 1024
MAX_SECRET_BYTES = 4096
MAX_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_STDERR_BYTES = 64 * 1024

SIDECAR_ERROR_CODES = frozenset(
    {
        "access_denied",
        "already_exists",
        "authentication_failed",
        "content_too_large",
        "directory_not_empty",
        "insecure_exchange_root",
        "internal_error",
        "invalid_exchange_path",
        "invalid_exchange_root",
        "invalid_json",
        "invalid_key_id",
        "invalid_passphrase",
        "invalid_path",
        "invalid_request",
        "invalid_secret",
        "invalid_utf8",
        "invalid_vault",
        "invalid_vault_path",
        "io_error",
        "not_directory",
        "not_found",
        "overlapping_roots",
        "request_too_large",
        "storage_mapping_ambiguous",
        "storage_mapping_missing",
        "unknown_method",
        "vault_already_open",
        "vault_not_empty",
        "vault_not_open",
        "vault_too_large",
    }
)


JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]
JSONObject: TypeAlias = dict[str, JSONValue]
P = ParamSpec("P")
T = TypeVar("T")


class CryptomatorError(RuntimeError):
    """Base class for failures in the Cryptomator sidecar client."""


class CryptomatorProcessError(CryptomatorError):
    """Raised when the sidecar cannot start or exits unexpectedly."""


class CryptomatorProtocolError(CryptomatorError):
    """Raised when the sidecar violates the expected protocol."""


class CryptomatorTimeoutError(CryptomatorError):
    """Raised when a sidecar request does not finish in time."""


class CryptomatorRequestError(CryptomatorError):
    """Raised when the sidecar rejects a valid request.

    The sidecar's message is intentionally not copied into this exception. This keeps
    secret material out of errors even if a faulty sidecar echoes its input.
    """

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"The Cryptomator sidecar rejected the request ({code}).")


def _fail_on_protocol_error(
    method: Callable[Concatenate["CryptomatorClient", P], T],
) -> Callable[Concatenate["CryptomatorClient", P], T]:
    """Stop the process when a typed result violates the protocol."""

    @wraps(method)
    def wrapped(self: CryptomatorClient, /, *args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return method(self, *args, **kwargs)
        except CryptomatorProtocolError as exc:
            self._fail_all(exc)
            raise

    return wrapped


@dataclass(frozen=True, slots=True)
class SidecarHello:
    """Version and capability information returned by the sidecar."""

    protocol_version: int
    sidecar_version: str
    cryptofs_version: str
    cryptolib_version: str
    vault_format: int
    max_inline_bytes: int
    vault_open: bool


@dataclass(frozen=True, slots=True)
class VaultInfo:
    """Information about an open Cryptomator vault."""

    vault_path: Path
    vault_format: int
    shortening_threshold: int
    key_id: str


VaultEntryType = Literal["file", "directory", "symlink", "other"]


@dataclass(frozen=True, slots=True)
class VaultEntry:
    """Metadata for a cleartext path in a Cryptomator vault."""

    path: str
    type: VaultEntryType
    size: int
    modified_ms: int
    sha256: str | None = None
    link_target: str | None = None


StorageEntryType = Literal["file", "directory", "symlink"]


@dataclass(frozen=True, slots=True)
class StorageMapEntry:
    """Mapping from a cleartext path to its physical vault path."""

    path: str
    type: StorageEntryType
    storage_path: str


class CryptomatorClient:
    """Thread-safe client for the official Cryptomator sidecar.

    ``sidecar_path`` names either a native executable or a runnable JAR. A JAR is
    started with ``java -jar``. ``executable_arguments`` exists for launch wrappers and
    tests. A vault password is never added to this command or the child environment.

    :param sidecar_path: Absolute path to the sidecar executable or JAR.
    :param executable_arguments: Arguments placed before ``--exchange-root``.
    :param java_executable: Java executable used for a JAR. A command name is resolved
        to an absolute path before launch.
    :param exchange_root: Private data exchange directory. A temporary directory is
        created and removed when omitted.
    :param request_timeout: Default timeout for one protocol request, in seconds.
    """

    def __init__(
        self,
        sidecar_path: str | os.PathLike[str],
        *,
        executable_arguments: tuple[str, ...] = (),
        java_executable: str | os.PathLike[str] = "java",
        exchange_root: str | os.PathLike[str] | None = None,
        request_timeout: float = 30.0,
    ) -> None:
        if request_timeout <= 0:
            raise ValueError("The request timeout must be greater than zero")

        self._sidecar_path = _require_file(sidecar_path, "sidecar")
        self._request_timeout = request_timeout
        self._exchange_root, self._owns_exchange_root = _prepare_exchange_root(
            exchange_root
        )
        self._input_root = self._exchange_root / "input"
        self._output_root = self._exchange_root / "output"

        try:
            _make_private_directory(self._input_root)
            _make_private_directory(self._output_root)
            command = self._build_command(java_executable, executable_arguments)
            self._process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
                close_fds=True,
            )
        except CryptomatorError:
            self._remove_owned_exchange_root()
            raise
        except OSError:
            self._remove_owned_exchange_root()
            raise CryptomatorProcessError(
                "The Cryptomator sidecar could not start."
            ) from None
        except BaseException:
            self._remove_owned_exchange_root()
            raise

        self._pending: dict[int, concurrent.futures.Future[JSONValue]] = {}
        self._next_request_id = 1
        self._state_lock = threading.RLock()
        self._write_lock = threading.Lock()
        self._stderr_lock = threading.Lock()
        self._stderr_tail = bytearray()
        self._fatal_error: CryptomatorError | None = None
        self._closing = False
        self._closed = False

        self._stdout_thread = threading.Thread(
            target=self._read_stdout,
            name="maestral-cryptomator-stdout",
            daemon=True,
        )
        self._stderr_thread = threading.Thread(
            target=self._read_stderr,
            name="maestral-cryptomator-stderr",
            daemon=True,
        )
        self._stdout_thread.start()
        self._stderr_thread.start()

        try:
            self._hello = self.hello()
            self._check_hello(self._hello)
        except BaseException:
            self.terminate()
            raise

    @property
    def exchange_root(self) -> Path:
        """Private directory used to exchange file content with the sidecar."""
        return self._exchange_root

    @property
    def hello_info(self) -> SidecarHello:
        """Validated capabilities reported when the sidecar started."""
        return self._hello

    @property
    def closed(self) -> bool:
        """Whether the sidecar client is closed."""
        with self._state_lock:
            return self._closed

    @_fail_on_protocol_error
    def hello(self, *, timeout: float | None = None) -> SidecarHello:
        """Return and validate the sidecar's version information."""
        result = _require_object(self._request("hello", {}, timeout=timeout), "hello")
        if set(result) != {
            "protocol_version",
            "sidecar_version",
            "cryptofs_version",
            "cryptolib_version",
            "vault_format",
            "max_inline_bytes",
            "vault_open",
        }:
            raise CryptomatorProtocolError(
                "The Cryptomator sidecar returned invalid version information."
            )
        return SidecarHello(
            protocol_version=_require_int(result, "protocol_version"),
            sidecar_version=_require_str(result, "sidecar_version"),
            cryptofs_version=_require_str(result, "cryptofs_version"),
            cryptolib_version=_require_str(result, "cryptolib_version"),
            vault_format=_require_int(result, "vault_format"),
            max_inline_bytes=_require_int(result, "max_inline_bytes"),
            vault_open=_require_bool(result, "vault_open"),
        )

    @_fail_on_protocol_error
    def initialize(
        self,
        vault_path: str | os.PathLike[str],
        password: str | bytes | bytearray,
        *,
        timeout: float | None = None,
    ) -> VaultInfo:
        """Create and open a format-8 vault."""
        return self._vault_info(
            self._secret_request("initialize", vault_path, password, timeout=timeout)
        )

    @_fail_on_protocol_error
    def open(
        self,
        vault_path: str | os.PathLike[str],
        password: str | bytes | bytearray,
        *,
        timeout: float | None = None,
    ) -> VaultInfo:
        """Open an existing format-8 vault."""
        return self._vault_info(
            self._secret_request("open", vault_path, password, timeout=timeout)
        )

    @_fail_on_protocol_error
    def close_vault(self, *, timeout: float | None = None) -> None:
        """Close the current vault without stopping the sidecar."""
        self._require_ok(self._request("close", {}, timeout=timeout))

    @_fail_on_protocol_error
    def vault_info(self, *, timeout: float | None = None) -> VaultInfo:
        """Return information about the open vault."""
        return self._vault_info(self._request("vault_info", {}, timeout=timeout))

    @_fail_on_protocol_error
    def stat(
        self,
        path: str,
        *,
        include_hash: bool = False,
        timeout: float | None = None,
    ) -> VaultEntry:
        """Return metadata for a cleartext path."""
        result = self._request(
            "stat", {"path": path, "include_hash": include_hash}, timeout=timeout
        )
        return self._vault_entry(result)

    @_fail_on_protocol_error
    def list(
        self,
        path: str,
        *,
        include_hash: bool = False,
        timeout: float | None = None,
    ) -> builtins.list[VaultEntry]:
        """Return the direct children of a cleartext directory."""
        result = _require_list(
            self._request(
                "list", {"path": path, "include_hash": include_hash}, timeout=timeout
            ),
            "list",
        )
        return [self._vault_entry(item) for item in result]

    @_fail_on_protocol_error
    def snapshot(
        self,
        *,
        include_hash: bool = False,
        timeout: float | None = None,
    ) -> builtins.list[VaultEntry]:
        """Return metadata for all cleartext paths in the vault."""
        result = _require_list(
            self._request("snapshot", {"include_hash": include_hash}, timeout=timeout),
            "snapshot",
        )
        return [self._vault_entry(item) for item in result]

    @_fail_on_protocol_error
    def storage_map(
        self, *, timeout: float | None = None
    ) -> builtins.list[StorageMapEntry]:
        """Map cleartext paths to relative physical paths in the vault."""
        result = _require_list(
            self._request("storage_map", {}, timeout=timeout), "storage_map"
        )
        entries = [self._storage_map_entry(item) for item in result]
        if entries != sorted(entries, key=lambda entry: entry.path.encode("utf-16-be")):
            raise CryptomatorProtocolError(
                "The Cryptomator sidecar returned an unsorted storage map."
            )
        if len({entry.path for entry in entries}) != len(entries):
            raise CryptomatorProtocolError(
                "The Cryptomator sidecar returned duplicate storage paths."
            )
        return entries

    @_fail_on_protocol_error
    def mkdir(
        self,
        path: str,
        *,
        parents: bool = False,
        timeout: float | None = None,
    ) -> None:
        """Create a cleartext directory in the vault."""
        result = self._request(
            "mkdir", {"path": path, "parents": parents}, timeout=timeout
        )
        self._require_ok(result)

    @_fail_on_protocol_error
    def put_file(
        self,
        path: str,
        source_path: str | os.PathLike[str],
        *,
        modified_ms: int | None = None,
        replace: bool = False,
        timeout: float | None = None,
    ) -> None:
        """Copy a local file into the vault through a private staging file."""
        relative, staged = self._new_stage_path(self._input_root, "input")
        try:
            self._copy_to_stage(Path(source_path), staged)
            params: JSONObject = {
                "path": path,
                "exchange_path": relative,
                "replace": replace,
            }
            if modified_ms is not None:
                params["modified_ms"] = modified_ms
            self._require_ok(self._request("put_file", params, timeout=timeout))
        finally:
            staged.unlink(missing_ok=True)

    @_fail_on_protocol_error
    def get_file(
        self,
        path: str,
        destination_path: str | os.PathLike[str],
        *,
        replace: bool = False,
        timeout: float | None = None,
    ) -> None:
        """Copy a vault file to a local path through a private staging file."""
        destination = Path(destination_path)
        if destination.exists() and not replace:
            raise FileExistsError(destination)
        if not destination.parent.is_dir():
            raise FileNotFoundError(destination.parent)

        relative, staged = self._new_stage_path(self._output_root, "output")
        try:
            self._require_ok(
                self._request(
                    "get_file",
                    {
                        "path": path,
                        "exchange_path": relative,
                        "replace": False,
                    },
                    timeout=timeout,
                )
            )
            stage_stat = staged.lstat()
            if not stat_module.S_ISREG(stage_stat.st_mode):
                raise CryptomatorProtocolError(
                    "The Cryptomator sidecar returned an unsafe staging file."
                )
            self._install_staged_file(staged, destination, replace)
        finally:
            staged.unlink(missing_ok=True)

    @_fail_on_protocol_error
    def write_inline(
        self,
        path: str,
        content: bytes,
        *,
        modified_ms: int | None = None,
        replace: bool = False,
        timeout: float | None = None,
    ) -> None:
        """Write at most one MiB of file content without a staging file."""
        if len(content) > MAX_INLINE_BYTES:
            raise ValueError("Inline content exceeds the sidecar limit")
        params: JSONObject = {
            "path": path,
            "content": base64.b64encode(content).decode("ascii"),
            "replace": replace,
        }
        if modified_ms is not None:
            params["modified_ms"] = modified_ms
        self._require_ok(self._request("write_inline", params, timeout=timeout))

    @_fail_on_protocol_error
    def read_inline(self, path: str, *, timeout: float | None = None) -> bytes:
        """Read at most one MiB of file content without a staging file."""
        result = _require_object(
            self._request("read_inline", {"path": path}, timeout=timeout),
            "read_inline",
        )
        encoded = _require_str(result, "content")
        try:
            content = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as exc:
            self._protocol_failure(
                "The Cryptomator sidecar returned invalid inline content."
            )
            raise CryptomatorProtocolError(
                "The Cryptomator sidecar returned invalid inline content."
            ) from exc
        if len(content) > MAX_INLINE_BYTES:
            self._protocol_failure(
                "The Cryptomator sidecar returned oversized inline content."
            )
            raise CryptomatorProtocolError(
                "The Cryptomator sidecar returned oversized inline content."
            )
        return content

    @_fail_on_protocol_error
    def move(
        self,
        source: str,
        target: str,
        *,
        replace: bool = False,
        timeout: float | None = None,
    ) -> None:
        """Move a cleartext path inside the vault."""
        result = self._request(
            "move",
            {"source": source, "target": target, "replace": replace},
            timeout=timeout,
        )
        self._require_ok(result)

    @_fail_on_protocol_error
    def delete(
        self,
        path: str,
        *,
        recursive: bool = False,
        timeout: float | None = None,
    ) -> None:
        """Delete a cleartext path inside the vault."""
        result = self._request(
            "delete", {"path": path, "recursive": recursive}, timeout=timeout
        )
        self._require_ok(result)

    @_fail_on_protocol_error
    def symlink(self, path: str, target: str, *, timeout: float | None = None) -> None:
        """Create a symbolic link inside the vault."""
        result = self._request(
            "symlink", {"path": path, "target": target}, timeout=timeout
        )
        self._require_ok(result)

    @_fail_on_protocol_error
    def readlink(self, path: str, *, timeout: float | None = None) -> str:
        """Return the target of a symbolic link inside the vault."""
        result = _require_object(
            self._request("readlink", {"path": path}, timeout=timeout), "readlink"
        )
        return _require_str(result, "target")

    def close(self) -> None:
        """Ask the sidecar to shut down, then release all local resources."""
        with self._state_lock:
            if self._closed or self._closing:
                return
            self._closing = True

        try:
            if self._process.poll() is None and self._fatal_error is None:
                try:
                    result = self._request(
                        "shutdown",
                        {},
                        timeout=min(self._request_timeout, 5.0),
                        _allow_closing=True,
                    )
                    self._require_ok(result)
                except CryptomatorError:
                    pass
        finally:
            self._stop_process()

    def shutdown(self) -> None:
        """Shut down the sidecar and release all local resources."""
        self.close()

    def terminate(self) -> None:
        """Stop the sidecar without a protocol shutdown."""
        with self._state_lock:
            if self._closed:
                return
            self._closing = True
        self._fail_all(
            CryptomatorProcessError("The Cryptomator sidecar was stopped."),
            terminate=False,
        )
        self._stop_process()

    def __enter__(self) -> CryptomatorClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(sidecar={str(self._sidecar_path)!r})>"

    def _build_command(
        self,
        java_executable: str | os.PathLike[str],
        executable_arguments: tuple[str, ...],
    ) -> builtins.list[str]:
        if self._sidecar_path.suffix.casefold() == ".jar":
            java_path = _resolve_command(java_executable)
            command = [str(java_path), "-jar", str(self._sidecar_path)]
        else:
            if os.name != "nt" and not os.access(self._sidecar_path, os.X_OK):
                raise CryptomatorProcessError(
                    "The Cryptomator sidecar is not executable."
                )
            command = [str(self._sidecar_path)]

        command.extend(executable_arguments)
        command.extend(["--exchange-root", str(self._exchange_root)])
        return command

    def _request(
        self,
        method: str,
        params: JSONObject,
        *,
        timeout: float | None,
        _allow_closing: bool = False,
    ) -> JSONValue:
        request_timeout = self._effective_timeout(timeout)
        request_id, future = self._register_request(allow_closing=_allow_closing)
        payload = (
            json.dumps(
                {"id": request_id, "method": method, "params": params},
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
        )
        return self._send_and_wait(request_id, future, payload, request_timeout)

    def _secret_request(
        self,
        method: Literal["initialize", "open"],
        vault_path: str | os.PathLike[str],
        password: str | bytes | bytearray,
        *,
        timeout: float | None,
    ) -> JSONValue:
        request_timeout = self._effective_timeout(timeout)
        request_id, future = self._register_request()
        secret = (
            bytearray(password, "utf-8")
            if isinstance(password, str)
            else bytearray(password)
        )
        encoded_secret = bytearray()
        payload = bytearray()
        try:
            if not secret or len(secret) > MAX_SECRET_BYTES:
                raise ValueError("The vault password length is invalid")
            encoded_secret.extend(base64.b64encode(secret))
            encoded_path = json.dumps(
                str(Path(vault_path).absolute()), ensure_ascii=False
            ).encode("utf-8")
            payload.extend(b'{"id":')
            payload.extend(str(request_id).encode("ascii"))
            payload.extend(b',"method":"')
            payload.extend(method.encode("ascii"))
            payload.extend(b'","params":{"vault_path":')
            payload.extend(encoded_path)
            payload.extend(b',"secret":"')
            payload.extend(encoded_secret)
            payload.extend(b'"}}\n')
            _wipe(secret)
            _wipe(encoded_secret)
            return self._send_and_wait(request_id, future, payload, request_timeout)
        except BaseException:
            self._discard_request(request_id, future)
            raise
        finally:
            _wipe(secret)
            _wipe(encoded_secret)
            _wipe(payload)

    def _register_request(
        self,
        *,
        allow_closing: bool = False,
    ) -> tuple[int, concurrent.futures.Future[JSONValue]]:
        with self._state_lock:
            if self._closed:
                raise CryptomatorProcessError("The Cryptomator sidecar is closed.")
            if self._closing and not allow_closing:
                raise CryptomatorProcessError("The Cryptomator sidecar is closing.")
            if self._fatal_error is not None:
                raise self._fatal_error
            if self._process.poll() is not None:
                error = CryptomatorProcessError(
                    "The Cryptomator sidecar exited unexpectedly."
                )
                self._fatal_error = error
                raise error

            request_id = self._next_request_id
            self._next_request_id += 1
            future: concurrent.futures.Future[JSONValue] = concurrent.futures.Future()
            self._pending[request_id] = future
            return request_id, future

    def _send_and_wait(
        self,
        request_id: int,
        future: concurrent.futures.Future[JSONValue],
        payload: bytes | bytearray,
        timeout: float,
    ) -> JSONValue:
        try:
            with self._write_lock:
                stream = self._process.stdin
                if stream is None:
                    raise BrokenPipeError
                view = memoryview(payload)
                while view:
                    written = stream.write(view)
                    if not written:
                        raise BrokenPipeError
                    view = view[written:]
        except (BrokenPipeError, OSError, ValueError):
            process_error = CryptomatorProcessError(
                "The Cryptomator sidecar input pipe closed unexpectedly."
            )
            self._fail_all(process_error)
            raise process_error from None
        finally:
            if isinstance(payload, bytearray):
                _wipe(payload)

        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            timeout_error = CryptomatorTimeoutError(
                "The Cryptomator sidecar request timed out."
            )
            self._fail_all(timeout_error)
            raise timeout_error from None
        except BaseException:
            if not future.done():
                self._discard_request(request_id, future)
                self._fail_all(
                    CryptomatorProcessError(
                        "The Cryptomator sidecar request was interrupted."
                    )
                )
            raise

    def _effective_timeout(self, timeout: float | None) -> float:
        request_timeout = self._request_timeout if timeout is None else timeout
        if request_timeout <= 0:
            raise ValueError("The request timeout must be greater than zero")
        return request_timeout

    def _discard_request(
        self, request_id: int, future: concurrent.futures.Future[JSONValue]
    ) -> None:
        with self._state_lock:
            if self._pending.get(request_id) is future:
                self._pending.pop(request_id)
                future.cancel()

    def _read_stdout(self) -> None:
        stream = self._process.stdout
        if stream is None:
            self._fail_all(
                CryptomatorProcessError(
                    "The Cryptomator sidecar output pipe is unavailable."
                )
            )
            return

        while True:
            try:
                line = stream.readline(MAX_RESPONSE_BYTES + 1)
            except OSError:
                line = b""
            if not line:
                with self._state_lock:
                    clean_exit = self._closing and not self._pending
                if not clean_exit:
                    self._fail_all(
                        CryptomatorProcessError(
                            "The Cryptomator sidecar exited unexpectedly."
                        )
                    )
                return
            if len(line) > MAX_RESPONSE_BYTES or not line.endswith(b"\n"):
                self._protocol_failure(
                    "The Cryptomator sidecar returned an oversized response."
                )
                return
            try:
                response = json.loads(line)
                self._dispatch_response(response)
            except (CryptomatorProtocolError, UnicodeDecodeError, json.JSONDecodeError):
                self._protocol_failure(
                    "The Cryptomator sidecar returned invalid protocol data."
                )
                return

    def _read_stderr(self) -> None:
        stream = self._process.stderr
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

    def _dispatch_response(self, value: Any) -> None:
        if not isinstance(value, dict):
            raise CryptomatorProtocolError("The sidecar response is not an object.")
        response = cast(dict[str, Any], value)
        request_id = response.get("id")
        if isinstance(request_id, bool) or not isinstance(request_id, int):
            raise CryptomatorProtocolError("The sidecar response has an invalid ID.")

        has_result = "result" in response
        has_error = "error" in response
        expected_keys = {"id", "result"} if has_result else {"id", "error"}
        if has_result == has_error or set(response) != expected_keys:
            raise CryptomatorProtocolError(
                "The sidecar response has an invalid result envelope."
            )

        if has_result:
            result = cast(JSONValue, response["result"])
            request_error = None
        else:
            error = response["error"]
            if not isinstance(error, dict) or set(error) != {"code", "message"}:
                raise CryptomatorProtocolError(
                    "The sidecar response has an invalid error envelope."
                )
            code = error.get("code")
            message = error.get("message")
            if (
                not isinstance(code, str)
                or not code
                or code not in SIDECAR_ERROR_CODES
                or not isinstance(message, str)
            ):
                raise CryptomatorProtocolError(
                    "The sidecar response has invalid error fields."
                )
            result = None
            request_error = CryptomatorRequestError(code)

        with self._state_lock:
            future = self._pending.pop(request_id, None)
        if future is None:
            raise CryptomatorProtocolError(
                "The sidecar response has an unknown request ID."
            )
        if request_error is None:
            future.set_result(result)
        else:
            future.set_exception(request_error)

    def _protocol_failure(self, message: str) -> None:
        self._fail_all(CryptomatorProtocolError(message))

    def _fail_all(self, error: CryptomatorError, *, terminate: bool = True) -> None:
        with self._state_lock:
            if self._fatal_error is None:
                self._fatal_error = error
            pending = list(self._pending.values())
            self._pending.clear()

        for future in pending:
            if not future.done():
                future.set_exception(error)

        if terminate and self._process.poll() is None:
            try:
                self._process.terminate()
            except OSError:
                pass

    def _stop_process(self) -> None:
        stream = self._process.stdin
        if stream is not None:
            try:
                stream.close()
            except OSError:
                pass

        if self._process.poll() is None:
            try:
                self._process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                try:
                    self._process.terminate()
                    self._process.wait(timeout=1.0)
                except (OSError, subprocess.TimeoutExpired):
                    try:
                        self._process.kill()
                    except OSError:
                        pass
                    try:
                        self._process.wait(timeout=1.0)
                    except (OSError, subprocess.TimeoutExpired):
                        pass

        with self._state_lock:
            self._closed = True
            self._pending.clear()

        current = threading.current_thread()
        for thread in (self._stdout_thread, self._stderr_thread):
            if thread is not current:
                thread.join(timeout=1.0)

        with self._stderr_lock:
            _wipe(self._stderr_tail)
            self._stderr_tail.clear()

        self._remove_owned_exchange_root()

    def _remove_owned_exchange_root(self) -> None:
        if getattr(self, "_owns_exchange_root", False):
            shutil.rmtree(self._exchange_root, ignore_errors=True)

    def _check_hello(self, hello: SidecarHello) -> None:
        expected = SidecarHello(
            protocol_version=PROTOCOL_VERSION,
            sidecar_version=SIDECAR_VERSION,
            cryptofs_version=CRYPTOFS_VERSION,
            cryptolib_version=CRYPTOLIB_VERSION,
            vault_format=VAULT_FORMAT,
            max_inline_bytes=MAX_INLINE_BYTES,
            vault_open=False,
        )
        if hello != expected:
            raise CryptomatorProtocolError(
                "The Cryptomator sidecar version or vault format is not supported."
            )

    def _vault_info(self, value: JSONValue) -> VaultInfo:
        result = _require_object(value, "vault_info")
        vault_format = _require_int(result, "vault_format")
        if vault_format != VAULT_FORMAT:
            self._protocol_failure(
                "The Cryptomator sidecar returned an unsupported vault format."
            )
            raise CryptomatorProtocolError(
                "The Cryptomator sidecar returned an unsupported vault format."
            )
        shortening_threshold = _require_int(result, "shortening_threshold")
        if shortening_threshold <= 0:
            raise CryptomatorProtocolError(
                "The Cryptomator sidecar returned an invalid shortening threshold."
            )
        return VaultInfo(
            vault_path=Path(_require_str(result, "vault_path")),
            vault_format=vault_format,
            shortening_threshold=shortening_threshold,
            key_id=_require_str(result, "key_id"),
        )

    def _vault_entry(self, value: JSONValue) -> VaultEntry:
        result = _require_object(value, "metadata")
        path = _require_logical_path(_require_str(result, "path"))
        entry_type = _require_str(result, "type")
        if entry_type not in {"file", "directory", "symlink", "other"}:
            raise CryptomatorProtocolError(
                "The Cryptomator sidecar returned an invalid entry type."
            )
        size = _require_int(result, "size")
        if size < 0:
            raise CryptomatorProtocolError(
                "The Cryptomator sidecar returned an invalid entry size."
            )
        sha256 = _optional_str(result, "sha256")
        if sha256 is not None and (
            len(sha256) != 64
            or any(character not in "0123456789abcdef" for character in sha256)
        ):
            raise CryptomatorProtocolError(
                "The Cryptomator sidecar returned an invalid content hash."
            )
        link_target = _optional_str(result, "link_target")
        return VaultEntry(
            path=path,
            type=cast(VaultEntryType, entry_type),
            size=size,
            modified_ms=_require_int(result, "modified_ms"),
            sha256=sha256,
            link_target=link_target,
        )

    def _storage_map_entry(self, value: JSONValue) -> StorageMapEntry:
        result = _require_object(value, "storage_map entry")
        logical_path = _require_logical_path(_require_str(result, "path"))
        entry_type = _require_str(result, "type")
        if entry_type not in {"file", "directory", "symlink"}:
            raise CryptomatorProtocolError(
                "The Cryptomator sidecar returned an invalid storage entry type."
            )
        storage_path = _require_storage_path(_require_str(result, "storage_path"))
        return StorageMapEntry(
            path=logical_path,
            type=cast(StorageEntryType, entry_type),
            storage_path=storage_path,
        )

    @staticmethod
    def _require_ok(value: JSONValue) -> None:
        result = _require_object(value, "operation result")
        if result != {"ok": True}:
            raise CryptomatorProtocolError(
                "The Cryptomator sidecar returned an invalid operation result."
            )

    def _new_stage_path(self, root: Path, prefix: str) -> tuple[str, Path]:
        name = f"{prefix}-{uuid.uuid4().hex}.bin"
        path = root / name
        relative = path.relative_to(self._exchange_root).as_posix()
        return relative, path

    @staticmethod
    def _copy_to_stage(source: Path, staged: Path) -> None:
        source_stat = source.stat()
        if not stat_module.S_ISREG(source_stat.st_mode):
            raise ValueError("The staging source must be a regular file")
        descriptor = os.open(staged, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with (
                source.open("rb") as source_file,
                os.fdopen(descriptor, "wb") as staged_file,
            ):
                descriptor = -1
                shutil.copyfileobj(source_file, staged_file)
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    @staticmethod
    def _install_staged_file(staged: Path, destination: Path, replace: bool) -> None:
        temporary = destination.parent / (
            f".{destination.name}.maestral-{uuid.uuid4().hex}.tmp"
        )
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with (
                staged.open("rb") as source_file,
                os.fdopen(descriptor, "wb") as destination_file,
            ):
                descriptor = -1
                shutil.copyfileobj(source_file, destination_file)
            if replace:
                os.replace(temporary, destination)
            else:
                os.link(temporary, destination)
                temporary.unlink()
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            temporary.unlink(missing_ok=True)


def _require_file(path_value: str | os.PathLike[str], name: str) -> Path:
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        raise CryptomatorProcessError(f"The {name} path must be absolute.")
    try:
        resolved = path.resolve(strict=True)
    except OSError:
        raise CryptomatorProcessError(f"The {name} file does not exist.") from None
    if not resolved.is_file():
        raise CryptomatorProcessError(f"The {name} path is not a file.")
    return resolved


def resolve_sidecar_path(
    configured_path: str | os.PathLike[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> Path:
    """Resolve the packaged sidecar launcher.

    An explicit configuration value has highest priority. The
    ``MAESTRAL_CRYPTOMATOR_SIDECAR`` environment variable comes next. Packaged native
    launchers and the all-dependencies JAR are checked last.
    """
    if configured_path is not None and os.fspath(configured_path).strip():
        return _require_file(configured_path, "sidecar")

    environment = os.environ if environ is None else environ
    environment_path = environment.get("MAESTRAL_CRYPTOMATOR_SIDECAR", "").strip()
    if environment_path:
        return _require_file(environment_path, "sidecar")

    executable_name = (
        "maestral-cryptomator-sidecar.exe"
        if os.name == "nt"
        else "maestral-cryptomator-sidecar"
    )
    relative_candidates = (
        Path("cryptomator") / executable_name,
        Path("cryptomator") / "maestral-cryptomator-sidecar-all.jar",
    )
    package_bases = [Path(sys.executable).resolve().parent]
    frozen_root = getattr(sys, "_MEIPASS", None)
    if isinstance(frozen_root, str):
        package_bases.append(Path(frozen_root))

    for base in package_bases:
        for relative_path in relative_candidates:
            candidate = base / relative_path
            if candidate.is_file():
                return candidate.resolve()

    raise CryptomatorProcessError("The packaged Cryptomator sidecar is unavailable.")


def _resolve_command(command: str | os.PathLike[str]) -> Path:
    value = os.fspath(command)
    if os.path.isabs(value):
        executable = _require_file(value, "Java executable")
    else:
        found = shutil.which(value)
        if found is None:
            raise CryptomatorProcessError("The Java executable is unavailable.")
        executable = Path(found).resolve()
    if os.name != "nt" and not os.access(executable, os.X_OK):
        raise CryptomatorProcessError("The Java executable is not executable.")
    return executable


def _prepare_exchange_root(
    path_value: str | os.PathLike[str] | None,
) -> tuple[Path, bool]:
    if path_value is None:
        path = Path(tempfile.mkdtemp(prefix="maestral-cryptomator-"))
        path.chmod(0o700)
        owns_path = True
    else:
        requested = Path(path_value).expanduser()
        if requested.exists() or requested.is_symlink():
            path = requested
        else:
            requested.mkdir(mode=0o700)
            path = requested
        owns_path = False

    if path.is_symlink() or not path.is_dir():
        if owns_path:
            shutil.rmtree(path, ignore_errors=True)
        raise CryptomatorProcessError(
            "The Cryptomator exchange root must be a real directory."
        )
    path = path.resolve(strict=True)
    _require_private_directory(path)
    return path, owns_path


def _make_private_directory(path: Path) -> None:
    path.mkdir(mode=0o700, exist_ok=False)
    path.chmod(0o700)
    _require_private_directory(path)


def _require_private_directory(path: Path) -> None:
    status = path.stat(follow_symlinks=False)
    if hasattr(os, "getuid") and status.st_uid != os.getuid():
        raise CryptomatorProcessError(
            "The current user must own the Cryptomator exchange root."
        )
    if os.name == "posix" and stat_module.S_IMODE(status.st_mode) & 0o077:
        raise CryptomatorProcessError(
            "The Cryptomator exchange root grants access to another user."
        )


def _require_object(value: JSONValue, context: str) -> JSONObject:
    if not isinstance(value, dict):
        raise CryptomatorProtocolError(
            f"The Cryptomator sidecar returned an invalid {context}."
        )
    return value


def _require_list(value: JSONValue, context: str) -> list[JSONValue]:
    if not isinstance(value, list):
        raise CryptomatorProtocolError(
            f"The Cryptomator sidecar returned an invalid {context}."
        )
    return value


def _require_str(value: JSONObject, key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str):
        raise CryptomatorProtocolError(
            "The Cryptomator sidecar returned an invalid string field."
        )
    return item


def _optional_str(value: JSONObject, key: str) -> str | None:
    item = value.get(key)
    if item is not None and not isinstance(item, str):
        raise CryptomatorProtocolError(
            "The Cryptomator sidecar returned an invalid string field."
        )
    return item


def _require_int(value: JSONObject, key: str) -> int:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, int):
        raise CryptomatorProtocolError(
            "The Cryptomator sidecar returned an invalid integer field."
        )
    return item


def _require_bool(value: JSONObject, key: str) -> bool:
    item = value.get(key)
    if not isinstance(item, bool):
        raise CryptomatorProtocolError(
            "The Cryptomator sidecar returned an invalid boolean field."
        )
    return item


def _require_logical_path(path: str) -> str:
    if path == "/":
        return path
    if (
        not path.startswith("/")
        or "\\" in path
        or "\0" in path
        or "//" in path
        or any(part in {"", ".", ".."} for part in path[1:].split("/"))
    ):
        raise CryptomatorProtocolError(
            "The Cryptomator sidecar returned an invalid logical path."
        )
    return path


def _require_storage_path(path: str) -> str:
    if (
        "\\" in path
        or "\0" in path
        or "//" in path
        or any(part in {"", ".", ".."} for part in path.split("/"))
    ):
        raise CryptomatorProtocolError(
            "The Cryptomator sidecar returned an unsafe storage path."
        )
    pure_path = PurePosixPath(path)
    if pure_path.is_absolute() or not pure_path.parts or pure_path.parts[0] != "d":
        raise CryptomatorProtocolError(
            "The Cryptomator sidecar returned an unsafe storage path."
        )
    return path


def _wipe(value: bytearray) -> None:
    value[:] = b"\0" * len(value)
