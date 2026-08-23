"""Start and stop the sync daemon and connect local API clients."""

from __future__ import annotations

import argparse
import enum
import ipaddress
import json
import os
import platform
import signal
import struct
import subprocess
import sys
import tempfile
import threading
import time
from pprint import pformat
from types import TracebackType
from typing import TYPE_CHECKING, Any, ContextManager, Iterable

from fasteners import InterProcessLock

from .constants import ENV, IS_MACOS
from .rpc import (
    PROTOCOL_VERSION,
    CommunicationError,
    JsonRpcConnection,
    JsonRpcServer,
    ProtocolError,
    RpcEndpoint,
)
from .utils import exc_info_tuple
from .utils.appdirs import get_runtime_path
from .utils.integration import SystemdNotifier

try:
    import fcntl
except ImportError:  # pragma: no cover - only used on Windows
    fcntl = None  # type: ignore[assignment]

if TYPE_CHECKING:
    from .main import Maestral


__all__ = [
    "Stop",
    "Start",
    "Lock",
    "maestral_lock",
    "get_maestral_pid",
    "sockpath_for_config",
    "endpoint_path_for_config",
    "endpoint_for_config",
    "lockpath_for_config",
    "wait_for_startup",
    "is_running",
    "freeze_support",
    "start_maestral_daemon",
    "start_maestral_daemon_process",
    "stop_maestral_daemon_process",
    "MaestralClient",
    "CommunicationError",
    "ProtocolError",
]


# systemd environment
NOTIFY_SOCKET = os.getenv("NOTIFY_SOCKET")
WATCHDOG_USEC = os.getenv("WATCHDOG_USEC")
WATCHDOG_PID = os.getenv("WATCHDOG_PID")
IS_WATCHDOG = WATCHDOG_PID is None or WATCHDOG_PID == str(os.getpid())


_DAEMON_START_SCRIPT = (
    "from maestral.daemon import start_maestral_daemon; "
    "import sys; start_maestral_daemon(sys.argv[1])"
)


def freeze_support() -> None:
    """
    Call this as early as possible in the main entry point of a frozen executable.
    This call will start the sync daemon if a matching command line arguments are
    detected and do nothing otherwise.
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-c")
    parser.add_argument("config_name", nargs="?")
    parsed_args, _ = parser.parse_known_args()

    if parsed_args.c == _DAEMON_START_SCRIPT and parsed_args.config_name:
        start_maestral_daemon(parsed_args.config_name)
        sys.exit()


class Stop(enum.Enum):
    """Enumeration of daemon exit results"""

    Ok = 0
    Killed = 1
    NotRunning = 2
    Failed = 3


class Start(enum.Enum):
    """Enumeration of daemon start results"""

    Ok = 0
    AlreadyRunning = 1
    Failed = 2
    Uninitialized = 3


# ==== interprocess locking ============================================================


class Lock:
    """An inter-process and inter-thread lock

    This internally uses :class:`fasteners.InterProcessLock` but provides non-blocking
    acquire. It also guarantees thread-safety when using the :meth:`singleton` class
    method to create / retrieve a lock instance.

    :param path: Path of the lock file to use / create.
    """

    _instances: dict[str, Lock] = {}
    _singleton_lock = threading.Lock()

    @classmethod
    def singleton(cls, path: str) -> Lock:
        """
        Retrieve an existing lock object with a given 'name' or create a new one. Use
        this method for thread-safe locks.

        :param path: Path of the lock file to use / create.
        """
        with cls._singleton_lock:
            path = os.path.abspath(path)

            if path not in cls._instances:
                cls._instances[path] = cls(path)

            return cls._instances[path]

    def __init__(self, path: str) -> None:
        self.path = path
        self.pid = os.getpid()
        self._external_lock = InterProcessLock(self.path)
        self._lock = threading.RLock()

    def acquire(self) -> bool:
        """
        Attempts to acquire the given lock.

        :returns: Whether the acquisition succeeded.
        """
        with self._lock:
            if self._external_lock.acquired:
                return False
            acquired = self._external_lock.acquire(blocking=False)
            if acquired and fcntl is None:
                try:
                    self._write_pid_file()
                except Exception:
                    self._external_lock.release()
                    raise
            return acquired

    def _write_pid_file(self) -> None:
        pid_path = f"{self.path}.pid"
        directory = os.path.dirname(pid_path)
        fd, temporary = tempfile.mkstemp(prefix=".pid-", dir=directory)

        try:
            os.chmod(temporary, 0o600)
            with os.fdopen(fd, "w") as file:
                file.write(str(self.pid))
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, pid_path)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    def release(self) -> None:
        """Release the previously acquired lock."""
        with self._lock:
            if not self._external_lock.acquired:
                raise RuntimeError(
                    "Cannot release a lock, it was acquired by a different process"
                )

            if fcntl is None:
                try:
                    os.unlink(f"{self.path}.pid")
                except FileNotFoundError:
                    pass
            self._external_lock.release()

    def locked(self) -> bool:
        """
        Checks if the lock is currently held by any thread or process.

        :returns: Whether the lock is acquired.
        """
        with self._lock:
            if self._external_lock.acquired:
                return True
            if fcntl is not None:
                return self.locking_pid() is not None

            probe = InterProcessLock(self.path)
            if probe.acquire(blocking=False):
                probe.release()
                return False
            return True

    def locking_pid(self) -> int | None:
        """
        Returns the PID of the process which currently holds the lock or ``None``. This
        uses a read-only ``F_GETLK`` query and should work on macOS, OpenBSD and Linux.

        :returns: The PID of the process which currently holds the lock or ``None``.
        """
        with self._lock:
            if self._external_lock.acquired:
                return self.pid

            if fcntl is None:
                if not self.locked():
                    return None
                try:
                    with open(f"{self.path}.pid") as file:
                        pid = int(file.read())
                except (FileNotFoundError, OSError, ValueError):
                    return None
                return pid if pid > 0 else None

            try:
                fh = open(self._external_lock.path, "a")
            except OSError:
                return None

            if IS_MACOS:
                fmt = "qqihh"
                pid_index = 2
                flock = struct.pack(fmt, 0, 0, 0, fcntl.F_WRLCK, 0)
            else:
                fmt = "hhqqih"
                pid_index = 4
                flock = struct.pack(fmt, fcntl.F_WRLCK, 0, 0, 0, 0, 0)

            with fh:
                lockdata = fcntl.fcntl(fh.fileno(), fcntl.F_GETLK, flock)
            lockdata_list = struct.unpack(fmt, lockdata)
            pid = lockdata_list[pid_index]

            if pid > 0:
                return pid

            return None


# ==== helpers for daemon management ===================================================


def _send_signal(pid: int, sig: int) -> None:
    try:
        os.kill(pid, sig)
    except ProcessLookupError:
        pass


def maestral_lock(config_name: str) -> Lock:
    """
    Returns an inter-process and inter-thread lock for Maestral. This is a wrapper
    around :class:`Lock` which fills out the appropriate lockfile path for the given
    config name.

    :param config_name: The name of the Maestral configuration.
    :returns: Lock instance for the config name
    """
    return Lock.singleton(lockpath_for_config(config_name))


def _is_windows() -> bool:
    return platform.system() == "Windows"


def sockpath_for_config(config_name: str) -> str:
    """
    Returns the unix socket location to be used for the config. This should default to
    the apps runtime directory + 'CONFIG_NAME.sock'.

    :param config_name: The name of the Maestral configuration.
    :returns: Socket path.
    """
    return get_runtime_path("maestral", f"{config_name}.sock")


def endpoint_path_for_config(config_name: str) -> str:
    """Return the Windows TCP endpoint discovery-file path."""
    return get_runtime_path("maestral", f"{config_name}.endpoint")


def _validate_tcp_endpoint(host: Any, port: Any) -> tuple[str, int]:
    if not isinstance(host, str) or not isinstance(port, int):
        raise CommunicationError("Invalid daemon endpoint")
    try:
        address = ipaddress.ip_address(host)
    except ValueError as exc:
        raise CommunicationError("Invalid daemon endpoint host") from exc
    if not address.is_loopback:
        raise CommunicationError("Refusing a non-loopback daemon endpoint")
    if isinstance(port, bool) or not 1 <= port <= 65535:
        raise CommunicationError("Invalid daemon endpoint port")
    return host, port


def _publish_tcp_endpoint(config_name: str, host: str, port: int) -> None:
    """Atomically publish a Windows TCP endpoint for local clients."""
    host, port = _validate_tcp_endpoint(host, port)
    destination = endpoint_path_for_config(config_name)
    directory = os.path.dirname(destination)
    fd, temporary = tempfile.mkstemp(prefix=f".{config_name}.", dir=directory)

    try:
        os.chmod(temporary, 0o600)
        with os.fdopen(fd, "w") as file:
            json.dump({"host": host, "port": port}, file, separators=(",", ":"))
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, destination)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def endpoint_for_config(config_name: str) -> RpcEndpoint:
    """Discover the local JSON-RPC endpoint for a configuration."""
    if not _is_windows():
        return RpcEndpoint("unix", sockpath_for_config(config_name))

    try:
        with open(endpoint_path_for_config(config_name)) as file:
            endpoint = json.load(file)
        host, port = _validate_tcp_endpoint(endpoint["host"], endpoint["port"])
    except CommunicationError:
        raise
    except (FileNotFoundError, OSError, KeyError, TypeError, ValueError) as exc:
        raise CommunicationError("Could not read the daemon endpoint") from exc

    return RpcEndpoint("tcp", (host, port))


def lockpath_for_config(config_name: str) -> str:
    """
    Returns the lock file location to be used for the config. This will be the apps
    runtime directory + 'CONFIG_NAME.lock'.

    :param config_name: The name of the Maestral configuration.
    :returns: Path of lock file to use.
    """
    return get_runtime_path("maestral", f"{config_name}.lock")


def get_maestral_pid(config_name: str) -> int | None:
    """
    Returns the PID of the daemon if it is running, ``None`` otherwise.

    :param config_name: The name of the Maestral configuration.
    :returns: The daemon's PID.
    """
    return maestral_lock(config_name).locking_pid()


def is_running(config_name: str) -> bool:
    """
    Checks if a daemon is currently running.

    :param config_name: The name of the Maestral configuration.
    :returns: Whether the daemon is running.
    """
    return maestral_lock(config_name).locked()


def _validate_handshake(
    handshake: Any,
) -> tuple[set[str], set[str], set[str]]:
    try:
        protocol_version = handshake["protocol_version"]
        daemon_version = handshake["daemon_version"]
        methods = handshake["methods"]
        properties = handshake["properties"]
        readable = properties["read"]
        writable = properties["write"]
    except (KeyError, TypeError) as exc:
        raise ProtocolError("Invalid RPC handshake") from exc

    if type(protocol_version) is not int or protocol_version != PROTOCOL_VERSION:
        raise ProtocolError("The daemon uses an incompatible RPC protocol")
    if not isinstance(daemon_version, str) or not daemon_version:
        raise ProtocolError("Invalid RPC handshake")
    if not all(
        isinstance(values, list) and all(isinstance(value, str) for value in values)
        for values in (methods, readable, writable)
    ):
        raise ProtocolError("Invalid RPC handshake")

    return set(methods), set(readable), set(writable)


def wait_for_startup(
    config_name: str,
    timeout: float = 30,
    process: subprocess.Popen[bytes] | None = None,
) -> None:
    """
    Waits until we can communicate with the maestral daemon for ``config_name``.

    :param config_name: Configuration to connect to.
    :param timeout: Timeout it seconds until we raise an error.
    :param process: Daemon process to monitor for an early exit.
    :raises CommunicationError: if we cannot communicate with the daemon within the
        given timeout.
    """
    t0 = time.time()

    while True:
        connection: JsonRpcConnection | None = None
        try:
            connection = JsonRpcConnection(endpoint_for_config(config_name))
            handshake = connection.request("rpc.handshake")
            _validate_handshake(handshake)
            return
        except Exception as exc:
            if process is not None and process.poll() is not None:
                raise ChildProcessError(
                    f"Daemon exited with status {process.returncode}"
                ) from exc
            if time.time() - t0 > timeout:
                raise exc
            else:
                time.sleep(0.2)
        finally:
            if connection is not None:
                connection.close()


# ==== main functions to manage daemon =================================================


def start_maestral_daemon(
    config_name: str = "maestral", log_to_stderr: bool = False
) -> None:
    """
    Starts the Maestral daemon with event loop in the current thread.

    Startup is race free: there will never be more than one daemon running with the same
    config name. The daemon exposes a :class:`maestral.main.Maestral` instance through
    JSON-RPC. It listens on a Unix socket on macOS and Linux, or loopback TCP on
    Windows. This call starts an asyncio event loop and blocks until shutdown.

    :param config_name: The name of the Maestral configuration to use.
    :param log_to_stderr: If ``True``, write logs to stderr.
    :raises RuntimeError: if a daemon for the given ``config_name`` is already running.
    """

    import asyncio

    from .config import validate_config_name
    from .logging import scoped_logger, setup_logging
    from .main import Maestral

    config_name = validate_config_name(config_name)

    setup_logging(config_name, stderr=log_to_stderr)
    dlogger = scoped_logger(__name__, config_name)
    sd_notifier = SystemdNotifier()

    dlogger.info("Starting daemon")

    # ==== Process and thread management ===========================================

    if threading.current_thread() is not threading.main_thread():
        dlogger.error("Must run daemon in main thread")
        return

    dlogger.debug("Environment:\n%s", pformat(os.environ.copy()))

    # Acquire PID lock file.
    lock = maestral_lock(config_name)

    if lock.acquire():
        dlogger.debug("Acquired daemon lock: %r", lock.path)
    else:
        dlogger.error("Could not acquire lock, daemon is already running")
        return

    loop: asyncio.AbstractEventLoop | None = None
    socket_path: str | None = None
    endpoint_path: str | None = None
    maestral_daemon: Maestral | None = None

    try:
        # Get the default event loop.
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Notify systemd that we have started.
        if NOTIFY_SOCKET:
            dlogger.debug("Running as systemd notify service")
            dlogger.debug("NOTIFY_SOCKET = %s", NOTIFY_SOCKET)

        # Notify systemd periodically if alive.
        if IS_WATCHDOG and WATCHDOG_USEC:

            async def periodic_watchdog() -> None:
                if WATCHDOG_USEC:
                    sleep = int(WATCHDOG_USEC)
                    while True:
                        sd_notifier.notify("WATCHDOG=1")
                        await asyncio.sleep(sleep / (2 * 10**6))

            dlogger.debug("Running as systemd watchdog service")
            dlogger.debug("WATCHDOG_USEC = %s", WATCHDOG_USEC)
            dlogger.debug("WATCHDOG_PID = %s", WATCHDOG_PID)
            loop.create_task(periodic_watchdog())

        # ==== Run Maestral as JSON-RPC server ========================================
        shutdown_future = loop.create_future()
        maestral_daemon = Maestral(
            config_name,
            log_to_stderr=log_to_stderr,
            event_loop=loop,
            shutdown_future=shutdown_future,
        )
        rpc_server = JsonRpcServer(maestral_daemon)

        if _is_windows():
            loop.run_until_complete(rpc_server.start_tcp("127.0.0.1"))
            server_socket = rpc_server.sockets[0]
            host, port = server_socket.getsockname()[:2]
            host, port = _validate_tcp_endpoint(host, port)
            _publish_tcp_endpoint(config_name, host, port)
            endpoint_path = endpoint_path_for_config(config_name)
            dlogger.debug("TCP endpoint: %s:%s", host, port)
        else:
            socket_path = sockpath_for_config(config_name)
            dlogger.debug("Socket path: %r", socket_path)
            try:
                os.remove(socket_path)
            except (FileNotFoundError, NotADirectoryError):
                pass
            loop.run_until_complete(rpc_server.start_unix(socket_path))
            os.chmod(socket_path, 0o600)

        sd_notifier.notify("READY=1")

        dlogger.debug("Starting event loop")

        handled_signals = [signal.SIGTERM, signal.SIGINT]
        if hasattr(signal, "SIGHUP"):
            handled_signals.insert(0, signal.SIGHUP)

        for handled_signal in handled_signals:
            try:
                loop.add_signal_handler(handled_signal, maestral_daemon.shutdown_daemon)
            except (NotImplementedError, RuntimeError):
                signal.signal(
                    handled_signal,
                    lambda _signum, _frame: maestral_daemon.shutdown_daemon(),
                )

        try:
            loop.run_until_complete(shutdown_future)
        finally:
            loop.run_until_complete(rpc_server.close())

    except Exception as exc:
        dlogger.error(str(exc), exc_info=True)
    finally:
        # Notify systemd that we are shutting down.
        sd_notifier.notify("STOPPING=1")

        for path in (socket_path, endpoint_path):
            if path:
                try:
                    os.remove(path)
                except (FileNotFoundError, NotADirectoryError):
                    pass

        if loop is not None:
            pending_tasks = asyncio.all_tasks(loop)
            for task in pending_tasks:
                task.cancel()
            if pending_tasks:
                loop.run_until_complete(
                    asyncio.gather(*pending_tasks, return_exceptions=True)
                )
            loop.close()
            asyncio.set_event_loop(None)

        if maestral_daemon is not None:
            maestral_daemon.manager.shutdown()
            maestral_daemon.sync._connection.close()

        lock.release()


def start_maestral_daemon_process(
    config_name: str = "maestral", timeout: float = 30
) -> Start:
    """
    Starts the Maestral daemon in a new process by calling :func:`start_maestral_daemon`.

    Startup is race free: there will never be more than one daemon running for the same
    config name. This function will use :obj:`sys.executable` as a Python executable to
    start the daemon.

    Environment variables from the current process will be preserved and updated with
    the environment variables defined in :const:`constants.ENV`.

    :param config_name: The name of the Maestral configuration to use.
    :param timeout: Time in sec to wait for daemon to start.
    :returns: :attr:`Start.Ok` if successful, :attr:`Start.AlreadyRunning` if the daemon
        was already running or :attr:`Start.Failed` if startup failed. It is possible
        that :attr:`Start.Ok` may be returned instead of :attr:`Start.AlreadyRunning`
        in case of a race but the daemon is nevertheless started only once.
    """
    from .config import validate_config_name

    config_name = validate_config_name(config_name)

    if is_running(config_name):
        return Start.AlreadyRunning

    env = os.environ.copy()
    env.update(ENV)

    popen_kwargs: dict[str, Any] = {}
    if _is_windows():
        popen_kwargs.update(
            creationflags=getattr(subprocess, "DETACHED_PROCESS", 0x00000008),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    process = subprocess.Popen(
        [sys.executable, "-c", _DAEMON_START_SCRIPT, config_name],
        env=env,
        **popen_kwargs,
    )

    try:
        wait_for_startup(config_name, timeout, process)
    except Exception as exc:
        from .logging import scoped_logger, setup_logging

        setup_logging(config_name, stderr=False)
        clogger = scoped_logger(__name__, config_name)

        clogger.error("Could not communicate with daemon", exc_info=exc_info_tuple(exc))

        if process.poll() is None:
            clogger.error("Daemon is running but not responsive, killing now")
            process.terminate()

            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        else:
            clogger.error("Daemon quit unexpectedly with status %s", process.returncode)

        return Start.Failed
    else:
        threading.Thread(
            target=process.wait,
            name=f"maestral-daemon-reaper-{config_name}",
            daemon=True,
        ).start()
        return Start.Ok


def stop_maestral_daemon_process(
    config_name: str = "maestral", timeout: float = 10
) -> Stop:
    """Stops a maestral daemon process by finding its PID and shutting it down.

    This function first tries to shut down Maestral gracefully. On Unix, it then sends
    SIGTERM and finally SIGKILL. On Windows, it uses ``TerminateProcess`` through
    :func:`os.kill` if the graceful shutdown fails.

    :param config_name: The name of the Maestral configuration to use.
    :param timeout: Number of sec to wait for daemon to shut down before killing it.
    :returns: :attr:`Stop.Ok` if successful, :attr:`Stop.Killed` if killed,
        :attr:`Stop.NotRunning` if the daemon was not running and :attr:`Stop.Failed`
        if killing the process failed because we could not retrieve its PID.
    """
    if not is_running(config_name):
        return Stop.NotRunning

    pid = get_maestral_pid(config_name)
    requested_shutdown = False
    connection: JsonRpcConnection | None = None

    try:
        rpc_timeout = max(0.1, min(timeout, 1.0))
        connection = JsonRpcConnection(
            endpoint_for_config(config_name), timeout=rpc_timeout
        )
        connection.request("shutdown_daemon")
        requested_shutdown = True
    except (CommunicationError, ProtocolError):
        pass
    finally:
        if connection is not None:
            connection.close()

    if not requested_shutdown and not _is_windows() and pid is not None:
        _send_signal(pid, signal.SIGTERM)

    deadline = time.monotonic() + max(timeout, 0)
    while time.monotonic() < deadline:
        if not is_running(config_name):
            return Stop.Ok
        time.sleep(0.2)

    if not is_running(config_name):
        return Stop.Ok

    if pid is None or pid <= 0:
        return Stop.Failed

    # Windows os.kill uses TerminateProcess for SIGTERM. Unix has SIGKILL.
    kill_signal = signal.SIGTERM if _is_windows() else signal.SIGKILL
    try:
        _send_signal(pid, kill_signal)
    except OSError:
        return Stop.Failed
    return Stop.Killed


class MaestralClient(ContextManager["MaestralClient"]):
    """A JSON-RPC client for the Maestral daemon.

    Public methods and properties mirror :class:`maestral.main.Maestral`. When
    ``fallback`` is true and no daemon runs, calls use an in-process Maestral instance.
    """

    _m: Maestral | JsonRpcConnection

    def __init__(self, config_name: str = "maestral", fallback: bool = False) -> None:
        from .config import validate_config_name

        self._config_name = validate_config_name(config_name)
        self._is_fallback = False
        self._remote_methods: set[str] = set()
        self._readable_properties: set[str] = set()
        self._writable_properties: set[str] = set()

        if is_running(self._config_name):
            connection = JsonRpcConnection(endpoint_for_config(self._config_name))
            try:
                handshake = connection.request("rpc.handshake")
                (
                    self._remote_methods,
                    self._readable_properties,
                    self._writable_properties,
                ) = _validate_handshake(handshake)
            except Exception:
                connection.close()
                raise
            self._m = connection
        elif fallback:
            from .main import Maestral

            self._m = Maestral(self._config_name)
            self._is_fallback = True
        else:
            raise CommunicationError(
                f"Could not connect to daemon for '{self._config_name}'"
            )

    def _disconnect(self) -> None:
        if isinstance(self._m, JsonRpcConnection):
            self._m.close()
        else:
            self._m.manager.shutdown()
            self._m.sync._connection.close()

    def __enter__(self) -> MaestralClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._disconnect()

    def __getattr__(self, item: str) -> Any:
        if item.startswith("_"):
            raise AttributeError(item)

        target = self._m
        if not isinstance(target, JsonRpcConnection):
            return getattr(target, item)

        if item in self._readable_properties:
            return target.request("rpc.get", {"name": item})

        if item in self._remote_methods:

            def remote_method(*args: Any, **kwargs: Any) -> Any:
                if args and kwargs:
                    params: dict[str, Any] | list[Any] = {
                        "__maestral_args__": list(args),
                        "__maestral_kwargs__": kwargs,
                    }
                elif kwargs:
                    params = kwargs
                else:
                    params = list(args)
                return target.request(item, params)

            remote_method.__name__ = item
            return remote_method

        raise AttributeError(item)

    def __setattr__(self, key: str, value: Any) -> None:
        if key.startswith("_"):
            super().__setattr__(key, value)
            return

        target = self._m
        if not isinstance(target, JsonRpcConnection):
            setattr(target, key, value)
        elif key in self._writable_properties:
            target.request("rpc.set", {"name": key, "value": value})
        elif key in self._readable_properties:
            raise AttributeError(f"Property '{key}' is read-only")
        else:
            raise AttributeError(key)

    def __dir__(self) -> Iterable[str]:
        own_result = dir(self.__class__) + list(self.__dict__.keys())
        if isinstance(self._m, JsonRpcConnection):
            remote_result = self._remote_methods | self._readable_properties
        else:
            remote_result = {key for key in dir(self._m) if not key.startswith("_")}
        return sorted(set(own_result) | remote_result)

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__}(config={self._config_name!r}, "
            f"is_fallback={self._is_fallback})>"
        )
