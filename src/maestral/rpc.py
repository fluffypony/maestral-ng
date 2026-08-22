"""Safe JSON-RPC transport for the Maestral daemon."""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import enum
import inspect
import json
import socket
import threading
from collections.abc import Iterator, Mapping
from datetime import datetime
from pathlib import (
    Path,
    PosixPath,
    PurePath,
    PurePosixPath,
    PureWindowsPath,
    WindowsPath,
)
from types import ModuleType
from typing import Any, Callable, Literal

from . import __version__, constants, core, exceptions, models
from .database.orm import Model
from .exceptions import MaestralApiError

PROTOCOL_VERSION = 1
MAX_MESSAGE_SIZE = 16 * 1024 * 1024


class CommunicationError(OSError):
    """Raised when a client cannot communicate with the daemon."""


class ProtocolError(CommunicationError):
    """Raised when a client and daemon use incompatible RPC protocols."""


class RemoteError(Exception):
    """An error returned by the JSON-RPC server."""

    def __init__(self, code: int, message: str, data: Any = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data


@dataclasses.dataclass(frozen=True)
class RpcEndpoint:
    """A local transport endpoint."""

    kind: Literal["unix", "tcp"]
    address: str | tuple[str, int]


def _classes_from(module: ModuleType) -> dict[str, type[Any]]:
    return {
        f"{klass.__module__}.{klass.__qualname__}": klass
        for _, klass in inspect.getmembers(module, inspect.isclass)
        if klass.__module__ == module.__name__
    }


_API_CLASSES: dict[str, type[Any]] = {}
for _module in (constants, core, exceptions, models):
    _API_CLASSES.update(_classes_from(_module))


def _type_name(obj: object) -> str:
    klass = type(obj)
    return f"{klass.__module__}.{klass.__qualname__}"


def to_json_value(obj: Any) -> Any:
    """Convert a supported API value to plain JSON data without using pickle."""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj

    if isinstance(obj, bytes):
        return {
            "__maestral_type__": "bytes",
            "value": base64.b64encode(obj).decode("ascii"),
        }

    if isinstance(obj, datetime):
        return {"__maestral_type__": "datetime", "value": obj.isoformat()}

    if isinstance(obj, PurePath):
        return {
            "__maestral_type__": "path",
            "class": type(obj).__name__,
            "value": str(obj),
        }

    if isinstance(obj, enum.Enum):
        type_name = _type_name(obj)
        if type_name not in _API_CLASSES:
            raise TypeError(f"Unsupported enum type: {type_name}")
        return {
            "__maestral_type__": "enum",
            "class": type_name,
            "value": to_json_value(obj.value),
        }

    if isinstance(obj, MaestralApiError):
        type_name = _type_name(obj)
        if type_name not in _API_CLASSES:
            raise TypeError(f"Unsupported API error type: {type_name}")
        return {
            "__maestral_type__": "api_error",
            "class": type_name,
            "title": obj.title,
            "message": obj.message,
            "dbx_path": obj.dbx_path,
            "dbx_path_from": obj.dbx_path_from,
            "local_path": obj.local_path,
            "local_path_from": obj.local_path_from,
        }

    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        type_name = _type_name(obj)
        if type_name not in _API_CLASSES:
            raise TypeError(f"Unsupported dataclass type: {type_name}")
        return {
            "__maestral_type__": "dataclass",
            "class": type_name,
            "fields": {
                field.name: to_json_value(getattr(obj, field.name))
                for field in dataclasses.fields(obj)
            },
        }

    if isinstance(obj, Model):
        type_name = _type_name(obj)
        if type_name not in _API_CLASSES:
            raise TypeError(f"Unsupported model type: {type_name}")
        return {
            "__maestral_type__": "model",
            "class": type_name,
            "fields": {
                column.name: to_json_value(getattr(obj, column.name))
                for column in sorted(obj.__columns__, key=lambda item: item.name)
            },
        }

    if isinstance(obj, Mapping):
        if not all(isinstance(key, str) for key in obj):
            raise TypeError("JSON-RPC dictionaries require string keys")
        return {key: to_json_value(value) for key, value in obj.items()}

    if isinstance(obj, list):
        return [to_json_value(value) for value in obj]

    if isinstance(obj, tuple):
        return {
            "__maestral_type__": "tuple",
            "items": [to_json_value(value) for value in obj],
        }

    if isinstance(obj, set):
        return {
            "__maestral_type__": "set",
            "items": [to_json_value(value) for value in obj],
        }

    if isinstance(obj, frozenset):
        return {
            "__maestral_type__": "frozenset",
            "items": [to_json_value(value) for value in obj],
        }

    if isinstance(obj, Iterator):
        return [to_json_value(value) for value in obj]

    raise TypeError(f"Unsupported JSON-RPC value: {type(obj).__name__}")


_PATH_CLASSES: dict[str, type[PurePath]] = {
    "Path": Path,
    "PosixPath": PosixPath,
    "PurePath": PurePath,
    "PurePosixPath": PurePosixPath,
    "PureWindowsPath": PureWindowsPath,
    "WindowsPath": WindowsPath,
}


def from_json_value(value: Any) -> Any:
    """Restore supported API values from plain JSON data."""
    if isinstance(value, list):
        return [from_json_value(item) for item in value]

    if not isinstance(value, dict):
        return value

    tag = value.get("__maestral_type__")
    if tag is None:
        return {key: from_json_value(item) for key, item in value.items()}

    if tag == "bytes":
        return base64.b64decode(value["value"], validate=True)

    if tag == "datetime":
        return datetime.fromisoformat(value["value"])

    if tag == "path":
        path_class_name = value.get("class")
        if not isinstance(path_class_name, str):
            raise ValueError("Unsupported path class")
        path_class = _PATH_CLASSES.get(path_class_name)
        if path_class is None:
            raise ValueError("Unsupported path class")
        try:
            return path_class(value["value"])
        except NotImplementedError:
            if path_class is WindowsPath:
                return PureWindowsPath(value["value"])
            return PurePosixPath(value["value"])

    if tag in {"enum", "dataclass", "model", "api_error"}:
        type_name = value.get("class")
        if not isinstance(type_name, str):
            raise ValueError(f"Unsupported API class: {type_name}")
        klass = _API_CLASSES.get(type_name)
        if klass is None:
            raise ValueError(f"Unsupported API class: {type_name}")

        if tag == "enum":
            return klass(from_json_value(value["value"]))

        if tag in {"dataclass", "model"}:
            fields = from_json_value(value["fields"])
            return klass(**fields)

        return klass(
            title=value["title"],
            message=value.get("message", ""),
            dbx_path=value.get("dbx_path"),
            dbx_path_from=value.get("dbx_path_from"),
            local_path=value.get("local_path"),
            local_path_from=value.get("local_path_from"),
        )

    if tag in {"tuple", "set", "frozenset"}:
        items = [from_json_value(item) for item in value["items"]]
        if tag == "tuple":
            return tuple(items)
        if tag == "set":
            return set(items)
        return frozenset(items)

    raise ValueError(f"Unsupported Maestral JSON tag: {tag}")


def encode_message(message: Any) -> bytes:
    """Encode one newline-delimited JSON message."""
    value = to_json_value(message)
    text = json.dumps(value, allow_nan=False, separators=(",", ":"))
    return text.encode("utf-8") + b"\n"


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"Invalid JSON constant: {value}")


def decode_message(message: bytes) -> Any:
    """Decode one newline-delimited JSON message."""
    return from_json_value(json.loads(message, parse_constant=_reject_json_constant))


class JsonRpcDispatcher:
    """Dispatch JSON-RPC calls to a Maestral API instance."""

    def __init__(self, target: object) -> None:
        self.target = target
        self.methods: set[str] = set()
        self.readable_properties: set[str] = set()
        self.writable_properties: set[str] = set()

        for name, member in inspect.getmembers(type(target)):
            if name.startswith("_"):
                continue
            if isinstance(member, property):
                if member.fget is not None:
                    self.readable_properties.add(name)
                if member.fset is not None:
                    self.writable_properties.add(name)
            elif callable(member):
                self.methods.add(name)

    def dispatch(self, request: Any) -> Any:
        """Dispatch a JSON-RPC request or batch and return a response."""
        if isinstance(request, list):
            if not request:
                return self._error(None, -32600, "Invalid Request")
            responses = [self._dispatch_one(item) for item in request]
            return [response for response in responses if response is not None] or None

        return self._dispatch_one(request)

    def _dispatch_one(self, request: Any) -> dict[str, Any] | None:
        request_id: str | int | None = None

        if isinstance(request, dict):
            candidate_id = request.get("id")
            if candidate_id is None or (
                isinstance(candidate_id, (str, int))
                and not isinstance(candidate_id, bool)
            ):
                request_id = candidate_id

        if (
            not isinstance(request, dict)
            or request.get("jsonrpc") != "2.0"
            or not isinstance(request.get("method"), str)
            or (
                "id" in request and request.get("id") is not None and request_id is None
            )
        ):
            return self._error(request_id, -32600, "Invalid Request")

        is_notification = "id" not in request
        method_name = request["method"]
        params = request.get("params", {})

        try:
            result = self._invoke(method_name, params)
            if isinstance(result, Iterator):
                result = list(result)
        except _MethodNotFound:
            response = self._error(request_id, -32601, "Method not found")
        except _InvalidParams as exc:
            response = self._error(request_id, -32602, "Invalid params", str(exc))
        except MaestralApiError as exc:
            response = self._error(request_id, -32010, str(exc), exc)
        except Exception as exc:
            response = self._error(
                request_id,
                -32000,
                "Remote call failed",
                {"type": type(exc).__name__, "message": str(exc)},
            )
        else:
            response = {"jsonrpc": "2.0", "id": request_id, "result": result}

        return None if is_notification else response

    def _invoke(self, method_name: str, params: Any) -> Any:
        callable_obj: Callable[..., Any]
        if method_name == "rpc.handshake":
            callable_obj = self._handshake
        elif method_name == "rpc.get":
            callable_obj = self._get_property
        elif method_name == "rpc.set":
            callable_obj = self._set_property
        elif method_name in self.methods:
            callable_obj = getattr(self.target, method_name)
        else:
            raise _MethodNotFound(method_name)

        if not isinstance(params, (dict, list)):
            raise _InvalidParams("params must be an object or an array")

        if isinstance(params, dict):
            if set(params) == {"__maestral_args__", "__maestral_kwargs__"}:
                args = params["__maestral_args__"]
                kwargs = params["__maestral_kwargs__"]
                if not isinstance(args, list) or not isinstance(kwargs, dict):
                    raise _InvalidParams("invalid mixed parameters")
                try:
                    inspect.signature(callable_obj).bind(*args, **kwargs)
                except TypeError as exc:
                    raise _InvalidParams(str(exc)) from exc
                return callable_obj(*args, **kwargs)

            try:
                inspect.signature(callable_obj).bind(**params)
            except TypeError as exc:
                raise _InvalidParams(str(exc)) from exc
            return callable_obj(**params)

        try:
            inspect.signature(callable_obj).bind(*params)
        except TypeError as exc:
            raise _InvalidParams(str(exc)) from exc
        return callable_obj(*params)

    def _handshake(self) -> dict[str, Any]:
        return {
            "protocol_version": PROTOCOL_VERSION,
            "daemon_version": __version__,
            "methods": sorted(self.methods),
            "properties": {
                "read": sorted(self.readable_properties),
                "write": sorted(self.writable_properties),
            },
        }

    def _get_property(self, name: str) -> Any:
        if name not in self.readable_properties:
            raise _MethodNotFound(name)
        return getattr(self.target, name)

    def _set_property(self, name: str, value: Any) -> None:
        if name not in self.writable_properties:
            raise _MethodNotFound(name)
        setattr(self.target, name, value)

    @staticmethod
    def _error(
        request_id: str | int | None,
        code: int,
        message: str,
        data: Any = None,
    ) -> dict[str, Any]:
        error: dict[str, Any] = {"code": code, "message": message}
        if data is not None:
            error["data"] = data
        return {"jsonrpc": "2.0", "id": request_id, "error": error}


class _InvalidParams(Exception):
    pass


class _MethodNotFound(Exception):
    pass


class JsonRpcServer:
    """An asyncio newline-delimited JSON-RPC server."""

    def __init__(self, target: object) -> None:
        self.dispatcher = JsonRpcDispatcher(target)
        self._server: asyncio.AbstractServer | None = None
        self._writers: set[asyncio.StreamWriter] = set()

    @property
    def sockets(self) -> tuple[socket.socket, ...]:
        server_sockets = getattr(self._server, "sockets", None)
        if server_sockets is None:
            return ()
        return tuple(server_sockets)

    async def start_unix(self, path: str) -> None:
        self._server = await asyncio.start_unix_server(
            self._handle_client,
            path=path,
            limit=MAX_MESSAGE_SIZE,
        )

    async def start_tcp(self, host: str, port: int = 0) -> None:
        self._server = await asyncio.start_server(
            self._handle_client,
            host=host,
            port=port,
            limit=MAX_MESSAGE_SIZE,
        )

    async def close(self) -> None:
        if self._server is None:
            return
        server = self._server
        self._server = None
        server.close()

        writers = tuple(self._writers)
        for writer in writers:
            writer.close()
        if writers:
            try:
                await asyncio.wait_for(
                    asyncio.gather(
                        *(writer.wait_closed() for writer in writers),
                        return_exceptions=True,
                    ),
                    timeout=1,
                )
            except TimeoutError:
                pass

        try:
            await asyncio.wait_for(server.wait_closed(), timeout=1)
        except TimeoutError:
            pass

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._writers.add(writer)
        try:
            while True:
                try:
                    message = await reader.readline()
                except (ValueError, asyncio.LimitOverrunError):
                    response = JsonRpcDispatcher._error(None, -32700, "Parse error")
                    writer.write(encode_message(response))
                    await writer.drain()
                    return

                if not message:
                    return

                try:
                    request = decode_message(message)
                except (
                    UnicodeDecodeError,
                    ValueError,
                    TypeError,
                    json.JSONDecodeError,
                ):
                    response = JsonRpcDispatcher._error(None, -32700, "Parse error")
                else:
                    response = await asyncio.to_thread(
                        self.dispatcher.dispatch, request
                    )

                if response is not None:
                    try:
                        encoded = encode_message(response)
                    except (TypeError, ValueError) as exc:
                        fallback = JsonRpcDispatcher._error(
                            request.get("id") if isinstance(request, dict) else None,
                            -32603,
                            "Internal error",
                            str(exc),
                        )
                        encoded = encode_message(fallback)
                    writer.write(encoded)
                    await writer.drain()
        except (ConnectionError, OSError):
            return
        finally:
            self._writers.discard(writer)
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=1)
            except (TimeoutError, ConnectionError, OSError):
                pass


class JsonRpcConnection:
    """A blocking client connection to a local JSON-RPC server."""

    def __init__(self, endpoint: RpcEndpoint, timeout: float | None = None) -> None:
        self.endpoint = endpoint
        self.timeout = timeout
        self._socket: socket.socket | None = None
        self._reader: Any = None
        self._request_id = 0
        self._lock = threading.Lock()

    def connect(self) -> None:
        if self._socket is not None:
            return

        try:
            if self.endpoint.kind == "unix":
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(self.timeout)
                sock.connect(self.endpoint.address)
            else:
                if not isinstance(self.endpoint.address, tuple):
                    raise ValueError("Invalid TCP endpoint")
                sock = socket.create_connection(
                    self.endpoint.address, timeout=self.timeout
                )
        except (OSError, ValueError) as exc:
            raise CommunicationError(str(exc)) from exc

        self._socket = sock
        self._reader = sock.makefile("rb")

    def close(self) -> None:
        reader, sock = self._reader, self._socket
        self._reader = None
        self._socket = None

        if reader is not None:
            reader.close()
        if sock is not None:
            sock.close()

    def request(
        self, method: str, params: dict[str, Any] | list[Any] | None = None
    ) -> Any:
        with self._lock:
            self.connect()
            self._request_id += 1
            request_id = self._request_id
            request: dict[str, Any] = {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
            }
            if params is not None:
                request["params"] = params

            try:
                assert self._socket is not None
                self._socket.sendall(encode_message(request))
                response_bytes = self._reader.readline(MAX_MESSAGE_SIZE + 1)
                if len(response_bytes) > MAX_MESSAGE_SIZE:
                    raise CommunicationError("JSON-RPC response is too large")
                if not response_bytes:
                    raise CommunicationError("The daemon closed the connection")
                response = decode_message(response_bytes)
            except CommunicationError:
                self.close()
                raise
            except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
                self.close()
                raise CommunicationError(str(exc)) from exc

            if not isinstance(response, dict) or response.get("id") != request_id:
                self.close()
                raise CommunicationError("Invalid JSON-RPC response")

            if "error" in response:
                error = response["error"]
                data = error.get("data")
                if isinstance(data, MaestralApiError):
                    raise data
                raise RemoteError(error["code"], error["message"], data)

            if "result" not in response:
                raise CommunicationError("Invalid JSON-RPC response")

            return response["result"]

    def __enter__(self) -> JsonRpcConnection:
        self.connect()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
