import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path, PureWindowsPath
from unittest.mock import Mock

import pytest

import maestral.daemon as daemon_module
from maestral.core import FileMetadata
from maestral.daemon import CommunicationError, MaestralClient
from maestral.exceptions import NotLinkedError
from maestral.models import (
    ChangeType,
    ItemType,
    SyncDirection,
    SyncEvent,
    SyncStatus,
)
from maestral.rpc import (
    PROTOCOL_VERSION,
    JsonRpcConnection,
    JsonRpcDispatcher,
    RpcEndpoint,
    decode_message,
    encode_message,
    from_json_value,
)


def make_sync_event(path: str = "/notes.txt") -> SyncEvent:
    return SyncEvent(
        id=7,
        dbx_path=path,
        direction=SyncDirection.Down,
        status=SyncStatus.Done,
        local_path=f"/tmp{path}",
        dbx_path_lower=path.lower(),
        change_type=ChangeType.Added,
        completed=12,
        size=12,
        item_type=ItemType.File,
        sync_time=1.0,
        change_time=2.0,
    )


def test_safe_api_value_round_trip(tmp_path):
    modified = datetime(2025, 2, 3, 4, 5, tzinfo=timezone.utc)
    metadata = FileMetadata(
        name="notes.txt",
        path_lower="/notes.txt",
        path_display="/Notes.txt",
        id="id:123",
        client_modified=modified,
        server_modified=modified,
        rev="abc",
        size=12,
        symlink_target=None,
        shared=False,
        modified_by=None,
        is_downloadable=True,
        content_hash="hash",
    )
    error = NotLinkedError("Not linked", "Link an account.")
    source = {
        "metadata": metadata,
        "event": make_sync_event(),
        "direction": SyncDirection.Down,
        "datetime": modified,
        "path": Path(tmp_path),
        "windows_path": PureWindowsPath("C:/Dropbox/notes.txt"),
        "set": {"/one", "/two"},
        "tuple": (1, 2),
        "error": error,
    }

    restored = decode_message(encode_message(source))

    assert restored["metadata"] == metadata
    assert restored["event"].dbx_path == "/notes.txt"
    assert restored["event"].direction is SyncDirection.Down
    assert restored["datetime"] == modified
    assert restored["path"] == tmp_path
    assert restored["windows_path"] == PureWindowsPath("C:/Dropbox/notes.txt")
    assert restored["set"] == {"/one", "/two"}
    assert restored["tuple"] == (1, 2)
    assert isinstance(restored["error"], NotLinkedError)
    assert restored["error"].message == "Link an account."


def test_decoder_rejects_unregistered_class():
    value = {
        "__maestral_type__": "dataclass",
        "class": "builtins.object",
        "fields": {},
    }

    with pytest.raises(ValueError, match="Unsupported API class"):
        from_json_value(value)


@pytest.mark.parametrize("constant", [b"NaN", b"Infinity", b"-Infinity"])
def test_decoder_rejects_non_json_numbers(constant):
    with pytest.raises(ValueError, match="Invalid JSON constant"):
        decode_message(b'{"value":' + constant + b"}\n")


class ApiTarget:
    def __init__(self) -> None:
        self._level = 10

    @property
    def level(self) -> int:
        return self._level

    @level.setter
    def level(self, value: int) -> None:
        self._level = value

    @property
    def read_only(self) -> str:
        return "fixed"

    def add(self, first: int, second: int = 0) -> int:
        return first + second

    def fail(self) -> None:
        raise NotLinkedError("Not linked", "Link an account.")


def rpc_request(method: str, params=None, request_id: int = 1):
    request = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        request["params"] = params
    return request


def test_protocol_handshake_methods_and_properties():
    dispatcher = JsonRpcDispatcher(ApiTarget())

    response = dispatcher.dispatch(rpc_request("rpc.handshake"))
    result = response["result"]

    assert result["protocol_version"] == PROTOCOL_VERSION
    assert result["daemon_version"]
    assert result["methods"] == ["add", "fail"]
    assert result["properties"]["read"] == ["level", "read_only"]
    assert result["properties"]["write"] == ["level"]


def test_protocol_method_property_and_error_dispatch():
    target = ApiTarget()
    dispatcher = JsonRpcDispatcher(target)

    added = dispatcher.dispatch(rpc_request("add", {"first": 2, "second": 3}))
    assert added["result"] == 5

    changed = dispatcher.dispatch(
        rpc_request("rpc.set", {"name": "level", "value": logging.INFO})
    )
    assert changed["result"] is None
    assert (
        dispatcher.dispatch(rpc_request("rpc.get", {"name": "level"}))["result"]
        == logging.INFO
    )

    failed = dispatcher.dispatch(rpc_request("fail"))
    restored = decode_message(encode_message(failed))
    assert restored["error"]["code"] == -32010
    assert isinstance(restored["error"]["data"], NotLinkedError)

    missing = dispatcher.dispatch(rpc_request("private_method"))
    assert missing["error"]["code"] == -32601

    read_only = dispatcher.dispatch(
        rpc_request("rpc.set", {"name": "read_only", "value": "changed"})
    )
    assert read_only["error"]["code"] == -32601


def test_protocol_batch_and_notification():
    dispatcher = JsonRpcDispatcher(ApiTarget())
    batch = [
        rpc_request("add", [1, 2], request_id=1),
        {"jsonrpc": "2.0", "method": "rpc.set", "params": ["level", 30]},
        rpc_request("rpc.get", ["level"], request_id=2),
    ]

    response = dispatcher.dispatch(batch)

    assert [item["result"] for item in response] == [3, 30]


def test_client_encodes_mixed_positional_and_named_arguments():
    connection = JsonRpcConnection(RpcEndpoint("unix", "/unused"))
    connection.request = Mock(return_value=5)
    client = object.__new__(MaestralClient)
    client._config_name = "test"
    client._is_fallback = False
    client._m = connection
    client._remote_methods = {"add"}
    client._readable_properties = set()
    client._writable_properties = set()

    assert client.add(2, second=3) == 5
    connection.request.assert_called_once_with(
        "add",
        {
            "__maestral_args__": [2],
            "__maestral_kwargs__": {"second": 3},
        },
    )


def test_selective_sync_api_is_available_over_rpc(m, monkeypatch):
    setter = Mock(return_value=None)
    monkeypatch.setattr(m, "set_selective_sync", setter)
    dispatcher = JsonRpcDispatcher(m)

    handshake = dispatcher.dispatch(rpc_request("rpc.handshake"))["result"]
    response = dispatcher.dispatch(
        rpc_request(
            "set_selective_sync",
            {"mode": "include", "dbx_paths": ["/Notes/todo.txt"]},
        )
    )

    assert "set_selective_sync" in handshake["methods"]
    assert "selective_sync_mode" in handshake["properties"]["read"]
    assert "selective_sync_paths" in handshake["properties"]["read"]
    assert response["result"] is None
    setter.assert_called_once_with(mode="include", dbx_paths=["/Notes/todo.txt"])


def test_direct_shared_link_lookup_is_available_over_rpc(m, monkeypatch):
    lookup = Mock(return_value=[])
    monkeypatch.setattr(m, "list_shared_links", lookup)
    dispatcher = JsonRpcDispatcher(m)

    response = dispatcher.dispatch(
        rpc_request(
            "list_shared_links",
            {"dbx_path": "/Notes/todo.txt", "direct_only": True},
        )
    )

    assert response["result"] == []
    lookup.assert_called_once_with(dbx_path="/Notes/todo.txt", direct_only=True)


def test_sync_event_longpoll(m):
    result = {}

    def wait_for_event():
        result.update(m.wait_for_sync_events(cursor=0, timeout=2))

    waiter = threading.Thread(target=wait_for_event)
    waiter.start()
    m._publish_sync_events([make_sync_event()])
    waiter.join(timeout=3)

    assert not waiter.is_alive()
    assert result["cursor"] == 1
    assert result["events"][0].dbx_path == "/notes.txt"

    timed_out = m.wait_for_sync_events(cursor=1, timeout=0)
    assert timed_out == {"cursor": 1, "events": []}

    baseline = m.wait_for_sync_events(cursor=None, timeout=0)
    assert baseline == {"cursor": 1, "events": []}


def test_app_snapshot_uses_cached_usage_and_deduplicates_history(m, monkeypatch):
    m.set_state("account", "type", "business")
    m.set_state("account", "usage_used", 123)
    m.set_state("account", "usage_allocated", 456)
    active = make_sync_event("/active.txt")
    duplicate = make_sync_event("/active.txt")
    recent = make_sync_event("/recent.txt")
    recent.id = 8
    m.sync.activity.add(active)
    monkeypatch.setattr(m.sync, "get_history", Mock(return_value=[duplicate, recent]))

    snapshot = m.get_app_snapshot()

    assert snapshot["account"]["type"] == "business"
    assert snapshot["space_usage"] == {"used": 123, "allocated": 456}
    assert [event.id for event in snapshot["activity"]] == [7, 8]


def test_applied_upload_and_download_batches_publish(m, monkeypatch):
    callback = Mock()
    sync = m.sync
    sync.event_callback = callback
    monkeypatch.setattr(sync, "is_excluded", lambda _path: False)
    monkeypatch.setattr(sync, "is_mignore", lambda _event: False)
    monkeypatch.setattr(sync, "is_excluded_by_selective_sync", lambda _path: False)

    upload = make_sync_event("/upload.txt")
    upload.direction = SyncDirection.Up
    upload.status = SyncStatus.Queued

    def complete_upload(event):
        event.status = SyncStatus.Done
        return event

    monkeypatch.setattr(sync, "_create_remote_entry", complete_upload)
    assert sync.apply_local_changes([upload]) == [upload]
    callback.assert_called_once_with([upload])

    callback.reset_mock()
    download = make_sync_event("/download.txt")
    download.status = SyncStatus.Queued

    def complete_download(event):
        event.status = SyncStatus.Done
        return event

    monkeypatch.setattr(sync, "_create_local_entry", complete_download)
    assert sync.apply_remote_changes([download]) == [download]
    callback.assert_called_once_with([download])


def test_remote_property_and_longpoll(config_name):
    assert (
        daemon_module.start_maestral_daemon_process(config_name, timeout=20).value == 0
    )

    with MaestralClient(config_name) as client:
        original_level = client.log_level
        client.log_level = logging.DEBUG
        assert client.log_level == logging.DEBUG
        client.log_level = original_level
        assert client.status_change_longpoll(timeout=0) is False
        assert client.wait_for_sync_events(cursor=0, timeout=0) == {
            "cursor": 0,
            "events": [],
        }


def test_windows_endpoint_publication_and_discovery(tmp_path, monkeypatch):
    monkeypatch.setattr(daemon_module.platform, "system", lambda: "Windows")
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))

    daemon_module._publish_tcp_endpoint("work", "127.0.0.1", 4242)
    endpoint = daemon_module.endpoint_for_config("work")

    assert endpoint.kind == "tcp"
    assert endpoint.address == ("127.0.0.1", 4242)
    endpoint_path = daemon_module.endpoint_path_for_config("work")
    assert json.loads(Path(endpoint_path).read_text()) == {
        "host": "127.0.0.1",
        "port": 4242,
    }
    assert not list(Path(endpoint_path).parent.glob(".work.*"))


def test_windows_discovery_rejects_non_loopback(tmp_path, monkeypatch):
    monkeypatch.setattr(daemon_module.platform, "system", lambda: "Windows")
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))
    endpoint_path = Path(daemon_module.endpoint_path_for_config("work"))
    endpoint_path.write_text('{"host":"192.0.2.10","port":4242}')

    with pytest.raises(CommunicationError, match="non-loopback"):
        daemon_module.endpoint_for_config("work")
