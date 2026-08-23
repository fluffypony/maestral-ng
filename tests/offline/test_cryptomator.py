from __future__ import annotations

import os
import stat
import sys
import threading
from pathlib import Path

import pytest

from maestral.cryptomator import (
    CryptomatorClient,
    CryptomatorProcessError,
    CryptomatorProtocolError,
    CryptomatorRequestError,
    CryptomatorTimeoutError,
    StorageMapEntry,
    resolve_sidecar_path,
)

FAKE_SIDECAR = r"""
import base64
import json
import os
import sys
import time
from pathlib import Path

mode = sys.argv[1]
exchange_root = Path(sys.argv[sys.argv.index("--exchange-root") + 1])
files = {}
directories = {"/"}
links = {}
page_states = {}


def send(request_id, *, result=None, error=None):
    response = {"id": request_id}
    if error is None:
        response["result"] = result
    else:
        response["error"] = error
    print(json.dumps(response, separators=(",", ":")), flush=True)


def metadata(path, include_hash=False):
    if path in directories:
        return {"path": path, "type": "directory", "size": 0, "modified_ms": 1}
    if path in links:
        target = links[path]
        return {
            "path": path,
            "type": "symlink",
            "size": len(target.encode()),
            "modified_ms": 1,
            "link_target": target,
        }
    content = files[path]
    result = {"path": path, "type": "file", "size": len(content), "modified_ms": 1}
    if include_hash:
        import hashlib
        result["sha256"] = hashlib.sha256(content).hexdigest()
    return result


def paged_result(name, entries, params):
    cursor = params.get("cursor")
    if cursor is None:
        offset = 0
    else:
        entries, offset = page_states.pop(cursor)
    end = min(offset + 2, len(entries))
    next_cursor = f"{name}:{end}" if end < len(entries) else None
    if next_cursor is not None:
        page_states[next_cursor] = (entries, end)
    return {"entries": entries[offset:end], "next_cursor": next_cursor}


for line in sys.stdin.buffer:
    request = json.loads(line)
    request_id = request["id"]
    method = request["method"]
    params = request["params"]

    if method == "hello":
        send(
            request_id,
            result={
                "protocol_version": 1 if mode == "version" else 2,
                "sidecar_version": "0.2.0",
                "cryptofs_version": "2.10.0",
                "cryptolib_version": "2.2.2",
                "vault_format": 8,
                "max_inline_bytes": 1048576,
                "vault_open": False,
            },
        )
        continue

    if mode == "crash":
        os._exit(7)
    if mode == "timeout":
        time.sleep(10)
        continue
    if mode == "malformed":
        time.sleep(0.2)
        print("{not-json", flush=True)
        continue

    if method in {"initialize", "open"}:
        secret = base64.b64decode(params["secret"]).decode()
        if mode == "redact":
            sys.stderr.write(secret)
            sys.stderr.flush()
            send(
                request_id,
                error={
                    "code": "invalid_passphrase",
                    "message": f"{secret}:{params['secret']}",
                },
            )
            continue
        leaked = secret in "\0".join(sys.argv) or any(
            secret in value for value in os.environ.values()
        )
        if leaked:
            send(
                request_id,
                error={"code": "secret_leaked", "message": "unsafe launch"},
            )
            continue
        send(
            request_id,
            result={
                "vault_path": params["vault_path"],
                "vault_format": 8,
                "shortening_threshold": 220,
                "key_id": "fake-key",
            },
        )
    elif method == "vault_info":
        send(
            request_id,
            result={
                "vault_path": "/fake-vault",
                "vault_format": 8,
                "shortening_threshold": 220,
                "key_id": "fake-key",
            },
        )
    elif method == "mkdir":
        directories.add(params["path"])
        send(request_id, result={"ok": True})
    elif method == "put_file":
        files[params["path"]] = (exchange_root / params["exchange_path"]).read_bytes()
        send(request_id, result={"ok": True})
    elif method == "get_file":
        output = exchange_root / params["exchange_path"]
        output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        output.write_bytes(files[params["path"]])
        send(request_id, result={"ok": True})
    elif method == "write_inline":
        files[params["path"]] = base64.b64decode(params["content"])
        send(request_id, result={"ok": True})
    elif method == "read_inline":
        send(request_id, result={"content": base64.b64encode(files[params["path"]]).decode()})
    elif method == "stat":
        send(request_id, result=metadata(params["path"], params["include_hash"]))
    elif method == "list":
        prefix = params["path"].rstrip("/") + "/"
        children = []
        for path in sorted(directories | files.keys() | links.keys()):
            if path.startswith(prefix) and "/" not in path[len(prefix):]:
                children.append(metadata(path, params["include_hash"]))
        send(request_id, result=children)
    elif method == "snapshot":
        if mode == "repeat_cursor":
            send(
                request_id,
                result={
                    "entries": [
                        {"path": "/loop", "type": "directory", "size": 0, "modified_ms": 1}
                    ],
                    "next_cursor": "repeat",
                },
            )
            continue
        paths = sorted((directories - {"/"}) | files.keys() | links.keys())
        entries = [metadata(path, params.get("include_hash", False)) for path in paths]
        send(request_id, result=paged_result("snapshot", entries, params))
    elif method == "storage_map":
        entries = [
            {"path": "/folder", "type": "directory", "storage_path": "d/aa/folder/dir.c9r"},
            {"path": "/folder/data.bin", "type": "file", "storage_path": "d/bb/data.c9r"},
            {"path": "/link", "type": "symlink", "storage_path": "d/cc/link/symlink.c9r"},
        ]
        send(
            request_id,
            result=paged_result("storage_map", entries, params),
        )
    elif method == "move":
        source = params["source"]
        target = params["target"]
        if source in files:
            files[target] = files.pop(source)
        elif source in directories:
            directories.remove(source)
            directories.add(target)
        send(request_id, result={"ok": True})
    elif method == "delete":
        files.pop(params["path"], None)
        directories.discard(params["path"])
        links.pop(params["path"], None)
        send(request_id, result={"ok": True})
    elif method == "symlink":
        links[params["path"]] = params["target"]
        send(request_id, result={"ok": True})
    elif method == "readlink":
        send(request_id, result={"target": links[params["path"]]})
    elif method == "close":
        send(request_id, result={"ok": True})
    elif method == "shutdown":
        send(request_id, result={"ok": True})
        break
    else:
        send(request_id, error={"code": "unknown_method", "message": "unsupported"})
"""


@pytest.fixture
def fake_sidecar(tmp_path: Path) -> Path:
    path = tmp_path / "fake_cryptomator_sidecar.py"
    path.write_text(FAKE_SIDECAR)
    return path


def make_client(
    fake_sidecar: Path, mode: str = "normal", request_timeout: float = 30.0
) -> CryptomatorClient:
    return CryptomatorClient(
        sys.executable,
        executable_arguments=(str(fake_sidecar), mode),
        request_timeout=request_timeout,
    )


def test_sidecar_storage_round_trip(fake_sidecar: Path, tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    destination = tmp_path / "destination.bin"
    source.write_bytes(b"staged content\0")

    client = make_client(fake_sidecar)
    exchange_root = client.exchange_root
    try:
        password = bytearray(b"unique vault password")
        vault = client.initialize(tmp_path / "vault", password)
        assert password == b"unique vault password"
        assert vault.vault_format == 8
        assert vault.shortening_threshold == 220

        client.mkdir("/folder")
        client.put_file("/folder/data.bin", source, modified_ms=42)
        client.get_file("/folder/data.bin", destination)
        assert destination.read_bytes() == source.read_bytes()

        client.write_inline("/inline.bin", b"inline", replace=True)
        assert client.read_inline("/inline.bin") == b"inline"
        assert client.stat("/folder/data.bin", include_hash=True).sha256 is not None
        assert [entry.path for entry in client.snapshot(include_hash=True)] == [
            "/folder",
            "/folder/data.bin",
            "/inline.bin",
        ]

        client.symlink("/link", "folder/data.bin")
        assert client.readlink("/link") == "folder/data.bin"
        client.move("/inline.bin", "/moved.bin")
        client.delete("/moved.bin")

        assert client.storage_map() == [
            StorageMapEntry("/folder", "directory", "d/aa/folder/dir.c9r"),
            StorageMapEntry("/folder/data.bin", "file", "d/bb/data.c9r"),
            StorageMapEntry("/link", "symlink", "d/cc/link/symlink.c9r"),
        ]

        assert not any((exchange_root / "input").iterdir())
        assert not any((exchange_root / "output").iterdir())
        if os.name == "posix":
            assert stat.S_IMODE(exchange_root.stat().st_mode) == 0o700
            assert exchange_root.stat().st_uid == os.getuid()
    finally:
        client.shutdown()

    assert client.closed
    assert not exchange_root.exists()


def test_sidecar_timeout_stops_process(fake_sidecar: Path) -> None:
    client = make_client(fake_sidecar, "timeout", request_timeout=0.1)

    with pytest.raises(CryptomatorTimeoutError):
        client.stat("/file")

    client.terminate()
    assert client.closed


def test_sidecar_crash_fails_request(fake_sidecar: Path) -> None:
    client = make_client(fake_sidecar, "crash")

    with pytest.raises(CryptomatorProcessError):
        client.stat("/file")

    client.terminate()


def test_malformed_output_fails_all_pending(fake_sidecar: Path) -> None:
    client = make_client(fake_sidecar, "malformed")
    barrier = threading.Barrier(3)
    errors: list[BaseException] = []

    def call() -> None:
        barrier.wait()
        try:
            client.stat("/file")
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=call) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=2)

    assert len(errors) == 2
    assert all(isinstance(error, CryptomatorProtocolError) for error in errors)
    client.terminate()


def test_sidecar_version_mismatch(fake_sidecar: Path) -> None:
    with pytest.raises(CryptomatorProtocolError, match="not supported"):
        make_client(fake_sidecar, "version")


def test_repeated_page_cursor_stops_process(fake_sidecar: Path) -> None:
    client = make_client(fake_sidecar, "repeat_cursor")

    with pytest.raises(CryptomatorProtocolError, match="cursor"):
        client.snapshot()

    client.terminate()


def test_password_is_redacted(fake_sidecar: Path, tmp_path: Path) -> None:
    password = "unique-secret-not-in-the-environment"
    client = make_client(fake_sidecar, "redact")

    try:
        with pytest.raises(CryptomatorRequestError) as exc_info:
            client.open(tmp_path / "vault", password)
    finally:
        client.terminate()

    encoded = __import__("base64").b64encode(password.encode()).decode()
    assert password not in str(exc_info.value)
    assert password not in repr(exc_info.value)
    assert encoded not in str(exc_info.value)
    assert password not in repr(client)


def test_resolve_sidecar_path_prefers_config_then_environment(
    tmp_path: Path,
) -> None:
    configured = tmp_path / "configured"
    environment = tmp_path / "environment"
    configured.touch()
    environment.touch()

    assert (
        resolve_sidecar_path(
            configured, environ={"MAESTRAL_CRYPTOMATOR_SIDECAR": str(environment)}
        )
        == configured
    )
    assert (
        resolve_sidecar_path(environ={"MAESTRAL_CRYPTOMATOR_SIDECAR": str(environment)})
        == environment
    )


def test_insecure_exchange_root_is_rejected(fake_sidecar: Path, tmp_path: Path) -> None:
    exchange_root = tmp_path / "insecure"
    exchange_root.mkdir(mode=0o755)
    exchange_root.chmod(0o755)

    if os.name == "posix":
        with pytest.raises(CryptomatorProcessError, match="another user"):
            CryptomatorClient(
                sys.executable,
                executable_arguments=(str(fake_sidecar), "normal"),
                exchange_root=exchange_root,
            )
