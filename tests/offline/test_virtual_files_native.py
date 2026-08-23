from __future__ import annotations

import hashlib
import json
import os
import stat
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest

from maestral.config import MaestralConfig
from maestral.exceptions import (
    VirtualFileBusyError,
    VirtualFileRevisionError,
    VirtualFilesUnsupportedError,
)
from maestral.main import Maestral
from maestral.virtual_files import (
    MIRROR_MODE,
    VIRTUAL_MODE,
    UnsupportedVirtualFileBackend,
    VirtualFileDescriptor,
    VirtualFileIdentity,
    VirtualFileRootBinding,
)
from maestral.virtual_files_native import (
    MAX_LINE_BYTES,
    NativeProcessVirtualFileBackend,
    NativeVirtualFileProcessError,
    NativeVirtualFileProtocolError,
    NativeVirtualFileRequestError,
    NativeVirtualFileTimeoutError,
    create_process_virtual_file_backend,
    resolve_native_virtual_file_executable,
)

ROOT_MARKER_ID = "0123456789abcdef0123456789abcdef"


def native_config_name(tmp_path: Path) -> str:
    digest = hashlib.sha256(os.fsencode(tmp_path)).hexdigest()[:16]
    return f"native-{digest}"


FAKE_ADAPTER = r"""#!/usr/bin/env python3
import json
import os
import pathlib
import sys
import threading
import time

mode = sys.argv[1] if len(sys.argv) > 1 else "normal"
log_path = pathlib.Path(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] else None
event_stage = sys.argv[3] if len(sys.argv) > 3 else None
writer_lock = threading.Lock()
log_lock = threading.Lock()
items = {}
stopped = False
root_identity = None
source_root_path = None
recovery_snapshot = None


def write(value):
    data = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"
    with writer_lock:
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()


def result(request_id, value):
    write({"protocolVersion": 1, "requestId": request_id, "result": value})


def error(request_id, code, message):
    write({
        "protocolVersion": 1,
        "requestId": request_id,
        "error": {"code": code, "message": message},
    })


def append_log(value):
    if log_path is None:
        return
    with log_lock:
        with log_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(value, separators=(",", ":")) + "\n")


def native_request(request_id, method, params):
    write({
        "protocolVersion": 1,
        "requestId": request_id,
        "method": method,
        "params": params,
    })


def send_hydration(item, suffix="1"):
    time.sleep(0.03)
    native_request(
        f"native-hydrate-{suffix}",
        "hydrate",
        {"providerId": item["providerId"], "expectedRevision": item["revision"]},
    )


def send_hydration_pair(item):
    send_hydration(item, "1")
    time.sleep(0.1)
    native_request(
        "native-hydrate-2",
        "hydrate",
        {"providerId": item["providerId"], "expectedRevision": item["revision"]},
    )


def send_events():
    time.sleep(0.03)
    native_request("native-enumerate", "enumerate", {
        "parentProviderId": "id:folder", "cursor": None, "limit": 10,
    })
    native_request("native-dehydrate", "dehydrate", {"providerId": "id:file"})
    native_request(
        "native-pin", "pin_changed", {"providerId": "id:file", "pinned": True}
    )
    native_request("native-change", "local_change", {
        "kind": "created",
        "providerId": None,
        "path": "/local.txt",
        "previousPath": None,
        "stagedPath": event_stage,
    })
    write({
        "protocolVersion": 1,
        "requestId": "native-invalid",
        "method": "hydrate",
        "params": {"providerId": "id:file", "expectedRevision": "rev-1"},
        "legacy": True,
    })
    native_request("native-bad-path", "local_change", {
        "kind": "created",
        "providerId": None,
        "path": "/../escape",
        "previousPath": None,
        "stagedPath": "relative-stage",
    })


def inspect_value(item):
    return {
        "identity": {
            "providerId": item["providerId"],
            "path": item["path"],
            "isDirectory": item["isDirectory"],
            "revision": item["revision"],
        },
        "hydratedRevision": item.get("hydratedRevision"),
        "pinned": item["pinned"],
        "dirty": item.get("dirty", False),
        "openCount": item.get("openCount", 0),
    }


def max_field(pattern, length, suffix):
    prefix_length = length - len(suffix)
    return (pattern * ((prefix_length // len(pattern)) + 1))[:prefix_length] + suffix


def handle(request):
    global recovery_snapshot, root_identity, source_root_path, stopped
    request_id = request["requestId"]
    method = request["method"]
    params = request["params"]

    if method == "start":
        append_log({"start": params})
        root_identity = params["rootIdentity"]
        source_root_path = params["rootPath"]
        if mode.startswith("recover-"):
            count = 8 if mode == "recover-max-fields" else 9
            for index in range(count):
                if mode == "recover-max-fields":
                    suffix = f"{index:06d}"
                    provider_id = max_field('p"\\', 4096, suffix)
                    path = "/" + max_field('a"\\', 32767, suffix)
                    revision = max_field('r"\\', 4096, suffix)
                else:
                    provider_id = f"id:recover-{index:04d}"
                    path = f"/recover-{index:04d}.txt"
                    revision = "rev-1"
                items[provider_id] = {
                    "providerId": provider_id,
                    "path": path,
                    "isDirectory": False,
                    "revision": revision,
                    "contentHash": None,
                    "size": 0,
                    "symlinkTarget": None,
                    "pinned": False,
                    "hydratedRevision": (
                        revision if mode == "recover-max-fields" else None
                    ),
                    "dirty": False,
                    "openCount": 0,
                }
        if mode == "unknown-response":
            result("wrong-id", {"adapter": "linux-fuse", "capabilities": []})
            return
        if mode == "bad-start":
            result(request_id, {
                "adapter": "linux-fuse",
                "capabilities": [
                    "placeholders", "hydration", "dehydration", "pinning", "recovery"
                ],
                "legacy": True,
            })
            return
        start_result = {
            "adapter": (
                "macos-file-provider" if mode.startswith("macos") else "linux-fuse"
            ),
            "capabilities": [
                "placeholders", "hydration", "dehydration", "pinning", "recovery"
            ],
            "rootPath": params["rootPath"],
            "cachePath": params["cachePath"],
            "rootIdentity": params["rootIdentity"],
        }
        if mode.startswith("macos"):
            source_root = pathlib.Path(params["rootPath"])
            actual_root = source_root.parent / "macos-visible-root"
            actual_cache = source_root.parent / "macos-app-group-cache"
            actual_root.mkdir(mode=0o700, exist_ok=True)
            actual_cache.mkdir(mode=0o700, exist_ok=True)
            actual_cache.chmod(0o700)
            (actual_cache / "source-root").write_text(str(source_root))
            actual_status = actual_root.lstat()
            start_result["rootPath"] = str(actual_root)
            start_result["cachePath"] = str(actual_cache)
            start_result["rootIdentity"] = {
                "device": str(actual_status.st_dev),
                "inode": str(actual_status.st_ino),
                "mode": actual_status.st_mode,
            }
        elif mode == "bad-root-mode":
            start_result["rootIdentity"] = dict(start_result["rootIdentity"])
            start_result["rootIdentity"]["mode"] = 0o100600
        result(request_id, start_result)
        if mode == "stop-reading":
            time.sleep(60)
            return
        if mode == "events":
            threading.Thread(target=send_events, daemon=True).start()
        return

    if method == "validate_root":
        append_log({"validate_root": params})
        if root_identity is None and mode != "macos":
            error(request_id, "BUSY", "The Linux native root is inactive")
            stopped = True
            return
        if mode == "validate-busy":
            error(request_id, "BUSY", "The native root is inactive")
            return
        validated_identity = params["rootIdentity"]
        if mode == "bad-validate-root":
            validated_identity = dict(validated_identity)
            validated_identity["inode"] = "999"
        result(request_id, {"rootIdentity": validated_identity})
        if root_identity is None:
            stopped = True
        return

    if method == "stop":
        if mode == "stop-slow":
            append_log({"stop": params})
            time.sleep(0.8)
        result(request_id, {})
        stopped = True
        return

    if method == "detach":
        append_log({"detach": params})
        if mode.startswith("macos") and source_root_path is None:
            source_root_path = (
                pathlib.Path(params["cachePath"]) / "source-root"
            ).read_text()
        detach_result = {
            "rootPath": (
                source_root_path if mode.startswith("macos") else params["rootPath"]
            )
        }
        if mode == "macos-detach-lost-response":
            completed = pathlib.Path(params["cachePath"]) / "detach-complete"
            if not completed.exists():
                pathlib.Path(params["rootPath"]).rmdir()
                completed.write_text("complete")
                os._exit(0)
        if mode == "bad-detach":
            detach_result["legacy"] = True
        elif mode == "bad-detach-path":
            unsafe_root = pathlib.Path(params["rootPath"]).parent / "wrong-detach-root"
            unsafe_root.mkdir(mode=0o700, exist_ok=True)
            detach_result["rootPath"] = str(unsafe_root)
        result(request_id, detach_result)
        stopped = True
        return

    if method == "upsert":
        item = dict(params["item"])
        previous = items.get(item["providerId"])
        expected_identity = params["expectedIdentity"]
        target_keys = {
            "providerId", "path", "isDirectory", "revision", "contentHash", "size",
            "symlinkTarget", "pinned",
        }
        if previous is not None and all(
            previous[key] == item[key] for key in target_keys
        ):
            result(request_id, {"operation": "unchanged"})
            return
        if previous is None and expected_identity is not None:
            error(request_id, "REVISION_MISMATCH", "The placeholder was deleted")
            return
        current_identity = None if previous is None else {
            "providerId": previous["providerId"],
            "path": previous["path"],
            "isDirectory": previous["isDirectory"],
            "revision": previous["revision"],
        }
        if current_identity != expected_identity:
            error(request_id, "REVISION_MISMATCH", "The placeholder identity changed")
            return
        if previous is not None and previous.get("dirty") and any(
            previous[key] != item[key] for key in target_keys
        ):
            error(request_id, "BUSY", "The placeholder is dirty")
            return
        operation = "created"
        if previous is not None:
            operation = "moved" if previous["path"] != item["path"] else "updated"
            if mode == "bad-upsert-result":
                operation = "created"
        item["hydratedRevision"] = (
            previous.get("hydratedRevision")
            if previous and previous["revision"] == item["revision"]
            else None
        )
        item["dirty"] = mode == "dirty"
        item["openCount"] = 0
        items[item["providerId"]] = item
        result(request_id, {"operation": operation})
        if mode in {"callback", "callback-error"}:
            threading.Thread(target=send_hydration, args=(item,), daemon=True).start()
        elif mode == "callback-pair":
            threading.Thread(
                target=send_hydration_pair, args=(item,), daemon=True
            ).start()
        return

    if method == "remove":
        expected_identity = params["expectedIdentity"]
        provider_id = expected_identity["providerId"]
        item = items.get(provider_id)
        if item is None:
            if mode == "remove-not-found":
                error(request_id, "NOT_FOUND", "The placeholder does not exist")
            else:
                result(request_id, {})
            return
        current_identity = {
            "providerId": item["providerId"],
            "path": item["path"],
            "isDirectory": item["isDirectory"],
            "revision": item["revision"],
        }
        if current_identity != expected_identity:
            error(request_id, "REVISION_MISMATCH", "The placeholder identity changed")
            return
        if item.get("dirty") or item.get("openCount"):
            error(request_id, "BUSY", "The placeholder is busy")
            return
        items.pop(provider_id)
        result(request_id, {})
        return

    if method == "set_pinned":
        item = items.get(params["providerId"])
        if item is None:
            error(request_id, "NOT_FOUND", "The placeholder does not exist")
        else:
            item["pinned"] = params["pinned"]
            result(request_id, {})
        return

    if method == "materialize":
        staged = pathlib.Path(params["stagedPath"])
        append_log({"materialize": params})
        if mode == "materialize-slow":
            time.sleep(0.15)
        if mode == "materialize-timeout":
            time.sleep(60)
            return
        if mode == "materialize-busy":
            error(request_id, "BUSY", "The placeholder is dirty")
            return
        item = items.get(params["item"]["providerId"])
        if item is None:
            error(request_id, "NOT_FOUND", "The placeholder does not exist")
        elif item["revision"] != params["expectedRevision"]:
            error(request_id, "REVISION_MISMATCH", "The placeholder revision changed")
        elif item.get("dirty"):
            error(request_id, "BUSY", "The placeholder is dirty")
        else:
            item["content"] = staged.read_bytes().decode("latin1")
            item["hydratedRevision"] = params["expectedRevision"]
            result(request_id, {})
        return

    if method == "evict":
        item = items.get(params["providerId"])
        if item is None:
            error(request_id, "NOT_FOUND", "The placeholder does not exist")
        elif item["revision"] != params["expectedRevision"]:
            error(request_id, "REVISION_MISMATCH", "The placeholder revision changed")
        elif item["pinned"] or item.get("dirty") or item.get("openCount"):
            error(request_id, "BUSY", "The placeholder is busy")
        else:
            item["hydratedRevision"] = None
            result(request_id, {})
        return

    if method == "inspect":
        if mode == "timeout":
            time.sleep(60)
            return
        if mode == "crash":
            sys.stderr.write("x" * (70 * 1024) + "adapter crash detail\n")
            sys.stderr.flush()
            os._exit(17)
        if mode == "malformed":
            sys.stdout.buffer.write(b"not-json\n")
            sys.stdout.buffer.flush()
            return
        if mode == "duplicate-keys":
            raw_id = json.dumps(request_id).encode()
            sys.stdout.buffer.write(
                b'{"protocolVersion":1,"protocolVersion":1,"requestId":'
                + raw_id
                + b',"result":{}}\n'
            )
            sys.stdout.buffer.flush()
            return
        if mode == "invalid-utf8":
            sys.stdout.buffer.write(b"\xff\n")
            sys.stdout.buffer.flush()
            return
        if mode == "bad-version":
            write({"protocolVersion": True, "requestId": request_id, "result": {}})
            return
        if mode == "oversized":
            sys.stdout.buffer.write(b"{" + b" " * (1024 * 1024) + b"}\n")
            sys.stdout.buffer.flush()
            return
        if mode == "io-error":
            error(request_id, "IO_ERROR", "The cache could not be read")
            return
        item = items.get(params["providerId"])
        if item is None:
            error(request_id, "NOT_FOUND", "The placeholder does not exist")
            return
        value = inspect_value(item)
        if mode == "bad-result":
            value["openCount"] = True
        if mode == "concurrent":
            delay = (10 - int(item["providerId"].rsplit("-", 1)[-1])) * 0.003
            threading.Thread(
                target=lambda: (time.sleep(delay), result(request_id, value)), daemon=True
            ).start()
        else:
            result(request_id, value)
        return

    if method == "recover":
        append_log({"recover": params})
        limit = params["limit"]
        cursor = params["cursor"]
        if mode == "recover-endless":
            offset = 0 if cursor is None else int(cursor.rsplit(":", 1)[-1])
            provider_id = f"id:endless-{offset}"
            endless_item = inspect_value({
                "providerId": provider_id,
                "path": f"/endless-{offset}",
                "isDirectory": False,
                "revision": "rev-1",
                "pinned": False,
            })
            result(request_id, {
                "items": [endless_item],
                "cursor": f"snapshot:{offset + 1}",
                "hasMore": True,
            })
            return
        if cursor is None:
            recovery_snapshot = [
                inspect_value(item)
                for item in sorted(items.values(), key=lambda item: item["providerId"])
            ]
            offset = 0
        else:
            offset = int(cursor.rsplit(":", 1)[-1])
        snapshot = recovery_snapshot or []
        page = snapshot[offset:offset + limit]
        if mode == "recover-too-many":
            page = [inspect_value({
                "providerId": f"id:extra-{index}",
                "path": f"/extra-{index}",
                "isDirectory": False,
                "revision": "rev-1",
                "pinned": False,
            }) for index in range(limit + 1)]
        if mode == "recover-duplicate-id" and offset:
            page[0]["identity"]["providerId"] = snapshot[0]["identity"]["providerId"]
        if mode == "recover-duplicate-path" and offset:
            page[0]["identity"]["path"] = snapshot[0]["identity"]["path"]
        next_offset = offset + len(page)
        has_more = next_offset < len(snapshot)
        page_cursor = f"snapshot:{next_offset}"
        if mode == "recover-empty-cursor":
            page_cursor = ""
        elif mode == "recover-empty-page":
            page = []
            page_cursor = "snapshot:empty"
            has_more = True
        elif mode == "recover-nonadvancing" and cursor is not None:
            page_cursor = cursor
            has_more = True
        result(request_id, {
            "items": page,
            "cursor": page_cursor,
            "hasMore": has_more,
        })
        return

    error(request_id, "INVALID_REQUEST", "Unknown method")


for raw_line in sys.stdin.buffer:
    try:
        message = json.loads(raw_line)
        if "method" in message:
            handle(message)
        else:
            append_log(message)
    except Exception as exception:
        sys.stderr.write(f"{type(exception).__name__}: {exception}\n")
        sys.stderr.flush()
        os._exit(23)
    if stopped:
        break
"""


@pytest.fixture
def fake_adapter(tmp_path: Path) -> Path:
    executable = tmp_path / "fake-smart-sync"
    executable.write_text(FAKE_ADAPTER)
    executable.chmod(0o700)
    return executable


def make_backend(
    fake_adapter: Path,
    tmp_path: Path,
    mode: str = "normal",
    *,
    timeout: float = 2.0,
    log_path: Path | None = None,
    event_stage: Path | None = None,
    platform_name: str = "Linux",
    cache_path: Path | None = None,
) -> NativeProcessVirtualFileBackend:
    config_name = native_config_name(tmp_path)
    MaestralConfig(config_name).set("sync", "root_marker_id", ROOT_MARKER_ID)
    arguments = (
        mode,
        str(log_path) if log_path is not None else "",
        str(event_stage) if event_stage is not None else "",
    )
    return NativeProcessVirtualFileBackend(
        config_name,
        fake_adapter,
        cache_path=cache_path or tmp_path / "cache",
        request_timeout=timeout,
        executable_arguments=arguments,
        platform_name=platform_name,
    )


def descriptor(
    provider_id: str = "id:file", revision: str = "rev-1"
) -> VirtualFileDescriptor:
    return VirtualFileDescriptor(
        provider_id=provider_id,
        path=f"/{provider_id.replace(':', '-')}.txt",
        is_directory=False,
        revision=revision,
        content_hash=None,
        size=4,
        symlink_target=None,
        pinned=False,
    )


def identity(
    item: VirtualFileDescriptor,
    *,
    path: str | None = None,
    is_directory: bool | None = None,
    revision: str | None = None,
) -> VirtualFileIdentity:
    return VirtualFileIdentity(
        provider_id=item.provider_id,
        path=item.path if path is None else path,
        is_directory=item.is_directory if is_directory is None else is_directory,
        revision=item.revision if revision is None else revision,
    )


def start_backend(backend: NativeProcessVirtualFileBackend, tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir(exist_ok=True)
    marker = root / ".maestral-root"
    marker.write_text(f"maestral-root-v1:{ROOT_MARKER_ID}\n")
    marker.chmod(0o600)
    backend.start(str(root), lambda _provider_id, _revision: {})


def private_stage(tmp_path: Path, name: str = "stage") -> Path:
    stage = tmp_path / name
    stage.write_bytes(b"data")
    stage.chmod(0o600)
    return stage


def wait_for_log(log_path: Path, count: int) -> list[dict[str, object]]:
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if log_path.exists():
            lines = log_path.read_text().splitlines()
            if len(lines) >= count:
                return [json.loads(line) for line in lines]
        time.sleep(0.01)
    raise AssertionError(f"Expected {count} protocol log entries")


def require_object(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return value


def error_code(response: dict[str, object]) -> str:
    code = require_object(response["error"])["code"]
    assert isinstance(code, str)
    return code


def test_native_backend_lifecycle_and_exact_results(
    fake_adapter: Path, tmp_path: Path
) -> None:
    log_path = tmp_path / "requests.jsonl"
    backend = make_backend(fake_adapter, tmp_path, log_path=log_path)
    start_backend(backend, tmp_path)
    try:
        assert backend.root_id == f"maestral:{backend.config_name}:{ROOT_MARKER_ID}"
        assert backend.backend_id == "linux-fuse"
        assert backend.capabilities == (
            "placeholders",
            "hydration",
            "dehydration",
            "pinning",
            "recovery",
        )
        assert stat.S_IMODE(Path(backend.cache_path).stat().st_mode) == 0o700
        start_params = require_object(wait_for_log(log_path, 1)[0]["start"])
        assert set(start_params) == {
            "rootId",
            "rootPath",
            "cachePath",
            "rootIdentity",
            "rootMarker",
        }
        assert start_params["rootId"] == (
            f"maestral:{backend.config_name}:{ROOT_MARKER_ID}"
        )
        assert start_params["cachePath"] == backend.cache_path
        root_identity = require_object(start_params["rootIdentity"])
        root_stat = os.lstat(tmp_path / "root")
        assert root_identity == {
            "device": str(root_stat.st_dev),
            "inode": str(root_stat.st_ino),
            "mode": root_stat.st_mode,
        }
        assert start_params["rootMarker"] == {
            "name": ".maestral-root",
            "content": f"maestral-root-v1:{ROOT_MARKER_ID}\n",
        }

        item = descriptor()
        backend.upsert(item, expected_identity=None)
        state = backend.inspect(item.provider_id)
        assert state is not None
        assert state.hydrated_revision is None
        stage = private_stage(tmp_path)
        staged_stat = os.lstat(stage)
        backend.materialize(item, str(stage), expected_revision=item.revision)
        materialize_params = require_object(
            wait_for_log(log_path, 2)[-1]["materialize"]
        )
        assert set(materialize_params) == {
            "item",
            "stagedPath",
            "stagedIdentity",
            "expectedRevision",
        }
        assert materialize_params["stagedIdentity"] == {
            "device": str(staged_stat.st_dev),
            "inode": str(staged_stat.st_ino),
            "size": staged_stat.st_size,
            "mode": staged_stat.st_mode,
        }
        assert stage.read_bytes() == b"data"
        hydrated = backend.inspect(item.provider_id)
        assert hydrated is not None
        assert hydrated.hydrated_revision == item.revision

        backend.set_pinned(item.provider_id, True)
        with pytest.raises(VirtualFileBusyError):
            backend.evict(item.provider_id, expected_revision=item.revision)
        backend.set_pinned(item.provider_id, False)
        backend.evict(item.provider_id, expected_revision=item.revision)
        evicted = backend.inspect(item.provider_id)
        assert evicted is not None
        assert evicted.hydrated_revision is None
        assert [record.identity.provider_id for record in backend.recover()] == [
            item.provider_id
        ]
        backend.remove(identity(item))
        assert backend.inspect(item.provider_id) is None
    finally:
        backend.stop()
    assert backend._process is None
    backend.stop()


def test_macos_start_uses_returned_platform_paths(
    fake_adapter: Path, tmp_path: Path
) -> None:
    log_path = tmp_path / "macos-paths.jsonl"
    backend = make_backend(
        fake_adapter,
        tmp_path,
        mode="macos",
        platform_name="Darwin",
        log_path=log_path,
    )
    root = tmp_path / "root"
    root.mkdir()
    marker = root / ".maestral-root"
    marker.write_text(f"maestral-root-v1:{ROOT_MARKER_ID}\n")
    marker.chmod(0o600)
    binding = backend.start(str(root), lambda _provider_id, _revision: {})
    try:
        assert isinstance(binding, VirtualFileRootBinding)
        assert binding.source_root_path == str(root)
        assert binding.root_path == str(tmp_path / "macos-visible-root")
        assert binding.cache_path == str(tmp_path / "macos-app-group-cache")
        assert backend.root_path == str(tmp_path / "macos-visible-root")
        assert backend.cache_path == str(tmp_path / "macos-app-group-cache")
        actual_status = os.lstat(binding.root_path)
        assert binding.root_identity.device == str(actual_status.st_dev)
        assert binding.root_identity.inode == str(actual_status.st_ino)
        assert binding.root_identity.mode == actual_status.st_mode
        backend.stop()
        backend.validate_root(binding.root_path, ROOT_MARKER_ID)
        assert backend.detach(binding.root_path, ROOT_MARKER_ID) == str(root)
        entries = wait_for_log(log_path, 3)
        lifecycle_params = [
            require_object(entry[method])
            for entry in entries
            for method in ("validate_root", "detach")
            if method in entry
        ]
        assert len(lifecycle_params) == 2
        for params in lifecycle_params:
            assert params["rootPath"] == binding.root_path
            assert params["cachePath"] == binding.cache_path
            assert params["rootIdentity"] == {
                "device": binding.root_identity.device,
                "inode": binding.root_identity.inode,
                "mode": binding.root_identity.mode,
            }
    finally:
        backend.stop()


def test_recover_reads_one_stable_bounded_snapshot(
    fake_adapter: Path, tmp_path: Path
) -> None:
    log_path = tmp_path / "recover.jsonl"
    backend = make_backend(
        fake_adapter, tmp_path, mode="recover-pages", log_path=log_path
    )
    start_backend(backend, tmp_path)
    try:
        records = backend.recover()
        assert len(records) == 9
        assert len({record.identity.provider_id for record in records}) == 9
        assert len({record.identity.path for record in records}) == 9
        entries = wait_for_log(log_path, 3)
        requests = [entry["recover"] for entry in entries if "recover" in entry]
        assert requests == [
            {"cursor": None, "limit": 8},
            {"cursor": "snapshot:8", "limit": 8},
        ]
    finally:
        backend.stop()


def test_macos_restart_maps_the_adopted_visible_root_to_the_saved_source(
    fake_adapter: Path, tmp_path: Path
) -> None:
    log_path = tmp_path / "macos-restart.jsonl"
    first = make_backend(
        fake_adapter,
        tmp_path,
        mode="macos",
        platform_name="Darwin",
        log_path=log_path,
    )
    root = tmp_path / "root"
    root.mkdir()
    marker = root / ".maestral-root"
    marker.write_text(f"maestral-root-v1:{ROOT_MARKER_ID}\n")
    marker.chmod(0o600)
    binding = first.start(str(root), lambda _provider_id, _revision: {})
    first.stop()

    second = make_backend(
        fake_adapter,
        tmp_path,
        mode="macos",
        platform_name="Darwin",
        log_path=log_path,
    )
    restarted = second.start(binding.root_path, lambda _provider_id, _revision: {})
    try:
        assert restarted == binding
        starts = [require_object(entry["start"]) for entry in wait_for_log(log_path, 2)]
        assert starts[1]["rootPath"] == str(root)
        assert starts[1]["cachePath"] == str(tmp_path / "cache")
    finally:
        second.stop()


@pytest.mark.parametrize(
    ("mode", "message"),
    [
        ("recover-duplicate-id", "duplicate recovery identities"),
        ("recover-duplicate-path", "duplicate recovery identities"),
        ("recover-empty-cursor", "invalid recovery page"),
        ("recover-empty-page", "empty intermediate recovery page"),
        ("recover-nonadvancing", "non-advancing recovery cursor"),
        ("recover-too-many", "invalid recovery page"),
    ],
)
def test_recover_rejects_invalid_pages(
    fake_adapter: Path, tmp_path: Path, mode: str, message: str
) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode=mode)
    start_backend(backend, tmp_path)
    try:
        with pytest.raises(NativeVirtualFileProtocolError, match=message):
            backend.recover()
    finally:
        backend.stop()


def test_recover_handles_maximum_length_records_within_the_line_limit(
    fake_adapter: Path, tmp_path: Path
) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode="recover-max-fields")
    start_backend(backend, tmp_path)
    try:
        records = backend.recover()
        assert len(records) == 8
        assert all(
            len(record.identity.provider_id.encode()) == 4096 for record in records
        )
        assert all(len(record.identity.path.encode()) == 32768 for record in records)
        assert all('"' in record.identity.path for record in records)
        assert all("\\" in record.identity.path for record in records)
    finally:
        backend.stop()


def test_recover_caps_an_endless_snapshot(
    fake_adapter: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("maestral.virtual_files_native.MAX_RECOVERY_PAGES", 2)
    backend = make_backend(fake_adapter, tmp_path, mode="recover-endless")
    start_backend(backend, tmp_path)
    try:
        with pytest.raises(NativeVirtualFileProtocolError, match="too many recovery"):
            backend.recover()
    finally:
        backend.stop()


def test_recover_caps_total_items(
    fake_adapter: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("maestral.virtual_files_native.MAX_RECOVERY_ITEMS", 8)
    backend = make_backend(fake_adapter, tmp_path, mode="recover-pages")
    start_backend(backend, tmp_path)
    try:
        with pytest.raises(NativeVirtualFileProtocolError, match="too many recovery"):
            backend.recover()
    finally:
        backend.stop()


@pytest.mark.parametrize("keep_process", [False, True])
def test_detach_is_idempotent_across_process_restarts(
    fake_adapter: Path,
    tmp_path: Path,
    keep_process: bool,
) -> None:
    log_path = tmp_path / "detach.jsonl"
    backend = make_backend(fake_adapter, tmp_path, log_path=log_path)
    root = tmp_path / "root"
    root.mkdir()
    marker = root / ".maestral-root"
    marker.write_text(f"maestral-root-v1:{ROOT_MARKER_ID}\n")
    marker.chmod(0o600)
    backend.start(str(root), lambda _provider_id, _revision: {})
    assert backend.registration_committed(ROOT_MARKER_ID)
    start_params = require_object(wait_for_log(log_path, 1)[0]["start"])
    if not keep_process:
        backend.stop()
        backend = make_backend(fake_adapter, tmp_path, log_path=log_path)

    assert backend.detach(str(root), ROOT_MARKER_ID) == str(root)
    assert backend._process is None
    assert backend.detach(str(root), ROOT_MARKER_ID) == str(root)
    with pytest.raises(NativeVirtualFileProcessError, match="detached native root"):
        backend.detach(str(tmp_path / "other-root"), ROOT_MARKER_ID)
    assert backend._process is None
    assert not backend.registration_committed(ROOT_MARKER_ID)

    entries = wait_for_log(log_path, 2)
    detach_params = [entry["detach"] for entry in entries if "detach" in entry]
    assert detach_params == [
        {
            "rootId": f"maestral:{backend.config_name}:{ROOT_MARKER_ID}",
            "rootPath": str(root),
            "cachePath": str(tmp_path / "cache"),
            "rootIdentity": start_params["rootIdentity"],
        }
    ]


def test_macos_detach_replays_after_domain_removal_and_lost_response(
    fake_adapter: Path, tmp_path: Path
) -> None:
    log_path = tmp_path / "macos-detach-replay.jsonl"
    backend = make_backend(
        fake_adapter,
        tmp_path,
        mode="macos-detach-lost-response",
        platform_name="Darwin",
        log_path=log_path,
    )
    root = tmp_path / "root"
    root.mkdir()
    marker = root / ".maestral-root"
    marker.write_text(f"maestral-root-v1:{ROOT_MARKER_ID}\n")
    marker.chmod(0o600)
    binding = backend.start(str(root), lambda _provider_id, _revision: {})
    backend.accept_binding(ROOT_MARKER_ID)

    with pytest.raises(NativeVirtualFileTimeoutError):
        backend.detach(binding.root_path, ROOT_MARKER_ID)
    assert not Path(binding.root_path).exists()

    restarted = make_backend(
        fake_adapter,
        tmp_path,
        mode="macos-detach-lost-response",
        platform_name="Darwin",
        log_path=log_path,
    )
    assert restarted.detach(binding.root_path, ROOT_MARKER_ID) == str(root)
    assert not restarted.binding_accepted(ROOT_MARKER_ID)
    assert restarted.detach(str(root), ROOT_MARKER_ID) == str(root)
    detach_requests = [
        entry["detach"] for entry in wait_for_log(log_path, 3) if "detach" in entry
    ]
    assert len(detach_requests) == 2


def test_detach_requires_an_exact_result(fake_adapter: Path, tmp_path: Path) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode="bad-detach")
    root = tmp_path / "root"
    root.mkdir()
    marker = root / ".maestral-root"
    marker.write_text(f"maestral-root-v1:{ROOT_MARKER_ID}\n")
    marker.chmod(0o600)
    backend.start(str(root), lambda _provider_id, _revision: {})
    with pytest.raises(NativeVirtualFileProtocolError):
        backend.detach(str(root), ROOT_MARKER_ID)
    assert backend._process is None
    assert backend.registration_committed(ROOT_MARKER_ID)


def test_detach_rejects_a_different_result_path(
    fake_adapter: Path, tmp_path: Path
) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode="bad-detach-path")
    root = tmp_path / "root"
    root.mkdir()
    marker = root / ".maestral-root"
    marker.write_text(f"maestral-root-v1:{ROOT_MARKER_ID}\n")
    marker.chmod(0o600)
    backend.start(str(root), lambda _provider_id, _revision: {})

    with pytest.raises(NativeVirtualFileProtocolError, match="source root"):
        backend.detach(str(root), ROOT_MARKER_ID)
    assert backend.registration_committed(ROOT_MARKER_ID)


def test_stopped_linux_validation_uses_the_saved_start_binding(
    fake_adapter: Path, tmp_path: Path
) -> None:
    log_path = tmp_path / "validate.jsonl"
    backend = make_backend(fake_adapter, tmp_path, log_path=log_path)
    start_backend(backend, tmp_path)
    start_params = require_object(wait_for_log(log_path, 1)[0]["start"])
    saved_identity = start_params["rootIdentity"]

    backend.stop()
    backend.validate_root(str(tmp_path / "root"), ROOT_MARKER_ID)
    assert backend._process is None

    entries = wait_for_log(log_path, 2)
    restart_params = require_object(entries[-1]["start"])
    assert restart_params == {
        "rootId": f"maestral:{backend.config_name}:{ROOT_MARKER_ID}",
        "rootPath": str(tmp_path / "root"),
        "cachePath": str(tmp_path / "cache"),
        "rootIdentity": saved_identity,
        "rootMarker": {
            "name": ".maestral-root",
            "content": f"maestral-root-v1:{ROOT_MARKER_ID}\n",
        },
    }
    assert all("validate_root" not in entry for entry in entries)


def test_validate_root_maps_inactive_registration_to_busy(
    fake_adapter: Path, tmp_path: Path
) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode="validate-busy")
    start_backend(backend, tmp_path)
    try:
        with pytest.raises(VirtualFileBusyError):
            backend.validate_root(str(tmp_path / "root"), ROOT_MARKER_ID)
        assert backend._process is not None
    finally:
        backend.stop()


def test_validate_root_requires_the_exact_saved_identity(
    fake_adapter: Path, tmp_path: Path
) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode="bad-validate-root")
    start_backend(backend, tmp_path)
    try:
        with pytest.raises(
            NativeVirtualFileProtocolError, match="different native root"
        ):
            backend.validate_root(str(tmp_path / "root"), ROOT_MARKER_ID)
        assert backend._process is None or backend._process.poll() is not None
    finally:
        backend.stop()


def test_restart_rejects_a_replaced_source_root_before_launch(
    fake_adapter: Path, tmp_path: Path
) -> None:
    log_path = tmp_path / "replace-root.jsonl"
    backend = make_backend(fake_adapter, tmp_path, log_path=log_path)
    start_backend(backend, tmp_path)
    backend.stop()
    backend._platform_name = "Darwin"

    root = tmp_path / "root"
    old_root = tmp_path / "old-root"
    root.rename(old_root)
    root.mkdir()
    marker = root / ".maestral-root"
    marker.write_text(f"maestral-root-v1:{ROOT_MARKER_ID}\n")
    marker.chmod(0o600)

    with pytest.raises(NativeVirtualFileProcessError, match="source was replaced"):
        backend.start(str(root), lambda _provider_id, _revision: {})
    assert backend._process is None
    assert len(wait_for_log(log_path, 1)) == 1


def test_linux_restart_checks_a_replaced_source_after_adapter_start(
    fake_adapter: Path, tmp_path: Path
) -> None:
    log_path = tmp_path / "replace-linux-root.jsonl"
    backend = make_backend(fake_adapter, tmp_path, log_path=log_path)
    start_backend(backend, tmp_path)
    backend.stop()

    root = tmp_path / "root"
    root.rename(tmp_path / "old-root")
    root.mkdir()
    marker = root / ".maestral-root"
    marker.write_text(f"maestral-root-v1:{ROOT_MARKER_ID}\n")
    marker.chmod(0o600)

    with pytest.raises(NativeVirtualFileProtocolError, match="root binding"):
        backend.start(str(root), lambda _provider_id, _revision: {})
    assert backend._process is None
    assert len(wait_for_log(log_path, 2)) == 2


@pytest.mark.parametrize("operation", ["validate", "detach"])
@pytest.mark.parametrize("replacement", ["missing", "replaced"])
def test_recovery_rejects_a_missing_or_replaced_saved_cache_without_launch(
    fake_adapter: Path,
    tmp_path: Path,
    operation: str,
    replacement: str,
) -> None:
    log_path = tmp_path / "cache-binding.jsonl"
    backend = make_backend(fake_adapter, tmp_path, log_path=log_path)
    start_backend(backend, tmp_path)
    backend.stop()
    cache = tmp_path / "cache"
    if replacement == "missing":
        cache.rmdir()
    else:
        cache.rename(tmp_path / "old-cache")
        cache.mkdir(mode=0o700)

    with pytest.raises(NativeVirtualFileProcessError, match="cache"):
        if operation == "validate":
            backend.validate_root(str(tmp_path / "root"), ROOT_MARKER_ID)
        else:
            backend.detach(str(tmp_path / "root"), ROOT_MARKER_ID)
    with pytest.raises(NativeVirtualFileProcessError, match="cache"):
        backend.cache_path_for_root(ROOT_MARKER_ID)
    assert backend._process is None
    assert not cache.exists() if replacement == "missing" else cache.is_dir()
    assert backend.registration_committed(ROOT_MARKER_ID)


def test_registration_is_not_committed_after_an_invalid_start(
    fake_adapter: Path, tmp_path: Path
) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode="bad-start")
    root = tmp_path / "root"
    root.mkdir()
    marker = root / ".maestral-root"
    marker.write_text(f"maestral-root-v1:{ROOT_MARKER_ID}\n")
    marker.chmod(0o600)

    with pytest.raises(NativeVirtualFileProtocolError):
        backend.start(str(root), lambda _provider_id, _revision: {})
    assert not backend.registration_committed(ROOT_MARKER_ID)
    with pytest.raises(NativeVirtualFileProcessError, match="did not complete"):
        backend.detach(str(root), ROOT_MARKER_ID)


def test_core_acceptance_is_durable_and_separate_from_native_registration(
    fake_adapter: Path, tmp_path: Path
) -> None:
    backend = make_backend(fake_adapter, tmp_path)
    start_backend(backend, tmp_path)

    assert backend.registration_committed(ROOT_MARKER_ID)
    assert not backend.binding_accepted(ROOT_MARKER_ID)

    backend.accept_binding(ROOT_MARKER_ID)
    backend.stop()
    restarted = make_backend(fake_adapter, tmp_path)

    assert restarted.registration_committed(ROOT_MARKER_ID)
    assert restarted.binding_accepted(ROOT_MARKER_ID)


def test_concurrent_out_of_order_responses_are_routed_by_id(
    fake_adapter: Path, tmp_path: Path
) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode="concurrent")
    start_backend(backend, tmp_path)
    items = [descriptor(f"id:file-{index}") for index in range(10)]
    try:
        for item in items:
            backend.upsert(item, expected_identity=None)
        with ThreadPoolExecutor(max_workers=10) as executor:
            states = list(
                executor.map(backend.inspect, [item.provider_id for item in items])
            )
        assert [state.hydrated_revision for state in states if state is not None] == [
            None
        ] * 10
    finally:
        backend.stop()


def test_adapter_hydration_callback_can_issue_materialize_request(
    fake_adapter: Path, tmp_path: Path
) -> None:
    log_path = tmp_path / "callback.jsonl"
    backend = make_backend(fake_adapter, tmp_path, mode="callback", log_path=log_path)
    root = tmp_path / "root"
    root.mkdir()
    item = descriptor()
    calls: list[tuple[str, str]] = []

    def hydrate(provider_id: str, revision: str) -> dict[str, object]:
        calls.append((provider_id, revision))
        stage = private_stage(tmp_path, "callback-stage")
        backend.materialize(item, str(stage), expected_revision=revision)
        return {
            "provider_id": provider_id,
            "revision": revision,
            "hydration_state": "hydrated",
        }

    backend.start(str(root), hydrate)
    try:
        backend.upsert(item, expected_identity=None)
        responses = wait_for_log(log_path, 2)
        callback_response = next(
            response
            for response in responses
            if response.get("requestId") == "native-hydrate-1"
        )
        assert callback_response["result"] == {}
        assert calls == [(item.provider_id, item.revision)]
        native = backend.inspect(item.provider_id)
        assert native is not None
        assert native.hydrated_revision == item.revision
    finally:
        backend.stop()


def test_hydration_callback_maps_exact_revision_failure(
    fake_adapter: Path, tmp_path: Path
) -> None:
    log_path = tmp_path / "callback-error.jsonl"
    backend = make_backend(
        fake_adapter, tmp_path, mode="callback-error", log_path=log_path
    )
    root = tmp_path / "root"
    root.mkdir()

    def fail_revision(_provider_id: str, _revision: str) -> dict[str, object]:
        raise VirtualFileRevisionError("Revision changed", "Use the new revision.")

    backend.start(str(root), fail_revision)
    try:
        backend.upsert(descriptor(), expected_identity=None)
        responses = wait_for_log(log_path, 2)
        callback_response = next(
            response
            for response in responses
            if response.get("requestId") == "native-hydrate-1"
        )
        assert error_code(callback_response) == "REVISION_MISMATCH"
    finally:
        backend.stop()


def test_unimplemented_adapter_events_are_rejected_without_consuming_content(
    fake_adapter: Path, tmp_path: Path
) -> None:
    log_path = tmp_path / "events.jsonl"
    stage = private_stage(tmp_path, "local-change-stage")
    backend = make_backend(
        fake_adapter,
        tmp_path,
        mode="events",
        log_path=log_path,
        event_stage=stage,
    )
    start_backend(backend, tmp_path)
    try:
        responses = wait_for_log(log_path, 7)[1:]
        by_id = {response["requestId"]: response for response in responses}
        for request_id in (
            "native-enumerate",
            "native-dehydrate",
            "native-pin",
            "native-change",
        ):
            assert error_code(by_id[request_id]) == "UNSUPPORTED"
        assert error_code(by_id["native-invalid"]) == "INVALID_REQUEST"
        assert error_code(by_id["native-bad-path"]) == "INVALID_REQUEST"
        assert stage.read_bytes() == b"data"
    finally:
        backend.stop()


@pytest.mark.parametrize(
    "mode",
    [
        "malformed",
        "duplicate-keys",
        "invalid-utf8",
        "bad-version",
        "oversized",
        "bad-result",
    ],
)
def test_invalid_adapter_data_stops_the_process(
    fake_adapter: Path, tmp_path: Path, mode: str
) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode=mode)
    start_backend(backend, tmp_path)
    backend.upsert(descriptor(), expected_identity=None)
    with pytest.raises(NativeVirtualFileProtocolError):
        backend.inspect("id:file")
    process = backend._process
    assert process is not None
    process.wait(timeout=2)
    backend.stop()


def test_unknown_response_id_fails_start(fake_adapter: Path, tmp_path: Path) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode="unknown-response")
    root = tmp_path / "root"
    root.mkdir()
    with pytest.raises(NativeVirtualFileProtocolError):
        backend.start(str(root), lambda _provider_id, _revision: {})
    assert backend._process is None


def test_exact_start_result_rejects_extra_fields(
    fake_adapter: Path, tmp_path: Path
) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode="bad-start")
    root = tmp_path / "root"
    root.mkdir()
    with pytest.raises(NativeVirtualFileProtocolError):
        backend.start(str(root), lambda _provider_id, _revision: {})
    assert backend._process is None


def test_start_rejects_a_non_directory_root_identity(
    fake_adapter: Path, tmp_path: Path
) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode="bad-root-mode")
    root = tmp_path / "root"
    root.mkdir()
    with pytest.raises(NativeVirtualFileProtocolError, match="root binding"):
        backend.start(str(root), lambda _provider_id, _revision: {})
    assert backend._process is None


def test_request_timeout_stops_and_reaps_the_process(
    fake_adapter: Path, tmp_path: Path
) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode="timeout", timeout=0.5)
    start_backend(backend, tmp_path)
    backend.upsert(descriptor(), expected_identity=None)
    process = backend._process
    assert process is not None
    with pytest.raises(NativeVirtualFileTimeoutError):
        backend.inspect("id:file")
    process.wait(timeout=2)
    backend.stop()


def test_blocked_protocol_write_uses_the_request_deadline(
    fake_adapter: Path, tmp_path: Path
) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode="stop-reading", timeout=0.8)
    start_backend(backend, tmp_path)
    process = backend._process
    assert process is not None
    started = time.monotonic()
    with pytest.raises(NativeVirtualFileTimeoutError):
        backend._request("recover", {"padding": "x" * (512 * 1024)})
    assert time.monotonic() - started < 2
    process.wait(timeout=2)
    backend.stop()


def test_crash_keeps_a_bounded_stderr_tail(fake_adapter: Path, tmp_path: Path) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode="crash")
    start_backend(backend, tmp_path)
    backend.upsert(descriptor(), expected_identity=None)
    with pytest.raises(NativeVirtualFileProcessError):
        backend.inspect("id:file")
    deadline = time.monotonic() + 2
    while (
        "adapter crash detail" not in backend.stderr_tail
        and time.monotonic() < deadline
    ):
        time.sleep(0.01)
    assert "adapter crash detail" in backend.stderr_tail
    assert len(backend.stderr_tail.encode()) <= 64 * 1024
    backend.stop()


@pytest.mark.parametrize("mode", ["materialize-busy", "materialize-timeout"])
def test_materialize_failure_preserves_the_core_stage(
    fake_adapter: Path, tmp_path: Path, mode: str
) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode=mode, timeout=0.5)
    start_backend(backend, tmp_path)
    item = descriptor()
    backend.upsert(item, expected_identity=None)
    stage = private_stage(tmp_path)
    expected_error = (
        NativeVirtualFileTimeoutError
        if mode == "materialize-timeout"
        else VirtualFileBusyError
    )
    with pytest.raises(expected_error):
        backend.materialize(item, str(stage), expected_revision=item.revision)
    assert stage.read_bytes() == b"data"
    backend.stop()


def test_revision_mismatch_and_adapter_io_error_have_distinct_types(
    fake_adapter: Path, tmp_path: Path
) -> None:
    backend = make_backend(fake_adapter, tmp_path)
    start_backend(backend, tmp_path)
    backend.upsert(descriptor(), expected_identity=None)
    try:
        with pytest.raises(VirtualFileRevisionError):
            backend.evict("id:file", expected_revision="old-revision")
    finally:
        backend.stop()

    failing = make_backend(fake_adapter, tmp_path, mode="io-error")
    start_backend(failing, tmp_path)
    failing.upsert(descriptor(), expected_identity=None)
    try:
        with pytest.raises(NativeVirtualFileRequestError) as exc_info:
            failing.inspect("id:file")
        assert exc_info.value.code == "IO_ERROR"
    finally:
        failing.stop()


def test_identity_bound_upsert_and_remove(fake_adapter: Path, tmp_path: Path) -> None:
    backend = make_backend(fake_adapter, tmp_path)
    start_backend(backend, tmp_path)
    first = descriptor(revision="rev-1")
    second = descriptor(revision="rev-2")
    try:
        backend.upsert(first, expected_identity=None)
        backend.upsert(first, expected_identity=None)
        with pytest.raises(VirtualFileRevisionError):
            backend.upsert(
                second,
                expected_identity=identity(first, path="/stale-path.txt"),
            )
        with pytest.raises(VirtualFileRevisionError):
            backend.upsert(
                second,
                expected_identity=identity(first, is_directory=True),
            )
        with pytest.raises(ValueError, match="provider identity"):
            backend.upsert(
                second,
                expected_identity=VirtualFileIdentity(
                    provider_id="id:other",
                    path=first.path,
                    is_directory=first.is_directory,
                    revision=first.revision,
                ),
            )
        backend.upsert(second, expected_identity=identity(first))
        with pytest.raises(VirtualFileRevisionError):
            backend.remove(identity(first))
        backend.remove(identity(second))
        backend.remove(identity(second))
    finally:
        backend.stop()


@pytest.mark.parametrize(
    "path",
    [
        "/.maestral-root",
        "/.maestral-root/child",
        "/.~maestral-root-recovery",
        "/.~maestral-root-recovery/child",
    ],
)
def test_items_reject_reserved_root_paths(
    fake_adapter: Path, tmp_path: Path, path: str
) -> None:
    backend = make_backend(fake_adapter, tmp_path)
    start_backend(backend, tmp_path)
    try:
        with pytest.raises(ValueError, match="canonical"):
            backend.upsert(replace(descriptor(), path=path), expected_identity=None)
    finally:
        backend.stop()


def test_remove_rejects_not_found_from_adapter(
    fake_adapter: Path, tmp_path: Path
) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode="remove-not-found")
    start_backend(backend, tmp_path)
    try:
        with pytest.raises(NativeVirtualFileProtocolError, match="idempotent remove"):
            backend.remove(identity(descriptor()))
    finally:
        backend.stop()


def test_upsert_rejects_an_inconsistent_operation_result(
    fake_adapter: Path, tmp_path: Path
) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode="bad-upsert-result")
    start_backend(backend, tmp_path)
    first = descriptor()
    backend.upsert(first, expected_identity=None)
    second = descriptor(revision="rev-2")
    try:
        with pytest.raises(NativeVirtualFileProtocolError, match="inconsistent"):
            backend.upsert(second, expected_identity=identity(first))
    finally:
        backend.stop()


def test_stop_drains_an_active_materialize(fake_adapter: Path, tmp_path: Path) -> None:
    log_path = tmp_path / "drain.jsonl"
    backend = make_backend(
        fake_adapter,
        tmp_path,
        mode="materialize-slow",
        timeout=1.0,
        log_path=log_path,
    )
    start_backend(backend, tmp_path)
    item = descriptor()
    backend.upsert(item, expected_identity=None)
    stage = private_stage(tmp_path)
    errors: list[BaseException] = []

    def materialize() -> None:
        try:
            backend.materialize(item, str(stage), expected_revision=item.revision)
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=materialize)
    worker.start()
    wait_for_log(log_path, 2)
    backend.stop()
    worker.join(timeout=2)
    assert not worker.is_alive()
    assert errors == []
    assert stage.read_bytes() == b"data"


def test_slow_stop_response_leaves_the_process_active_for_retry(
    fake_adapter: Path, tmp_path: Path
) -> None:
    log_path = tmp_path / "slow-stop.jsonl"
    backend = make_backend(
        fake_adapter,
        tmp_path,
        mode="stop-slow",
        timeout=0.3,
        log_path=log_path,
    )
    start_backend(backend, tmp_path)
    process = backend._process
    assert process is not None

    with pytest.raises(NativeVirtualFileTimeoutError, match="still active"):
        backend.stop()
    assert process.poll() is None
    assert backend._process is process
    assert backend.registration_committed(ROOT_MARKER_ID)

    process.wait(timeout=2)
    backend.stop()
    assert backend._process is None
    assert backend.registration_committed(ROOT_MARKER_ID)
    assert len([entry for entry in wait_for_log(log_path, 2) if "stop" in entry]) == 1


def test_stop_drains_active_hydration_and_rejects_new_callbacks(
    fake_adapter: Path, tmp_path: Path
) -> None:
    log_path = tmp_path / "hydrate-stop.jsonl"
    backend = make_backend(
        fake_adapter,
        tmp_path,
        mode="callback-pair",
        timeout=1.0,
        log_path=log_path,
    )
    root = tmp_path / "root"
    root.mkdir()
    callback_started = threading.Event()
    release_callback = threading.Event()
    calls: list[tuple[str, str]] = []

    def hydrate(provider_id: str, revision: str) -> dict[str, object]:
        calls.append((provider_id, revision))
        callback_started.set()
        assert release_callback.wait(2)
        return {
            "provider_id": provider_id,
            "revision": revision,
            "hydration_state": "hydrated",
        }

    backend.start(str(root), hydrate)
    item = descriptor()
    backend.upsert(item, expected_identity=None)
    assert callback_started.wait(2)

    stop_errors: list[BaseException] = []

    def stop() -> None:
        try:
            backend.stop()
        except BaseException as exc:
            stop_errors.append(exc)

    stop_thread = threading.Thread(target=stop)
    stop_thread.start()
    deadline = time.monotonic() + 2
    while not backend._closing and time.monotonic() < deadline:
        time.sleep(0.01)
    assert backend._closing

    responses = wait_for_log(log_path, 2)
    rejected = next(
        response
        for response in responses
        if response.get("requestId") == "native-hydrate-2"
    )
    assert error_code(rejected) == "BUSY"
    assert stop_thread.is_alive()

    release_callback.set()
    stop_thread.join(timeout=2)
    assert not stop_thread.is_alive()
    assert stop_errors == []
    assert calls == [(item.provider_id, item.revision)]


@pytest.mark.parametrize("operation", ["stop", "detach"])
def test_lifecycle_waits_past_request_deadline_for_active_content(
    fake_adapter: Path, tmp_path: Path, operation: str
) -> None:
    backend = make_backend(fake_adapter, tmp_path, mode="callback", timeout=0.5)
    root = tmp_path / "root"
    root.mkdir()
    marker = root / ".maestral-root"
    marker.write_text(f"maestral-root-v1:{ROOT_MARKER_ID}\n")
    marker.chmod(0o600)
    callback_started = threading.Event()
    release_callback = threading.Event()
    item = descriptor()

    def hydrate(provider_id: str, revision: str) -> dict[str, object]:
        callback_started.set()
        assert release_callback.wait(2)
        stage = private_stage(tmp_path, "lifecycle-stage")
        backend.materialize(item, str(stage), expected_revision=revision)
        return {
            "provider_id": provider_id,
            "revision": revision,
            "hydration_state": "hydrated",
        }

    backend.start(str(root), hydrate)
    backend.upsert(item, expected_identity=None)
    assert callback_started.wait(2)

    errors: list[BaseException] = []

    def finish_lifecycle() -> None:
        try:
            if operation == "stop":
                backend.stop()
            else:
                backend.detach(str(root), ROOT_MARKER_ID)
        except BaseException as exc:
            errors.append(exc)

    lifecycle_thread = threading.Thread(target=finish_lifecycle)
    lifecycle_thread.start()
    time.sleep(0.7)
    assert lifecycle_thread.is_alive()
    assert backend._process is not None
    assert backend._process.poll() is None
    assert backend.registration_committed(ROOT_MARKER_ID)

    release_callback.set()
    lifecycle_thread.join(timeout=2)
    assert not lifecycle_thread.is_alive()
    assert errors == []

    if operation == "stop":
        assert backend.registration_committed(ROOT_MARKER_ID)
    else:
        assert not backend.registration_committed(ROOT_MARKER_ID)
    assert backend._process is None


def test_outbound_messages_have_a_one_mib_limit(
    fake_adapter: Path, tmp_path: Path
) -> None:
    backend = make_backend(fake_adapter, tmp_path)
    start_backend(backend, tmp_path)
    try:
        with pytest.raises(NativeVirtualFileProtocolError, match="oversized"):
            backend._request("recover", {"padding": "x" * MAX_LINE_BYTES})
        assert backend._process is not None
        assert backend._process.poll() is None
    finally:
        backend.stop()


def test_resolver_requires_the_exact_platform_executable(tmp_path: Path) -> None:
    adapter = tmp_path / "maestral-smart-sync-fuse"
    adapter.write_text("#!/bin/sh\n")
    adapter.chmod(0o700)
    assert (
        resolve_native_virtual_file_executable(
            environ={"MAESTRAL_SMART_SYNC_ADAPTER": str(adapter)},
            platform_name="Linux",
        )
        == adapter.resolve()
    )
    with pytest.raises(NativeVirtualFileProcessError, match="absolute"):
        resolve_native_virtual_file_executable(
            environ={"MAESTRAL_SMART_SYNC_ADAPTER": "relative-adapter"},
            platform_name="Linux",
        )
    wrong_name = tmp_path / "other-adapter"
    wrong_name.write_text("#!/bin/sh\n")
    wrong_name.chmod(0o700)
    with pytest.raises(NativeVirtualFileProcessError, match="must be named"):
        resolve_native_virtual_file_executable(
            wrong_name,
            platform_name="Linux",
        )
    with pytest.raises(NativeVirtualFileProcessError, match="no native"):
        resolve_native_virtual_file_executable(platform_name="Plan9", environ={})


@pytest.mark.parametrize(
    ("platform_name", "executable_name"),
    [
        ("Darwin", "maestral-smart-sync-macos"),
        ("Windows", "MaestralSmartSync.exe"),
        ("Linux", "maestral-smart-sync-fuse"),
    ],
)
def test_resolver_uses_each_exact_platform_name(
    tmp_path: Path,
    platform_name: str,
    executable_name: str,
) -> None:
    adapter = tmp_path / executable_name
    adapter.write_text("#!/bin/sh\n")
    adapter.chmod(0o700)
    assert (
        resolve_native_virtual_file_executable(
            adapter,
            platform_name=platform_name,
        )
        == adapter.resolve()
    )


def test_missing_binary_returns_an_inactive_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "maestral.virtual_files_native.resolve_native_virtual_file_executable",
        lambda: (_ for _ in ()).throw(
            NativeVirtualFileProcessError("Unavailable", "Missing adapter")
        ),
    )
    assert isinstance(
        create_process_virtual_file_backend("test-profile"),
        UnsupportedVirtualFileBackend,
    )


def test_windows_factory_returns_an_inactive_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "maestral.virtual_files_native.platform.system", lambda: "Windows"
    )

    def unexpected_resolve() -> Path:
        raise AssertionError("The disabled Windows adapter must not be resolved")

    monkeypatch.setattr(
        "maestral.virtual_files_native.resolve_native_virtual_file_executable",
        unexpected_resolve,
    )
    backend = create_process_virtual_file_backend("test-windows-profile")
    assert isinstance(backend, UnsupportedVirtualFileBackend)
    assert not backend.supported


def test_missing_saved_adapter_allows_a_return_to_mirror_mode(
    config_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = MaestralConfig(config_name)
    config.set("sync", "mode", VIRTUAL_MODE)
    monkeypatch.setattr(
        "maestral.virtual_files_native.resolve_native_virtual_file_executable",
        lambda: (_ for _ in ()).throw(
            NativeVirtualFileProcessError("Unavailable", "Missing adapter")
        ),
    )

    maestral = Maestral(config_name)
    try:
        assert maestral.sync_mode == VIRTUAL_MODE
        assert not maestral.virtual_files.supported
        maestral.set_sync_mode(MIRROR_MODE)
        assert maestral.sync_mode == MIRROR_MODE
        with pytest.raises(VirtualFilesUnsupportedError):
            maestral.set_sync_mode(VIRTUAL_MODE)
        assert maestral.sync_mode == MIRROR_MODE
    finally:
        maestral.manager.shutdown()
        maestral.sync._connection.close()
        maestral.client.close()


@pytest.mark.parametrize("timeout", [0.0, 301.0, float("inf"), float("nan"), True])
def test_deadlines_are_bounded(
    fake_adapter: Path, tmp_path: Path, timeout: float
) -> None:
    with pytest.raises(ValueError, match="between"):
        make_backend(fake_adapter, tmp_path, timeout=timeout)


def test_private_stage_rejects_group_access(fake_adapter: Path, tmp_path: Path) -> None:
    if os.name == "nt":
        pytest.skip("POSIX permission bits are unavailable")
    backend = make_backend(fake_adapter, tmp_path)
    start_backend(backend, tmp_path)
    item = descriptor()
    backend.upsert(item, expected_identity=None)
    stage = private_stage(tmp_path)
    stage.chmod(0o640)
    try:
        with pytest.raises(NativeVirtualFileProtocolError, match="mode must be 0600"):
            backend.materialize(item, str(stage), expected_revision=item.revision)
    finally:
        backend.stop()


def test_private_stage_rejects_a_symbolic_link(
    fake_adapter: Path, tmp_path: Path
) -> None:
    backend = make_backend(fake_adapter, tmp_path)
    start_backend(backend, tmp_path)
    item = descriptor()
    backend.upsert(item, expected_identity=None)
    target = private_stage(tmp_path, "target-stage")
    link = tmp_path / "linked-stage"
    link.symlink_to(target)
    try:
        with pytest.raises(NativeVirtualFileProtocolError, match="regular file"):
            backend.materialize(item, str(link), expected_revision=item.revision)
        assert target.read_bytes() == b"data"
    finally:
        backend.stop()


def test_private_cache_rejects_a_symbolic_link(
    fake_adapter: Path, tmp_path: Path
) -> None:
    config_name = native_config_name(tmp_path)
    MaestralConfig(config_name).set("sync", "root_marker_id", ROOT_MARKER_ID)
    cache_target = tmp_path / "cache-target"
    cache_target.mkdir()
    cache_link = tmp_path / "cache-link"
    cache_link.symlink_to(cache_target, target_is_directory=True)
    backend = NativeProcessVirtualFileBackend(
        config_name,
        fake_adapter,
        cache_path=cache_link,
        platform_name="Linux",
    )
    root = tmp_path / "root"
    root.mkdir()
    with pytest.raises(NativeVirtualFileProcessError, match="linked ancestor"):
        backend.start(str(root), lambda _provider_id, _revision: {})
    assert backend._process is None


@pytest.mark.parametrize("cache_contains_root", [False, True])
def test_start_rejects_root_cache_containment_before_launch(
    fake_adapter: Path, tmp_path: Path, cache_contains_root: bool
) -> None:
    if cache_contains_root:
        cache = tmp_path / "cache"
        cache.mkdir()
        root = cache / "root"
        root.mkdir()
    else:
        root = tmp_path / "root"
        root.mkdir()
        cache = root / "cache"
    backend = make_backend(fake_adapter, tmp_path, cache_path=cache)

    with pytest.raises(NativeVirtualFileProcessError, match="paths overlap"):
        backend.start(str(root), lambda _provider_id, _revision: {})
    assert backend._process is None


def test_private_cache_rejects_a_linked_ancestor(
    fake_adapter: Path, tmp_path: Path
) -> None:
    target = tmp_path / "target"
    target.mkdir()
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(target, target_is_directory=True)
    backend = make_backend(
        fake_adapter, tmp_path, cache_path=linked_parent / "native-cache"
    )
    root = tmp_path / "root"
    root.mkdir()

    with pytest.raises(NativeVirtualFileProcessError, match="linked ancestor"):
        backend.start(str(root), lambda _provider_id, _revision: {})
    assert backend._process is None


def test_default_cache_is_partitioned_by_root_marker(
    fake_adapter: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache_base = tmp_path / "cache-base"
    monkeypatch.setattr(
        "maestral.virtual_files_native.get_cache_path",
        lambda *_args, **_kwargs: str(cache_base),
    )
    backend = NativeProcessVirtualFileBackend(
        native_config_name(tmp_path), fake_adapter, platform_name="Linux"
    )
    second_marker = "fedcba9876543210fedcba9876543210"

    first = Path(backend.cache_path_for_root(ROOT_MARKER_ID))
    second = Path(backend.cache_path_for_root(second_marker))
    assert first == cache_base / ROOT_MARKER_ID
    assert second == cache_base / second_marker
    assert first != second


def test_detached_root_allows_a_new_marker_and_cache_partition(
    fake_adapter: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache_base = tmp_path / "cache-base"
    monkeypatch.setattr(
        "maestral.virtual_files_native.get_cache_path",
        lambda *_args, **_kwargs: str(cache_base),
    )
    config_name = native_config_name(tmp_path)
    config = MaestralConfig(config_name)
    config.set("sync", "root_marker_id", ROOT_MARKER_ID)
    backend = NativeProcessVirtualFileBackend(
        config_name, fake_adapter, platform_name="Linux"
    )
    root = tmp_path / "root"
    root.mkdir()
    marker = root / ".maestral-root"
    marker.write_text(f"maestral-root-v1:{ROOT_MARKER_ID}\n")
    marker.chmod(0o600)
    first = backend.start(str(root), lambda _provider_id, _revision: {})
    assert first.cache_path == str(cache_base / ROOT_MARKER_ID)
    assert backend.detach(first.root_path, ROOT_MARKER_ID) == str(root)

    second_marker = "fedcba9876543210fedcba9876543210"
    config.set("sync", "root_marker_id", second_marker)
    marker.write_text(f"maestral-root-v1:{second_marker}\n")
    marker.chmod(0o600)
    second = backend.start(str(root), lambda _provider_id, _revision: {})
    try:
        assert second.cache_path == str(cache_base / second_marker)
        assert not backend.registration_committed(ROOT_MARKER_ID)
        assert backend.registration_committed(second_marker)
    finally:
        backend.stop()


def test_new_marker_requires_the_prior_registration_to_detach(
    fake_adapter: Path, tmp_path: Path
) -> None:
    backend = make_backend(fake_adapter, tmp_path)
    start_backend(backend, tmp_path)
    backend.stop()
    second_marker = "fedcba9876543210fedcba9876543210"
    MaestralConfig(backend.config_name).set("sync", "root_marker_id", second_marker)
    marker = tmp_path / "root" / ".maestral-root"
    marker.write_text(f"maestral-root-v1:{second_marker}\n")
    marker.chmod(0o600)

    with pytest.raises(VirtualFileBusyError, match="Detach the prior"):
        backend.start(str(tmp_path / "root"), lambda _provider_id, _revision: {})
    assert backend._process is None
