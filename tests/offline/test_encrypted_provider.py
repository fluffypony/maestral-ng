from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import posixpath
import shutil
import stat
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from dataclasses import replace as dataclass_replace
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO

import pytest

from maestral.core import (
    FileMetadata,
    FolderMetadata,
    ListFolderResult,
    Metadata,
    WriteMode,
)
from maestral.cryptomator import StorageMapEntry, VaultEntry, VaultInfo
from maestral.exceptions import FileConflictError, NotFoundError
from maestral.providers.encrypted import (
    EncryptedRemoteProvider,
    EncryptedVaultError,
    PhysicalVaultMirror,
)
from maestral.utils.hashing import sha256_content_hasher
from maestral.utils.path import normalize
from maestral.virtual_files import VirtualFileController

from .virtual_files_fakes import FakeVirtualFileBackend


class FakeRemoteProvider:
    provider_id = "fake"
    namespace_id = "namespace:fake"
    api_url = "https://example.invalid"
    content_hasher_factory = staticmethod(sha256_content_hasher)
    bandwidth_limit_up = 0.0
    bandwidth_limit_down = 0.0

    def __init__(self, root: Path) -> None:
        self.config_name = "encrypted-test"
        self.linked = True
        self.root = root
        self.root.mkdir()
        self._ids = {"/": "id:root"}
        self._revisions: dict[str, str] = {}
        self._next_id = 0
        self._cursor = 1

    def close(self) -> None:
        pass

    def _local(self, remote_path: str) -> Path:
        if not remote_path.startswith("/") or ".." in remote_path.split("/"):
            raise ValueError("unsafe fake path")
        return self.root.joinpath(*remote_path.split("/")[1:])

    def _new_id(self) -> str:
        self._next_id += 1
        return f"id:{self._next_id}"

    def _touch_cursor(self) -> None:
        self._cursor += 1

    def _metadata(self, remote_path: str) -> FileMetadata | FolderMetadata:
        path = self._local(remote_path)
        if not path.exists():
            raise NotFoundError("Not found", dbx_path=remote_path)
        item_id = self._ids[remote_path]
        name = posixpath.basename(remote_path)
        if path.is_dir():
            return FolderMetadata(
                name, normalize(remote_path), remote_path, item_id, False
            )
        modified = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
        content = path.read_bytes()
        return FileMetadata(
            name=name,
            path_lower=normalize(remote_path),
            path_display=remote_path,
            id=item_id,
            client_modified=modified,
            server_modified=modified,
            rev=self._revisions[remote_path],
            size=len(content),
            symlink_target=None,
            shared=False,
            modified_by=None,
            is_downloadable=True,
            content_hash=hashlib.sha256(content).hexdigest(),
        )

    def get_metadata(
        self, remote_path: str, include_deleted: bool = False
    ) -> Metadata | None:
        del include_deleted
        try:
            return self._metadata(remote_path)
        except NotFoundError:
            return None

    def list_folder_iterator(
        self,
        remote_path: str,
        recursive: bool = False,
        include_deleted: bool = False,
        include_mounted_folders: bool = True,
        limit: int | None = None,
        include_non_downloadable_files: bool = False,
    ) -> Iterator[ListFolderResult]:
        del include_deleted, include_mounted_folders, limit
        del include_non_downloadable_files
        root = self._local(remote_path)
        if not root.is_dir():
            raise NotFoundError("Not found", dbx_path=remote_path)
        entries: list[Metadata] = []
        paths = root.rglob("*") if recursive else root.iterdir()
        for path in sorted(paths):
            relative = path.relative_to(self.root).as_posix()
            entries.append(self._metadata("/" + relative))
        yield ListFolderResult(entries, False, f"cursor:{self._cursor}")

    def list_remote_changes_iterator(
        self,
        last_cursor: str,
        indexed_paths: object = None,
    ) -> Iterator[ListFolderResult]:
        del indexed_paths
        current = f"cursor:{self._cursor}"
        entries: list[Metadata] = []
        if last_cursor != current:
            entries = [
                self._metadata(path) for path in sorted(self._ids) if path != "/"
            ]
        yield ListFolderResult(entries, False, current)

    def wait_for_remote_changes(self, last_cursor: str, timeout: int = 40) -> bool:
        del timeout
        return last_cursor != f"cursor:{self._cursor}"

    def make_dir(self, remote_path: str, autorename: bool = False) -> FolderMetadata:
        del autorename
        path = self._local(remote_path)
        if path.exists():
            raise FileConflictError("Already exists", dbx_path=remote_path)
        if not path.parent.is_dir():
            raise NotFoundError("Parent not found", dbx_path=remote_path)
        path.mkdir()
        self._ids[remote_path] = self._new_id()
        self._touch_cursor()
        return self._metadata(remote_path)  # type: ignore[return-value]

    def upload(
        self,
        local_file: str | os.PathLike[str] | BinaryIO,
        remote_path: str,
        write_mode: WriteMode = WriteMode.Add,
        update_rev: str | None = None,
        autorename: bool = False,
        **_kwargs: object,
    ) -> FileMetadata:
        del autorename
        path = self._local(remote_path)
        exists = path.exists()
        if write_mode is WriteMode.Add and exists:
            raise FileConflictError("Already exists", dbx_path=remote_path)
        if write_mode is WriteMode.Update and (
            not exists or self._revisions.get(remote_path) != update_rev
        ):
            raise FileConflictError("Revision changed", dbx_path=remote_path)
        if not path.parent.is_dir():
            raise NotFoundError("Parent not found", dbx_path=remote_path)
        if isinstance(local_file, (str, bytes, os.PathLike)):
            with open(local_file, "rb") as source:
                content = source.read()
        else:
            content = local_file.read()
        path.write_bytes(content)
        if not exists:
            self._ids[remote_path] = self._new_id()
        self._revisions[remote_path] = f"rev:{self._cursor}"
        self._touch_cursor()
        return self._metadata(remote_path)  # type: ignore[return-value]

    def download(
        self,
        remote_path: str,
        local_path: str | BinaryIO,
        *,
        rev: str | None = None,
        provider_id: str | None = None,
        **_kwargs: object,
    ) -> FileMetadata:
        metadata = self._metadata(remote_path)
        if not isinstance(metadata, FileMetadata):
            raise IsADirectoryError(remote_path)
        if rev is not None and rev != metadata.rev:
            raise FileConflictError("Revision changed", dbx_path=remote_path)
        if provider_id is not None and provider_id != metadata.id:
            raise NotFoundError("ID changed", dbx_path=remote_path)
        content = self._local(remote_path).read_bytes()
        if isinstance(local_path, str):
            Path(local_path).write_bytes(content)
        else:
            local_path.write(content)
        return metadata

    def move(
        self,
        remote_path: str,
        new_path: str,
        autorename: bool = False,
        *,
        expected_provider_id: str,
    ) -> FileMetadata | FolderMetadata:
        del autorename
        source = self._local(remote_path)
        target = self._local(new_path)
        if not source.exists() or self._ids[remote_path] != expected_provider_id:
            raise NotFoundError("Not found", dbx_path=remote_path)
        if target.exists():
            raise FileConflictError("Already exists", dbx_path=new_path)
        source.rename(target)
        moved_ids: dict[str, str] = {}
        moved_revisions: dict[str, str] = {}
        for path, item_id in tuple(self._ids.items()):
            if path == remote_path or path.startswith(remote_path + "/"):
                destination = new_path + path[len(remote_path) :]
                moved_ids[destination] = item_id
                self._ids.pop(path)
                if path in self._revisions:
                    moved_revisions[destination] = self._revisions.pop(path)
        self._ids.update(moved_ids)
        self._revisions.update(moved_revisions)
        self._touch_cursor()
        return self._metadata(new_path)

    def remove(
        self,
        remote_path: str,
        parent_rev: str | None = None,
        *,
        expected_provider_id: str,
    ) -> FileMetadata | FolderMetadata:
        metadata = self._metadata(remote_path)
        if metadata.id != expected_provider_id:
            raise NotFoundError("ID changed", dbx_path=remote_path)
        if (
            parent_rev is not None
            and isinstance(metadata, FileMetadata)
            and metadata.rev != parent_rev
        ):
            raise FileConflictError("Revision changed", dbx_path=remote_path)
        path = self._local(remote_path)
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        for candidate in tuple(self._ids):
            if candidate == remote_path or candidate.startswith(remote_path + "/"):
                self._ids.pop(candidate)
                self._revisions.pop(candidate, None)
        self._touch_cursor()
        return metadata


def write_vault(root: Path) -> None:
    (root / "d" / "aa" / "long.c9s").mkdir(parents=True)
    (root / "vault.cryptomator").write_bytes(b"signed config")
    (root / "masterkey.cryptomator").write_bytes(b"protected key")
    (root / "d" / "aa" / "file.c9r").write_bytes(b"ciphertext one")
    (root / "d" / "aa" / "long.c9s" / "contents.c9r").write_bytes(b"long ciphertext")
    (root / "d" / "aa" / "long.c9s" / "name.c9s").write_bytes(b"old name")


def backing_revision(logical_revision: str) -> str:
    encoded = logical_revision.rsplit(":", 1)[1].encode("ascii")
    encoded += b"=" * (-len(encoded) % 4)
    return base64.urlsafe_b64decode(encoded).decode("utf-8")


class FakeSecretStore:
    def __init__(self) -> None:
        self.secret: str | None = None

    def load_password(self) -> str | None:
        return self.secret

    def save_password(self, secret: str) -> None:
        self.secret = secret

    def delete_password(self) -> None:
        self.secret = None


class FakeCryptomatorSession:
    def __init__(self, password: str) -> None:
        self.password = password
        self.vault_path: Path | None = None
        self.opened = False
        self.shutdown_count = 0
        self.entries: dict[str, VaultEntry] = {}
        self.storage: dict[str, StorageMapEntry] = {}
        self.contents: dict[str, bytes] = {}

    def _vault(self) -> Path:
        assert self.vault_path is not None
        return self.vault_path

    @staticmethod
    def _password(secret: bytes | bytearray) -> str:
        return bytes(secret).decode("utf-8")

    @staticmethod
    def _cipher_name(name: str) -> str:
        return hashlib.sha256(name.encode()).hexdigest()[:24] + ".c9r"

    def _parent_storage(self, logical_path: str) -> str:
        parent = posixpath.dirname(logical_path) or "/"
        if parent == "/":
            return "d/root"
        return self.storage[parent].storage_path

    def _entry_path(self, logical_path: str) -> str:
        return (
            self._parent_storage(logical_path)
            + "/"
            + self._cipher_name(posixpath.basename(logical_path))
        )

    def _physical(self, storage_path: str) -> Path:
        return self._vault().joinpath(*storage_path.split("/"))

    def initialize(
        self, vault_path: str | os.PathLike[str], secret: bytes | bytearray
    ) -> VaultInfo:
        assert self._password(secret) == self.password
        self.vault_path = Path(vault_path)
        (self._vault() / "d" / "root").mkdir(parents=True)
        (self._vault() / "vault.cryptomator").write_bytes(b"signed vault config")
        (self._vault() / "masterkey.cryptomator").write_bytes(b"protected key")
        self.opened = True
        return VaultInfo(self._vault(), 8, 220, "masterkeyfile:masterkey.cryptomator")

    def open(
        self, vault_path: str | os.PathLike[str], secret: bytes | bytearray
    ) -> VaultInfo:
        assert self._password(secret) == self.password
        self.vault_path = Path(vault_path)
        assert (self._vault() / "vault.cryptomator").is_file()
        self.opened = True
        return VaultInfo(self._vault(), 8, 220, "masterkeyfile:masterkey.cryptomator")

    def close_vault(self) -> None:
        self.opened = False

    def shutdown(self) -> None:
        self.opened = False
        self.shutdown_count += 1

    def snapshot(self, *, include_hash: bool = False) -> list[VaultEntry]:
        assert self.opened
        assert include_hash
        return [self.entries[path] for path in sorted(self.entries)]

    def storage_map(self) -> list[StorageMapEntry]:
        assert self.opened
        return [self.storage[path] for path in sorted(self.storage)]

    def put_file(
        self,
        logical_path: str,
        source: str | os.PathLike[str] | BinaryIO,
        *,
        modified_ms: int | None = None,
        replace: bool = False,
    ) -> None:
        assert self.opened
        assert replace == (logical_path in self.entries)
        if isinstance(source, (str, bytes, os.PathLike)):
            content = Path(source).read_bytes()
        else:
            source.seek(0)
            content = source.read()
            source.seek(0)
        current_storage = self.storage.get(logical_path)
        storage_path = (
            current_storage.storage_path
            if current_storage is not None
            else self._entry_path(logical_path)
        )
        physical = self._physical(storage_path)
        physical.parent.mkdir(parents=True, exist_ok=True)
        physical.write_bytes(bytes(byte ^ 0xA5 for byte in content))
        self.contents[logical_path] = content
        self.entries[logical_path] = VaultEntry(
            logical_path,
            "file",
            len(content),
            modified_ms or int(time.time() * 1000),
            hashlib.sha256(content).hexdigest(),
            None,
        )
        self.storage[logical_path] = StorageMapEntry(logical_path, "file", storage_path)

    def get_file(
        self,
        logical_path: str,
        destination: str | os.PathLike[str] | BinaryIO,
        *,
        replace: bool = False,
    ) -> None:
        del replace
        content = self.contents[logical_path]
        if isinstance(destination, (str, bytes, os.PathLike)):
            Path(destination).write_bytes(content)
        else:
            destination.write(content)

    def mkdir(self, logical_path: str, *, parents: bool = False) -> None:
        del parents
        token = uuid.uuid4().hex[:24]
        storage_path = "d/" + token
        self._physical(storage_path).mkdir()
        marker = self._physical(self._entry_path(logical_path))
        marker.mkdir()
        (marker / "dir.c9r").write_bytes(token.encode())
        self.entries[logical_path] = VaultEntry(
            logical_path,
            "directory",
            0,
            int(time.time() * 1000),
        )
        self.storage[logical_path] = StorageMapEntry(
            logical_path, "directory", storage_path
        )

    def move(self, source: str, target: str, *, replace: bool = False) -> None:
        assert not replace
        moving = [
            path
            for path in self.entries
            if path == source or path.startswith(source + "/")
        ]
        source_marker = self._physical(self._entry_path(source))
        target_marker = self._physical(self._entry_path(target))
        source_marker.rename(target_marker)
        for old_path in sorted(moving, key=len):
            new_path = target + old_path[len(source) :]
            entry = self.entries.pop(old_path)
            storage = self.storage.pop(old_path)
            storage_path = storage.storage_path
            if entry.type == "file" and old_path == source:
                storage_path = target_marker.relative_to(self._vault()).as_posix()
            self.entries[new_path] = dataclass_replace(entry, path=new_path)
            self.storage[new_path] = dataclass_replace(
                storage, path=new_path, storage_path=storage_path
            )
            if old_path in self.contents:
                self.contents[new_path] = self.contents.pop(old_path)

    def delete(self, logical_path: str, *, recursive: bool = False) -> None:
        selected = [
            path
            for path in self.entries
            if path == logical_path
            or (recursive and path.startswith(logical_path + "/"))
        ]
        marker = self._physical(self._entry_path(logical_path))
        if marker.is_dir():
            shutil.rmtree(marker)
        else:
            marker.unlink()
        for path in sorted(selected, key=len, reverse=True):
            if self.entries[path].type == "directory":
                shutil.rmtree(
                    self._physical(self.storage[path].storage_path),
                    ignore_errors=True,
                )
            self.entries.pop(path)
            self.storage.pop(path)
            self.contents.pop(path, None)


def test_ciphertext_commit_preserves_ids_for_moves_and_updates(tmp_path: Path) -> None:
    local = tmp_path / "local-vault"
    local.mkdir()
    write_vault(local)
    provider = FakeRemoteProvider(tmp_path / "remote")
    mirror = PhysicalVaultMirror(provider, "/Encrypted", local)

    mirror.initialise_remote()
    file_id = mirror.remote_entry("d/aa/file.c9r").id
    long_file_id = mirror.remote_entry("d/aa/long.c9s/contents.c9r").id

    before = mirror.snapshot_local()
    (local / "d" / "aa" / "file.c9r").rename(local / "d" / "aa" / "renamed.c9r")
    mirror.commit(before)
    assert mirror.remote_entry("d/aa/renamed.c9r").id == file_id

    before = mirror.snapshot_local()
    (local / "d" / "aa" / "renamed.c9r").write_bytes(b"ciphertext two")
    mirror.commit(before)
    assert mirror.remote_entry("d/aa/renamed.c9r").id == file_id

    before = mirror.snapshot_local()
    (local / "d" / "aa" / "long.c9s").rename(local / "d" / "aa" / "moved.c9s")
    (local / "d" / "aa" / "moved.c9s" / "name.c9s").write_bytes(b"new name")
    mirror.commit(before)
    assert mirror.remote_entry("d/aa/moved.c9s/contents.c9r").id == long_file_id
    assert not (provider.root / "Encrypted" / "d" / "aa" / "long.c9s").exists()


def test_refresh_replaces_only_the_private_ciphertext_mirror(tmp_path: Path) -> None:
    local = tmp_path / "local-vault"
    local.mkdir()
    write_vault(local)
    provider = FakeRemoteProvider(tmp_path / "remote")
    outside = provider.make_dir("/Outside")
    assert outside.path_display == "/Outside"
    mirror = PhysicalVaultMirror(provider, "/Encrypted", local)
    mirror.initialise_remote()

    (local / "uncommitted").write_bytes(b"discard me")
    cursor = mirror.refresh()

    assert cursor.startswith("cursor:")
    assert not (local / "uncommitted").exists()
    assert (local / "d" / "aa" / "file.c9r").read_bytes() == b"ciphertext one"
    assert (provider.root / "Outside").is_dir()


def test_ciphertext_transaction_resumes_after_lost_upload_reply(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local = tmp_path / "local-vault"
    local.mkdir()
    write_vault(local)
    remote = FakeRemoteProvider(tmp_path / "remote")
    mirror = PhysicalVaultMirror(remote, "/Encrypted", local)
    mirror.initialise_remote()

    before = mirror.snapshot_local()
    (local / "d" / "aa" / "new.c9r").write_bytes(b"new ciphertext")
    original_upload = remote.upload
    failed = False

    def upload_then_disconnect(*args: object, **kwargs: object) -> FileMetadata:
        nonlocal failed
        result = original_upload(*args, **kwargs)  # type: ignore[arg-type]
        if not failed:
            failed = True
            raise ConnectionError("reply lost after upload")
        return result

    monkeypatch.setattr(remote, "upload", upload_then_disconnect)
    with pytest.raises(ConnectionError, match="reply lost"):
        mirror.commit(before)

    assert mirror.journal_path.is_file()
    assert (local / "d" / "aa" / "new.c9r").read_bytes() == b"new ciphertext"
    monkeypatch.setattr(remote, "upload", original_upload)

    restarted = PhysicalVaultMirror(remote, "/Encrypted", local)
    restarted.claim_local_root(allow_nonempty=False)
    restarted.ensure_remote_root(create=False)
    restarted.resume_pending_transaction()

    assert not restarted.journal_path.exists()
    assert (
        remote.root / "Encrypted" / "d" / "aa" / "new.c9r"
    ).read_bytes() == b"new ciphertext"


def test_ciphertext_transaction_keeps_old_objects_until_uploads_finish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local = tmp_path / "local-vault"
    local.mkdir()
    write_vault(local)
    remote = FakeRemoteProvider(tmp_path / "remote")
    mirror = PhysicalVaultMirror(remote, "/Encrypted", local)
    mirror.initialise_remote()

    before = mirror.snapshot_local()
    (local / "d" / "aa" / "file.c9r").unlink()
    (local / "d" / "aa" / "replacement.c9r").write_bytes(b"different ciphertext")

    def reject_upload(*_args: object, **_kwargs: object) -> FileMetadata:
        raise ConnectionError("upload unavailable")

    monkeypatch.setattr(remote, "upload", reject_upload)
    with pytest.raises(ConnectionError, match="unavailable"):
        mirror.commit(before)

    assert (remote.root / "Encrypted" / "d" / "aa" / "file.c9r").is_file()
    assert not (remote.root / "Encrypted" / "d" / "aa" / "replacement.c9r").exists()


def test_local_ciphertext_snapshot_rejects_symbolic_links(tmp_path: Path) -> None:
    local = tmp_path / "local-vault"
    local.mkdir()
    write_vault(local)
    os.symlink(local / "vault.cryptomator", local / "unsafe-link")
    mirror = PhysicalVaultMirror(
        FakeRemoteProvider(tmp_path / "remote"), "/Encrypted", local
    )

    with pytest.raises(EncryptedVaultError, match="symbolic link"):
        mirror.snapshot_local()


def test_ciphertext_cache_rejects_a_symbolic_link_ancestor(tmp_path: Path) -> None:
    real_parent = tmp_path / "real-parent"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked-parent"
    os.symlink(real_parent, linked_parent, target_is_directory=True)
    mirror = PhysicalVaultMirror(
        FakeRemoteProvider(tmp_path / "remote"),
        "/Encrypted",
        linked_parent / "ciphertext-cache",
    )

    with pytest.raises(EncryptedVaultError, match="symbolic link or junction"):
        mirror.claim_local_root(allow_nonempty=False)


def test_ciphertext_cache_rejects_a_windows_junction_ancestor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    junction = tmp_path / "junction"
    junction.mkdir()
    monkeypatch.setattr(
        os.path,
        "isjunction",
        lambda path: Path(path) == junction,
        raising=False,
    )
    mirror = PhysicalVaultMirror(
        FakeRemoteProvider(tmp_path / "remote"),
        "/Encrypted",
        junction / "ciphertext-cache",
    )

    with pytest.raises(EncryptedVaultError, match="symbolic link or junction"):
        mirror.claim_local_root(allow_nonempty=False)


def test_encrypted_provider_cleans_only_owned_plaintext_transfer_files(
    tmp_path: Path,
) -> None:
    transfer_root = tmp_path / ".plaintext-transfers"
    transfer_root.mkdir(mode=0o700)
    stale = transfer_root / "maestral-encrypted-test-upload-stale"
    stale.write_bytes(b"plaintext")
    stale.chmod(0o600)
    another_profile = transfer_root / "maestral-other-upload-stale"
    another_profile.write_bytes(b"other")
    another_profile.chmod(0o600)
    unsafe_target = tmp_path / "outside"
    unsafe_target.write_bytes(b"outside")
    unsafe_link = transfer_root / "maestral-encrypted-test-upload-link"
    unsafe_link.symlink_to(unsafe_target)

    provider = EncryptedRemoteProvider(
        FakeRemoteProvider(tmp_path / "remote-provider"),
        FakeSecretStore(),
        "/Encrypted",
        tmp_path / "ciphertext-cache",
        sidecar=FakeCryptomatorSession("password"),
    )
    try:
        provider.initialise_vault("password")
        assert not stale.exists()
        assert another_profile.read_bytes() == b"other"
        assert unsafe_link.is_symlink()
        assert unsafe_target.read_bytes() == b"outside"
        assert stat.S_IMODE(os.lstat(transfer_root).st_mode) == 0o700
    finally:
        provider.close()


def test_encrypted_provider_rejects_a_busy_plaintext_transfer_cache(
    tmp_path: Path,
) -> None:
    lock_path = tmp_path / ".plaintext-transfers.encrypted-test.lock"
    ready = tmp_path / "lock-ready"
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import pathlib,sys; from fasteners import InterProcessLock; "
                "lock=InterProcessLock(sys.argv[1]); assert lock.acquire(False); "
                "pathlib.Path(sys.argv[2]).write_text('ready'); "
                "sys.stdin.buffer.read(1); lock.release()"
            ),
            str(lock_path),
            str(ready),
        ],
        stdin=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 2
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.exists()
        provider = EncryptedRemoteProvider(
            FakeRemoteProvider(tmp_path / "remote-provider"),
            FakeSecretStore(),
            "/Encrypted",
            tmp_path / "ciphertext-cache",
            sidecar=FakeCryptomatorSession("password"),
        )
        with pytest.raises(EncryptedVaultError, match="transfer cache is busy"):
            provider.initialise_vault("password")
        provider.close()
    finally:
        if holder.poll() is None:
            assert holder.stdin is not None
            holder.stdin.write(b"x")
            holder.stdin.close()
            holder.wait(timeout=2)


def test_remote_vault_root_must_be_dedicated() -> None:
    with pytest.raises(ValueError, match="account root"):
        PhysicalVaultMirror._normalise_remote_root("/")

    with pytest.raises(ValueError, match="invalid"):
        PhysicalVaultMirror._normalise_remote_root("/Encrypted/../escape")


def test_encrypted_provider_round_trip_and_stable_identity(tmp_path: Path) -> None:
    remote = FakeRemoteProvider(tmp_path / "remote-provider")
    secrets = FakeSecretStore()
    sidecar = FakeCryptomatorSession("vault password")
    provider = EncryptedRemoteProvider(
        remote,
        secrets,
        "/Encrypted",
        tmp_path / "ciphertext-cache",
        sidecar=sidecar,
    )

    provider.initialise_vault("vault password")
    assert secrets.secret == "vault password"
    assert provider.vault_open
    assert provider.list_folder("/").entries == []

    folder = provider.make_dir("/Documents")
    assert folder.path_display == "/Documents"
    source = io.BytesIO(b"cleartext must stay local")
    uploaded = provider.upload(source, "/Documents/report.txt")
    assert uploaded.content_hash == hashlib.sha256(source.getvalue()).hexdigest()
    stable_id = uploaded.id
    assert str(uuid.UUID(stable_id)) == stable_id

    remote_bytes = b"".join(
        path.read_bytes() for path in remote.root.rglob("*") if path.is_file()
    )
    assert b"cleartext must stay local" not in remote_bytes

    with pytest.raises(NotFoundError):
        provider.move(
            "/Documents/report.txt",
            "/Documents/wrong.txt",
            expected_provider_id="logical:replacement",
        )
    assert provider.get_metadata("/Documents/report.txt") == uploaded

    moved = provider.move(
        "/Documents/report.txt",
        "/Documents/final report.txt",
        expected_provider_id=uploaded.id,
    )
    assert moved.id == stable_id
    assert provider.get_metadata("/Documents/report.txt") is None

    downloaded = io.BytesIO()
    provider.download(
        "/Documents/final report.txt",
        downloaded,
        rev=moved.rev,
        provider_id=moved.id,
    )
    assert downloaded.getvalue() == b"cleartext must stay local"

    with pytest.raises(FileConflictError, match="changed"):
        provider.upload(
            io.BytesIO(b"new content"),
            "/Documents/final report.txt",
            write_mode=WriteMode.Update,
            update_rev="stale revision",
        )

    updated = provider.upload(
        io.BytesIO(b"new content"),
        "/Documents/final report.txt",
        write_mode=WriteMode.Update,
        update_rev=moved.rev,
    )
    assert updated.id == stable_id
    assert updated.rev != moved.rev

    with pytest.raises(NotFoundError):
        provider.remove(
            "/Documents/final report.txt",
            expected_provider_id="logical:replacement",
        )
    assert provider.get_metadata("/Documents/final report.txt") == updated

    provider.lock_vault()
    assert not provider.vault_open
    assert provider.get_metadata("/Documents/final report.txt") == updated
    assert provider.vault_open
    provider.close()


def test_identity_survives_backing_replacement_at_same_path(tmp_path: Path) -> None:
    remote = FakeRemoteProvider(tmp_path / "remote-provider")
    sidecar = FakeCryptomatorSession("password")
    provider = EncryptedRemoteProvider(
        remote,
        FakeSecretStore(),
        "/Encrypted",
        tmp_path / "ciphertext-cache",
        sidecar=sidecar,
    )
    provider.initialise_vault("password")
    uploaded = provider.upload(io.BytesIO(b"same cleartext"), "/file.txt")
    storage_path = sidecar.storage["/file.txt"].storage_path
    remote_path = "/Encrypted/" + storage_path

    provider.lock_vault()
    remote._ids[remote_path] = remote._new_id()
    remote._revisions[remote_path] = "rev:external-replacement"
    remote._touch_cursor()

    refreshed = provider.get_metadata("/file.txt")
    assert isinstance(refreshed, FileMetadata)
    assert refreshed.id == uploaded.id
    assert refreshed.rev != uploaded.rev
    assert backing_revision(refreshed.rev) == "rev:external-replacement"
    provider.close()


def test_external_move_uses_one_unique_content_match(tmp_path: Path) -> None:
    remote = FakeRemoteProvider(tmp_path / "remote-provider")
    sidecar = FakeCryptomatorSession("password")
    provider = EncryptedRemoteProvider(
        remote,
        FakeSecretStore(),
        "/Encrypted",
        tmp_path / "ciphertext-cache",
        sidecar=sidecar,
    )
    provider.initialise_vault("password")
    uploaded = provider.upload(io.BytesIO(b"unique content"), "/before.txt")
    old_storage = sidecar.storage["/before.txt"].storage_path

    provider.lock_vault()
    sidecar.opened = True
    sidecar.move("/before.txt", "/after.txt")
    sidecar.opened = False
    new_storage = sidecar.storage["/after.txt"].storage_path
    old_remote_path = "/Encrypted/" + old_storage
    new_remote_path = "/Encrypted/" + new_storage
    backing = remote.get_metadata(old_remote_path)
    assert isinstance(backing, FileMetadata)
    remote.move(
        old_remote_path,
        new_remote_path,
        expected_provider_id=backing.id,
    )
    remote._ids[new_remote_path] = remote._new_id()
    remote._touch_cursor()

    moved = provider.get_metadata("/after.txt")
    assert isinstance(moved, FileMetadata)
    assert moved.id == uploaded.id
    assert provider.get_metadata("/before.txt") is None
    provider.close()


def test_external_directory_move_uses_stable_storage(tmp_path: Path) -> None:
    remote = FakeRemoteProvider(tmp_path / "remote-provider")
    sidecar = FakeCryptomatorSession("password")
    provider = EncryptedRemoteProvider(
        remote,
        FakeSecretStore(),
        "/Encrypted",
        tmp_path / "ciphertext-cache",
        sidecar=sidecar,
    )
    provider.initialise_vault("password")
    folder = provider.make_dir("/Before")
    stable_storage = sidecar.storage["/Before"].storage_path
    old_marker = sidecar._entry_path("/Before")

    provider.lock_vault()
    sidecar.opened = True
    sidecar.move("/Before", "/After")
    sidecar.opened = False
    new_marker = sidecar._entry_path("/After")
    marker_metadata = remote.get_metadata("/Encrypted/" + old_marker)
    assert isinstance(marker_metadata, FolderMetadata)
    remote.move(
        "/Encrypted/" + old_marker,
        "/Encrypted/" + new_marker,
        expected_provider_id=marker_metadata.id,
    )
    remote._ids["/Encrypted/" + stable_storage] = remote._new_id()
    remote._touch_cursor()

    moved = provider.get_metadata("/After")
    assert isinstance(moved, FolderMetadata)
    assert moved.id == folder.id
    provider.close()


def test_ambiguous_external_content_moves_get_new_identities(tmp_path: Path) -> None:
    remote = FakeRemoteProvider(tmp_path / "remote-provider")
    sidecar = FakeCryptomatorSession("password")
    provider = EncryptedRemoteProvider(
        remote,
        FakeSecretStore(),
        "/Encrypted",
        tmp_path / "ciphertext-cache",
        sidecar=sidecar,
    )
    provider.initialise_vault("password")
    first = provider.upload(io.BytesIO(b"duplicate"), "/first.txt")
    second = provider.upload(io.BytesIO(b"duplicate"), "/second.txt")
    old_ids = {first.id, second.id}
    old_storage = {
        path: sidecar.storage[path].storage_path
        for path in ("/first.txt", "/second.txt")
    }

    provider.lock_vault()
    for source, target in (
        ("/first.txt", "/moved-one.txt"),
        ("/second.txt", "/moved-two.txt"),
    ):
        sidecar.opened = True
        sidecar.move(source, target)
        sidecar.opened = False
        old_remote_path = "/Encrypted/" + old_storage[source]
        new_remote_path = "/Encrypted/" + sidecar.storage[target].storage_path
        backing = remote.get_metadata(old_remote_path)
        assert isinstance(backing, FileMetadata)
        remote.move(
            old_remote_path,
            new_remote_path,
            expected_provider_id=backing.id,
        )
        remote._ids[new_remote_path] = remote._new_id()
    remote._touch_cursor()

    moved = provider.list_folder("/", recursive=True).entries
    new_ids = {entry.id for entry in moved}
    assert len(new_ids) == 2
    assert old_ids.isdisjoint(new_ids)
    provider.close()


def test_old_vault_bootstraps_manifest_through_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local = tmp_path / "ciphertext-cache"
    local.mkdir()
    sidecar = FakeCryptomatorSession("password")
    sidecar.initialize(local, bytearray(b"password"))
    sidecar.put_file("/legacy.txt", io.BytesIO(b"legacy"))
    sidecar.close_vault()
    remote = FakeRemoteProvider(tmp_path / "remote-provider")
    PhysicalVaultMirror(remote, "/Encrypted", local).initialise_remote()

    provider = EncryptedRemoteProvider(
        remote,
        FakeSecretStore(),
        "/Encrypted",
        local,
        sidecar=sidecar,
    )
    journal_seen = False
    execute_journal = provider._mirror._execute_journal

    def record_journal(journal: dict[str, object]) -> None:
        nonlocal journal_seen
        journal_seen = journal_seen or provider._mirror.journal_path.is_file()
        execute_journal(journal)

    monkeypatch.setattr(provider._mirror, "_execute_journal", record_journal)
    provider.attach_vault("password")

    metadata = provider.get_metadata("/legacy.txt")
    assert isinstance(metadata, FileMetadata)
    assert str(uuid.UUID(metadata.id)) == metadata.id
    assert journal_seen
    assert not provider._mirror.journal_path.exists()
    assert EncryptedRemoteProvider._IDENTITY_MANIFEST in sidecar.entries
    provider.close()


def test_manifest_bootstrap_keeps_local_state_after_remote_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local = tmp_path / "ciphertext-cache"
    local.mkdir()
    sidecar = FakeCryptomatorSession("password")
    sidecar.initialize(local, bytearray(b"password"))
    sidecar.put_file("/legacy.txt", io.BytesIO(b"legacy"))
    sidecar.close_vault()
    remote = FakeRemoteProvider(tmp_path / "remote-provider")
    PhysicalVaultMirror(remote, "/Encrypted", local).initialise_remote()
    provider = EncryptedRemoteProvider(
        remote,
        FakeSecretStore(),
        "/Encrypted",
        local,
        sidecar=sidecar,
    )
    original_upload = remote.upload

    def fail_upload(*_args: object, **_kwargs: object) -> FileMetadata:
        raise ConnectionError("bootstrap upload unavailable")

    monkeypatch.setattr(remote, "upload", fail_upload)
    with pytest.raises(ConnectionError, match="bootstrap upload unavailable"):
        provider.attach_vault("password")

    manifest_path = EncryptedRemoteProvider._IDENTITY_MANIFEST
    manifest_storage = sidecar.storage[manifest_path].storage_path
    assert provider._mirror.journal_path.is_file()
    assert sidecar.contents[manifest_path]
    assert sidecar._physical(manifest_storage).is_file()

    monkeypatch.setattr(remote, "upload", original_upload)
    restarted = EncryptedRemoteProvider(
        remote,
        FakeSecretStore(),
        "/Encrypted",
        local,
        sidecar=sidecar,
    )
    restarted.attach_vault("password")
    metadata = restarted.get_metadata("/legacy.txt")
    assert isinstance(metadata, FileMetadata)
    assert not restarted._mirror.journal_path.exists()
    restarted.close()


def test_identity_manifest_is_encrypted_hidden_and_reserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    remote = FakeRemoteProvider(tmp_path / "remote-provider")
    sidecar = FakeCryptomatorSession("password")
    provider = EncryptedRemoteProvider(
        remote,
        FakeSecretStore(),
        "/Encrypted",
        tmp_path / "ciphertext-cache",
        sidecar=sidecar,
    )
    upload_saw_manifest: list[bool] = []
    original_upload = remote.upload

    def track_initial_upload(*args: object, **kwargs: object) -> FileMetadata:
        upload_saw_manifest.append(
            EncryptedRemoteProvider._IDENTITY_MANIFEST in sidecar.entries
        )
        return original_upload(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(remote, "upload", track_initial_upload)
    provider.initialise_vault("password")

    assert upload_saw_manifest and all(upload_saw_manifest)
    assert provider.list_folder("/", recursive=True).entries == []
    assert provider.get_metadata(EncryptedRemoteProvider._IDENTITY_DIRECTORY) is None
    with pytest.raises(EncryptedVaultError, match="reserved"):
        provider.upload(
            io.BytesIO(b"blocked"),
            EncryptedRemoteProvider._IDENTITY_DIRECTORY + "/user.txt",
        )
    manifest = sidecar.contents[EncryptedRemoteProvider._IDENTITY_MANIFEST]
    vault_identity = provider._require_vault_identity().encode("ascii")
    remote_bytes = b"".join(
        path.read_bytes() for path in remote.root.rglob("*") if path.is_file()
    )
    assert manifest not in remote_bytes
    assert vault_identity not in remote_bytes
    provider.close()


def test_cursor_rejects_another_manifest_vault_identity(tmp_path: Path) -> None:
    provider = EncryptedRemoteProvider(
        FakeRemoteProvider(tmp_path / "remote-provider"),
        FakeSecretStore(),
        "/Encrypted",
        tmp_path / "ciphertext-cache",
        sidecar=FakeCryptomatorSession("password"),
    )
    provider.initialise_vault("password")
    uploaded = provider.upload(io.BytesIO(b"content"), "/file.txt")
    cursor = provider.list_folder("/", recursive=True).cursor
    encoded = cursor.split(":", 1)[1].encode("ascii")
    encoded += b"=" * (-len(encoded) % 4)
    payload = json.loads(base64.urlsafe_b64decode(encoded).decode("utf-8"))
    payload["vault_id"] = str(uuid.uuid4())
    mismatched = EncryptedRemoteProvider._CURSOR_PREFIX + base64.urlsafe_b64encode(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).rstrip(b"=").decode("ascii")

    assert provider._decode_cursor(mismatched) is None
    page = next(
        provider.list_remote_changes_iterator(
            mismatched, {uploaded.id: uploaded.path_display}
        )
    )
    assert any(
        entry.id == uploaded.id for entry in page.entries if hasattr(entry, "id")
    )
    provider.close()


def test_corrupt_identity_manifest_fails_closed(tmp_path: Path) -> None:
    sidecar = FakeCryptomatorSession("password")
    provider = EncryptedRemoteProvider(
        FakeRemoteProvider(tmp_path / "remote-provider"),
        FakeSecretStore(),
        "/Encrypted",
        tmp_path / "ciphertext-cache",
        sidecar=sidecar,
    )
    provider.initialise_vault("password")
    provider.lock_vault()
    sidecar.contents[EncryptedRemoteProvider._IDENTITY_MANIFEST] = b'{"schema":'

    with pytest.raises(EncryptedVaultError, match="identity manifest"):
        provider.unlock_vault()
    assert provider._state == "failed"
    assert provider._secret is None


def test_duplicate_identity_manifest_fails_closed(tmp_path: Path) -> None:
    sidecar = FakeCryptomatorSession("password")
    provider = EncryptedRemoteProvider(
        FakeRemoteProvider(tmp_path / "remote-provider"),
        FakeSecretStore(),
        "/Encrypted",
        tmp_path / "ciphertext-cache",
        sidecar=sidecar,
    )
    provider.initialise_vault("password")
    provider.lock_vault()
    duplicate = "/.MAESTRAL/identity-manifest.json"
    original = EncryptedRemoteProvider._IDENTITY_MANIFEST
    sidecar.entries[duplicate] = dataclass_replace(
        sidecar.entries[original], path=duplicate
    )
    sidecar.storage[duplicate] = dataclass_replace(
        sidecar.storage[original], path=duplicate
    )
    sidecar.contents[duplicate] = sidecar.contents[original]

    with pytest.raises(EncryptedVaultError, match="duplicate identity manifest"):
        provider.unlock_vault()
    assert provider._state == "failed"


def test_encrypted_provider_lists_deletions_after_remote_refresh(
    tmp_path: Path,
) -> None:
    remote = FakeRemoteProvider(tmp_path / "remote-provider")
    provider = EncryptedRemoteProvider(
        remote,
        FakeSecretStore(),
        "/Encrypted",
        tmp_path / "ciphertext-cache",
        sidecar=FakeCryptomatorSession("password"),
    )
    provider.initialise_vault("password")
    first = provider.upload(io.BytesIO(b"one"), "/one.txt")
    cursor = provider.list_folder("/", recursive=True).cursor
    provider.remove("/one.txt", expected_provider_id=first.id)

    page = next(provider.list_remote_changes_iterator(cursor, {first.id: "/one.txt"}))
    assert page.cursor != cursor
    assert len(page.entries) == 1
    assert page.entries[0].path_display == "/one.txt"
    assert not isinstance(page.entries[0], FileMetadata)
    provider.close()


def test_encrypted_virtual_sync_accepts_a_long_provider_cursor(
    config_name: str, tmp_path: Path
) -> None:
    remote = FakeRemoteProvider(tmp_path / "remote-provider")
    provider = EncryptedRemoteProvider(
        remote,
        FakeSecretStore(),
        "/Encrypted",
        tmp_path / "ciphertext-cache",
        sidecar=FakeCryptomatorSession("password"),
    )
    provider.initialise_vault("password")
    provider.upload(io.BytesIO(b"one"), "/one.txt")
    remote._cursor = "x" * 8_192  # type: ignore[assignment]

    root = tmp_path / "virtual-root"
    root.mkdir()
    backend = FakeVirtualFileBackend()
    controller = VirtualFileController(
        config_name,
        provider,
        backend,
        database_path=str(tmp_path / "virtual-files.db"),
        remote_polling=False,
    )
    controller.start(str(root))
    try:
        controller.refresh_remote()

        assert len(controller.cursor.encode("utf-8")) > 1_024
        assert provider._decode_cursor(controller.cursor) == f"cursor:{remote._cursor}"
        assert len(controller.status_page()["items"]) == 1
    finally:
        controller.close()
        provider.close()


def test_encrypted_provider_rejects_storage_map_mismatch(tmp_path: Path) -> None:
    remote = FakeRemoteProvider(tmp_path / "remote-provider")
    sidecar = FakeCryptomatorSession("password")
    provider = EncryptedRemoteProvider(
        remote,
        FakeSecretStore(),
        "/Encrypted",
        tmp_path / "ciphertext-cache",
        sidecar=sidecar,
    )
    provider.initialise_vault("password")
    provider.upload(io.BytesIO(b"content"), "/file.txt")
    sidecar.storage.clear()

    with pytest.raises(EncryptedVaultError, match="different sizes"):
        provider._rebuild_logical_metadata()
    provider.close()


def test_failed_vault_rebuild_terminates_sidecar_and_wipes_secret(
    tmp_path: Path,
) -> None:
    sidecar = FakeCryptomatorSession("password")
    provider = EncryptedRemoteProvider(
        FakeRemoteProvider(tmp_path / "remote-provider"),
        FakeSecretStore(),
        "/Encrypted",
        tmp_path / "ciphertext-cache",
        sidecar=sidecar,
    )
    provider.initialise_vault("password")
    provider.upload(io.BytesIO(b"content"), "/file.txt")
    provider.lock_vault()
    sidecar.storage.clear()

    with pytest.raises(EncryptedVaultError, match="different sizes"):
        provider.unlock_vault()

    assert provider._state == "failed"
    assert provider._secret is None
    assert provider._metadata_by_path == {}
    assert sidecar.shutdown_count == 1


def test_failed_vault_close_terminates_sidecar_and_wipes_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sidecar = FakeCryptomatorSession("password")
    provider = EncryptedRemoteProvider(
        FakeRemoteProvider(tmp_path / "remote-provider"),
        FakeSecretStore(),
        "/Encrypted",
        tmp_path / "ciphertext-cache",
        sidecar=sidecar,
    )
    provider.initialise_vault("password")

    def fail_close() -> None:
        raise RuntimeError("close failed")

    monkeypatch.setattr(sidecar, "close_vault", fail_close)
    with pytest.raises(RuntimeError, match="close failed"):
        provider.lock_vault()

    assert provider._state == "failed"
    assert provider._secret is None
    assert provider._metadata_by_path == {}
    assert sidecar.shutdown_count == 1
