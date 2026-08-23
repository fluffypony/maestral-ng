from __future__ import annotations

import hashlib
import os
import posixpath
import shutil
from collections.abc import Iterator
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
from maestral.exceptions import FileConflictError, NotFoundError
from maestral.providers.encrypted import EncryptedVaultError, PhysicalVaultMirror
from maestral.utils.hashing import sha256_content_hasher
from maestral.utils.path import normalize


class FakeRemoteProvider:
    provider_id = "fake"
    api_url = "https://example.invalid"
    content_hasher_factory = staticmethod(sha256_content_hasher)
    bandwidth_limit_up = 0.0
    bandwidth_limit_down = 0.0

    def __init__(self, root: Path) -> None:
        self.config_name = "encrypted-test"
        self.root = root
        self.root.mkdir()
        self._ids = {"/": "id:root"}
        self._revisions: dict[str, str] = {}
        self._next_id = 0
        self._cursor = 1

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
        self, remote_path: str, new_path: str, autorename: bool = False
    ) -> FileMetadata | FolderMetadata:
        del autorename
        source = self._local(remote_path)
        target = self._local(new_path)
        if not source.exists():
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
        self, remote_path: str, parent_rev: str | None = None
    ) -> FileMetadata | FolderMetadata:
        metadata = self._metadata(remote_path)
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


def test_remote_vault_root_must_be_dedicated() -> None:
    with pytest.raises(ValueError, match="account root"):
        PhysicalVaultMirror._normalise_remote_root("/")

    with pytest.raises(ValueError, match="invalid"):
        PhysicalVaultMirror._normalise_remote_root("/Encrypted/../escape")
