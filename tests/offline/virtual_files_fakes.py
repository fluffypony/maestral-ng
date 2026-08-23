from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import BinaryIO

from maestral.core import FileMetadata, FolderMetadata, ListFolderResult
from maestral.exceptions import (
    VirtualFileBusyError,
    VirtualFileNotFoundError,
    VirtualFileRevisionError,
)
from maestral.virtual_files import (
    HydrationRequest,
    NativeFileIdentity,
    NativeFileRecord,
    NativeFileState,
    VirtualFileDescriptor,
)


def make_file(
    provider_id: str,
    path: str,
    revision: str,
    content: bytes,
) -> FileMetadata:
    now = datetime.now(timezone.utc)
    return FileMetadata(
        name=path.rsplit("/", 1)[-1],
        path_lower=path.lower(),
        path_display=path,
        id=provider_id,
        client_modified=now,
        server_modified=now,
        rev=revision,
        size=len(content),
        symlink_target=None,
        shared=False,
        modified_by=None,
        is_downloadable=True,
        content_hash=hashlib.sha256(content).hexdigest(),
    )


def make_folder(provider_id: str, path: str) -> FolderMetadata:
    return FolderMetadata(
        name=path.rsplit("/", 1)[-1],
        path_lower=path.lower(),
        path_display=path,
        id=provider_id,
        shared=False,
    )


class FakeVirtualProvider:
    provider_id = "fake"

    def __init__(self) -> None:
        self.files: dict[tuple[str, str], tuple[FileMetadata, bytes]] = {}
        self.current: dict[str, FileMetadata] = {}
        self.download_calls: list[tuple[str, str]] = []
        self.download_started = threading.Event()
        self.release_download = threading.Event()
        self.release_download.set()
        self.change_poll_started = threading.Event()
        self.pages: list[ListFolderResult] = []

    def add_file(
        self, provider_id: str, path: str, revision: str, content: bytes
    ) -> FileMetadata:
        metadata = make_file(provider_id, path, revision, content)
        self.files[(provider_id, revision)] = metadata, content
        self.current[provider_id] = metadata
        return metadata

    def block_downloads(self) -> None:
        self.download_started.clear()
        self.release_download.clear()

    def download(
        self,
        remote_path: str,
        local_path: str | BinaryIO,
        sync_event: object | None = None,
        *,
        rev: str | None = None,
        provider_id: str | None = None,
    ) -> FileMetadata:
        del remote_path, sync_event
        assert rev is not None
        assert provider_id is not None
        metadata, content = self.files[(provider_id, rev)]
        self.download_calls.append((provider_id, rev))
        self.download_started.set()
        if not self.release_download.wait(5):
            raise TimeoutError("The fake download was not released")
        if isinstance(local_path, str):
            with open(local_path, "wb") as file:
                file.write(content)
        else:
            local_path.write(content)
        return metadata

    def list_folder_iterator(
        self, remote_path: str, recursive: bool = False, **kwargs: object
    ) -> object:
        del remote_path, recursive, kwargs
        yield from self.pages

    def list_remote_changes_iterator(
        self,
        last_cursor: str,
        indexed_paths: dict[str, str] | None = None,
    ) -> object:
        del last_cursor, indexed_paths
        self.change_poll_started.set()
        yield from self.pages

    def wait_for_remote_changes(self, last_cursor: str, timeout: int = 40) -> bool:
        del last_cursor, timeout
        return False


@dataclass
class FakeNativeItem:
    descriptor: VirtualFileDescriptor
    content: bytes | None = None
    hydrated_revision: str | None = None
    pinned: bool = False
    dirty: bool = False
    open_count: int = 0


class FakeVirtualFileBackend:
    backend_id = "fake_native"
    supported = True

    def __init__(self) -> None:
        self.items: dict[str, FakeNativeItem] = {}
        self.calls: list[tuple[object, ...]] = []
        self.request_hydration: HydrationRequest | None = None
        self.started = False
        self.require_empty_directories = False
        self.cascade_directory_removals = False

    def start(self, root_path: str, request_hydration: HydrationRequest) -> None:
        self.calls.append(("start", root_path))
        self.request_hydration = request_hydration
        self.started = True

    def stop(self) -> None:
        self.calls.append(("stop",))
        self.started = False
        self.request_hydration = None

    def upsert(
        self,
        item: VirtualFileDescriptor,
        *,
        expected: NativeFileIdentity | None,
    ) -> None:
        self.calls.append(("upsert", item.provider_id, item.revision, expected))
        old = self.items.get(item.provider_id)
        if old is None:
            if expected is not None:
                raise VirtualFileRevisionError(
                    "Fake placeholder revision changed", item.provider_id
                )
            self.items[item.provider_id] = FakeNativeItem(
                descriptor=item, pinned=item.pinned
            )
            return
        if old.descriptor == item and old.pinned == item.pinned:
            return
        current_identity = NativeFileIdentity(
            provider_id=old.descriptor.provider_id,
            path=old.descriptor.path,
            is_directory=old.descriptor.is_directory,
            revision=old.descriptor.revision,
        )
        if current_identity != expected:
            raise VirtualFileRevisionError(
                "Fake placeholder revision changed", item.provider_id
            )
        if old.dirty or old.open_count:
            raise VirtualFileBusyError(
                "Cannot update the fake placeholder",
                "The file has local changes or is open.",
            )
        old_path = old.descriptor.path
        if old.descriptor.is_directory and old_path != item.path:
            old_prefix = old_path.rstrip("/") + "/"
            for child in self.items.values():
                if child is old or not child.descriptor.path.startswith(old_prefix):
                    continue
                suffix = child.descriptor.path[len(old_path) :]
                child.descriptor = replace(child.descriptor, path=item.path + suffix)
        if old.descriptor.revision != item.revision:
            old.content = None
            old.hydrated_revision = None
        old.descriptor = item
        old.pinned = item.pinned

    def remove(self, provider_id: str, *, expected: NativeFileIdentity) -> None:
        self.calls.append(("remove", provider_id, expected))
        item = self.items.get(provider_id)
        if item is None:
            return
        current_identity = NativeFileIdentity(
            provider_id=item.descriptor.provider_id,
            path=item.descriptor.path,
            is_directory=item.descriptor.is_directory,
            revision=item.descriptor.revision,
        )
        if current_identity != expected:
            raise VirtualFileRevisionError(
                "Fake placeholder revision changed", provider_id
            )
        if item.dirty or item.open_count:
            raise VirtualFileBusyError(
                "Cannot remove the fake placeholder",
                "The file has local changes or is open.",
            )
        if item is not None and self.require_empty_directories:
            prefix = item.descriptor.path.rstrip("/") + "/"
            if any(
                child_id != provider_id and child.descriptor.path.startswith(prefix)
                for child_id, child in self.items.items()
            ):
                raise VirtualFileBusyError(
                    "Cannot remove the fake placeholder", "The directory is not empty."
                )
        if item is not None and self.cascade_directory_removals:
            prefix = item.descriptor.path.rstrip("/") + "/"
            child_ids = [
                child_id
                for child_id, child in self.items.items()
                if child_id != provider_id and child.descriptor.path.startswith(prefix)
            ]
            for child_id in child_ids:
                self.items.pop(child_id)
        self.items.pop(provider_id, None)

    def set_pinned(self, provider_id: str, pinned: bool) -> None:
        self.calls.append(("pin", provider_id, pinned))
        try:
            self.items[provider_id].pinned = pinned
        except KeyError as exc:
            raise VirtualFileNotFoundError(
                "Fake placeholder not found", provider_id
            ) from exc

    def materialize(
        self,
        item: VirtualFileDescriptor,
        staged_path: str,
        *,
        expected_revision: str,
    ) -> None:
        self.calls.append(("materialize", item.provider_id, expected_revision))
        native = self.items.get(item.provider_id)
        if native is None:
            raise VirtualFileNotFoundError(
                "Fake placeholder not found", item.provider_id
            )
        if native.descriptor.revision != expected_revision:
            raise VirtualFileRevisionError(
                "Fake placeholder revision changed", item.provider_id
            )
        if native.dirty:
            raise VirtualFileBusyError(
                "Cannot materialize the fake placeholder",
                "The file has local changes.",
            )
        with open(staged_path, "rb") as file:
            native.content = file.read()
        native.hydrated_revision = expected_revision

    def evict(self, provider_id: str, *, expected_revision: str) -> None:
        self.calls.append(("evict", provider_id, expected_revision))
        native = self.items.get(provider_id)
        if native is None:
            raise VirtualFileNotFoundError("Fake placeholder not found", provider_id)
        if native.descriptor.revision != expected_revision:
            raise VirtualFileRevisionError(
                "Fake placeholder revision changed", provider_id
            )
        if native.pinned or native.dirty or native.open_count:
            raise VirtualFileBusyError(
                "Cannot evict the fake placeholder", "The file is not safe to evict."
            )
        native.content = None
        native.hydrated_revision = None

    def inspect(self, provider_id: str) -> NativeFileState | None:
        native = self.items.get(provider_id)
        if native is None:
            return None
        return NativeFileState(
            identity=NativeFileIdentity(
                provider_id=native.descriptor.provider_id,
                path=native.descriptor.path,
                is_directory=native.descriptor.is_directory,
                revision=native.descriptor.revision,
            ),
            hydrated_revision=native.hydrated_revision,
            pinned=native.pinned,
            dirty=native.dirty,
            open_count=native.open_count,
        )

    def recover(self) -> list[NativeFileRecord]:
        self.calls.append(("recover",))
        return [
            NativeFileRecord(
                provider_id=provider_id,
                path=item.descriptor.path,
                is_directory=item.descriptor.is_directory,
                revision=item.descriptor.revision,
                hydrated_revision=item.hydrated_revision,
                pinned=item.pinned,
                dirty=item.dirty,
                open_count=item.open_count,
            )
            for provider_id, item in sorted(self.items.items())
        ]

    def open(self, provider_id: str) -> dict[str, object]:
        if self.request_hydration is None:
            raise RuntimeError("The fake backend is stopped")
        revision = self.items[provider_id].descriptor.revision
        return self.request_hydration(provider_id, revision)

    def set_access_state(
        self,
        provider_id: str,
        *,
        dirty: bool = False,
        open_count: int = 0,
    ) -> None:
        item = self.items[provider_id]
        item.dirty = dirty
        item.open_count = open_count
