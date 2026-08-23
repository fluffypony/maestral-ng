"""Provider-neutral remote storage interface."""

from __future__ import annotations

import os
import posixpath
from bisect import bisect_right
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, BinaryIO, Final, Literal, Protocol, cast, overload

import requests

from ..core import (
    Account,
    FileMetadata,
    FolderMetadata,
    FullAccount,
    LinkAccessLevel,
    LinkAudience,
    ListFolderResult,
    Metadata,
    PersonalSpaceUsage,
    RootInfo,
    SharedLinkMetadata,
    WriteMode,
)
from ..utils.hashing import ContentHasherFactory
from ..utils.path import normalize

if TYPE_CHECKING:
    from ..keyring import CredentialStorage
    from ..models import SyncEvent


DROPBOX = "dropbox"
GOOGLE_DRIVE = "google_drive"
PROVIDER_NAMES = (DROPBOX, GOOGLE_DRIVE)
MAX_LOGICAL_PAGE_SIZE: Final = 4096

LogicalCursorPhase = Literal["steady", "snapshot", "changes"]
LogicalOperationKind = Literal["delete", "upsert"]
LogicalOperationKey = tuple[LogicalOperationKind, int, str, str]
LogicalListingScope = tuple[str, bool, int]


@dataclass(frozen=True)
class LogicalCursor:
    """A durable cursor for a bounded logical provider page."""

    phase: LogicalCursorPhase
    cursor: str | None
    origin_cursor: str | None
    final_cursor: str | None
    last_key: LogicalOperationKey | None
    scope: LogicalListingScope | None

    @classmethod
    def steady(cls, cursor: str) -> LogicalCursor:
        return cls("steady", cursor, None, None, None, None)

    @classmethod
    def continuation(
        cls,
        phase: Literal["snapshot", "changes"],
        origin_cursor: str,
        final_cursor: str,
        last_key: LogicalOperationKey,
        scope: LogicalListingScope,
    ) -> LogicalCursor:
        return cls(phase, None, origin_cursor, final_cursor, last_key, scope)

    def to_payload(self) -> dict[str, object]:
        """Return the strict JSON payload shared by logical providers."""
        return {
            "phase": self.phase,
            "cursor": self.cursor,
            "origin_cursor": self.origin_cursor,
            "final_cursor": self.final_cursor,
            "last_key": list(self.last_key) if self.last_key is not None else None,
            "scope": list(self.scope) if self.scope is not None else None,
        }

    @classmethod
    def from_payload(cls, payload: object) -> LogicalCursor | None:
        """Parse one strict shared cursor payload."""
        if not isinstance(payload, dict) or set(payload) != {
            "phase",
            "cursor",
            "origin_cursor",
            "final_cursor",
            "last_key",
            "scope",
        }:
            return None

        phase = payload["phase"]
        cursor = payload["cursor"]
        origin = payload["origin_cursor"]
        final = payload["final_cursor"]
        raw_key = payload["last_key"]
        raw_scope = payload["scope"]
        if phase == "steady":
            if (
                not isinstance(cursor, str)
                or not cursor
                or origin is not None
                or final is not None
                or raw_key is not None
                or raw_scope is not None
            ):
                return None
            return cls.steady(cursor)

        if phase not in {"snapshot", "changes"}:
            return None
        if (
            cursor is not None
            or not isinstance(origin, str)
            or not origin
            or not isinstance(final, str)
            or not final
            or not isinstance(raw_key, list)
            or len(raw_key) != 4
            or not isinstance(raw_scope, list)
            or len(raw_scope) != 3
        ):
            return None

        kind, depth, path, provider_id = raw_key
        scope_path, recursive, limit = raw_scope
        canonical_path = (
            isinstance(path, str)
            and path.startswith("/")
            and "\x00" not in path
            and posixpath.normpath(path) == path
            and normalize(path) == path
        )
        expected_depth = (
            len([part for part in path.split("/") if part])
            if isinstance(path, str)
            else 0
        )
        if (
            kind not in {"delete", "upsert"}
            or type(depth) is not int
            or depth < 1
            or depth != expected_depth
            or not canonical_path
            or not isinstance(provider_id, str)
            or not provider_id
            or len(provider_id) > 4096
            or any(ord(character) < 0x20 for character in provider_id)
            or not isinstance(scope_path, str)
            or not scope_path.startswith("/")
            or "\x00" in scope_path
            or type(recursive) is not bool
            or type(limit) is not int
            or not 1 <= limit <= MAX_LOGICAL_PAGE_SIZE
        ):
            return None
        key = cast(LogicalOperationKey, (kind, depth, path, provider_id))
        scope = (scope_path, recursive, limit)
        return cls.continuation(phase, origin, final, key, scope)


def validate_logical_page_limit(limit: int | None) -> int:
    """Validate and cap a requested logical result-page size."""
    if limit is None:
        return MAX_LOGICAL_PAGE_SIZE
    if type(limit) is not int or limit <= 0:
        raise ValueError("The page limit must be a positive integer")
    return min(limit, MAX_LOGICAL_PAGE_SIZE)


def logical_operation_key(
    kind: LogicalOperationKind,
    path: str,
    provider_id: str,
) -> LogicalOperationKey:
    """Return the stable ordering key for one logical remote operation."""
    normalised_path = normalize(path)
    depth = len([part for part in posixpath.normpath(path).split("/") if part])
    if depth < 1 or not provider_id:
        raise ValueError("A logical operation must identify a non-root item")
    return kind, depth, normalised_path, provider_id


def iter_logical_pages(
    operations: list[tuple[LogicalOperationKey, Metadata]],
    limit: int,
    last_key: LogicalOperationKey | None = None,
) -> Iterator[tuple[list[Metadata], LogicalOperationKey | None, bool]]:
    """Yield stable pages after an optional durable operation key."""
    ordered = sorted(operations, key=lambda operation: operation[0])
    keys = [operation[0] for operation in ordered]
    start = bisect_right(keys, last_key) if last_key is not None else 0
    if start >= len(ordered):
        yield [], None, False
        return
    while start < len(ordered):
        selected = ordered[start : start + limit]
        start += len(selected)
        has_more = start < len(ordered)
        yield [operation[1] for operation in selected], selected[-1][0], has_more


def normalise_provider_name(value: str) -> str:
    """Return one canonical provider name."""
    if not isinstance(value, str):
        raise ValueError("The remote provider must be a string")
    normalised = value.strip().lower().replace("-", "_").replace(" ", "_")
    if normalised not in PROVIDER_NAMES:
        choices = ", ".join(PROVIDER_NAMES)
        raise ValueError(f"Unknown remote provider {value!r}. Choose one of: {choices}")
    return normalised


class RemoteProvider(Protocol):
    """The complete remote interface used by the sync core and public API."""

    config_name: str
    provider_id: str
    api_url: str
    content_hasher_factory: ContentHasherFactory
    bandwidth_limit_up: float
    bandwidth_limit_down: float

    @property
    def linked(self) -> bool: ...

    @property
    def account_info(self) -> FullAccount: ...

    @property
    def namespace_id(self) -> str: ...

    @property
    def is_team_space(self) -> bool: ...

    def close(self) -> None: ...

    def get_auth_url(self) -> str: ...

    def link(
        self,
        code: str | None = None,
        refresh_token: str | None = None,
        access_token: str | None = None,
        allow_plaintext_keyring: bool = False,
    ) -> int: ...

    def unlink(self) -> None: ...

    @overload
    def get_account_info(self, dbid: None = None) -> FullAccount: ...

    @overload
    def get_account_info(self, dbid: str) -> Account: ...

    def get_account_info(self, dbid: str | None = None) -> Account: ...

    def get_space_usage(self) -> PersonalSpaceUsage: ...

    def update_path_root(self, root_info: RootInfo) -> None: ...

    def get_metadata(
        self, remote_path: str, include_deleted: bool = False
    ) -> Metadata | None: ...

    def list_folder(
        self,
        remote_path: str,
        recursive: bool = False,
        include_deleted: bool = False,
        include_mounted_folders: bool = True,
        include_non_downloadable_files: bool = False,
    ) -> ListFolderResult: ...

    def list_folder_iterator(
        self,
        remote_path: str,
        recursive: bool = False,
        include_deleted: bool = False,
        include_mounted_folders: bool = True,
        limit: int | None = None,
        include_non_downloadable_files: bool = False,
    ) -> Iterator[ListFolderResult]: ...

    def list_remote_changes_iterator(
        self,
        last_cursor: str,
        indexed_paths: Mapping[str, str] | None = None,
    ) -> Iterator[ListFolderResult]: ...

    def wait_for_remote_changes(self, last_cursor: str, timeout: int = 40) -> bool: ...

    def download(
        self,
        remote_path: str,
        local_path: str | BinaryIO,
        sync_event: SyncEvent | None = None,
        *,
        rev: str | None = None,
        provider_id: str | None = None,
    ) -> FileMetadata: ...

    def upload(
        self,
        local_file: str | os.PathLike[str] | BinaryIO,
        remote_path: str,
        write_mode: WriteMode = WriteMode.Add,
        update_rev: str | None = None,
        autorename: bool = False,
        sync_event: SyncEvent | None = None,
        *,
        local_path: str | None = None,
    ) -> FileMetadata: ...

    def make_dir(
        self, remote_path: str, autorename: bool = False
    ) -> FolderMetadata: ...

    def move(
        self,
        remote_path: str,
        new_path: str,
        autorename: bool = False,
        *,
        expected_provider_id: str,
    ) -> FileMetadata | FolderMetadata: ...

    def remove(
        self,
        remote_path: str,
        parent_rev: str | None = None,
        *,
        expected_provider_id: str,
    ) -> FileMetadata | FolderMetadata: ...

    def share_dir(
        self, remote_path: str, force_async: bool = False
    ) -> FolderMetadata | None: ...

    def list_revisions(
        self, remote_path: str, mode: str = "path", limit: int = 10
    ) -> list[FileMetadata]: ...

    def restore(self, remote_path: str, rev: str) -> FileMetadata: ...

    def create_shared_link(
        self,
        remote_path: str,
        visibility: LinkAudience = LinkAudience.Public,
        access_level: LinkAccessLevel = LinkAccessLevel.Viewer,
        allow_download: bool | None = None,
        password: str | None = None,
        expires: datetime | None = None,
    ) -> SharedLinkMetadata: ...

    def revoke_shared_link(self, url: str) -> None: ...

    def list_shared_links(
        self, remote_path: str | None = None, *, direct_only: bool = False
    ) -> list[SharedLinkMetadata]: ...


def create_provider(
    provider: str,
    config_name: str,
    cred_storage: CredentialStorage,
    *,
    session: requests.Session | None = None,
    bandwidth_limit_up: float = 0,
    bandwidth_limit_down: float = 0,
) -> RemoteProvider:
    """Create the selected provider without a provider-specific core branch."""
    provider = normalise_provider_name(provider)
    if provider == DROPBOX:
        from ..client import DropboxClient

        return DropboxClient(
            config_name,
            cred_storage,
            session=session,
            bandwidth_limit_up=bandwidth_limit_up,
            bandwidth_limit_down=bandwidth_limit_down,
        )

    from .google_drive import GoogleDriveProvider

    return GoogleDriveProvider(
        config_name,
        cred_storage,
        session=cast("HttpSession", session) if session is not None else None,
        bandwidth_limit_up=bandwidth_limit_up,
        bandwidth_limit_down=bandwidth_limit_down,
    )


if TYPE_CHECKING:
    from .google_drive import HttpSession
