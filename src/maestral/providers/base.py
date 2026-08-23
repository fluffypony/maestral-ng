"""Provider-neutral remote storage interface."""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from datetime import datetime
from typing import TYPE_CHECKING, BinaryIO, Protocol, cast, overload

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

if TYPE_CHECKING:
    from ..keyring import CredentialStorage
    from ..models import SyncEvent


DROPBOX = "dropbox"
GOOGLE_DRIVE = "google_drive"
PROVIDER_NAMES = (DROPBOX, GOOGLE_DRIVE)


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
