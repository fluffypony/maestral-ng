"""Cryptomator-backed remote storage for Maestral."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import posixpath
import re
import shutil
import stat
import tempfile
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager, nullcontext
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import (
    TYPE_CHECKING,
    BinaryIO,
    Literal,
    Protocol,
    cast,
    overload,
)

from maestral.core import (
    Account,
    DeletedMetadata,
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
from maestral.cryptomator import StorageMapEntry, VaultEntry, VaultInfo
from maestral.exceptions import (
    DataChangedError,
    DataCorruptionError,
    FileConflictError,
    FolderConflictError,
    IsAFolderError,
    MaestralApiError,
    NotAFolderError,
    NotFoundError,
    UnsupportedProviderOperationError,
)
from maestral.providers.base import RemoteProvider
from maestral.utils.hashing import ContentHasherFactory, sha256_content_hasher
from maestral.utils.path import normalize

if TYPE_CHECKING:
    from maestral.models import SyncEvent


class EncryptedVaultError(MaestralApiError):
    """A local Cryptomator vault or mirror operation failed."""


class CryptomatorSession(Protocol):
    """The sidecar methods used by the encrypted provider."""

    def initialize(
        self, vault_path: str | os.PathLike[str], secret: bytes | bytearray
    ) -> VaultInfo: ...

    def open(
        self, vault_path: str | os.PathLike[str], secret: bytes | bytearray
    ) -> VaultInfo: ...

    def close_vault(self) -> None: ...

    def shutdown(self) -> None: ...

    def snapshot(self, *, include_hash: bool = False) -> list[VaultEntry]: ...

    def storage_map(self) -> list[StorageMapEntry]: ...

    def put_file(
        self,
        logical_path: str,
        source: str | os.PathLike[str],
        *,
        modified_ms: int | None = None,
        replace: bool = False,
    ) -> None: ...

    def get_file(
        self,
        logical_path: str,
        destination: str | os.PathLike[str],
        *,
        replace: bool = False,
    ) -> None: ...

    def mkdir(self, logical_path: str, *, parents: bool = False) -> None: ...

    def move(self, source: str, target: str, *, replace: bool = False) -> None: ...

    def delete(self, logical_path: str, *, recursive: bool = False) -> None: ...


class VaultSecretStore(Protocol):
    """Secure storage used for one vault password."""

    def load_password(self) -> str | None: ...

    def save_password(self, secret: str) -> None: ...

    def delete_password(self) -> None: ...


_PhysicalKind = Literal["file", "directory"]
_PhysicalOperationKind = Literal["move", "mkdir", "upload", "remove"]
_VaultState = Literal["locked", "opening", "open", "closing", "failed", "closed"]
_WINDOWS_RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{index}" for index in range(1, 10)),
    *(f"LPT{index}" for index in range(1, 10)),
}


@dataclass(frozen=True)
class _LocalPhysicalEntry:
    kind: _PhysicalKind
    identity: tuple[int, int, _PhysicalKind]
    size: int
    modified_ns: int
    sha256: str | None


@dataclass(frozen=True)
class _RemotePhysicalEntry:
    kind: _PhysicalKind
    provider_id: str | None
    rev: str | None


@dataclass(frozen=True)
class _PhysicalOperation:
    kind: _PhysicalOperationKind
    target: str
    expected_kind: _PhysicalKind
    source: str | None = None
    expected_id: str | None = None
    expected_rev: str | None = None
    sha256: str | None = None


def _path_depth(path: str) -> int:
    return path.count("/") + 1


def _is_below(path: str, parent: str) -> bool:
    return path.startswith(parent + "/")


def _replace_prefix(path: str, source: str, target: str) -> str:
    if path == source:
        return target
    if _is_below(path, source):
        return target + path[len(source) :]
    return path


class PhysicalVaultMirror:
    """Mirror one remote Cryptomator vault into a private ciphertext directory."""

    _OWNER_MARKER_CONTENT = b"maestral-cryptomator-cache-v1\n"
    _STATE_SCHEMA = 1

    def __init__(
        self,
        provider: RemoteProvider,
        remote_root: str,
        local_root: str | os.PathLike[str],
    ) -> None:
        self.provider = provider
        self.remote_root = self._normalise_remote_root(remote_root)
        self.local_root = Path(local_root).absolute()
        self._remote: dict[str, FileMetadata | FolderMetadata] = {}
        self._remote_root_id: str | None = None
        self._manifest_local: dict[str, _LocalPhysicalEntry] = {}

    @staticmethod
    def _normalise_remote_root(path: str) -> str:
        if not isinstance(path, str) or "\x00" in path or not path.startswith("/"):
            raise ValueError("The encrypted vault path must be an absolute remote path")
        if path != "/" and any(part in {"", ".", ".."} for part in path.split("/")[1:]):
            raise ValueError("The encrypted vault path is invalid")
        normalised = posixpath.normpath(path)
        if normalised == "/":
            raise ValueError("The encrypted vault cannot use the remote account root")
        return normalised

    @property
    def remote_entries(self) -> Mapping[str, FileMetadata | FolderMetadata]:
        return self._remote.copy()

    def remote_entry(self, relative_path: str) -> FileMetadata | FolderMetadata:
        try:
            return self._remote[relative_path]
        except KeyError as exc:
            raise EncryptedVaultError(
                "Encrypted vault is inconsistent",
                "A logical item has no remote ciphertext object.",
            ) from exc

    @property
    def remote_root_id(self) -> str:
        if self._remote_root_id is None:
            raise EncryptedVaultError(
                "Encrypted vault identity is unavailable",
                "Open or initialise the remote vault first.",
            )
        return self._remote_root_id

    @property
    def owner_marker(self) -> Path:
        return self.local_root.with_name(
            f".{self.local_root.name}.maestral-cryptomator-cache"
        )

    @property
    def manifest_path(self) -> Path:
        return self.local_root.with_name(
            f".{self.local_root.name}.maestral-cryptomator-manifest.json"
        )

    @property
    def journal_path(self) -> Path:
        return self.local_root.with_name(
            f".{self.local_root.name}.maestral-cryptomator-transaction.json"
        )

    def claim_local_root(self, *, allow_nonempty: bool) -> None:
        """Claim a cache root before any code may replace its contents."""
        parent = self.local_root.parent
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if parent.is_symlink() or self.local_root.is_symlink():
            raise EncryptedVaultError(
                "Encrypted vault mirror is unsafe",
                "The ciphertext cache cannot use a symbolic link.",
            )
        if self.local_root.exists() and not self.local_root.is_dir():
            raise EncryptedVaultError(
                "Encrypted vault cache is unsafe",
                "The ciphertext cache root must be a directory.",
            )

        marker = self.owner_marker
        if os.path.lexists(marker):
            marker_stat = os.lstat(marker)
            if (
                not stat.S_ISREG(marker_stat.st_mode)
                or stat.S_ISLNK(marker_stat.st_mode)
                or marker_stat.st_nlink != 1
                or marker.read_bytes() != self._OWNER_MARKER_CONTENT
            ):
                raise EncryptedVaultError(
                    "Encrypted vault cache has invalid ownership",
                    "Choose a new private ciphertext cache.",
                )
            return

        if self.local_root.exists():
            if any(self.local_root.iterdir()) and not allow_nonempty:
                raise EncryptedVaultError(
                    "Encrypted vault cache is not owned by Maestral",
                    "Choose a new cache path or restore its ownership marker.",
                )

        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        descriptor = os.open(marker, flags, 0o600)
        with os.fdopen(descriptor, "wb") as marker_file:
            marker_file.write(self._OWNER_MARKER_CONTENT)
        os.chmod(marker, 0o600)

    def ensure_remote_root(self, *, create: bool) -> FolderMetadata:
        """Return the remote vault folder and optionally create missing parents."""
        current = ""
        metadata: Metadata | None = None
        for component in PurePosixPath(self.remote_root).parts[1:]:
            current += "/" + component
            metadata = self.provider.get_metadata(current)
            if metadata is None:
                if not create:
                    raise EncryptedVaultError(
                        "Encrypted vault not found",
                        "Check the remote vault path and try again.",
                    )
                metadata = self.provider.make_dir(current)
            if not isinstance(metadata, FolderMetadata):
                raise EncryptedVaultError(
                    "Encrypted vault path is not a folder",
                    "Choose a different remote vault path.",
                )
        folder = cast(FolderMetadata, metadata)
        self._remote_root_id = folder.id
        return folder

    def refresh(self) -> str:
        """Download one consistent remote ciphertext snapshot and return its cursor."""
        self.claim_local_root(allow_nonempty=False)
        self.ensure_remote_root(create=False)
        self.resume_pending_transaction()
        remote, cursor = self._list_remote()
        self._materialise(remote)
        self._remote = remote
        local = self.snapshot_local(reuse_manifest=False)
        self._save_manifest(local, remote)
        return cursor

    def initialise_remote(self) -> None:
        """Create an empty remote root and upload the current local vault."""
        self.claim_local_root(allow_nonempty=True)
        self.ensure_remote_root(create=True)
        self.resume_pending_transaction()
        remote, _ = self._list_remote()
        if remote:
            raise EncryptedVaultError(
                "Encrypted vault is not empty",
                "Choose an empty remote folder for a new vault.",
            )
        self._remote = {}
        self.commit({})

    def snapshot_local(
        self, *, reuse_manifest: bool = True
    ) -> dict[str, _LocalPhysicalEntry]:
        """Return a no-follow snapshot of every local ciphertext object."""
        root_stat = os.lstat(self.local_root)
        if not stat.S_ISDIR(root_stat.st_mode) or stat.S_ISLNK(root_stat.st_mode):
            raise EncryptedVaultError(
                "Encrypted vault mirror is unsafe",
                "The local ciphertext root must be a private directory.",
            )

        cached = self._load_manifest_local() if reuse_manifest else {}
        result: dict[str, _LocalPhysicalEntry] = {}
        for current, dir_names, file_names in os.walk(
            self.local_root, topdown=True, followlinks=False
        ):
            current_path = Path(current)
            for name in [*dir_names, *file_names]:
                path = current_path / name
                file_stat = os.lstat(path)
                relative = path.relative_to(self.local_root).as_posix()
                if stat.S_ISLNK(file_stat.st_mode):
                    raise EncryptedVaultError(
                        "Encrypted vault mirror is unsafe",
                        "The ciphertext tree contains a symbolic link.",
                    )
                if stat.S_ISDIR(file_stat.st_mode):
                    kind: _PhysicalKind = "directory"
                    digest = None
                elif stat.S_ISREG(file_stat.st_mode):
                    kind = "file"
                    old_entry = cached.get(relative)
                    identity = (file_stat.st_dev, file_stat.st_ino, kind)
                    if (
                        old_entry is not None
                        and old_entry.kind == kind
                        and old_entry.identity == identity
                        and old_entry.size == file_stat.st_size
                        and old_entry.modified_ns == file_stat.st_mtime_ns
                        and old_entry.sha256 is not None
                    ):
                        digest = old_entry.sha256
                    else:
                        digest = self._sha256(path)
                else:
                    raise EncryptedVaultError(
                        "Encrypted vault mirror is unsafe",
                        "The ciphertext tree contains an unsupported object.",
                    )
                result[relative] = _LocalPhysicalEntry(
                    kind,
                    (file_stat.st_dev, file_stat.st_ino, kind),
                    file_stat.st_size,
                    file_stat.st_mtime_ns,
                    digest,
                )
        return result

    def commit(self, before: Mapping[str, _LocalPhysicalEntry]) -> None:
        """Durably apply a closed vault's physical changes to the remote provider."""
        after = self.snapshot_local()
        if self.journal_path.exists():
            self.resume_pending_transaction()
            self._require_matching_baseline(before)
        else:
            self._require_matching_baseline(before)
        journal = self._new_journal(before, after)
        self._write_json(self.journal_path, journal)
        self._execute_journal(journal)

    def resume_pending_transaction(self) -> None:
        """Resume a durable ciphertext transaction without replacing local state."""
        if not self.journal_path.exists():
            return
        journal = self._read_json(self.journal_path)
        if (
            journal.get("schema") != self._STATE_SCHEMA
            or journal.get("remote_root") != self.remote_root
            or journal.get("remote_root_id") != self.remote_root_id
        ):
            raise EncryptedVaultError(
                "Encrypted transaction belongs to another vault",
                "Keep the ciphertext cache and restore its matching account settings.",
            )
        desired_value = journal.get("desired")
        if not isinstance(desired_value, dict):
            raise self._invalid_journal()
        desired = self._decode_local_entries(desired_value)
        current = self.snapshot_local(reuse_manifest=False)
        if not self._same_local_content(current, desired):
            raise EncryptedVaultError(
                "Encrypted transaction local state changed",
                "Restore the private ciphertext cache before another remote update.",
            )
        remote, _ = self._list_remote()
        self._remote = remote
        self._execute_journal(journal)

    def _new_journal(
        self,
        before: Mapping[str, _LocalPhysicalEntry],
        after: Mapping[str, _LocalPhysicalEntry],
    ) -> dict[str, object]:
        remote, _ = self._list_remote()
        self._require_same_remote_state(self._remote, remote)
        self._remote = remote
        transaction_id = uuid.uuid4().hex
        operations = self._build_operations(before, after, transaction_id)
        return {
            "schema": self._STATE_SCHEMA,
            "transaction_id": transaction_id,
            "remote_root": self.remote_root,
            "remote_root_id": self.remote_root_id,
            "baseline": self._encode_remote_entries(remote),
            "before": self._encode_local_entries(before),
            "desired": self._encode_local_entries(after),
            "operations": [asdict(operation) for operation in operations],
            "completed": 0,
            "started": None,
            "results": {},
        }

    def _build_operations(
        self,
        before: Mapping[str, _LocalPhysicalEntry],
        after: Mapping[str, _LocalPhysicalEntry],
        transaction_id: str,
    ) -> list[_PhysicalOperation]:
        operations: list[_PhysicalOperation] = []
        remote = {
            path: self._remote_state(metadata)
            for path, metadata in self._remote.items()
        }
        working_before = dict(before)
        backup_index = 0

        def drop_local_prefix(path: str) -> None:
            for candidate in tuple(working_before):
                if candidate == path or _is_below(candidate, path):
                    working_before.pop(candidate)

        def move_remote_prefix(source: str, target: str) -> None:
            moved = {
                _replace_prefix(path, source, target): entry
                for path, entry in remote.items()
                if path == source or _is_below(path, source)
            }
            for path in tuple(remote):
                if path == source or _is_below(path, source):
                    remote.pop(path)
            remote.update(moved)

        def quarantine(path: str) -> None:
            nonlocal backup_index
            entry = remote.get(path)
            if entry is None or entry.provider_id is None:
                raise EncryptedVaultError(
                    "Encrypted transaction cannot preserve a conflict",
                    "Refresh the ciphertext cache and try again.",
                )
            while True:
                backup_index += 1
                target = f"maestral-tx-{transaction_id}-{backup_index:04d}.c9r"
                if target not in remote and target not in after:
                    break
            operations.append(
                _PhysicalOperation(
                    "move",
                    target,
                    entry.kind,
                    source=path,
                    expected_id=entry.provider_id,
                    expected_rev=entry.rev,
                )
            )
            move_remote_prefix(path, target)
            drop_local_prefix(path)

        conflicts = sorted(
            (
                path
                for path, local in after.items()
                if path in remote and remote[path].kind != local.kind
            ),
            key=_path_depth,
        )
        conflict_roots: list[str] = []
        for path in conflicts:
            if not any(_is_below(path, parent) for parent in conflict_roots):
                conflict_roots.append(path)
                quarantine(path)

        def ensure_parent_chain(path: str) -> None:
            parent = posixpath.dirname(path)
            if not parent:
                return
            components = parent.split("/")
            for index in range(1, len(components) + 1):
                candidate = "/".join(components[:index])
                existing = remote.get(candidate)
                if existing is not None:
                    if existing.kind != "directory":
                        raise EncryptedVaultError(
                            "Encrypted transaction parent is not a folder",
                            "Refresh the ciphertext cache and try again.",
                        )
                    continue
                desired = after.get(candidate)
                if desired is None or desired.kind != "directory":
                    raise EncryptedVaultError(
                        "Encrypted transaction parent is missing",
                        "Restore the local ciphertext cache and try again.",
                    )
                operations.append(_PhysicalOperation("mkdir", candidate, "directory"))
                remote[candidate] = _RemotePhysicalEntry("directory", None, None)

        directory_moves = self._directory_moves(working_before, after)
        for source, target in directory_moves:
            blocker = remote.get(target)
            expected = remote.get(source)
            if blocker is not None and (
                expected is None or blocker.provider_id != expected.provider_id
            ):
                quarantine(target)
        directory_moves = self._directory_moves(working_before, after)
        for source, target in sorted(
            directory_moves, key=lambda move: _path_depth(move[1])
        ):
            expected = remote.get(source)
            if (
                expected is None
                or expected.kind != "directory"
                or expected.provider_id is None
            ):
                raise EncryptedVaultError(
                    "Encrypted transaction lost a folder move source",
                    "Refresh the ciphertext cache and try again.",
                )
            ensure_parent_chain(target)
            operations.append(
                _PhysicalOperation(
                    "move",
                    target,
                    "directory",
                    source=source,
                    expected_id=expected.provider_id,
                )
            )
            move_remote_prefix(source, target)

        working_before = self._transform_snapshot(working_before, directory_moves)
        file_moves = self._file_moves(working_before, after)
        for source, target in file_moves:
            blocker = remote.get(target)
            expected = remote.get(source)
            if blocker is not None and (
                expected is None or blocker.provider_id != expected.provider_id
            ):
                quarantine(target)
        file_moves = self._file_moves(working_before, after)
        for source, target in file_moves:
            expected = remote.get(source)
            if (
                expected is None
                or expected.kind != "file"
                or expected.provider_id is None
            ):
                raise EncryptedVaultError(
                    "Encrypted transaction lost a file move source",
                    "Refresh the ciphertext cache and try again.",
                )
            ensure_parent_chain(target)
            operations.append(
                _PhysicalOperation(
                    "move",
                    target,
                    "file",
                    source=source,
                    expected_id=expected.provider_id,
                    expected_rev=expected.rev,
                )
            )
            remote[target] = expected
            remote.pop(source)

        working_before = self._transform_snapshot(working_before, file_moves)
        for path, entry in sorted(after.items(), key=lambda item: _path_depth(item[0])):
            if entry.kind == "directory" and path not in remote:
                ensure_parent_chain(path)
                operations.append(_PhysicalOperation("mkdir", path, "directory"))
                remote[path] = _RemotePhysicalEntry("directory", None, None)

        for path, local_entry in sorted(after.items()):
            if local_entry.kind != "file":
                continue
            previous = working_before.get(path)
            current = remote.get(path)
            if current is None:
                expected_id = None
                expected_rev = None
            elif current.kind != "file":
                raise EncryptedVaultError(
                    "Encrypted transaction file target is a folder",
                    "Refresh the ciphertext cache and try again.",
                )
            elif previous is not None and previous.sha256 == local_entry.sha256:
                continue
            else:
                expected_id = current.provider_id
                expected_rev = current.rev
            ensure_parent_chain(path)
            operations.append(
                _PhysicalOperation(
                    "upload",
                    path,
                    "file",
                    source=path,
                    expected_id=expected_id,
                    expected_rev=expected_rev,
                    sha256=local_entry.sha256,
                )
            )
            remote[path] = _RemotePhysicalEntry("file", expected_id, None)

        stale_directories = sorted(
            (
                path
                for path, entry in remote.items()
                if path not in after and entry.kind == "directory"
            ),
            key=_path_depth,
        )
        directory_roots: list[str] = []
        for path in stale_directories:
            if not any(_is_below(path, root) for root in directory_roots):
                directory_roots.append(path)
        stale_files = [
            path
            for path, entry in remote.items()
            if path not in after
            and entry.kind == "file"
            and not any(_is_below(path, root) for root in directory_roots)
        ]
        for path in sorted(stale_files, key=_path_depth, reverse=True):
            remote_entry = remote[path]
            if remote_entry.provider_id is None:
                raise self._invalid_journal()
            operations.append(
                _PhysicalOperation(
                    "remove",
                    path,
                    "file",
                    expected_id=remote_entry.provider_id,
                    expected_rev=remote_entry.rev,
                )
            )
            remote.pop(path)
        for path in sorted(directory_roots, key=_path_depth, reverse=True):
            remote_entry = remote[path]
            if remote_entry.provider_id is None:
                raise self._invalid_journal()
            operations.append(
                _PhysicalOperation(
                    "remove",
                    path,
                    "directory",
                    expected_id=remote_entry.provider_id,
                )
            )
            for candidate in tuple(remote):
                if candidate == path or _is_below(candidate, path):
                    remote.pop(candidate)

        if set(remote) != set(after) or any(
            remote[path].kind != after[path].kind for path in after
        ):
            raise EncryptedVaultError(
                "Encrypted transaction plan is incomplete",
                "Keep the ciphertext cache and report this error.",
            )
        return operations

    def _execute_journal(self, journal: dict[str, object]) -> None:
        operations_value = journal.get("operations")
        completed = journal.get("completed")
        started = journal.get("started")
        results = journal.get("results")
        if (
            not isinstance(operations_value, list)
            or not isinstance(completed, int)
            or isinstance(completed, bool)
            or not 0 <= completed <= len(operations_value)
            or (started is not None and started != completed)
            or not isinstance(results, dict)
        ):
            raise self._invalid_journal()
        operations = [self._decode_operation(value) for value in operations_value]
        for index in range(completed, len(operations)):
            recovering = journal.get("started") == index
            if not recovering:
                journal["started"] = index
                self._write_json(self.journal_path, journal)
            result = self._execute_operation(operations[index], recovering=recovering)
            if result is not None:
                results[str(index)] = asdict(self._remote_state(result))
            journal["completed"] = index + 1
            journal["started"] = None
            self._write_json(self.journal_path, journal)

        desired_value = journal.get("desired")
        if not isinstance(desired_value, dict):
            raise self._invalid_journal()
        desired = self._decode_local_entries(desired_value)
        remote, _ = self._list_remote()
        if set(remote) != set(desired) or any(
            self._remote_state(remote[path]).kind != desired[path].kind
            for path in desired
        ):
            raise EncryptedVaultError(
                "Encrypted transaction did not converge",
                "Keep the ciphertext cache and retry the remote update.",
            )
        local = self.snapshot_local(reuse_manifest=False)
        if not self._same_local_content(local, desired):
            raise EncryptedVaultError(
                "Encrypted transaction local state changed",
                "Restore the private ciphertext cache before another remote update.",
            )
        self._remote = remote
        self._save_manifest(local, remote)
        self._unlink_durable(self.journal_path)

    def _execute_operation(
        self, operation: _PhysicalOperation, *, recovering: bool
    ) -> FileMetadata | FolderMetadata | None:
        target_path = self._remote_path(operation.target)
        target = self.provider.get_metadata(target_path)
        source = (
            self.provider.get_metadata(self._remote_path(operation.source))
            if operation.source is not None
            else None
        )

        if operation.kind == "move":
            if operation.source is None or operation.expected_id is None:
                raise self._invalid_journal()
            if recovering and source is None:
                self._require_expected_remote(target, operation)
                return cast(FileMetadata | FolderMetadata, target)
            self._require_expected_remote(source, operation)
            if target is not None:
                raise EncryptedVaultError(
                    "Encrypted transaction move target exists",
                    "Keep the ciphertext cache and resolve the remote conflict.",
                )
            moved = self.provider.move(
                self._remote_path(operation.source),
                target_path,
                expected_provider_id=operation.expected_id,
            )
            self._require_expected_remote(moved, operation)
            return moved

        if operation.kind == "mkdir":
            if recovering and isinstance(target, FolderMetadata):
                return target
            if target is not None:
                raise EncryptedVaultError(
                    "Encrypted transaction folder target exists",
                    "Keep the ciphertext cache and resolve the remote conflict.",
                )
            return self.provider.make_dir(target_path)

        if operation.kind == "upload":
            if operation.source is None or operation.sha256 is None:
                raise self._invalid_journal()
            if recovering and isinstance(target, FileMetadata):
                if (
                    operation.expected_id is not None
                    and target.id != operation.expected_id
                ):
                    raise EncryptedVaultError(
                        "Encrypted transaction file identity changed",
                        "Keep the ciphertext cache and resolve the remote conflict.",
                    )
                baseline_revision = operation.expected_rev
                if baseline_revision is None or target.rev != baseline_revision:
                    if self._remote_file_sha256(target) == operation.sha256:
                        return target
                    raise EncryptedVaultError(
                        "Encrypted transaction file content changed",
                        "Keep the ciphertext cache and resolve the remote conflict.",
                    )
            if operation.expected_id is None:
                if target is not None:
                    raise EncryptedVaultError(
                        "Encrypted transaction file target exists",
                        "Keep the ciphertext cache and resolve the remote conflict.",
                    )
                mode = WriteMode.Add
            else:
                self._require_expected_remote(target, operation)
                mode = WriteMode.Update
            source_path = self._local_physical_path(self.local_root, operation.source)
            return self.provider.upload(
                source_path,
                target_path,
                write_mode=mode,
                update_rev=operation.expected_rev,
                autorename=False,
            )

        if operation.kind == "remove":
            if operation.expected_id is None:
                raise self._invalid_journal()
            if recovering and target is None:
                return None
            self._require_expected_remote(target, operation)
            return self.provider.remove(
                target_path,
                parent_rev=operation.expected_rev,
                expected_provider_id=operation.expected_id,
            )

        raise self._invalid_journal()

    def _require_expected_remote(
        self, metadata: Metadata | None, operation: _PhysicalOperation
    ) -> None:
        expected_type = (
            FileMetadata if operation.expected_kind == "file" else FolderMetadata
        )
        if not isinstance(metadata, expected_type) or (
            operation.expected_id is not None and metadata.id != operation.expected_id
        ):
            raise EncryptedVaultError(
                "Encrypted transaction remote identity changed",
                "Keep the ciphertext cache and resolve the remote conflict.",
            )
        if (
            operation.expected_rev is not None
            and isinstance(metadata, FileMetadata)
            and metadata.rev != operation.expected_rev
        ):
            raise EncryptedVaultError(
                "Encrypted transaction remote revision changed",
                "Keep the ciphertext cache and resolve the remote conflict.",
            )

    def _remote_file_sha256(self, metadata: FileMetadata) -> str:
        parent = self.local_root.parent
        descriptor, name = tempfile.mkstemp(prefix=".maestral-verify-", dir=parent)
        os.close(descriptor)
        path = Path(name)
        try:
            downloaded = self.provider.download(
                metadata.path_display,
                str(path),
                rev=metadata.rev,
                provider_id=metadata.id,
            )
            if downloaded.id != metadata.id or downloaded.rev != metadata.rev:
                raise EncryptedVaultError(
                    "Encrypted transaction verification changed",
                    "Retry the remote update.",
                )
            return self._sha256(path)
        finally:
            path.unlink(missing_ok=True)

    @staticmethod
    def _remote_state(
        metadata: FileMetadata | FolderMetadata,
    ) -> _RemotePhysicalEntry:
        if isinstance(metadata, FileMetadata):
            return _RemotePhysicalEntry("file", metadata.id, metadata.rev)
        return _RemotePhysicalEntry("directory", metadata.id, None)

    def _require_same_remote_state(
        self,
        expected: Mapping[str, FileMetadata | FolderMetadata],
        current: Mapping[str, FileMetadata | FolderMetadata],
    ) -> None:
        if set(expected) != set(current) or any(
            self._remote_state(expected[path]) != self._remote_state(current[path])
            for path in expected
        ):
            raise EncryptedVaultError(
                "Encrypted vault changed before commit",
                "Refresh the ciphertext cache and try again.",
            )

    @staticmethod
    def _same_local_content(
        left: Mapping[str, _LocalPhysicalEntry],
        right: Mapping[str, _LocalPhysicalEntry],
    ) -> bool:
        return set(left) == set(right) and all(
            left[path].kind == right[path].kind
            and left[path].size == right[path].size
            and left[path].sha256 == right[path].sha256
            for path in left
        )

    def _save_manifest(
        self,
        local: Mapping[str, _LocalPhysicalEntry],
        remote: Mapping[str, FileMetadata | FolderMetadata],
    ) -> None:
        root_stat = os.lstat(self.local_root)
        manifest: dict[str, object] = {
            "schema": self._STATE_SCHEMA,
            "remote_root": self.remote_root,
            "remote_root_id": self.remote_root_id,
            "cache_identity": [root_stat.st_dev, root_stat.st_ino],
            "local": self._encode_local_entries(local),
            "remote": self._encode_remote_entries(remote),
        }
        self._write_json(self.manifest_path, manifest)
        self._manifest_local = dict(local)

    def _load_manifest_local(self) -> dict[str, _LocalPhysicalEntry]:
        if self._manifest_local:
            return self._manifest_local
        if not self.manifest_path.exists():
            return {}
        manifest = self._read_json(self.manifest_path)
        local_value = manifest.get("local")
        cache_identity = manifest.get("cache_identity")
        if (
            manifest.get("schema") != self._STATE_SCHEMA
            or manifest.get("remote_root") != self.remote_root
            or manifest.get("remote_root_id") != self.remote_root_id
            or not isinstance(local_value, dict)
            or not isinstance(cache_identity, list)
            or len(cache_identity) != 2
            or any(
                not isinstance(value, int) or isinstance(value, bool)
                for value in cache_identity
            )
        ):
            raise EncryptedVaultError(
                "Encrypted cache manifest does not match this vault",
                "Keep the ciphertext cache and restore its matching settings.",
            )
        root_stat = os.lstat(self.local_root)
        if cache_identity != [root_stat.st_dev, root_stat.st_ino]:
            return {}
        self._manifest_local = self._decode_local_entries(local_value)
        return self._manifest_local

    @staticmethod
    def _encode_local_entries(
        entries: Mapping[str, _LocalPhysicalEntry],
    ) -> dict[str, object]:
        return {
            path: {
                "kind": entry.kind,
                "identity": list(entry.identity),
                "size": entry.size,
                "modified_ns": entry.modified_ns,
                "sha256": entry.sha256,
            }
            for path, entry in sorted(entries.items())
        }

    def _decode_local_entries(
        self, entries: Mapping[str, object]
    ) -> dict[str, _LocalPhysicalEntry]:
        result: dict[str, _LocalPhysicalEntry] = {}
        for path, raw_entry in entries.items():
            if not isinstance(path, str) or not isinstance(raw_entry, dict):
                raise self._invalid_journal()
            self._remote_path(path)
            if set(raw_entry) != {
                "kind",
                "identity",
                "size",
                "modified_ns",
                "sha256",
            }:
                raise self._invalid_journal()
            kind = raw_entry["kind"]
            identity = raw_entry["identity"]
            size = raw_entry["size"]
            modified_ns = raw_entry["modified_ns"]
            digest = raw_entry["sha256"]
            if (
                kind not in {"file", "directory"}
                or not isinstance(identity, list)
                or len(identity) != 3
                or identity[2] != kind
                or any(
                    not isinstance(value, int) or isinstance(value, bool)
                    for value in identity[:2]
                )
                or not isinstance(size, int)
                or isinstance(size, bool)
                or size < 0
                or not isinstance(modified_ns, int)
                or isinstance(modified_ns, bool)
                or modified_ns < 0
                or (
                    kind == "file"
                    and (
                        not isinstance(digest, str)
                        or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                    )
                )
                or (kind == "directory" and digest is not None)
            ):
                raise self._invalid_journal()
            result[path] = _LocalPhysicalEntry(
                cast(_PhysicalKind, kind),
                (
                    cast(int, identity[0]),
                    cast(int, identity[1]),
                    cast(_PhysicalKind, kind),
                ),
                size,
                modified_ns,
                cast(str | None, digest),
            )
        return result

    @staticmethod
    def _encode_remote_entries(
        entries: Mapping[str, FileMetadata | FolderMetadata],
    ) -> dict[str, object]:
        return {
            path: asdict(PhysicalVaultMirror._remote_state(entry))
            for path, entry in sorted(entries.items())
        }

    def _decode_operation(self, value: object) -> _PhysicalOperation:
        if not isinstance(value, dict) or set(value) != {
            "kind",
            "target",
            "expected_kind",
            "source",
            "expected_id",
            "expected_rev",
            "sha256",
        }:
            raise self._invalid_journal()
        kind = value["kind"]
        target = value["target"]
        expected_kind = value["expected_kind"]
        source = value["source"]
        expected_id = value["expected_id"]
        expected_rev = value["expected_rev"]
        digest = value["sha256"]
        if (
            kind not in {"move", "mkdir", "upload", "remove"}
            or not isinstance(target, str)
            or expected_kind not in {"file", "directory"}
            or (source is not None and not isinstance(source, str))
            or (expected_id is not None and not isinstance(expected_id, str))
            or (expected_rev is not None and not isinstance(expected_rev, str))
            or (digest is not None and not isinstance(digest, str))
            or (digest is not None and re.fullmatch(r"[0-9a-f]{64}", digest) is None)
        ):
            raise self._invalid_journal()
        self._remote_path(target)
        if source is not None:
            self._remote_path(source)
        return _PhysicalOperation(
            cast(_PhysicalOperationKind, kind),
            target,
            cast(_PhysicalKind, expected_kind),
            source,
            expected_id,
            expected_rev,
            digest,
        )

    def _read_json(self, path: Path) -> dict[str, object]:
        try:
            file_stat = os.lstat(path)
            if (
                not stat.S_ISREG(file_stat.st_mode)
                or stat.S_ISLNK(file_stat.st_mode)
                or file_stat.st_nlink != 1
                or file_stat.st_size > 512 * 1024 * 1024
            ):
                raise self._invalid_journal()
            flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(path, flags)
            with os.fdopen(descriptor, "rb") as source:
                if os.fstat(source.fileno()) != file_stat:
                    raise self._invalid_journal()
                raw = source.read(512 * 1024 * 1024 + 1)
            value = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise self._invalid_journal() from exc
        if not isinstance(value, dict):
            raise self._invalid_journal()
        return cast(dict[str, object], value)

    def _write_json(self, path: Path, value: Mapping[str, object]) -> None:
        parent = path.parent
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        payload = json.dumps(
            value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.", dir=parent
        )
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as destination:
                destination.write(payload)
                destination.flush()
                os.fsync(destination.fileno())
            os.replace(temporary, path)
            os.chmod(path, 0o600)
            self._sync_directory(parent)
        except BaseException:
            try:
                os.close(descriptor)
            except OSError:
                pass
            temporary.unlink(missing_ok=True)
            raise

    def _unlink_durable(self, path: Path) -> None:
        path.unlink()
        self._sync_directory(path.parent)

    @staticmethod
    def _sync_directory(path: Path) -> None:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError:
            return
        try:
            os.fsync(descriptor)
        except OSError:
            pass
        finally:
            os.close(descriptor)

    @staticmethod
    def _invalid_journal() -> EncryptedVaultError:
        return EncryptedVaultError(
            "Encrypted transaction state is invalid",
            "Keep the ciphertext cache and restore its transaction journal.",
        )

    def _list_remote(
        self,
    ) -> tuple[dict[str, FileMetadata | FolderMetadata], str]:
        result: dict[str, FileMetadata | FolderMetadata] = {}
        normalised_paths: set[str] = set()
        cursor = ""
        for page in self.provider.list_folder_iterator(
            self.remote_root,
            recursive=True,
            include_deleted=False,
            include_mounted_folders=False,
            include_non_downloadable_files=False,
        ):
            cursor = page.cursor
            for metadata in page.entries:
                if not isinstance(metadata, (FileMetadata, FolderMetadata)):
                    raise EncryptedVaultError(
                        "Encrypted vault contains invalid remote data",
                        "The remote ciphertext listing has an unsupported entry.",
                    )
                if isinstance(metadata, FileMetadata) and (
                    metadata.symlink_target is not None or not metadata.is_downloadable
                ):
                    raise EncryptedVaultError(
                        "Encrypted vault contains invalid remote data",
                        "The remote ciphertext tree contains an unsupported file.",
                    )
                relative = self._relative_remote_path(metadata.path_display)
                key = normalize("/" + relative)
                if key in normalised_paths:
                    raise EncryptedVaultError(
                        "Encrypted vault contains colliding paths",
                        "Move the colliding ciphertext object and try again.",
                    )
                normalised_paths.add(key)
                result[relative] = metadata
        if not cursor:
            raise EncryptedVaultError(
                "Encrypted vault listing failed",
                "The remote provider returned no snapshot cursor.",
            )
        return result, cursor

    def _relative_remote_path(self, path: str) -> str:
        root_parts = PurePosixPath(self.remote_root).parts
        path_parts = PurePosixPath(path).parts
        if len(path_parts) <= len(root_parts) or any(
            normalize(left) != normalize(right)
            for left, right in zip(path_parts[: len(root_parts)], root_parts)
        ):
            raise EncryptedVaultError(
                "Encrypted vault contains invalid remote data",
                "A ciphertext item is outside the configured vault folder.",
            )
        relative_parts = path_parts[len(root_parts) :]
        if any(not self._safe_physical_component(part) for part in relative_parts):
            raise EncryptedVaultError(
                "Encrypted vault contains invalid remote data",
                "A ciphertext item has an unsafe path.",
            )
        return "/".join(relative_parts)

    @staticmethod
    def _safe_physical_component(component: str) -> bool:
        if (
            component in {"", ".", ".."}
            or "\\" in component
            or ":" in component
            or "\x00" in component
            or component.endswith((".", " "))
        ):
            return False
        windows_stem = component.split(".", 1)[0].upper()
        return windows_stem not in _WINDOWS_RESERVED_NAMES

    @staticmethod
    def _local_physical_path(root: Path, relative: str) -> Path:
        components = relative.split("/")
        if any(not PhysicalVaultMirror._safe_physical_component(c) for c in components):
            raise EncryptedVaultError(
                "Encrypted vault contains invalid remote data",
                "A ciphertext item has an unsafe local path.",
            )
        target = root.joinpath(*components)
        resolved_root = root.resolve(strict=True)
        resolved_target = target.resolve(strict=False)
        try:
            resolved_target.relative_to(resolved_root)
        except ValueError:
            raise EncryptedVaultError(
                "Encrypted vault contains invalid remote data",
                "A ciphertext item escapes the private cache.",
            ) from None
        return target

    def _remote_path(self, relative: str) -> str:
        if (
            not relative
            or relative.startswith("/")
            or any(
                not self._safe_physical_component(component)
                for component in relative.split("/")
            )
        ):
            raise ValueError("Invalid relative ciphertext path")
        return self.remote_root + "/" + relative

    def _materialise(self, remote: Mapping[str, FileMetadata | FolderMetadata]) -> None:
        self.claim_local_root(allow_nonempty=False)
        parent = self.local_root.parent
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if parent.is_symlink():
            raise EncryptedVaultError(
                "Encrypted vault mirror is unsafe",
                "The ciphertext cache parent cannot be a symbolic link.",
            )
        candidate = Path(
            tempfile.mkdtemp(prefix=".maestral-vault-download-", dir=parent)
        )
        os.chmod(candidate, 0o700)
        installed = False
        try:
            directories = sorted(
                (
                    path
                    for path, metadata in remote.items()
                    if isinstance(metadata, FolderMetadata)
                ),
                key=_path_depth,
            )
            for relative in directories:
                target = self._local_physical_path(candidate, relative)
                target.mkdir(mode=0o700)

            for relative, metadata in sorted(remote.items()):
                if not isinstance(metadata, FileMetadata):
                    continue
                target = self._local_physical_path(candidate, relative)
                if not target.parent.is_dir():
                    raise EncryptedVaultError(
                        "Encrypted vault contains invalid remote data",
                        "A ciphertext file has no listed parent folder.",
                    )
                downloaded = self.provider.download(
                    metadata.path_display,
                    str(target),
                    rev=metadata.rev,
                    provider_id=metadata.id,
                )
                if downloaded.id != metadata.id or downloaded.rev != metadata.rev:
                    raise EncryptedVaultError(
                        "Encrypted vault changed during download",
                        "Retry the encrypted vault refresh.",
                    )
                target_stat = os.lstat(target)
                if (
                    not stat.S_ISREG(target_stat.st_mode)
                    or stat.S_ISLNK(target_stat.st_mode)
                    or target_stat.st_nlink != 1
                ):
                    raise EncryptedVaultError(
                        "Encrypted vault contains invalid remote data",
                        "A ciphertext download created an unsafe local object.",
                    )
                os.chmod(target, 0o600)

            self._replace_local_root(candidate)
            installed = True
        finally:
            if not installed:
                shutil.rmtree(candidate, ignore_errors=True)

    def _replace_local_root(self, candidate: Path) -> None:
        backup = self.local_root.with_name(
            f".{self.local_root.name}.old-{uuid.uuid4().hex}"
        )
        had_root = self.local_root.exists()
        if self.local_root.is_symlink():
            raise EncryptedVaultError(
                "Encrypted vault mirror is unsafe",
                "The ciphertext cache root cannot be a symbolic link.",
            )
        if had_root:
            os.replace(self.local_root, backup)
        try:
            os.replace(candidate, self.local_root)
        except BaseException:
            if had_root:
                os.replace(backup, self.local_root)
            raise
        if had_root:
            shutil.rmtree(backup)

    def _require_matching_baseline(
        self, before: Mapping[str, _LocalPhysicalEntry]
    ) -> None:
        if set(before) != set(self._remote):
            raise EncryptedVaultError(
                "Encrypted vault mirror is stale",
                "Refresh the remote ciphertext before the next change.",
            )
        for path, local in before.items():
            remote = self._remote[path]
            if local.kind == "file" and not isinstance(remote, FileMetadata):
                break
            if local.kind == "directory" and not isinstance(remote, FolderMetadata):
                break
        else:
            return
        raise EncryptedVaultError(
            "Encrypted vault mirror is stale",
            "A local ciphertext object changed type.",
        )

    def _remove_type_conflicts(
        self,
        after: Mapping[str, _LocalPhysicalEntry],
        remote: dict[str, FileMetadata | FolderMetadata],
    ) -> None:
        conflicts = [
            path
            for path, local in after.items()
            if path in remote
            and (
                (local.kind == "file" and isinstance(remote[path], FolderMetadata))
                or (
                    local.kind == "directory" and isinstance(remote[path], FileMetadata)
                )
            )
        ]
        for path in sorted(conflicts, key=_path_depth, reverse=True):
            metadata = remote[path]
            parent_rev = metadata.rev if isinstance(metadata, FileMetadata) else None
            self.provider.remove(
                self._remote_path(path),
                parent_rev=parent_rev,
                expected_provider_id=metadata.id,
            )
            self._drop_remote_prefix(remote, path)

    @staticmethod
    def _directory_moves(
        before: Mapping[str, _LocalPhysicalEntry],
        after: Mapping[str, _LocalPhysicalEntry],
    ) -> list[tuple[str, str]]:
        removed: dict[tuple[int, int, _PhysicalKind], list[str]] = {}
        added: dict[tuple[int, int, _PhysicalKind], list[str]] = {}
        for path, entry in before.items():
            if entry.kind == "directory" and path not in after:
                removed.setdefault(entry.identity, []).append(path)
        for path, entry in after.items():
            if entry.kind == "directory" and path not in before:
                added.setdefault(entry.identity, []).append(path)
        candidates = sorted(
            (
                (sources[0], added[identity][0])
                for identity, sources in removed.items()
                if len(sources) == 1 and len(added.get(identity, [])) == 1
            ),
            key=lambda move: _path_depth(move[0]),
        )
        moves: list[tuple[str, str]] = []
        for source, target in candidates:
            if any(
                _is_below(source, parent_source)
                and target == parent_target + source[len(parent_source) :]
                for parent_source, parent_target in moves
            ):
                continue
            moves.append((source, target))
        return moves

    @staticmethod
    def _file_moves(
        before: Mapping[str, _LocalPhysicalEntry],
        after: Mapping[str, _LocalPhysicalEntry],
    ) -> list[tuple[str, str]]:
        removed: dict[tuple[int, int, _PhysicalKind], list[str]] = {}
        added: dict[tuple[int, int, _PhysicalKind], list[str]] = {}
        for path, entry in before.items():
            if entry.kind == "file" and path not in after:
                removed.setdefault(entry.identity, []).append(path)
        for path, entry in after.items():
            if entry.kind == "file" and path not in before:
                added.setdefault(entry.identity, []).append(path)
        moves = [
            (sources[0], added[identity][0])
            for identity, sources in removed.items()
            if len(sources) == 1 and len(added.get(identity, [])) == 1
        ]

        moved_sources = {source for source, _ in moves}
        moved_targets = {target for _, target in moves}
        removed_by_content: dict[tuple[int, str | None], list[str]] = {}
        added_by_content: dict[tuple[int, str | None], list[str]] = {}
        for path, entry in before.items():
            if entry.kind == "file" and path not in after and path not in moved_sources:
                removed_by_content.setdefault((entry.size, entry.sha256), []).append(
                    path
                )
        for path, entry in after.items():
            if (
                entry.kind == "file"
                and path not in before
                and path not in moved_targets
            ):
                added_by_content.setdefault((entry.size, entry.sha256), []).append(path)
        for content, sources in removed_by_content.items():
            targets = added_by_content.get(content, [])
            if len(sources) == len(targets) == 1:
                moves.append((sources[0], targets[0]))
        return sorted(moves)

    def _apply_directory_moves(
        self,
        moves: list[tuple[str, str]],
        after: Mapping[str, _LocalPhysicalEntry],
        remote: dict[str, FileMetadata | FolderMetadata],
    ) -> None:
        targets = {target for _, target in moves}
        for source, target in sorted(moves, key=lambda move: _path_depth(move[1])):
            expected = remote.get(source)
            if not isinstance(expected, FolderMetadata):
                raise EncryptedVaultError(
                    "Encrypted vault mirror is stale",
                    "A ciphertext folder move has no matching remote source.",
                )
            self._require_current_remote(source, expected)
            parent = posixpath.dirname(target)
            if parent and parent not in remote and parent not in targets:
                self._create_parent_chain(parent, after, remote)
            moved = self.provider.move(
                self._remote_path(source),
                self._remote_path(target),
                expected_provider_id=expected.id,
            )
            if not isinstance(moved, FolderMetadata):
                raise EncryptedVaultError(
                    "Encrypted vault update failed",
                    "A ciphertext folder move returned invalid metadata.",
                )
            if moved.id != expected.id:
                self._restore_unexpected_move(source, target, moved)
                raise EncryptedVaultError(
                    "Encrypted vault changed during update",
                    "Retry the ciphertext folder move.",
                )
            self._move_remote_prefix(remote, source, target, moved)

    def _apply_file_moves(
        self,
        moves: list[tuple[str, str]],
        remote: dict[str, FileMetadata | FolderMetadata],
    ) -> None:
        for source, target in moves:
            expected = remote.get(source)
            if not isinstance(expected, FileMetadata):
                raise EncryptedVaultError(
                    "Encrypted vault mirror is stale",
                    "A ciphertext file move has no matching remote source.",
                )
            self._require_current_remote(source, expected)
            moved = self.provider.move(
                self._remote_path(source),
                self._remote_path(target),
                expected_provider_id=expected.id,
            )
            if not isinstance(moved, FileMetadata):
                raise EncryptedVaultError(
                    "Encrypted vault update failed",
                    "A ciphertext file move returned invalid metadata.",
                )
            if moved.id != expected.id:
                self._restore_unexpected_move(source, target, moved)
                raise EncryptedVaultError(
                    "Encrypted vault changed during update",
                    "Retry the ciphertext file move.",
                )
            remote.pop(source)
            remote[target] = moved

    def _require_current_remote(
        self, relative: str, expected: FileMetadata | FolderMetadata
    ) -> None:
        current = self.provider.get_metadata(self._remote_path(relative))
        matches = type(current) is type(expected) and current.id == expected.id
        if (
            matches
            and isinstance(current, FileMetadata)
            and isinstance(expected, FileMetadata)
        ):
            matches = current.rev == expected.rev
        if not matches:
            raise EncryptedVaultError(
                "Encrypted vault changed during update",
                "Refresh the ciphertext mirror and try again.",
            )

    def _restore_unexpected_move(
        self,
        source: str,
        target: str,
        moved: FileMetadata | FolderMetadata,
    ) -> None:
        source_path = self._remote_path(source)
        target_path = self._remote_path(target)
        current_source = self.provider.get_metadata(source_path)
        current_target = self.provider.get_metadata(target_path)
        if current_source is not None or current_target is None:
            return
        if type(current_target) is not type(moved) or current_target.id != moved.id:
            return
        restored = self.provider.move(
            target_path,
            source_path,
            expected_provider_id=moved.id,
        )
        if type(restored) is not type(moved) or restored.id != moved.id:
            raise EncryptedVaultError(
                "Encrypted vault recovery failed",
                "Inspect the remote ciphertext folder before another sync.",
            )

    def _create_parent_chain(
        self,
        relative: str,
        after: Mapping[str, _LocalPhysicalEntry],
        remote: dict[str, FileMetadata | FolderMetadata],
    ) -> None:
        components = relative.split("/")
        for index in range(1, len(components) + 1):
            parent = "/".join(components[:index])
            if parent in remote:
                continue
            entry = after.get(parent)
            if entry is None or entry.kind != "directory":
                raise EncryptedVaultError(
                    "Encrypted vault update failed",
                    "A ciphertext destination parent is missing.",
                )
            remote[parent] = self.provider.make_dir(self._remote_path(parent))

    def _create_directories(
        self,
        after: Mapping[str, _LocalPhysicalEntry],
        remote: dict[str, FileMetadata | FolderMetadata],
    ) -> None:
        for path, entry in sorted(after.items(), key=lambda item: _path_depth(item[0])):
            if entry.kind == "directory" and path not in remote:
                remote[path] = self.provider.make_dir(self._remote_path(path))

    def _upload_files(
        self,
        before: Mapping[str, _LocalPhysicalEntry],
        after: Mapping[str, _LocalPhysicalEntry],
        remote: dict[str, FileMetadata | FolderMetadata],
    ) -> None:
        for path, entry in sorted(after.items()):
            if entry.kind != "file":
                continue
            previous = before.get(path)
            metadata = remote.get(path)
            if metadata is None:
                mode = WriteMode.Add
                revision = None
            elif not isinstance(metadata, FileMetadata):
                raise EncryptedVaultError(
                    "Encrypted vault update failed",
                    "A ciphertext upload destination is not a file.",
                )
            elif previous is not None and previous.sha256 == entry.sha256:
                continue
            else:
                mode = WriteMode.Update
                revision = metadata.rev

            source = self.local_root.joinpath(*path.split("/"))
            uploaded = self.provider.upload(
                source,
                self._remote_path(path),
                write_mode=mode,
                update_rev=revision,
                autorename=False,
            )
            remote[path] = uploaded

    def _remove_stale(
        self,
        after: Mapping[str, _LocalPhysicalEntry],
        remote: dict[str, FileMetadata | FolderMetadata],
    ) -> None:
        stale_directories = sorted(
            (
                path
                for path, metadata in remote.items()
                if path not in after and isinstance(metadata, FolderMetadata)
            ),
            key=_path_depth,
        )
        directory_roots: list[str] = []
        for path in stale_directories:
            if not any(_is_below(path, root) for root in directory_roots):
                directory_roots.append(path)

        stale_files = [
            path
            for path, metadata in remote.items()
            if path not in after
            and isinstance(metadata, FileMetadata)
            and not any(_is_below(path, root) for root in directory_roots)
        ]
        for path in sorted(stale_files, key=_path_depth, reverse=True):
            metadata = cast(FileMetadata, remote[path])
            self.provider.remove(
                self._remote_path(path),
                parent_rev=metadata.rev,
                expected_provider_id=metadata.id,
            )
            remote.pop(path)
        for path in sorted(directory_roots, key=_path_depth, reverse=True):
            folder_metadata = remote[path]
            self.provider.remove(
                self._remote_path(path), expected_provider_id=folder_metadata.id
            )
            self._drop_remote_prefix(remote, path)

    @staticmethod
    def _transform_snapshot(
        snapshot: Mapping[str, _LocalPhysicalEntry],
        moves: list[tuple[str, str]],
    ) -> dict[str, _LocalPhysicalEntry]:
        result = dict(snapshot)
        for source, target in moves:
            result = {
                _replace_prefix(path, source, target): entry
                for path, entry in result.items()
            }
        return result

    @staticmethod
    def _move_remote_prefix(
        remote: dict[str, FileMetadata | FolderMetadata],
        source: str,
        target: str,
        root_metadata: FolderMetadata,
    ) -> None:
        moved = {
            _replace_prefix(path, source, target): metadata
            for path, metadata in remote.items()
            if path == source or _is_below(path, source)
        }
        PhysicalVaultMirror._drop_remote_prefix(remote, source)
        remote.update(moved)
        remote[target] = root_metadata

    @staticmethod
    def _drop_remote_prefix(
        remote: dict[str, FileMetadata | FolderMetadata], path: str
    ) -> None:
        for candidate in tuple(remote):
            if candidate == path or _is_below(candidate, path):
                remote.pop(candidate)

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as file:
            while chunk := file.read(1024 * 1024):
                digest.update(chunk)
        return digest.hexdigest()


class EncryptedRemoteProvider:
    """Expose an official Cryptomator vault as a logical remote provider."""

    content_hasher_factory: ContentHasherFactory = sha256_content_hasher
    _CURSOR_PREFIX = "cryptomator-v1:"

    def __init__(
        self,
        provider: RemoteProvider,
        secret_store: VaultSecretStore,
        remote_root: str,
        local_root: str | os.PathLike[str],
        *,
        sidecar: CryptomatorSession,
    ) -> None:
        self._provider = provider
        self._secret_store = secret_store
        self._sidecar = sidecar
        self._mirror = PhysicalVaultMirror(provider, remote_root, local_root)
        self._lock = threading.RLock()
        self._secret: bytearray | None = None
        self._vault_info: VaultInfo | None = None
        self._metadata_by_path: dict[str, FileMetadata | FolderMetadata] = {}
        self._metadata_by_id: dict[str, FileMetadata | FolderMetadata] = {}
        self._cursor = ""
        self._state: _VaultState = "locked"
        self._closed = False

        self.config_name = provider.config_name
        self.provider_id = f"cryptomator_{provider.provider_id}"
        self.api_url = provider.api_url

    @property
    def linked(self) -> bool:
        return self._provider.linked

    @property
    def account_info(self) -> FullAccount:
        return self._provider.account_info

    @property
    def namespace_id(self) -> str:
        return self._provider.namespace_id

    @property
    def is_team_space(self) -> bool:
        return self._provider.is_team_space

    @property
    def bandwidth_limit_up(self) -> float:
        return self._provider.bandwidth_limit_up

    @bandwidth_limit_up.setter
    def bandwidth_limit_up(self, value: float) -> None:
        self._provider.bandwidth_limit_up = value

    @property
    def bandwidth_limit_down(self) -> float:
        return self._provider.bandwidth_limit_down

    @bandwidth_limit_down.setter
    def bandwidth_limit_down(self, value: float) -> None:
        self._provider.bandwidth_limit_down = value

    @property
    def vault_open(self) -> bool:
        return self._state == "open"

    @property
    def vault_path(self) -> str:
        return self._mirror.remote_root

    def get_auth_url(self) -> str:
        return self._provider.get_auth_url()

    def link(
        self,
        code: str | None = None,
        refresh_token: str | None = None,
        access_token: str | None = None,
        allow_plaintext_keyring: bool = False,
    ) -> int:
        return self._provider.link(
            code,
            refresh_token,
            access_token,
            allow_plaintext_keyring,
        )

    def unlink(self) -> None:
        self.lock_vault()
        self._provider.unlink()

    @overload
    def get_account_info(self, dbid: None = None) -> FullAccount: ...

    @overload
    def get_account_info(self, dbid: str) -> Account: ...

    def get_account_info(self, dbid: str | None = None) -> Account:
        return self._provider.get_account_info(dbid)

    def get_space_usage(self) -> PersonalSpaceUsage:
        return self._provider.get_space_usage()

    def update_path_root(self, root_info: RootInfo) -> None:
        with self._lock:
            if self._state == "open":
                raise EncryptedVaultError(
                    "Cannot change the remote account root",
                    "Lock the encrypted vault before this account change.",
                )
            self._provider.update_path_root(root_info)

    def initialise_vault(self, secret: str) -> None:
        """Create a new format-eight vault in an empty remote folder."""
        secret_bytes = self._encode_secret(secret)
        adopted = False
        with self._lock:
            try:
                self._require_open_provider()
                self._require_locked_state()
            except BaseException:
                self._wipe(secret_bytes)
                raise
            try:
                self._prepare_local_root_for_initialisation()
                self._state = "opening"
                self._vault_info = self._sidecar.initialize(
                    self._mirror.local_root, secret_bytes
                )
                self._state = "closing"
                self._sidecar.close_vault()
                self._state = "locked"
                self._vault_info = None
                self._secret_store.save_password(secret)
                self._mirror.initialise_remote()
                self._open_with_secret(secret_bytes)
                adopted = True
            except BaseException:
                if self._state != "locked":
                    self._abort_sidecar(secret_bytes)
                else:
                    self._wipe(secret_bytes)
                raise
            finally:
                if not adopted:
                    self._wipe(secret_bytes)

    def attach_vault(self, secret: str) -> None:
        """Verify and save the password for an existing remote vault."""
        secret_bytes = self._encode_secret(secret)
        adopted = False
        with self._lock:
            try:
                self._require_open_provider()
                self._require_locked_state()
            except BaseException:
                self._wipe(secret_bytes)
                raise
            try:
                self._cursor = self._mirror.refresh()
                self._open_with_secret(secret_bytes)
                self._secret_store.save_password(secret)
                adopted = True
            except BaseException:
                if self._state != "locked":
                    self._abort_sidecar(secret_bytes)
                raise
            finally:
                if not adopted:
                    self._wipe(secret_bytes)

    def unlock_vault(self) -> None:
        """Load the saved password and open the configured remote vault."""
        with self._lock:
            if self._state == "open":
                return
            self._require_open_provider()
            self._require_locked_state()
            secret = self._secret_store.load_password()
            if secret is None:
                raise EncryptedVaultError(
                    "Encrypted vault is locked",
                    "Save the vault password in the system keyring and try again.",
                )
            secret_bytes = self._encode_secret(secret)
            adopted = False
            try:
                self._cursor = self._mirror.refresh()
                self._open_with_secret(secret_bytes)
                adopted = True
            except BaseException:
                if self._state != "locked":
                    self._abort_sidecar(secret_bytes)
                raise
            finally:
                if not adopted:
                    self._wipe(secret_bytes)

    def lock_vault(self) -> None:
        with self._lock:
            if self._state == "open":
                self._state = "closing"
                try:
                    self._sidecar.close_vault()
                except BaseException:
                    self._abort_sidecar()
                    raise
                self._state = "locked"
            self._clear_runtime_state()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            first_error: BaseException | None = None
            try:
                self.lock_vault()
            except BaseException as exc:
                first_error = exc
            try:
                self._sidecar.shutdown()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
            try:
                self._provider.close()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
            self._clear_runtime_state()
            self._closed = True
            self._state = "closed"
            if first_error is not None:
                raise first_error

    def __enter__(self) -> EncryptedRemoteProvider:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def get_metadata(
        self, remote_path: str, include_deleted: bool = False
    ) -> Metadata | None:
        del include_deleted
        with self._lock:
            self._ensure_loaded()
            path = self._normalise_logical_path(remote_path)
            if path == "/":
                return self._root_metadata()
            return self._metadata_by_path.get(normalize(path))

    def list_folder(
        self,
        remote_path: str,
        recursive: bool = False,
        include_deleted: bool = False,
        include_mounted_folders: bool = True,
        include_non_downloadable_files: bool = False,
    ) -> ListFolderResult:
        return next(
            self.list_folder_iterator(
                remote_path,
                recursive,
                include_deleted,
                include_mounted_folders,
                include_non_downloadable_files=include_non_downloadable_files,
            )
        )

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
        with self._lock:
            self._ensure_loaded()
            self._refresh_locked()
            path = self._normalise_logical_path(remote_path)
            entries = self._listed_metadata(path, recursive)
            result = ListFolderResult(entries, False, self._encode_cursor(self._cursor))
        yield result

    def list_remote_changes_iterator(
        self,
        last_cursor: str,
        indexed_paths: Mapping[str, str] | None = None,
    ) -> Iterator[ListFolderResult]:
        with self._lock:
            self._ensure_loaded()
            decoded_cursor = self._decode_cursor(last_cursor)
            if decoded_cursor is None:
                self._refresh_locked()
                result = self._logical_change_page(indexed_paths)
            else:
                final_cursor = decoded_cursor
                saw_entries = False
                physical_paths = {
                    metadata.id: self._mirror._remote_path(relative)
                    for relative, metadata in self._mirror.remote_entries.items()
                }
                for page in self._provider.list_remote_changes_iterator(
                    decoded_cursor, indexed_paths=physical_paths
                ):
                    final_cursor = page.cursor
                    saw_entries = saw_entries or bool(page.entries)

                if final_cursor == decoded_cursor and not saw_entries:
                    result = ListFolderResult([], False, last_cursor)
                else:
                    self._refresh_locked()
                    result = self._logical_change_page(indexed_paths)
        yield result

    def wait_for_remote_changes(self, last_cursor: str, timeout: int = 40) -> bool:
        with self._lock:
            self._ensure_loaded()
            decoded_cursor = self._decode_cursor(last_cursor)
        if decoded_cursor is None:
            return True
        return self._provider.wait_for_remote_changes(decoded_cursor, timeout)

    def download(
        self,
        remote_path: str,
        local_path: str | BinaryIO,
        sync_event: SyncEvent | None = None,
        *,
        rev: str | None = None,
        provider_id: str | None = None,
    ) -> FileMetadata:
        with self._lock:
            self._ensure_loaded()
            metadata = self._file_for_request(remote_path, provider_id)
            if rev is not None and rev != metadata.rev:
                raise DataChangedError(
                    "Encrypted file changed",
                    "Retry the encrypted download.",
                    dbx_path=remote_path,
                )
            with self._temporary_plaintext_file("download") as staged_path:
                self._sidecar.get_file(metadata.path_display, staged_path, replace=True)
                digest = hashlib.sha256()
                completed = 0
                with staged_path.open("rb") as staged_file:
                    while chunk := staged_file.read(1024 * 1024):
                        digest.update(chunk)
                        completed += len(chunk)
                        if sync_event is not None:
                            sync_event.completed = completed
                if digest.hexdigest() != metadata.content_hash:
                    raise DataCorruptionError(
                        "Encrypted download is corrupt",
                        "Retry the encrypted download.",
                        dbx_path=metadata.path_display,
                    )
                if isinstance(local_path, str):
                    self._install_verified_download(staged_path, Path(local_path))
                    modified = metadata.client_modified.timestamp()
                    os.utime(local_path, (time.time(), modified))
                else:
                    local_path.seek(0)
                    local_path.truncate()
                    with staged_path.open("rb") as staged_file:
                        shutil.copyfileobj(staged_file, local_path, 1024 * 1024)
                    local_path.seek(0)
            return metadata

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
    ) -> FileMetadata:
        del local_path
        with self._lock:
            self._ensure_loaded()
            target = self._normalise_logical_path(remote_path)
            existing = self._metadata_by_path.get(normalize(target))
            if write_mode is WriteMode.Add and existing is not None:
                if not autorename:
                    raise FileConflictError("File already exists", dbx_path=target)
                target = self._autorename_path(target)
                existing = None
            if write_mode is WriteMode.Update:
                if not isinstance(existing, FileMetadata) or existing.rev != update_rev:
                    raise FileConflictError("Encrypted file changed", dbx_path=target)
            if isinstance(existing, FolderMetadata):
                raise IsAFolderError("Cannot upload over folder", dbx_path=target)

            with self._prepared_upload_source(local_file) as prepared:
                source_path, digest, modified_ms, size = prepared

                def operation() -> None:
                    self._sidecar.put_file(
                        target,
                        source_path,
                        modified_ms=modified_ms,
                        replace=isinstance(existing, FileMetadata),
                    )

                self._mutate_locked(operation)
            metadata = self._metadata_by_path.get(normalize(target))
            if not isinstance(metadata, FileMetadata):
                raise EncryptedVaultError(
                    "Encrypted upload failed",
                    "The uploaded logical file is missing from the vault.",
                )
            if metadata.content_hash != digest or metadata.size != size:
                raise DataChangedError(
                    "File changed during encrypted upload",
                    local_path=(
                        os.fspath(local_file)
                        if isinstance(local_file, (str, os.PathLike))
                        else None
                    ),
                )
            if sync_event is not None:
                sync_event.completed = size
            return metadata

    def make_dir(self, remote_path: str, autorename: bool = False) -> FolderMetadata:
        with self._lock:
            self._ensure_loaded()
            target = self._normalise_logical_path(remote_path)
            existing = self._metadata_by_path.get(normalize(target))
            if existing is not None:
                if not autorename:
                    raise FolderConflictError("Folder already exists", dbx_path=target)
                target = self._autorename_path(target)
            self._require_parent_directory(target)
            self._mutate_locked(lambda: self._sidecar.mkdir(target))
            metadata = self._metadata_by_path.get(normalize(target))
            if not isinstance(metadata, FolderMetadata):
                raise EncryptedVaultError(
                    "Encrypted folder creation failed",
                    "The new logical folder is missing from the vault.",
                )
            return metadata

    def move(
        self,
        remote_path: str,
        new_path: str,
        autorename: bool = False,
        *,
        expected_provider_id: str,
    ) -> FileMetadata | FolderMetadata:
        with self._lock:
            self._ensure_loaded()
            source = self._normalise_logical_path(remote_path)
            target = self._normalise_logical_path(new_path)
            metadata = self._metadata_by_path.get(normalize(source))
            if metadata is None or metadata.id != expected_provider_id:
                raise NotFoundError("Encrypted item not found", dbx_path=source)
            if source == target:
                return metadata
            destination = (
                None
                if normalize(source) == normalize(target)
                else self._metadata_by_path.get(normalize(target))
            )
            if destination is not None:
                if not autorename:
                    raise (
                        FolderConflictError("Folder already exists", dbx_path=target)
                        if isinstance(destination, FolderMetadata)
                        else FileConflictError("File already exists", dbx_path=target)
                    )
                target = self._autorename_path(target)
            self._require_parent_directory(target)
            self._mutate_locked(lambda: self._sidecar.move(source, target))
            moved = self._metadata_by_path.get(normalize(target))
            if not isinstance(moved, (FileMetadata, FolderMetadata)):
                raise EncryptedVaultError(
                    "Encrypted move failed",
                    "The moved logical item is missing from the vault.",
                )
            return moved

    def remove(
        self,
        remote_path: str,
        parent_rev: str | None = None,
        *,
        expected_provider_id: str,
    ) -> FileMetadata | FolderMetadata:
        with self._lock:
            self._ensure_loaded()
            path = self._normalise_logical_path(remote_path)
            metadata = self._metadata_by_path.get(normalize(path))
            if metadata is None or metadata.id != expected_provider_id:
                raise NotFoundError("Encrypted item not found", dbx_path=path)
            if (
                parent_rev is not None
                and isinstance(metadata, FileMetadata)
                and metadata.rev != parent_rev
            ):
                raise FileConflictError("Encrypted item changed", dbx_path=path)
            self._mutate_locked(lambda: self._sidecar.delete(path, recursive=True))
            return metadata

    def share_dir(
        self, remote_path: str, force_async: bool = False
    ) -> FolderMetadata | None:
        del remote_path, force_async
        raise self._unsupported("shared folders")

    def list_revisions(
        self, remote_path: str, mode: str = "path", limit: int = 10
    ) -> list[FileMetadata]:
        del remote_path, mode, limit
        raise self._unsupported("revision history")

    def restore(self, remote_path: str, rev: str) -> FileMetadata:
        del remote_path, rev
        raise self._unsupported("revision restore")

    def create_shared_link(
        self,
        remote_path: str,
        visibility: LinkAudience = LinkAudience.Public,
        access_level: LinkAccessLevel = LinkAccessLevel.Viewer,
        allow_download: bool | None = None,
        password: str | None = None,
        expires: datetime | None = None,
    ) -> SharedLinkMetadata:
        del remote_path, visibility, access_level, allow_download, password, expires
        raise self._unsupported("shared links")

    def revoke_shared_link(self, url: str) -> None:
        del url
        raise self._unsupported("shared links")

    def list_shared_links(
        self, remote_path: str | None = None, *, direct_only: bool = False
    ) -> list[SharedLinkMetadata]:
        del remote_path, direct_only
        raise self._unsupported("shared links")

    def _ensure_loaded(self) -> None:
        if self._state != "open":
            self.unlock_vault()

    def _require_open_provider(self) -> None:
        if self._closed or self._state == "closed":
            raise EncryptedVaultError(
                "Encrypted provider is closed", "Start a new provider session."
            )
        if self._state == "failed":
            raise EncryptedVaultError(
                "Encrypted sidecar session failed",
                "Start a new provider session before another vault operation.",
            )
        if not self._provider.linked:
            raise EncryptedVaultError(
                "Remote account is not linked", "Link the remote account first."
            )

    def _require_locked_state(self) -> None:
        if self._state == "open":
            raise EncryptedVaultError(
                "Encrypted vault is already open",
                "Lock the current vault before opening another vault.",
            )
        if self._state != "locked":
            raise EncryptedVaultError(
                "Encrypted vault session is unavailable",
                "Start a new provider session before another vault operation.",
            )

    def _open_with_secret(self, secret: bytearray) -> None:
        self._require_locked_state()
        self._state = "opening"
        self._vault_info = self._sidecar.open(self._mirror.local_root, secret)
        self._rebuild_logical_metadata()
        self._secret = secret
        self._state = "open"

    def _close_for_transition(self) -> bytearray:
        secret = self._require_secret()
        if self._state != "open":
            raise EncryptedVaultError(
                "Encrypted vault is not open", "Unlock the vault and try again."
            )
        self._state = "closing"
        try:
            self._sidecar.close_vault()
        except BaseException:
            self._abort_sidecar(secret)
            raise
        self._state = "locked"
        self._vault_info = None
        self._metadata_by_path.clear()
        self._metadata_by_id.clear()
        return secret

    def _clear_runtime_state(self) -> None:
        self._metadata_by_path.clear()
        self._metadata_by_id.clear()
        self._vault_info = None
        if self._secret is not None:
            self._wipe(self._secret)
            self._secret = None

    def _abort_sidecar(self, candidate_secret: bytearray | None = None) -> None:
        try:
            self._sidecar.close_vault()
        except BaseException:
            pass
        try:
            self._sidecar.shutdown()
        except BaseException:
            pass
        current_secret = self._secret
        self._clear_runtime_state()
        if candidate_secret is not None and candidate_secret is not current_secret:
            self._wipe(candidate_secret)
        self._state = "failed"

    def _refresh_locked(self) -> None:
        secret = self._close_for_transition()
        try:
            self._cursor = self._mirror.refresh()
            self._open_with_secret(secret)
        except BaseException:
            self._abort_sidecar(secret)
            raise

    def _mutate_locked(self, operation: Callable[[], None]) -> None:
        if self._mirror.journal_path.exists():
            secret = self._close_for_transition()
            try:
                self._mirror.resume_pending_transaction()
                self._open_with_secret(secret)
            except BaseException:
                self._abort_sidecar(secret)
                raise
        before = self._mirror.snapshot_local()
        try:
            operation()
        except BaseException as operation_error:
            try:
                secret = self._close_for_transition()
                self._cursor = self._mirror.refresh()
                self._open_with_secret(secret)
            except BaseException as recovery_error:
                self._abort_sidecar(self._secret)
                raise operation_error from recovery_error
            raise
        secret = self._close_for_transition()
        try:
            self._mirror.commit(before)
        except BaseException as commit_error:
            try:
                self._open_with_secret(secret)
            except BaseException as reopen_error:
                self._abort_sidecar(secret)
                raise commit_error from reopen_error
            raise
        try:
            self._open_with_secret(secret)
        except BaseException:
            self._abort_sidecar(secret)
            raise

    def _logical_change_page(
        self, indexed_paths: Mapping[str, str] | None
    ) -> ListFolderResult:
        known = dict(indexed_paths or {})
        entries: list[Metadata] = []
        for provider_id, old_path in sorted(known.items()):
            current = self._metadata_by_id.get(provider_id)
            if current is None or current.path_lower != normalize(old_path):
                entries.append(
                    DeletedMetadata(
                        posixpath.basename(old_path),
                        normalize(old_path),
                        old_path,
                    )
                )
        entries.extend(
            sorted(
                self._metadata_by_path.values(),
                key=lambda metadata: metadata.path_lower,
            )
        )
        return ListFolderResult(entries, False, self._encode_cursor(self._cursor))

    def _encode_cursor(self, provider_cursor: str) -> str:
        payload = {
            "version": 1,
            "provider": self._provider.provider_id,
            "namespace": self._provider.namespace_id,
            "vault_root": self._mirror.remote_root,
            "vault_root_id": self._mirror.remote_root_id,
            "cursor": provider_cursor,
        }
        encoded = base64.urlsafe_b64encode(
            json.dumps(
                payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        ).rstrip(b"=")
        return self._CURSOR_PREFIX + encoded.decode("ascii")

    def _decode_cursor(self, cursor: str) -> str | None:
        if not cursor.startswith(self._CURSOR_PREFIX) or len(cursor) > 4 * 1024 * 1024:
            return None
        try:
            encoded = cursor[len(self._CURSOR_PREFIX) :].encode("ascii", "strict")
            encoded += b"=" * (-len(encoded) % 4)
            decoded = base64.b64decode(encoded, altchars=b"-_", validate=True)
            payload = json.loads(decoded.decode("utf-8"))
        except (binascii.Error, UnicodeError, json.JSONDecodeError, ValueError):
            return None
        if not isinstance(payload, dict) or set(payload) != {
            "version",
            "provider",
            "namespace",
            "vault_root",
            "vault_root_id",
            "cursor",
        }:
            return None
        if (
            payload["version"] != 1
            or payload["provider"] != self._provider.provider_id
            or payload["namespace"] != self._provider.namespace_id
            or payload["vault_root"] != self._mirror.remote_root
            or payload["vault_root_id"] != self._mirror.remote_root_id
            or not isinstance(payload["cursor"], str)
            or not payload["cursor"]
        ):
            return None
        return payload["cursor"]

    def _rebuild_logical_metadata(self) -> None:
        snapshot = self._sidecar.snapshot(include_hash=True)
        storage_entries = self._sidecar.storage_map()
        if len(snapshot) != len(storage_entries):
            raise EncryptedVaultError(
                "Encrypted vault is inconsistent",
                "The logical and physical vault maps have different sizes.",
            )
        storage_by_path = {
            self._normalise_logical_path(entry.path): entry for entry in storage_entries
        }
        if len(storage_by_path) != len(storage_entries):
            raise EncryptedVaultError(
                "Encrypted vault is inconsistent",
                "The vault has duplicate logical storage mappings.",
            )

        by_path: dict[str, FileMetadata | FolderMetadata] = {}
        by_id: dict[str, FileMetadata | FolderMetadata] = {}
        for entry in snapshot:
            path = self._normalise_logical_path(entry.path)
            storage = storage_by_path.get(path)
            if storage is None or storage.type != entry.type:
                raise EncryptedVaultError(
                    "Encrypted vault is inconsistent",
                    "A logical item has no matching ciphertext object.",
                )
            storage_path = self._normalise_storage_path(storage.storage_path)
            backing = self._mirror.remote_entry(storage_path)
            metadata = self._logical_metadata(entry, backing)
            if metadata.path_lower in by_path or metadata.id in by_id:
                raise EncryptedVaultError(
                    "Encrypted vault has colliding identities",
                    "A logical item path or stable provider ID is duplicated.",
                )
            by_path[metadata.path_lower] = metadata
            by_id[metadata.id] = metadata

        self._metadata_by_path = by_path
        self._metadata_by_id = by_id

    def _logical_metadata(
        self,
        entry: VaultEntry,
        backing: FileMetadata | FolderMetadata,
    ) -> FileMetadata | FolderMetadata:
        path = self._normalise_logical_path(entry.path)
        entry_type = entry.type
        name = posixpath.basename(path)
        path_lower = normalize(path)
        modified_value = entry.modified_ms
        if (
            not isinstance(modified_value, int)
            or isinstance(modified_value, bool)
            or modified_value < 0
        ):
            raise EncryptedVaultError(
                "Encrypted vault contains invalid metadata",
                "A logical modified time is invalid.",
            )
        modified = datetime.fromtimestamp(modified_value / 1000, timezone.utc)

        if entry_type == "directory":
            if not isinstance(backing, FolderMetadata):
                raise EncryptedVaultError(
                    "Encrypted vault is inconsistent",
                    "A logical folder has no ciphertext directory.",
                )
            return FolderMetadata(name, path_lower, path, backing.id, False)

        if not isinstance(backing, FileMetadata):
            raise EncryptedVaultError(
                "Encrypted vault is inconsistent",
                "A logical file has no ciphertext file.",
            )
        size_value = entry.size
        if (
            not isinstance(size_value, int)
            or isinstance(size_value, bool)
            or size_value < 0
        ):
            raise EncryptedVaultError(
                "Encrypted vault contains invalid metadata",
                "A logical file size is invalid.",
            )
        if entry_type == "file":
            digest = entry.sha256
            symlink_target = None
        elif entry_type == "symlink":
            symlink_target = entry.link_target
            if not isinstance(symlink_target, str) or "\x00" in symlink_target:
                raise EncryptedVaultError(
                    "Encrypted vault contains invalid metadata",
                    "A logical symbolic link target is invalid.",
                )
            digest = hashlib.sha256(symlink_target.encode("utf-8")).hexdigest()
        else:
            raise EncryptedVaultError(
                "Encrypted vault contains invalid metadata",
                "A logical item has an unsupported type.",
            )
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise EncryptedVaultError(
                "Encrypted vault contains invalid metadata",
                "A logical file checksum is invalid.",
            )
        return FileMetadata(
            name=name,
            path_lower=path_lower,
            path_display=path,
            id=backing.id,
            client_modified=modified,
            server_modified=modified,
            rev=f"cryptomator-v8:{digest}",
            size=size_value,
            symlink_target=symlink_target,
            shared=False,
            modified_by=None,
            is_downloadable=True,
            content_hash=digest,
        )

    def _root_metadata(self) -> FolderMetadata:
        root = self._mirror.ensure_remote_root(create=False)
        return FolderMetadata("", "/", "/", f"cryptomator-v8:{root.id}", False)

    def _listed_metadata(self, path: str, recursive: bool) -> list[Metadata]:
        if path != "/" and not isinstance(
            self._metadata_by_path.get(normalize(path)), FolderMetadata
        ):
            metadata = self._metadata_by_path.get(normalize(path))
            if metadata is None:
                raise NotFoundError("Encrypted folder not found", dbx_path=path)
            raise NotAFolderError("Encrypted path is not a folder", dbx_path=path)
        prefix = path.rstrip("/") + "/"
        entries: list[Metadata] = []
        for metadata in self._metadata_by_path.values():
            if path == "/":
                relative = metadata.path_display[1:]
            elif metadata.path_display.startswith(prefix):
                relative = metadata.path_display[len(prefix) :]
            else:
                continue
            if not recursive and "/" in relative:
                continue
            entries.append(metadata)
        return sorted(entries, key=lambda metadata: metadata.path_lower)

    def _file_for_request(
        self, remote_path: str, provider_id: str | None
    ) -> FileMetadata:
        path = self._normalise_logical_path(remote_path)
        metadata = (
            self._metadata_by_id.get(provider_id)
            if provider_id is not None
            else self._metadata_by_path.get(normalize(path))
        )
        if metadata is None:
            raise NotFoundError("Encrypted file not found", dbx_path=path)
        if isinstance(metadata, FolderMetadata):
            raise IsAFolderError("Cannot download folder", dbx_path=path)
        if metadata.path_lower != normalize(path):
            raise DataChangedError(
                "Encrypted file moved", "Retry the encrypted download.", dbx_path=path
            )
        if metadata.symlink_target is not None:
            raise IsAFolderError("Cannot download symbolic link content", dbx_path=path)
        return metadata

    def _require_parent_directory(self, path: str) -> None:
        parent = posixpath.dirname(path) or "/"
        if parent == "/":
            return
        metadata = self._metadata_by_path.get(normalize(parent))
        if metadata is None:
            raise NotFoundError("Encrypted parent folder not found", dbx_path=parent)
        if not isinstance(metadata, FolderMetadata):
            raise NotAFolderError("Encrypted parent is not a folder", dbx_path=parent)

    def _autorename_path(self, path: str) -> str:
        parent, name = posixpath.split(path)
        stem, extension = posixpath.splitext(name)
        for index in range(1, 10_000):
            candidate = posixpath.join(parent, f"{stem} ({index}){extension}")
            if normalize(candidate) not in self._metadata_by_path:
                return candidate
        raise EncryptedVaultError(
            "Cannot choose an encrypted conflict name",
            "Move an existing conflict copy and try again.",
        )

    @staticmethod
    def _normalise_logical_path(path: str) -> str:
        if (
            not isinstance(path, str)
            or not path.startswith("/")
            or "\x00" in path
            or "\\" in path
            or "//" in path
        ):
            raise ValueError("The logical vault path is invalid")
        if path == "/":
            return path
        components = path[1:].split("/")
        if any(component in {"", ".", ".."} for component in components):
            raise ValueError("The logical vault path is invalid")
        return path

    @staticmethod
    def _normalise_storage_path(path: str) -> str:
        if (
            not isinstance(path, str)
            or path.startswith("/")
            or "\\" in path
            or "\x00" in path
        ):
            raise EncryptedVaultError(
                "Encrypted vault contains invalid metadata",
                "A ciphertext storage path is invalid.",
            )
        components = path.split("/")
        if (
            not components
            or components[0] != "d"
            or any(
                not PhysicalVaultMirror._safe_physical_component(component)
                for component in components
            )
        ):
            raise EncryptedVaultError(
                "Encrypted vault contains invalid metadata",
                "A ciphertext storage path is invalid.",
            )
        return path

    @contextmanager
    def _prepared_upload_source(
        self, source: str | os.PathLike[str] | BinaryIO
    ) -> Iterator[tuple[Path, str, int | None, int]]:
        with self._temporary_plaintext_file("upload") as staged_path:
            context: AbstractContextManager[BinaryIO]
            if isinstance(source, (str, bytes, os.PathLike)):
                context = open(source, "rb")
            else:
                context = nullcontext(source)

            with context as source_file, staged_path.open("wb") as staged_file:
                source_file.seek(0)
                before: tuple[int, int, int, int] | None
                try:
                    source_stat = os.fstat(source_file.fileno())
                    before = (
                        source_stat.st_dev,
                        source_stat.st_ino,
                        source_stat.st_size,
                        source_stat.st_mtime_ns,
                    )
                    modified_ms = source_stat.st_mtime_ns // 1_000_000
                except (AttributeError, OSError):
                    before = None
                    modified_ms = None

                digest = hashlib.sha256()
                size = 0
                while chunk := source_file.read(1024 * 1024):
                    digest.update(chunk)
                    staged_file.write(chunk)
                    size += len(chunk)

                if before is not None:
                    source_stat = os.fstat(source_file.fileno())
                    after = (
                        source_stat.st_dev,
                        source_stat.st_ino,
                        source_stat.st_size,
                        source_stat.st_mtime_ns,
                    )
                    if after != before:
                        raise DataChangedError("File changed during encrypted staging")
                source_file.seek(0)

            os.chmod(staged_path, 0o600)
            yield staged_path, digest.hexdigest(), modified_ms, size

    @contextmanager
    def _temporary_plaintext_file(self, purpose: str) -> Iterator[Path]:
        transfer_root = self._mirror.local_root.parent / ".plaintext-transfers"
        transfer_root.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if transfer_root.is_symlink():
            raise EncryptedVaultError(
                "Encrypted transfer cache is unsafe",
                "The private plaintext cache cannot be a symbolic link.",
            )
        transfer_root.mkdir(mode=0o700, exist_ok=True)
        os.chmod(transfer_root, 0o700)
        descriptor, name = tempfile.mkstemp(
            prefix=f"maestral-{purpose}-", dir=transfer_root
        )
        os.close(descriptor)
        path = Path(name)
        os.chmod(path, 0o600)
        try:
            yield path
        finally:
            path.unlink(missing_ok=True)

    @staticmethod
    def _install_verified_download(source: Path, destination: Path) -> None:
        if not destination.parent.is_dir():
            raise FileNotFoundError(destination.parent)
        descriptor, candidate_name = tempfile.mkstemp(
            prefix=f".{destination.name}.maestral-", dir=destination.parent
        )
        candidate = Path(candidate_name)
        try:
            with (
                os.fdopen(descriptor, "wb") as candidate_file,
                source.open("rb") as source_file,
            ):
                shutil.copyfileobj(source_file, candidate_file, 1024 * 1024)
            os.chmod(candidate, 0o600)
            os.replace(candidate, destination)
        finally:
            candidate.unlink(missing_ok=True)

    def _require_secret(self) -> bytearray:
        if self._secret is None:
            raise EncryptedVaultError(
                "Encrypted vault secret is unavailable",
                "Unlock the encrypted vault and try again.",
            )
        return self._secret

    def _prepare_local_root_for_initialisation(self) -> None:
        root = self._mirror.local_root
        self._mirror.claim_local_root(allow_nonempty=False)
        if root.exists():
            if not root.is_dir() or any(root.iterdir()):
                raise EncryptedVaultError(
                    "Encrypted vault cache is not empty",
                    "Remove the old ciphertext cache or attach its vault.",
                )
        else:
            root.mkdir(mode=0o700)

    @staticmethod
    def _encode_secret(secret: str) -> bytearray:
        if not isinstance(secret, str) or not secret or "\x00" in secret:
            raise ValueError("The encrypted vault password is invalid")
        encoded = bytearray(secret, "utf-8")
        if len(encoded) > 4096:
            EncryptedRemoteProvider._wipe(encoded)
            raise ValueError("The encrypted vault password is too long")
        return encoded

    @staticmethod
    def _wipe(secret: bytearray) -> None:
        secret[:] = b"\x00" * len(secret)

    @staticmethod
    def _unsupported(operation: str) -> UnsupportedProviderOperationError:
        return UnsupportedProviderOperationError(
            f"Encrypted storage does not support {operation}",
            "The remote provider cannot expose this logical encrypted item.",
        )


def _remote_provider_contract(provider: EncryptedRemoteProvider) -> RemoteProvider:
    """Keep the wrapper checked against the sync engine's provider protocol."""
    return provider
