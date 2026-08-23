"""Cryptomator-backed remote storage for Maestral."""

from __future__ import annotations

import hashlib
import os
import posixpath
import shutil
import stat
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Literal, cast

from maestral.core import FileMetadata, FolderMetadata, Metadata, WriteMode
from maestral.exceptions import MaestralApiError
from maestral.providers.base import RemoteProvider
from maestral.utils.path import normalize

if TYPE_CHECKING:
    from collections.abc import Mapping


class EncryptedVaultError(MaestralApiError):
    """A local Cryptomator vault or mirror operation failed."""


_PhysicalKind = Literal["file", "directory"]


@dataclass(frozen=True)
class _LocalPhysicalEntry:
    kind: _PhysicalKind
    identity: tuple[int, int, _PhysicalKind]
    size: int
    sha256: str | None


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
        return cast(FolderMetadata, metadata)

    def refresh(self) -> str:
        """Download one consistent remote ciphertext snapshot and return its cursor."""
        self.ensure_remote_root(create=False)
        remote, cursor = self._list_remote()
        self._materialise(remote)
        self._remote = remote
        return cursor

    def initialise_remote(self) -> None:
        """Create an empty remote root and upload the current local vault."""
        self.ensure_remote_root(create=True)
        remote, _ = self._list_remote()
        if remote:
            raise EncryptedVaultError(
                "Encrypted vault is not empty",
                "Choose an empty remote folder for a new vault.",
            )
        self._remote = {}
        self.commit({})

    def snapshot_local(self) -> dict[str, _LocalPhysicalEntry]:
        """Return a no-follow snapshot of every local ciphertext object."""
        root_stat = os.lstat(self.local_root)
        if not stat.S_ISDIR(root_stat.st_mode) or stat.S_ISLNK(root_stat.st_mode):
            raise EncryptedVaultError(
                "Encrypted vault mirror is unsafe",
                "The local ciphertext root must be a private directory.",
            )

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
                    digest,
                )
        return result

    def commit(self, before: Mapping[str, _LocalPhysicalEntry]) -> None:
        """Apply a closed vault's physical changes to the remote provider."""
        after = self.snapshot_local()
        self._require_matching_baseline(before)
        remote = self._remote.copy()

        self._remove_type_conflicts(after, remote)
        directory_moves = self._directory_moves(before, after)
        self._apply_directory_moves(directory_moves, after, remote)

        transformed_before = self._transform_snapshot(before, directory_moves)
        file_moves = self._file_moves(transformed_before, after)
        self._apply_file_moves(file_moves, remote)
        transformed_before = self._transform_snapshot(transformed_before, file_moves)

        self._create_directories(after, remote)
        self._upload_files(transformed_before, after, remote)
        self._remove_stale(after, remote)

        if set(remote) != set(after):
            raise EncryptedVaultError(
                "Encrypted vault update failed",
                "The remote ciphertext tree does not match the local transaction.",
            )
        self._remote = remote

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
        if any(part in {"", ".", ".."} for part in relative_parts):
            raise EncryptedVaultError(
                "Encrypted vault contains invalid remote data",
                "A ciphertext item has an unsafe path.",
            )
        return "/".join(relative_parts)

    def _remote_path(self, relative: str) -> str:
        if not relative or relative.startswith("/") or ".." in relative.split("/"):
            raise ValueError("Invalid relative ciphertext path")
        return self.remote_root + "/" + relative

    def _materialise(self, remote: Mapping[str, FileMetadata | FolderMetadata]) -> None:
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
                target = candidate.joinpath(*relative.split("/"))
                target.mkdir(mode=0o700)

            for relative, metadata in sorted(remote.items()):
                if not isinstance(metadata, FileMetadata):
                    continue
                target = candidate.joinpath(*relative.split("/"))
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
            self.provider.remove(self._remote_path(path))
            self._drop_remote_prefix(remote, path)

    @staticmethod
    def _directory_moves(
        before: Mapping[str, _LocalPhysicalEntry],
        after: Mapping[str, _LocalPhysicalEntry],
    ) -> list[tuple[str, str]]:
        removed = {
            entry.identity: path
            for path, entry in before.items()
            if entry.kind == "directory" and path not in after
        }
        added = {
            entry.identity: path
            for path, entry in after.items()
            if entry.kind == "directory" and path not in before
        }
        candidates = sorted(
            (
                (source, added[identity])
                for identity, source in removed.items()
                if identity in added
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
        removed = {
            entry.identity: path
            for path, entry in before.items()
            if entry.kind == "file" and path not in after
        }
        added = {
            entry.identity: path
            for path, entry in after.items()
            if entry.kind == "file" and path not in before
        }
        moves = [
            (source, added[identity])
            for identity, source in removed.items()
            if identity in added
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
            parent = posixpath.dirname(target)
            if parent and parent not in remote and parent not in targets:
                self._create_parent_chain(parent, after, remote)
            moved = self.provider.move(
                self._remote_path(source), self._remote_path(target)
            )
            if not isinstance(moved, FolderMetadata):
                raise EncryptedVaultError(
                    "Encrypted vault update failed",
                    "A ciphertext folder move returned invalid metadata.",
                )
            self._move_remote_prefix(remote, source, target, moved)

    def _apply_file_moves(
        self,
        moves: list[tuple[str, str]],
        remote: dict[str, FileMetadata | FolderMetadata],
    ) -> None:
        for source, target in moves:
            moved = self.provider.move(
                self._remote_path(source), self._remote_path(target)
            )
            if not isinstance(moved, FileMetadata):
                raise EncryptedVaultError(
                    "Encrypted vault update failed",
                    "A ciphertext file move returned invalid metadata.",
                )
            remote.pop(source)
            remote[target] = moved

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
            self.provider.remove(self._remote_path(path), parent_rev=metadata.rev)
            remote.pop(path)
        for path in sorted(directory_roots, key=_path_depth, reverse=True):
            self.provider.remove(self._remote_path(path))
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
