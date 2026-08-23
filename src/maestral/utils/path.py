"""
This module contains functions for common path operations.
"""

from __future__ import annotations

import ctypes
import errno

# system imports
import os
import os.path as osp
import platform
import secrets
import shutil
import stat
import unicodedata
from contextlib import ExitStack, contextmanager
from stat import S_ISDIR, S_ISLNK
from typing import (
    Any,
    BinaryIO,
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

# third party imports
try:
    import fcntl
except ImportError:  # pragma: no cover - Windows only
    fcntl = None  # type: ignore[assignment]

try:
    import xattr
except ImportError:  # pragma: no cover - Windows only
    xattr = None

from ..constants import IS_LINUX, IS_WINDOWS, MOVE_TEMP_PREFIX, REMOVE_TEMP_PREFIX

# local imports
from .hashing import DropboxContentHasher

F_GETPATH = 50
_WINDOWS_LINK_REPARSE_TAGS = {
    getattr(stat, "IO_REPARSE_TAG_MOUNT_POINT", 0xA0000003),
    getattr(stat, "IO_REPARSE_TAG_SYMLINK", 0xA000000C),
}
_DIRECTORY_OPEN_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_CLOEXEC", 0)
)

_FILE_ATTRIBUTE_DIRECTORY = 0x00000010
_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
_FILE_READ_ATTRIBUTES = 0x00000080
_GENERIC_READ = 0x80000000
_GENERIC_WRITE = 0x40000000
_DELETE_ACCESS = 0x00010000
_FILE_SHARE_READ = 0x00000001
_FILE_SHARE_WRITE = 0x00000002
_OPEN_EXISTING = 3
_CREATE_NEW = 1
_FILE_ATTRIBUTE_NORMAL = 0x00000080
_FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
_FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
_FILE_ATTRIBUTE_TAG_INFO_CLASS = 9
_FILE_DISPOSITION_INFO_CLASS = 4
_FILE_RENAME_INFO_CLASS = 3
_SNAPSHOT_CHUNK_SIZE = 1024 * 1024

TreeSnapshotIdentity = tuple[int, int, int, int, int, int, str | None]


class RootedTemporaryFile:
    """A named temporary file held open independently of its pathname."""

    def __init__(self, path: str, file_descriptor: int) -> None:
        self.path = path
        self._file_descriptor = file_descriptor
        item_stat = os.fstat(file_descriptor)
        self.identity = (item_stat.st_dev, item_stat.st_ino, item_stat.st_mode)

    @property
    def closed(self) -> bool:
        return self._file_descriptor < 0

    def fileno(self) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed temporary file")
        return self._file_descriptor

    def open(self, mode: str = "r+b") -> BinaryIO:
        """Open a duplicate descriptor without resolving the pathname again."""
        valid_modes = {"rb", "r+b", "rb+", "wb", "w+b", "wb+", "ab", "a+b", "ab+"}
        if mode not in valid_modes:
            raise ValueError("Rooted temporary files support binary modes only")

        file_descriptor = os.dup(self.fileno())
        try:
            if mode.startswith("w"):
                os.ftruncate(file_descriptor, 0)
                os.lseek(file_descriptor, 0, os.SEEK_SET)
            elif mode.startswith("a"):
                os.lseek(file_descriptor, 0, os.SEEK_END)
            else:
                os.lseek(file_descriptor, 0, os.SEEK_SET)
            return cast(BinaryIO, os.fdopen(file_descriptor, mode))
        except BaseException:
            os.close(file_descriptor)
            raise

    def close(self) -> None:
        if not self.closed:
            os.close(self._file_descriptor)
            self._file_descriptor = -1

    def __enter__(self) -> RootedTemporaryFile:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except BaseException:
            pass


class _WindowsFileAttributeTagInfo(ctypes.Structure):
    _fields_ = [
        ("file_attributes", ctypes.c_uint32),
        ("reparse_tag", ctypes.c_uint32),
    ]


class _WindowsFileDispositionInfo(ctypes.Structure):
    _fields_ = [("delete_file", ctypes.c_ubyte)]


class _WindowsFileRenameInfo(ctypes.Structure):
    _fields_ = [
        ("replace_if_exists", ctypes.c_ubyte),
        ("root_directory", ctypes.c_void_p),
        ("file_name_length", ctypes.c_uint32),
        ("file_name", ctypes.c_uint16 * 1),
    ]


def is_fs_link(stat_result: os.stat_result) -> bool:
    """Return whether a stat result describes a symlink or Windows junction."""
    return S_ISLNK(stat_result.st_mode) or (
        getattr(stat_result, "st_reparse_tag", None) in _WINDOWS_LINK_REPARSE_TAGS
    )


def _path_components(path: str) -> List[str]:
    components = path.strip(osp.sep).split(osp.sep)
    return [c for c in components if c]


_AnyPath = Union[str, bytes, "os.PathLike[str]", "os.PathLike[bytes]"]

# ==== path relationships ==============================================================


def is_child(
    path: str | bytes, parent: str | bytes, case_sensitive: bool = True
) -> bool:
    """
    Checks if ``path`` semantically is inside ``parent``. Neither path needs to
    refer to an actual item on the drive. This function is case-sensitive.

    :param path: Item path.
    :param parent: Parent path.
    :param case_sensitive: Whether to do case-sensitive checks.
    :returns: Whether ``path`` semantically lies inside ``parent``.
    """
    path = normalize(path) if not case_sensitive else os.fsdecode(path)
    parent = normalize(parent) if not case_sensitive else os.fsdecode(parent)

    separator = "/" if path.startswith("/") and parent.startswith("/") else osp.sep

    if separator == osp.sep:
        path = osp.normpath(path)
        parent = osp.normpath(parent)

    parent = parent.rstrip(separator) + separator
    path = path.rstrip(separator)

    return path.startswith(parent)


def is_equal_or_child(
    path: str | bytes, parent: str | bytes, case_sensitive: bool = True
) -> bool:
    """
    Checks if ``path`` semantically is inside ``parent`` or equals ``parent``. Neither
    path needs to refer to an actual item on the drive. This function is case-sensitive.

    :param path: Item path.
    :param parent: Parent path.
    :param case_sensitive: Whether to do case-sensitive checks.
    :returns: ``True`` if ``path`` semantically lies inside ``parent`` or
        ``path == parent``.
    """
    path_cmp = normalize(path) if not case_sensitive else os.fsdecode(path)
    parent_cmp = normalize(parent) if not case_sensitive else os.fsdecode(parent)

    separator = (
        "/" if path_cmp.startswith("/") and parent_cmp.startswith("/") else osp.sep
    )
    if separator == osp.sep:
        path_cmp = osp.normpath(path_cmp)
        parent_cmp = osp.normpath(parent_cmp)

    equal = path_cmp.rstrip(separator) == parent_cmp.rstrip(separator)
    return equal or is_child(path_cmp, parent_cmp)


# ==== case sensitivity and normalization ==============================================


def normalize_case(string: str) -> str:
    """
    Converts a string to lower case. Todo: Follow Python 2.5 / Dropbox conventions.

    :param string: Original string.
    :returns: Lowercase string.
    """
    return string.lower()


def normalize_unicode(string: str) -> str:
    """
    Normalizes a string to replace all decomposed unicode characters with their single
    character equivalents.

    :param string: Original string.
    :returns: Normalized string.
    """
    return unicodedata.normalize("NFC", string)


def normalize(path: str | bytes) -> str:
    """
    Replicates the path normalization performed by Dropbox servers. This typically only
    involves converting the path to lower case, with a few (undocumented) exceptions:

    * Unicode normalization: decomposed characters are converted to composed characters.
    * Lower casing of non-ascii characters: Dropbox uses the Python 2.5 behavior for
      conversion to lower case. This means that some cyrillic characters are incorrectly
      lower-cased. For example:
      "Ꙋ".lower() -> "Ꙋ" instead of "ꙋ"
      "ΣΣΣ".lower() -> "σσσ" instead of "σσς"
    * Trailing spaces are stripped from folder names. We do not perform this
      normalization here because the Dropbox API will raise sync errors for such names
      anyways.

    Note that calling :func:`normalize` on an already normalized path will return the
    unmodified input.

    Todo: Follow Python 2.5 / Dropbox conventions instead of Python 3 conventions.

    :param path: Original path.
    :returns: Normalized path.
    """
    return normalize_case(normalize_unicode(os.fsdecode(path)))


def is_fs_case_sensitive(path: str) -> bool:
    """
    Checks if ``path`` lies on a partition with a case-sensitive file system.

    :param path: Path to check.
    :returns: Whether ``path`` lies on a partition with a case-sensitive file system.
    """
    if path == osp.sep:
        raise ValueError(f"Cannot check '{osp.sep}'")

    if path.islower():
        check_path = path.upper()
    else:
        check_path = path.lower()

    if exists(path) and not exists(check_path):
        return True
    else:
        return not osp.samefile(path, check_path)


def get_existing_equivalent_paths(
    path: str,
    root: str = osp.sep,
    norm_func: Callable[[str], str] = normalize,
) -> List[str]:
    """
    Given a "normalized" path using an injective (one-directional) normalization
    function, this method returns a list of matching un-normalized local paths. If no
    such local paths exist, list will be empty.

    :Example:

        Assume the normalization function is ``str.lower()``. If a root directory
        contains two folders "/parent/subfolder/child" and "/parent/Subfolder/child",
        two matches will be returned for "path = /parent/subfolder/child/file.txt".

    :param path: Normalized path relative to ``root``.
    :param root: Parent directory to search in. There are significant performance
        improvements if a root directory with a small tree is given.
    :param norm_func: Normalization function to use. Defaults to :func:`normalize`.
    :returns: List of existing paths for which `normalized(local_path) == normalized(path)`.
    """
    path_drive, _ = osp.splitdrive(path)
    root_drive, _ = osp.splitdrive(root)

    if path_drive and not root_drive:
        root = path_drive + osp.sep

    if osp.isabs(path):
        try:
            path = osp.relpath(path, root)
        except ValueError:
            return []

        if path == osp.pardir or path.startswith(osp.pardir + osp.sep):
            return []
    else:
        path = path.lstrip(osp.sep)

    if path == "":
        return [root]

    components = _path_components(path)
    candidates = [root]

    for depth, component in enumerate(components):
        component_normalized = norm_func(component)
        is_final = depth + 1 == len(components)
        next_candidates: list[str] = []

        for candidate in candidates:
            try:
                if is_fs_link(os.lstat(candidate)):
                    continue
                with os.scandir(candidate) as entries:
                    matching_entries = [
                        entry
                        for entry in entries
                        if norm_func(entry.name) == component_normalized
                    ]
            except OSError:
                continue

            for entry in matching_entries:
                try:
                    stat_result = os.lstat(entry.path)
                except OSError:
                    continue

                if is_final or (
                    S_ISDIR(stat_result.st_mode) and not is_fs_link(stat_result)
                ):
                    next_candidates.append(entry.path)

        candidates = next_candidates
        if not candidates:
            break

    return candidates


def _macos_get_canonically_cased_path(path: str) -> str:
    # Use fcntl to get FS path, there can only be one.
    if fcntl is None:  # pragma: no cover - guarded by the platform check
        raise OSError("fcntl is unavailable")
    with open(path, opener=opener_no_symlink) as fd:
        fs_path = fcntl.fcntl(fd.fileno(), F_GETPATH, b"\x00" * 1024)
    return os.fsdecode(fs_path.strip(b"\x00"))


def to_existing_unnormalized_path(
    path: str, root: str = osp.sep, norm_func: Callable[[str], str] = normalize
) -> str:
    """
    Returns a cased version of the given path if corresponding nodes (with arbitrary
    casing) exist in the given root directory. If multiple matches are found, only one
    is returned.

    This is similar to :func:`get_existing_equivalent_paths` but returns only the first
    candidate or raises a :class:`FileNotFoundError` if no candidates can be found.

    If the file system is not case-sensitive but case-preserving, this function
    effectively returns the "displayed" version of a path, as used for example in file
    managers.

    On macOS, we use fcntl F_GETPATH for a more efficient implementation.

    :param path: Original path relative to ``root``.
    :param root: Parent directory to search in. There are significant performance
        improvements if a root directory with a small tree is given.
    :param norm_func: Normalization function to use. Defaults to :func:`normalize`.
    :returns: Absolute and cased version of given path.
    :raises FileNotFoundError: if ``path`` does not exist in root ``root`` or ``root``
        itself does not exist.
    """
    if platform.system() == "Darwin" and norm_func is normalize:
        try:
            return _macos_get_canonically_cased_path(path)
        except FileNotFoundError:
            raise
        except OSError:
            # Fall back to cross-platform method.
            pass

    candidates = get_existing_equivalent_paths(path, root)

    if len(candidates) == 0:
        raise FileNotFoundError(f'No matches with different casing found in "{root}"')
    return candidates[0]


def normalized_path_exists(path: str, root: str = osp.sep) -> bool:
    """
    Checks if a ``path`` exists in given ``root`` directory, similar to
    ``os.path.exists`` but case-insensitive. Normalisation is performed as by Dropbox
    servers (lower case and unicode normalisation).

    :param path: Path relative to ``root``.
    :param root: Directory where we will look for ``path``. There are significant
        performance improvements if a root directory with a small tree is given.
    :returns: Whether an arbitrarily cased version of ``path`` exists.
    """
    candidates = get_existing_equivalent_paths(path, root)

    for c in candidates:
        if exists(c):
            return True

    return False


def generate_cc_name(path: str, suffix: str) -> str:
    """
    Generates a path for a conflicting copy of ``path``. The file name is created by
    inserting the given ``suffix`` between the filename and the extension. For example,
    for ``suffix = "conflicting copy"``:

        "my_file.txt" -> "my_file (conflicting copy).txt"

    If a file with the resulting path already exists (case-insensitive!), we
    additionally append an integer number, for instance:

        "my_file.txt" -> "my_file (conflicting copy 1).txt"

    :param path: Original path name.
    :param suffix: Suffix to use.
    :returns: New path.
    """
    dirname, basename = osp.split(path)
    filename, ext = osp.splitext(basename)

    i = 0
    cc_candidate = f"{filename} ({suffix}){ext}"

    while normalized_path_exists(cc_candidate, dirname):
        i += 1
        cc_candidate = f"{filename} ({suffix} {i}){ext}"

    return osp.join(dirname, cc_candidate)


# ==== higher level file operations ====================================================


def _rooted_path_parts(path: str, root_path: str) -> tuple[str, str, list[str]]:
    """Return absolute root, absolute path, and path components below the root."""
    absolute_root = osp.normpath(osp.abspath(root_path))
    absolute_path = osp.normpath(osp.abspath(path))

    try:
        relative_path = osp.relpath(absolute_path, absolute_root)
    except ValueError as exc:
        raise ValueError(f"Path '{path}' is not below root '{root_path}'") from exc

    if relative_path == osp.curdir:
        raise ValueError("A rooted operation cannot mutate its root directory")

    if relative_path == osp.pardir or relative_path.startswith(osp.pardir + osp.sep):
        raise ValueError(f"Path '{path}' is not below root '{root_path}'")

    components = relative_path.split(osp.sep)
    if any(component in {"", osp.curdir, osp.pardir} for component in components):
        raise ValueError(f"Path '{path}' is not below root '{root_path}'")

    return absolute_root, absolute_path, components


def _rooted_path_is_root(path: str, root_path: str) -> bool:
    absolute_path = osp.normpath(osp.abspath(path))
    absolute_root = osp.normpath(osp.abspath(root_path))
    if IS_WINDOWS:
        return osp.normcase(absolute_path) == osp.normcase(absolute_root)
    return absolute_path == absolute_root


@contextmanager
def _posix_rooted_parent(
    path: str,
    root_path: str,
    expected_root_identity: tuple[int, ...] | None = None,
) -> Iterator[tuple[int, str, str]]:
    """Hold no-follow directory descriptors from a root through a path's parent."""
    absolute_root, absolute_path, components = _rooted_path_parts(path, root_path)
    descriptors: list[int] = []

    try:
        parent_fd = os.open(absolute_root, _DIRECTORY_OPEN_FLAGS)
        descriptors.append(parent_fd)
        _validate_source_identity(
            os.fstat(parent_fd), expected_root_identity, absolute_root
        )

        for component in components[:-1]:
            parent_fd = os.open(component, _DIRECTORY_OPEN_FLAGS, dir_fd=parent_fd)
            descriptors.append(parent_fd)

        yield parent_fd, components[-1], absolute_path
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _raise_windows_error(path: str) -> None:  # pragma: no cover - Windows only
    error_number = getattr(ctypes, "get_last_error")()
    error = getattr(ctypes, "WinError")(error_number)
    error.filename = path
    raise error


def _windows_kernel32() -> Any:  # pragma: no cover - Windows only
    kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)

    kernel32.CreateFileW.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    ]
    kernel32.CreateFileW.restype = ctypes.c_void_p
    kernel32.GetFileInformationByHandleEx.argtypes = [
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_uint32,
    ]
    kernel32.GetFileInformationByHandleEx.restype = ctypes.c_int
    kernel32.SetFileInformationByHandle.argtypes = [
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_uint32,
    ]
    kernel32.SetFileInformationByHandle.restype = ctypes.c_int
    kernel32.ReadFile.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.c_void_p,
    ]
    kernel32.ReadFile.restype = ctypes.c_int
    kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
    kernel32.CloseHandle.restype = ctypes.c_int
    return kernel32


def _open_windows_handle(
    path: str,
    *,
    delete_access: bool = False,
    read_access: bool = False,
    share_write: bool = True,
) -> tuple[int, _WindowsFileAttributeTagInfo]:  # pragma: no cover - Windows only
    """Open an item itself, without following a reparse point."""
    kernel32 = _windows_kernel32()
    desired_access = _FILE_READ_ATTRIBUTES
    if delete_access:
        desired_access |= _DELETE_ACCESS
    if read_access:
        desired_access |= _GENERIC_READ

    share_mode = _FILE_SHARE_READ
    if share_write:
        share_mode |= _FILE_SHARE_WRITE

    handle = kernel32.CreateFileW(
        path,
        desired_access,
        share_mode,
        None,
        _OPEN_EXISTING,
        _FILE_FLAG_BACKUP_SEMANTICS | _FILE_FLAG_OPEN_REPARSE_POINT,
        None,
    )
    invalid_handle = ctypes.c_void_p(-1).value
    if handle is None or handle == invalid_handle:
        _raise_windows_error(path)

    info = _WindowsFileAttributeTagInfo()
    if not kernel32.GetFileInformationByHandleEx(
        handle,
        _FILE_ATTRIBUTE_TAG_INFO_CLASS,
        ctypes.byref(info),
        ctypes.sizeof(info),
    ):
        kernel32.CloseHandle(handle)
        _raise_windows_error(path)

    return handle, info


def _close_windows_handle(handle: int) -> None:  # pragma: no cover - Windows only
    _windows_kernel32().CloseHandle(handle)


@contextmanager
def _windows_rooted_parent(
    path: str,
    root_path: str,
    expected_root_identity: tuple[int, ...] | None = None,
) -> Iterator[tuple[str, str]]:  # pragma: no cover - Windows only
    """Hold non-reparse ancestors without delete sharing for one path operation."""
    absolute_root, absolute_path, components = _rooted_path_parts(path, root_path)
    handles: list[int] = []

    try:
        for depth in range(len(components)):
            ancestor = osp.join(absolute_root, *components[:depth])
            handle, info = _open_windows_handle(
                ancestor,
                read_access=True,
                share_write=False,
            )
            handles.append(handle)

            if info.file_attributes & _FILE_ATTRIBUTE_REPARSE_POINT:
                raise OSError(errno.ELOOP, os.strerror(errno.ELOOP), ancestor)
            if not info.file_attributes & _FILE_ATTRIBUTE_DIRECTORY:
                raise NotADirectoryError(
                    errno.ENOTDIR, os.strerror(errno.ENOTDIR), ancestor
                )
            if depth == 0:
                # This handle omits FILE_SHARE_DELETE, so the path cannot identify a
                # replacement between CreateFileW and lstat.
                _validate_source_identity(
                    os.lstat(ancestor), expected_root_identity, ancestor
                )

        yield absolute_path, components[-1]
    finally:
        for handle in reversed(handles):
            _close_windows_handle(handle)


def _same_stat_identity(first: os.stat_result, second: os.stat_result) -> bool:
    """Return whether two stat results refer to the same file-system item."""
    return (first.st_dev, first.st_ino, stat.S_IFMT(first.st_mode)) == (
        second.st_dev,
        second.st_ino,
        stat.S_IFMT(second.st_mode),
    )


def _posix_delete_at(parent_fd: int, name: str, recursive: bool) -> None:
    """Delete an entry relative to an already validated parent descriptor."""
    item_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)

    if not S_ISDIR(item_stat.st_mode) or S_ISLNK(item_stat.st_mode):
        os.unlink(name, dir_fd=parent_fd)
        return

    if not recursive:
        os.rmdir(name, dir_fd=parent_fd)
        return

    child_fd = os.open(name, _DIRECTORY_OPEN_FLAGS, dir_fd=parent_fd)
    try:
        opened_stat = os.fstat(child_fd)
        if not _same_stat_identity(item_stat, opened_stat):
            raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), name)

        with os.scandir(child_fd) as entries:
            for entry in entries:
                _posix_delete_at(child_fd, entry.name, recursive=True)

        current_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if not _same_stat_identity(opened_stat, current_stat):
            raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), name)

        os.rmdir(name, dir_fd=parent_fd)
    finally:
        os.close(child_fd)


@contextmanager
def _posix_private_quarantine(parent_fd: int) -> Iterator[int]:
    """Create a private sibling directory and hold it through one mutation."""
    quarantine_name = ""
    quarantine_fd = -1

    try:
        for _ in range(100):
            quarantine_name = f"{REMOVE_TEMP_PREFIX}{secrets.token_hex(16)}"
            try:
                os.mkdir(quarantine_name, 0o700, dir_fd=parent_fd)
            except FileExistsError:
                continue
            break
        else:
            raise FileExistsError("Could not reserve a private removal directory")

        quarantine_fd = os.open(
            quarantine_name,
            _DIRECTORY_OPEN_FLAGS,
            dir_fd=parent_fd,
        )
        yield quarantine_fd
    finally:
        if quarantine_fd >= 0:
            os.close(quarantine_fd)
        if quarantine_name:
            try:
                os.rmdir(quarantine_name, dir_fd=parent_fd)
            except FileNotFoundError:
                pass


def _posix_restore_quarantined(
    quarantine_parent_fd: int,
    quarantine_name: str,
    parent_fd: int,
    name: str,
    *,
    preserve_path: str | None = None,
) -> None:
    try:
        _rename_no_replace_at(
            quarantine_parent_fd,
            quarantine_name,
            parent_fd,
            name,
        )
    except FileExistsError as exc:
        if preserve_path is not None:
            raise OSError(
                errno.ESTALE,
                f"Target changed; preserved original as '{preserve_path}'",
                name,
                preserve_path,
            ) from exc

        recovery_name = f".~maestral-preserved-{secrets.token_hex(16)}"
        _rename_no_replace_at(
            quarantine_parent_fd,
            quarantine_name,
            parent_fd,
            recovery_name,
        )
        raise OSError(
            errno.ESTALE,
            f"Target changed; preserved replacement as '{recovery_name}'",
            name,
            recovery_name,
        ) from exc


def _posix_delete_expected_at(
    parent_fd: int,
    name: str,
    absolute_path: str,
    expected_target_identity: tuple[int, ...],
    *,
    recursive: bool,
    expect_directory: bool | None = None,
    expected_tree_snapshot: dict[str, TreeSnapshotIdentity] | None = None,
    quarantine: tuple[int, str, str] | None = None,
) -> None:
    """Move an entry out of reach, validate it, and delete only the match."""
    source_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    _validate_source_identity(source_stat, expected_target_identity, name)

    def validate_and_delete(
        item_parent_fd: int,
        item_name: str,
        *,
        restore_on_delete_error: bool,
        restore_path: str | None = None,
    ) -> None:
        moved = item_parent_fd != parent_fd or item_name != name

        try:
            item_stat = os.stat(
                item_name,
                dir_fd=item_parent_fd,
                follow_symlinks=False,
            )
            if not _same_stat_identity(source_stat, item_stat):
                raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), name)
            if _snapshot_identity(source_stat)[:5] != _snapshot_identity(item_stat)[:5]:
                raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), name)
            is_directory = S_ISDIR(item_stat.st_mode) and not S_ISLNK(item_stat.st_mode)
            if expect_directory is True and not is_directory:
                raise NotADirectoryError(
                    errno.ENOTDIR, os.strerror(errno.ENOTDIR), name
                )
            if expect_directory is False and is_directory:
                raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), name)
            if expected_tree_snapshot is not None:
                actual_tree_snapshot = _posix_snapshot_at(
                    item_parent_fd,
                    item_name,
                    absolute_path,
                )
                if not _tree_snapshots_match(
                    actual_tree_snapshot,
                    expected_tree_snapshot,
                    absolute_path,
                ):
                    raise OSError(
                        errno.ESTALE,
                        "The rooted tree changed before deletion",
                        absolute_path,
                    )
        except BaseException:
            if moved:
                _posix_restore_quarantined(
                    item_parent_fd,
                    item_name,
                    parent_fd,
                    name,
                    preserve_path=restore_path,
                )
            raise

        try:
            _posix_delete_at(item_parent_fd, item_name, recursive=recursive)
        except BaseException:
            if moved and restore_on_delete_error:
                _posix_restore_quarantined(
                    item_parent_fd,
                    item_name,
                    parent_fd,
                    name,
                    preserve_path=restore_path,
                )
            raise

    if quarantine is not None:
        quarantine_parent_fd, quarantine_name, quarantine_path = quarantine
        if quarantine_path == absolute_path:
            validate_and_delete(
                parent_fd,
                name,
                restore_on_delete_error=False,
            )
            return

        _rename_no_replace_at(
            parent_fd,
            name,
            quarantine_parent_fd,
            quarantine_name,
        )
        validate_and_delete(
            quarantine_parent_fd,
            quarantine_name,
            restore_on_delete_error=False,
            restore_path=quarantine_path,
        )
        return

    with _posix_private_quarantine(parent_fd) as quarantine_fd:
        _rename_no_replace_at(parent_fd, name, quarantine_fd, "item")
        validate_and_delete(
            quarantine_fd,
            "item",
            restore_on_delete_error=True,
        )


def _windows_mark_handle_for_deletion(
    path: str, handle: int
) -> None:  # pragma: no cover - Windows only
    disposition = _WindowsFileDispositionInfo(delete_file=1)
    kernel32 = _windows_kernel32()
    if not kernel32.SetFileInformationByHandle(
        handle,
        _FILE_DISPOSITION_INFO_CLASS,
        ctypes.byref(disposition),
        ctypes.sizeof(disposition),
    ):
        _raise_windows_error(path)


def _windows_rename_opened(
    source_path: str,
    source_handle: int,
    destination_path: str,
    *,
    replace: bool,
) -> None:  # pragma: no cover - Windows only
    """Rename the opened source, without resolving its final component again."""
    encoded_destination = destination_path.encode("utf-16-le")
    file_name_offset = _WindowsFileRenameInfo.file_name.offset
    buffer = ctypes.create_string_buffer(
        file_name_offset + len(encoded_destination) + ctypes.sizeof(ctypes.c_uint16)
    )
    rename_info = _WindowsFileRenameInfo.from_buffer(buffer)
    rename_info.replace_if_exists = replace
    rename_info.root_directory = None
    rename_info.file_name_length = len(encoded_destination)
    ctypes.memmove(
        ctypes.addressof(buffer) + file_name_offset,
        encoded_destination,
        len(encoded_destination),
    )

    kernel32 = _windows_kernel32()
    if not kernel32.SetFileInformationByHandle(
        source_handle,
        _FILE_RENAME_INFO_CLASS,
        buffer,
        len(buffer),
    ):
        _raise_windows_error(source_path)


def _windows_delete_opened(
    path: str,
    handle: int,
    info: _WindowsFileAttributeTagInfo,
    *,
    recursive: bool,
    expect_directory: bool | None = None,
) -> None:  # pragma: no cover - Windows only
    is_directory = bool(info.file_attributes & _FILE_ATTRIBUTE_DIRECTORY)
    is_reparse_point = bool(info.file_attributes & _FILE_ATTRIBUTE_REPARSE_POINT)

    if expect_directory is True and not is_directory:
        raise NotADirectoryError(errno.ENOTDIR, os.strerror(errno.ENOTDIR), path)
    if expect_directory is False and is_directory and not is_reparse_point:
        raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), path)

    if is_directory and not is_reparse_point and recursive:
        with os.scandir(path) as entries:
            for entry in entries:
                child_path = osp.join(path, entry.name)
                child_handle, child_info = _open_windows_handle(
                    child_path, delete_access=True
                )
                try:
                    _windows_delete_opened(
                        child_path,
                        child_handle,
                        child_info,
                        recursive=True,
                    )
                finally:
                    _close_windows_handle(child_handle)

    _windows_mark_handle_for_deletion(path, handle)


def _windows_delete_path(
    path: str,
    *,
    recursive: bool,
    expect_directory: bool | None = None,
    expected_target_identity: tuple[int, ...] | None = None,
    expected_tree_snapshot: dict[str, TreeSnapshotIdentity] | None = None,
    quarantine_path: str | None = None,
) -> None:  # pragma: no cover - Windows only
    handle = -1
    quarantine_handle = -1
    private_quarantine_path = ""
    opened_path = path
    preserve_quarantine = False
    deletion_started = False

    try:
        handle, info = _open_windows_handle(
            path,
            delete_access=True,
            read_access=expected_tree_snapshot is not None,
            share_write=expected_tree_snapshot is None,
        )
        _validate_source_identity(os.lstat(path), expected_target_identity, path)

        if expected_tree_snapshot is not None:
            if quarantine_path is None:
                parent_path = osp.dirname(path)
                for _ in range(100):
                    private_quarantine_path = osp.join(
                        parent_path,
                        f"{REMOVE_TEMP_PREFIX}{secrets.token_hex(16)}",
                    )
                    try:
                        os.mkdir(private_quarantine_path, 0o700)
                    except FileExistsError:
                        continue
                    break
                else:
                    raise FileExistsError(
                        "Could not reserve a private removal directory"
                    )

                quarantine_handle, quarantine_info = _open_windows_handle(
                    private_quarantine_path,
                    delete_access=True,
                    read_access=True,
                )
                if quarantine_info.file_attributes & _FILE_ATTRIBUTE_REPARSE_POINT:
                    raise OSError(
                        errno.ELOOP,
                        os.strerror(errno.ELOOP),
                        private_quarantine_path,
                    )

                destination_path = osp.join(private_quarantine_path, "item")
                _windows_rename_opened(
                    path,
                    handle,
                    destination_path,
                    replace=False,
                )
                opened_path = destination_path
            elif osp.normcase(quarantine_path) != osp.normcase(path):
                _windows_rename_opened(
                    path,
                    handle,
                    quarantine_path,
                    replace=False,
                )
                opened_path = quarantine_path
            else:
                opened_path = path

            actual_tree_snapshot = _rebase_tree_snapshot(
                _windows_snapshot_opened(opened_path, handle, info),
                opened_path,
                path,
            )
            if not _tree_snapshots_match(
                actual_tree_snapshot,
                expected_tree_snapshot,
                path,
            ):
                raise OSError(
                    errno.ESTALE,
                    "The rooted tree changed before deletion",
                    path,
                )

        deletion_started = True
        _windows_delete_opened(
            opened_path,
            handle,
            info,
            recursive=recursive,
            expect_directory=expect_directory,
        )
    except BaseException as error:
        if opened_path != path and not deletion_started and handle >= 0:
            try:
                _windows_rename_opened(
                    opened_path,
                    handle,
                    path,
                    replace=False,
                )
            except BaseException as restore_error:
                preserve_quarantine = True
                raise OSError(
                    errno.ESTALE,
                    f"Target changed; preserved original as '{opened_path}'",
                    path,
                    opened_path,
                ) from restore_error
        elif opened_path != path and deletion_started:
            preserve_quarantine = True
        raise error
    finally:
        if handle >= 0:
            _close_windows_handle(handle)
        if quarantine_handle >= 0:
            if not preserve_quarantine:
                try:
                    _windows_mark_handle_for_deletion(
                        private_quarantine_path,
                        quarantine_handle,
                    )
                except OSError:
                    pass
            _close_windows_handle(quarantine_handle)


def _rooted_name_has_exact_case(parent: int | str, name: str) -> bool:
    """Return whether a directory contains the requested Unicode-normalised name."""
    with os.scandir(parent) as entries:
        return any(equal_but_for_unicode_norm(entry.name, name) for entry in entries)


def rooted_name_has_exact_case(
    path: str,
    root_path: str,
    *,
    expected_root_identity: tuple[int, ...] | None = None,
) -> bool:
    """Check a final path spelling through held, no-follow root ancestors."""
    try:
        if IS_WINDOWS:
            with _windows_rooted_parent(
                path,
                root_path,
                expected_root_identity,
            ) as (absolute_path, name):
                return _rooted_name_has_exact_case(
                    osp.dirname(absolute_path),
                    name,
                )

        with _posix_rooted_parent(
            path,
            root_path,
            expected_root_identity,
        ) as (parent_fd, name, _):
            return _rooted_name_has_exact_case(parent_fd, name)
    except (FileNotFoundError, NotADirectoryError):
        return False


def _create_windows_file(path: str) -> int:  # pragma: no cover - Windows only
    """Create a new regular file and return an owning CRT descriptor."""
    kernel32 = _windows_kernel32()
    handle = kernel32.CreateFileW(
        path,
        _GENERIC_READ | _GENERIC_WRITE | _FILE_READ_ATTRIBUTES,
        _FILE_SHARE_READ | _FILE_SHARE_WRITE,
        None,
        _CREATE_NEW,
        _FILE_ATTRIBUTE_NORMAL,
        None,
    )
    invalid_handle = ctypes.c_void_p(-1).value
    if handle is None or handle == invalid_handle:
        _raise_windows_error(path)

    try:
        import msvcrt

        return getattr(msvcrt, "open_osfhandle")(
            handle,
            os.O_RDWR | getattr(os, "O_BINARY", 0),
        )
    except BaseException:
        kernel32.CloseHandle(handle)
        raise


def create_rooted_tempfile(
    directory: str,
    root_path: str,
    *,
    expected_root_identity: tuple[int, ...] | None = None,
    prefix: str = ".~maestral-",
    mode: int = 0o600,
) -> RootedTemporaryFile:
    """Create and hold a named temporary file below a validated root."""
    if (
        not prefix
        or osp.basename(prefix) != prefix
        or osp.sep in prefix
        or osp.altsep is not None
        and osp.altsep in prefix
    ):
        raise ValueError("The temporary-file prefix must be one file-name component")

    for _ in range(100):
        candidate = osp.join(directory, f"{prefix}{secrets.token_hex(16)}")

        if IS_WINDOWS:
            try:
                with _windows_rooted_parent(
                    candidate, root_path, expected_root_identity
                ) as (absolute_path, _):
                    file_descriptor = _create_windows_file(absolute_path)
            except FileExistsError:
                continue
        else:
            try:
                with _posix_rooted_parent(
                    candidate, root_path, expected_root_identity
                ) as (parent_fd, name, absolute_path):
                    flags = (
                        os.O_RDWR
                        | os.O_CREAT
                        | os.O_EXCL
                        | getattr(os, "O_NOFOLLOW", 0)
                        | getattr(os, "O_CLOEXEC", 0)
                    )
                    file_descriptor = os.open(
                        name,
                        flags,
                        mode,
                        dir_fd=parent_fd,
                    )
            except FileExistsError:
                continue

        try:
            return RootedTemporaryFile(absolute_path, file_descriptor)
        except BaseException:
            os.close(file_descriptor)
            raise

    raise FileExistsError("Could not reserve a unique temporary-file name")


def open_rooted_file(
    path: str,
    root_path: str,
    *,
    expected_root_identity: tuple[int, ...] | None = None,
    expected_file_identity: tuple[int, ...] | None = None,
) -> BinaryIO:
    """Open and hold a regular file below a validated root for binary reads."""
    if IS_WINDOWS:
        with _windows_rooted_parent(path, root_path, expected_root_identity) as (
            absolute_path,
            _,
        ):
            handle, info = _open_windows_handle(
                absolute_path,
                read_access=True,
                share_write=False,
            )
            file_descriptor = -1
            try:
                if info.file_attributes & _FILE_ATTRIBUTE_REPARSE_POINT:
                    raise OSError(errno.ELOOP, os.strerror(errno.ELOOP), absolute_path)
                if info.file_attributes & _FILE_ATTRIBUTE_DIRECTORY:
                    raise IsADirectoryError(
                        errno.EISDIR, os.strerror(errno.EISDIR), absolute_path
                    )

                path_stat = os.lstat(absolute_path)
                if not stat.S_ISREG(path_stat.st_mode):
                    raise OSError(
                        errno.EINVAL,
                        "The rooted item is not a regular file",
                        absolute_path,
                    )

                import msvcrt

                file_descriptor = getattr(msvcrt, "open_osfhandle")(
                    handle,
                    os.O_RDONLY | getattr(os, "O_BINARY", 0),
                )
                handle = -1
                opened_stat = os.fstat(file_descriptor)
                if not _same_stat_identity(path_stat, opened_stat):
                    raise OSError(
                        errno.ESTALE,
                        os.strerror(errno.ESTALE),
                        absolute_path,
                    )
                _validate_source_identity(
                    opened_stat,
                    expected_file_identity,
                    absolute_path,
                )

                file = cast(BinaryIO, os.fdopen(file_descriptor, "rb"))
                file_descriptor = -1
                return file
            finally:
                if file_descriptor >= 0:
                    os.close(file_descriptor)
                if handle >= 0:
                    _close_windows_handle(handle)

    with _posix_rooted_parent(path, root_path, expected_root_identity) as (
        parent_fd,
        name,
        absolute_path,
    ):
        path_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if S_ISDIR(path_stat.st_mode):
            raise IsADirectoryError(
                errno.EISDIR, os.strerror(errno.EISDIR), absolute_path
            )
        if not stat.S_ISREG(path_stat.st_mode):
            error_number = errno.ELOOP if S_ISLNK(path_stat.st_mode) else errno.EINVAL
            raise OSError(error_number, os.strerror(error_number), absolute_path)

        flags = (
            os.O_RDONLY
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        file_descriptor = os.open(name, flags, dir_fd=parent_fd)
        try:
            opened_stat = os.fstat(file_descriptor)
            if not stat.S_ISREG(opened_stat.st_mode) or not _same_stat_identity(
                path_stat, opened_stat
            ):
                raise OSError(
                    errno.ESTALE,
                    os.strerror(errno.ESTALE),
                    absolute_path,
                )
            _validate_source_identity(
                opened_stat,
                expected_file_identity,
                absolute_path,
            )

            file = cast(BinaryIO, os.fdopen(file_descriptor, "rb"))
            file_descriptor = -1
            return file
        finally:
            if file_descriptor >= 0:
                os.close(file_descriptor)


def mkdir(
    path: str,
    mode: int = 0o777,
    *,
    root_path: str | None = None,
    expected_root_identity: tuple[int, ...] | None = None,
) -> None:
    """Create a directory, optionally through a no-follow root anchor."""
    if root_path is None:
        os.mkdir(path, mode)
        return

    if IS_WINDOWS:
        with _windows_rooted_parent(path, root_path, expected_root_identity) as (
            absolute_path,
            _,
        ):
            os.mkdir(absolute_path, mode)
    else:
        with _posix_rooted_parent(path, root_path, expected_root_identity) as (
            parent_fd,
            name,
            absolute_path,
        ):
            os.mkdir(name, mode, dir_fd=parent_fd)


def makedirs(
    path: str,
    mode: int = 0o777,
    *,
    exist_ok: bool = False,
) -> tuple[int, int, int]:
    """Create a directory tree without following links in existing ancestors.

    Returns the device, inode, and mode of the final directory.
    """
    absolute_path = osp.normpath(osp.abspath(path))
    drive, _ = osp.splitdrive(absolute_path)
    volume_root = drive + osp.sep if drive else osp.abspath(osp.sep)
    volume_stat = os.lstat(volume_root)
    if not S_ISDIR(volume_stat.st_mode) or is_fs_link(volume_stat):
        raise NotADirectoryError(
            errno.ENOTDIR,
            os.strerror(errno.ENOTDIR),
            volume_root,
        )
    volume_identity = (
        volume_stat.st_dev,
        volume_stat.st_ino,
        volume_stat.st_mode,
    )

    missing_names: list[str] = []
    existing_path = absolute_path
    while True:
        try:
            os.lstat(existing_path)
        except (FileNotFoundError, NotADirectoryError):
            parent_path, name = osp.split(existing_path)
            if not name or parent_path == existing_path:
                raise
            missing_names.append(name)
            existing_path = parent_path
        else:
            break

    existing_snapshot = rooted_item_snapshot(
        existing_path,
        volume_root,
        expected_root_identity=volume_identity,
    )
    if not S_ISDIR(existing_snapshot[2]) or existing_snapshot[6] is not None:
        raise NotADirectoryError(
            errno.ENOTDIR,
            os.strerror(errno.ENOTDIR),
            existing_path,
        )

    if not missing_names:
        if not exist_ok:
            raise FileExistsError(
                errno.EEXIST,
                os.strerror(errno.EEXIST),
                absolute_path,
            )
        return existing_snapshot[:3]

    anchor_path = existing_path
    anchor_identity = existing_snapshot[:3]
    for name in reversed(missing_names):
        next_path = osp.join(anchor_path, name)
        try:
            mkdir(
                next_path,
                mode,
                root_path=anchor_path,
                expected_root_identity=anchor_identity,
            )
        except FileExistsError:
            pass

        next_snapshot = rooted_item_snapshot(
            next_path,
            anchor_path,
            expected_root_identity=anchor_identity,
        )
        if not S_ISDIR(next_snapshot[2]) or next_snapshot[6] is not None:
            raise NotADirectoryError(
                errno.ENOTDIR,
                os.strerror(errno.ENOTDIR),
                next_path,
            )
        anchor_path = next_path
        anchor_identity = next_snapshot[:3]

    final_snapshot = rooted_item_snapshot(
        absolute_path,
        volume_root,
        expected_root_identity=volume_identity,
    )
    if final_snapshot[:3] != anchor_identity:
        raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), absolute_path)
    return anchor_identity


def symlink(
    target: str,
    path: str,
    target_is_directory: bool = False,
    *,
    root_path: str | None = None,
    expected_root_identity: tuple[int, ...] | None = None,
) -> None:
    """Create a symbolic link, optionally through a no-follow root anchor."""
    if root_path is None:
        os.symlink(target, path, target_is_directory=target_is_directory)
        return

    if IS_WINDOWS:
        with _windows_rooted_parent(path, root_path, expected_root_identity) as (
            absolute_path,
            _,
        ):
            os.symlink(
                target,
                absolute_path,
                target_is_directory=target_is_directory,
            )
    else:
        with _posix_rooted_parent(path, root_path, expected_root_identity) as (
            parent_fd,
            name,
            absolute_path,
        ):
            os.symlink(
                target,
                name,
                target_is_directory=target_is_directory,
                dir_fd=parent_fd,
            )


def unlink(
    path: str,
    *,
    root_path: str | None = None,
    expected_root_identity: tuple[int, ...] | None = None,
    expected_target_identity: tuple[int, ...] | None = None,
) -> None:
    """Unlink a file or link, optionally through a no-follow root anchor."""
    if root_path is None:
        if expected_target_identity is not None:
            raise ValueError("A target identity requires a root path")
        os.unlink(path)
        return

    if IS_WINDOWS:
        with _windows_rooted_parent(path, root_path, expected_root_identity) as (
            absolute_path,
            _,
        ):
            _windows_delete_path(
                absolute_path,
                recursive=False,
                expect_directory=False,
                expected_target_identity=expected_target_identity,
            )
    else:
        with _posix_rooted_parent(path, root_path, expected_root_identity) as (
            parent_fd,
            name,
            absolute_path,
        ):
            if expected_target_identity is None:
                os.unlink(name, dir_fd=parent_fd)
            else:
                _posix_delete_expected_at(
                    parent_fd,
                    name,
                    absolute_path,
                    expected_target_identity,
                    recursive=False,
                    expect_directory=False,
                )


def rmdir(
    path: str,
    *,
    root_path: str | None = None,
    expected_root_identity: tuple[int, ...] | None = None,
    expected_target_identity: tuple[int, ...] | None = None,
) -> None:
    """Remove an empty directory, optionally through a no-follow root anchor."""
    if root_path is None:
        if expected_target_identity is not None:
            raise ValueError("A target identity requires a root path")
        os.rmdir(path)
        return

    if IS_WINDOWS:
        with _windows_rooted_parent(path, root_path, expected_root_identity) as (
            absolute_path,
            _,
        ):
            _windows_delete_path(
                absolute_path,
                recursive=False,
                expect_directory=True,
                expected_target_identity=expected_target_identity,
            )
    else:
        with _posix_rooted_parent(path, root_path, expected_root_identity) as (
            parent_fd,
            name,
            absolute_path,
        ):
            if expected_target_identity is None:
                os.rmdir(name, dir_fd=parent_fd)
            else:
                _posix_delete_expected_at(
                    parent_fd,
                    name,
                    absolute_path,
                    expected_target_identity,
                    recursive=False,
                    expect_directory=True,
                )


def _snapshot_identity(stat_result: os.stat_result) -> TreeSnapshotIdentity:
    return _snapshot_identity_with_content(stat_result, None)


def _snapshot_identity_with_content(
    stat_result: os.stat_result, content_identity: str | None
) -> TreeSnapshotIdentity:
    return (
        stat_result.st_dev,
        stat_result.st_ino,
        stat_result.st_mode,
        stat_result.st_size,
        stat_result.st_mtime_ns,
        stat_result.st_ctime_ns,
        content_identity,
    )


def _same_snapshot_stat(first: os.stat_result, second: os.stat_result) -> bool:
    return _snapshot_identity(first)[:-1] == _snapshot_identity(second)[:-1]


def _tree_snapshots_match(
    actual: dict[str, TreeSnapshotIdentity],
    expected: dict[str, TreeSnapshotIdentity],
    root_path: str,
) -> bool:
    """Compare complete snapshots, except for rename-updated root ctime."""
    if actual.keys() != expected.keys() or root_path not in actual:
        return False

    for path, expected_identity in expected.items():
        actual_identity = actual[path]
        if path == root_path:
            if (
                actual_identity[:5] != expected_identity[:5]
                or actual_identity[6:] != expected_identity[6:]
            ):
                return False
        elif actual_identity != expected_identity:
            return False

    return True


def _rebase_tree_snapshot(
    snapshot: dict[str, TreeSnapshotIdentity],
    source_root: str,
    destination_root: str,
) -> dict[str, TreeSnapshotIdentity]:
    """Change snapshot keys from one root path to another."""
    rebased: dict[str, TreeSnapshotIdentity] = {}
    for path, identity in snapshot.items():
        relative_path = osp.relpath(path, source_root)
        rebased_path = (
            destination_root
            if relative_path == osp.curdir
            else osp.join(destination_root, relative_path)
        )
        rebased[rebased_path] = identity
    return rebased


def _hash_posix_file_at(
    parent_fd: int, name: str, item_stat: os.stat_result, absolute_path: str
) -> tuple[os.stat_result, str]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    file_fd = os.open(name, flags, dir_fd=parent_fd)
    hasher = DropboxContentHasher()
    try:
        opened_stat = os.fstat(file_fd)
        if not _same_stat_identity(item_stat, opened_stat):
            raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), absolute_path)

        while True:
            data = os.read(file_fd, _SNAPSHOT_CHUNK_SIZE)
            if not data:
                break
            hasher.update(data)

        final_stat = os.fstat(file_fd)
        if not _same_snapshot_stat(opened_stat, final_stat):
            raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), absolute_path)

        path_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if not _same_snapshot_stat(final_stat, path_stat):
            raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), absolute_path)

        return path_stat, hasher.hexdigest()
    finally:
        os.close(file_fd)


def _read_posix_link_at(
    parent_fd: int, name: str, item_stat: os.stat_result, absolute_path: str
) -> tuple[os.stat_result, str]:
    target = os.readlink(name, dir_fd=parent_fd)
    final_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if not _same_snapshot_stat(item_stat, final_stat):
        raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), absolute_path)
    return final_stat, f"symlink:{target}"


def _posix_snapshot_at(
    parent_fd: int, name: str, absolute_path: str
) -> dict[str, TreeSnapshotIdentity]:
    item_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)

    if stat.S_ISREG(item_stat.st_mode):
        final_stat, content_identity = _hash_posix_file_at(
            parent_fd, name, item_stat, absolute_path
        )
        return {
            absolute_path: _snapshot_identity_with_content(final_stat, content_identity)
        }

    if S_ISLNK(item_stat.st_mode):
        final_stat, content_identity = _read_posix_link_at(
            parent_fd, name, item_stat, absolute_path
        )
        return {
            absolute_path: _snapshot_identity_with_content(final_stat, content_identity)
        }

    snapshot = {absolute_path: _snapshot_identity_with_content(item_stat, None)}

    if not S_ISDIR(item_stat.st_mode):
        return snapshot

    child_fd = os.open(name, _DIRECTORY_OPEN_FLAGS, dir_fd=parent_fd)
    try:
        opened_stat = os.fstat(child_fd)
        if not _same_stat_identity(item_stat, opened_stat):
            raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), absolute_path)

        with os.scandir(child_fd) as entries:
            for entry in entries:
                child_path = osp.join(absolute_path, entry.name)
                snapshot.update(_posix_snapshot_at(child_fd, entry.name, child_path))

        current_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if not _same_snapshot_stat(opened_stat, current_stat):
            raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), absolute_path)
        snapshot[absolute_path] = _snapshot_identity_with_content(current_stat, None)
    finally:
        os.close(child_fd)

    return snapshot


def _hash_windows_handle(path: str, handle: int) -> str:  # pragma: no cover
    kernel32 = _windows_kernel32()
    hasher = DropboxContentHasher()
    buffer = ctypes.create_string_buffer(_SNAPSHOT_CHUNK_SIZE)

    while True:
        bytes_read = ctypes.c_uint32()
        if not kernel32.ReadFile(
            handle,
            buffer,
            len(buffer),
            ctypes.byref(bytes_read),
            None,
        ):
            _raise_windows_error(path)
        if bytes_read.value == 0:
            break
        hasher.update(buffer.raw[: bytes_read.value])

    return hasher.hexdigest()


def _windows_directory_names(path: str) -> tuple[str, ...]:  # pragma: no cover
    with os.scandir(path) as entries:
        return tuple(sorted(entry.name for entry in entries))


def _windows_snapshot_opened(
    path: str, handle: int, info: _WindowsFileAttributeTagInfo
) -> dict[str, TreeSnapshotIdentity]:  # pragma: no cover - Windows only
    item_stat = os.lstat(path)
    is_directory = bool(info.file_attributes & _FILE_ATTRIBUTE_DIRECTORY)
    is_reparse_point = bool(info.file_attributes & _FILE_ATTRIBUTE_REPARSE_POINT)

    if is_reparse_point:
        target = os.readlink(path)
        final_stat = os.lstat(path)
        if not _same_snapshot_stat(item_stat, final_stat):
            raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), path)
        return {path: _snapshot_identity_with_content(final_stat, f"symlink:{target}")}

    if not is_directory and stat.S_ISREG(item_stat.st_mode):
        content_identity = _hash_windows_handle(path, handle)
        final_stat = os.lstat(path)

        if not _same_snapshot_stat(item_stat, final_stat):
            raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), path)
        return {path: _snapshot_identity_with_content(final_stat, content_identity)}

    snapshot = {path: _snapshot_identity_with_content(item_stat, None)}

    if is_directory:
        child_names = _windows_directory_names(path)
        children: list[tuple[str, int, _WindowsFileAttributeTagInfo]] = []
        try:
            for child_name in child_names:
                child_path = osp.join(path, child_name)
                child_handle, child_info = _open_windows_handle(
                    child_path,
                    read_access=True,
                    share_write=False,
                )
                children.append((child_path, child_handle, child_info))

            if _windows_directory_names(path) != child_names:
                raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), path)

            for child_path, child_handle, child_info in children:
                snapshot.update(
                    _windows_snapshot_opened(
                        child_path,
                        child_handle,
                        child_info,
                    )
                )

            if _windows_directory_names(path) != child_names:
                raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), path)
        finally:
            for _, child_handle, _ in reversed(children):
                _close_windows_handle(child_handle)

        final_stat = os.lstat(path)
        if not _same_snapshot_stat(item_stat, final_stat):
            raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), path)
        snapshot[path] = _snapshot_identity_with_content(final_stat, None)

    return snapshot


def _windows_snapshot_item_opened(
    path: str,
    handle: int,
    info: _WindowsFileAttributeTagInfo,
) -> TreeSnapshotIdentity:  # pragma: no cover - Windows only
    item_stat = os.lstat(path)
    is_directory = bool(info.file_attributes & _FILE_ATTRIBUTE_DIRECTORY)
    is_reparse_point = bool(info.file_attributes & _FILE_ATTRIBUTE_REPARSE_POINT)

    if is_reparse_point:
        content_identity = f"symlink:{os.readlink(path)}"
    elif not is_directory and stat.S_ISREG(item_stat.st_mode):
        content_identity = _hash_windows_handle(path, handle)
    else:
        content_identity = None

    final_stat = os.lstat(path)
    if not _same_snapshot_stat(item_stat, final_stat):
        raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), path)
    return _snapshot_identity_with_content(final_stat, content_identity)


def rooted_item_snapshot(
    path: str,
    root_path: str,
    *,
    expected_root_identity: tuple[int, ...] | None = None,
) -> TreeSnapshotIdentity:
    """Snapshot one item below a root without recursing into directories."""
    if _rooted_path_is_root(path, root_path):
        absolute_root = osp.normpath(osp.abspath(root_path))
        if IS_WINDOWS:
            handle, info = _open_windows_handle(
                absolute_root,
                read_access=True,
                share_write=False,
            )
            try:
                if info.file_attributes & _FILE_ATTRIBUTE_REPARSE_POINT:
                    raise OSError(
                        errno.ELOOP,
                        os.strerror(errno.ELOOP),
                        absolute_root,
                    )
                root_stat = os.lstat(absolute_root)
                _validate_source_identity(
                    root_stat,
                    expected_root_identity,
                    absolute_root,
                )
                return _windows_snapshot_item_opened(absolute_root, handle, info)
            finally:
                _close_windows_handle(handle)

        root_fd = os.open(absolute_root, _DIRECTORY_OPEN_FLAGS)
        try:
            root_stat = os.fstat(root_fd)
            _validate_source_identity(
                root_stat,
                expected_root_identity,
                absolute_root,
            )
            final_stat = os.lstat(absolute_root)
            if not _same_snapshot_stat(root_stat, final_stat):
                raise OSError(
                    errno.ESTALE,
                    os.strerror(errno.ESTALE),
                    absolute_root,
                )
            return _snapshot_identity_with_content(final_stat, None)
        finally:
            os.close(root_fd)

    if IS_WINDOWS:
        with _windows_rooted_parent(path, root_path, expected_root_identity) as (
            absolute_path,
            _,
        ):
            handle, info = _open_windows_handle(
                absolute_path,
                read_access=True,
                share_write=False,
            )
            try:
                return _windows_snapshot_item_opened(absolute_path, handle, info)
            finally:
                _close_windows_handle(handle)

    with _posix_rooted_parent(path, root_path, expected_root_identity) as (
        parent_fd,
        name,
        absolute_path,
    ):
        item_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)

        if S_ISLNK(item_stat.st_mode):
            target = os.readlink(name, dir_fd=parent_fd)
            final_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if not _same_snapshot_stat(item_stat, final_stat):
                raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), absolute_path)
            return _snapshot_identity_with_content(final_stat, f"symlink:{target}")

        if stat.S_ISREG(item_stat.st_mode):
            final_stat, content_identity = _hash_posix_file_at(
                parent_fd,
                name,
                item_stat,
                absolute_path,
            )
            return _snapshot_identity_with_content(final_stat, content_identity)

        if S_ISDIR(item_stat.st_mode):
            item_fd = os.open(name, _DIRECTORY_OPEN_FLAGS, dir_fd=parent_fd)
            try:
                opened_stat = os.fstat(item_fd)
                final_stat = os.stat(
                    name,
                    dir_fd=parent_fd,
                    follow_symlinks=False,
                )
                if not _same_snapshot_stat(
                    item_stat, opened_stat
                ) or not _same_snapshot_stat(opened_stat, final_stat):
                    raise OSError(
                        errno.ESTALE,
                        os.strerror(errno.ESTALE),
                        absolute_path,
                    )
            finally:
                os.close(item_fd)
        else:
            final_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if not _same_snapshot_stat(item_stat, final_stat):
                raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), absolute_path)

        return _snapshot_identity_with_content(final_stat, None)


def rooted_tree_snapshot(
    path: str,
    root_path: str,
    *,
    expected_root_identity: tuple[int, ...] | None = None,
) -> dict[str, TreeSnapshotIdentity]:
    """Snapshot one tree without following or releasing any of its ancestors.

    Keys are absolute local paths. Values contain device, inode, mode, size,
    nanosecond modification and change times, and a content identity. The content
    identity is a Dropbox hash for regular files and the target for links.
    Missing paths raise ``FileNotFoundError``.
    """
    if IS_WINDOWS:
        with _windows_rooted_parent(path, root_path, expected_root_identity) as (
            absolute_path,
            _,
        ):
            handle, info = _open_windows_handle(
                absolute_path,
                read_access=True,
                share_write=False,
            )
            try:
                return _windows_snapshot_opened(absolute_path, handle, info)
            finally:
                _close_windows_handle(handle)

    with _posix_rooted_parent(path, root_path, expected_root_identity) as (
        parent_fd,
        name,
        absolute_path,
    ):
        return _posix_snapshot_at(parent_fd, name, absolute_path)


def delete(
    path: str,
    force_case_sensitive: bool = False,
    raise_error: bool = False,
    *,
    root_path: str | None = None,
    expected_root_identity: tuple[int, ...] | None = None,
    expected_target_identity: tuple[int, ...] | None = None,
    expected_tree_snapshot: dict[str, TreeSnapshotIdentity] | None = None,
    quarantine_path: str | None = None,
) -> Optional[OSError]:
    """
    Deletes a file or folder at ``path``. Symlinks will not be followed.

    :param path: Path of item to delete.
    :param force_case_sensitive: Whether to perform the deletion only if the item
        appears with the same casing as provided in `path`. This can be used on
        case-insensitive but preserving file systems to ensure that the intended item is
        deleted.
    :param raise_error: Whether to raise errors or return them.
    :param root_path: Optional trusted root. When supplied, every ancestor below the
        root is held and checked while the item is deleted.
    :param expected_root_identity: Optional device, inode, and mode fields for the
        trusted root.
    :param expected_target_identity: Optional leading snapshot fields for the item to
        delete. A replacement item is preserved and causes ``ESTALE``.
    :param expected_tree_snapshot: Optional complete rooted snapshot. The item is moved
        to a private quarantine and deleted only if the full tree still matches.
    :param quarantine_path: Optional deterministic quarantine path for a tree-verified
        deletion. The target is moved here before verification. Pass the target path
        itself when recovery already operates on the journal-owned quarantine item.
    :returns: Any caught exception during the deletion.
    """
    err: Optional[OSError] = None

    if quarantine_path is not None and expected_tree_snapshot is None:
        raise ValueError("A quarantine path requires an expected tree snapshot")

    if root_path is not None:
        try:
            if IS_WINDOWS:
                with ExitStack() as stack:
                    absolute_path, name = stack.enter_context(
                        _windows_rooted_parent(
                            path,
                            root_path,
                            expected_root_identity,
                        )
                    )
                    absolute_quarantine_path = None
                    if quarantine_path is not None:
                        if _rooted_path_is_root(quarantine_path, path):
                            absolute_quarantine_path = absolute_path
                        else:
                            absolute_quarantine_path, _ = stack.enter_context(
                                _windows_rooted_parent(
                                    quarantine_path,
                                    root_path,
                                    expected_root_identity,
                                )
                            )
                    target_identity = expected_target_identity
                    if expected_tree_snapshot is not None:
                        try:
                            tree_root_identity = expected_tree_snapshot[absolute_path]
                        except KeyError as exc:
                            raise ValueError(
                                "The expected tree does not contain the deletion root"
                            ) from exc
                        if target_identity is None:
                            target_identity = tree_root_identity[:6]
                    if force_case_sensitive and not _rooted_name_has_exact_case(
                        osp.dirname(absolute_path), name
                    ):
                        raise FileNotFoundError(
                            errno.ENOENT, os.strerror(errno.ENOENT), path
                        )
                    _windows_delete_path(
                        absolute_path,
                        recursive=True,
                        expected_target_identity=target_identity,
                        expected_tree_snapshot=expected_tree_snapshot,
                        quarantine_path=absolute_quarantine_path,
                    )
            else:
                with ExitStack() as stack:
                    parent_fd, name, absolute_path = stack.enter_context(
                        _posix_rooted_parent(
                            path,
                            root_path,
                            expected_root_identity,
                        )
                    )
                    quarantine = None
                    if quarantine_path is not None:
                        if _rooted_path_is_root(quarantine_path, path):
                            quarantine = (parent_fd, name, absolute_path)
                        else:
                            quarantine = stack.enter_context(
                                _posix_rooted_parent(
                                    quarantine_path,
                                    root_path,
                                    expected_root_identity,
                                )
                            )
                    target_identity = expected_target_identity
                    if expected_tree_snapshot is not None:
                        try:
                            tree_root_identity = expected_tree_snapshot[absolute_path]
                        except KeyError as exc:
                            raise ValueError(
                                "The expected tree does not contain the deletion root"
                            ) from exc
                        if target_identity is None:
                            target_identity = tree_root_identity[:6]
                    if force_case_sensitive and not _rooted_name_has_exact_case(
                        parent_fd, name
                    ):
                        raise FileNotFoundError(
                            errno.ENOENT, os.strerror(errno.ENOENT), path
                        )
                    if target_identity is None:
                        _posix_delete_at(parent_fd, name, recursive=True)
                    else:
                        _posix_delete_expected_at(
                            parent_fd,
                            name,
                            absolute_path,
                            target_identity,
                            recursive=True,
                            expected_tree_snapshot=expected_tree_snapshot,
                            quarantine=quarantine,
                        )
        except OSError as exc:
            err = exc

        if raise_error and err:
            raise err
        return err

    if (
        expected_target_identity is not None
        or expected_tree_snapshot is not None
        or quarantine_path is not None
    ):
        raise ValueError(
            "A target identity, tree snapshot, or quarantine path requires a root path"
        )

    try:
        if force_case_sensitive and not equal_but_for_unicode_norm(
            path, to_existing_unnormalized_path(path)
        ):
            err = FileNotFoundError(f"No such file '{path}'")
    except OSError as exc:
        err = exc

    if err:
        if raise_error:
            raise err
        else:
            return err

    try:
        stat_result = os.lstat(path)
        if is_fs_link(stat_result):
            if getattr(stat_result, "st_reparse_tag", None) == getattr(
                stat, "IO_REPARSE_TAG_MOUNT_POINT", 0xA0000003
            ):
                os.rmdir(path)
            else:
                os.unlink(path)
        else:
            shutil.rmtree(path)
    except OSError:
        try:
            os.unlink(path)
        except OSError as e:
            err = e

    if raise_error and err:
        raise err
    else:
        return err


def move(
    src_path: str,
    dest_path: str,
    raise_error: bool = False,
    keep_target_permissions: bool = False,
    keep_target_xattrs: bool = False,
    replace: bool = True,
    metadata_source_path: str | None = None,
    root_path: str | None = None,
    expected_root_identity: tuple[int, ...] | None = None,
    expected_source_identity: tuple[int, ...] | None = None,
) -> Optional[OSError]:
    """
    Moves a file or folder from ``src_path`` to ``dest_path``. If either the source or
    the destination path no longer exists, this function does nothing unless
    ``raise_error`` is true. Any other exception is either raised or returned when
    ``raise_error`` is false.

    Uses ``os.rename`` internally.

    :param src_path: Path of item to move.
    :param dest_path: Destination path. Any existing file at this path will be replaced
        by the move. Any existing **empty** folder will be replaced if the source is
        also a folder.
    :param raise_error: Whether to raise errors or return them.
    :param keep_target_permissions: Whether to preserve the permissions of a file at the
        destination, if any.
    :param keep_target_xattrs: Whether to preserve the extended attributes of a file at
        the destination, if any.
    :param replace: Whether an existing destination may be replaced. If ``False``, the
        rename fails atomically when the destination exists.
    :param metadata_source_path: Optional path from which target permissions and
        attributes are copied. Defaults to ``dest_path``.
    :param root_path: Optional trusted root. When supplied, source and destination
        parents are opened without following links and held through the rename.
    :param expected_root_identity: Optional device, inode, and mode fields for the
        trusted root.
    :param expected_source_identity: Optional leading fields from
        ``(device, inode, mode, size, mtime_ns)``. The move fails if the source no
        longer has these fields.
    :returns: Any caught exception during the move.
    """
    err: Optional[OSError] = None
    metadata_path = metadata_source_path or dest_path

    try:
        if root_path is None:
            source_stat = os.lstat(src_path)
            _validate_source_identity(source_stat, expected_source_identity, src_path)
            if replace and expected_source_identity is not None:
                _require_same_replace_target(source_stat, dest_path)
            _copy_move_metadata(
                src_path,
                metadata_path,
                keep_target_permissions=keep_target_permissions,
                keep_target_xattrs=keep_target_xattrs,
            )
            pre_rename_stat = os.lstat(src_path)
            _validate_stable_move_source(
                source_stat,
                pre_rename_stat,
                expected_source_identity,
                src_path,
                keep_target_permissions=keep_target_permissions,
                keep_target_xattrs=keep_target_xattrs,
            )
            if replace and expected_source_identity is not None:
                _rename_no_replace(src_path, dest_path)
            elif replace:
                os.rename(src_path, dest_path)
            else:
                _rename_no_replace(src_path, dest_path)
            if expected_source_identity is not None:
                _validate_moved_source_path(
                    src_path,
                    dest_path,
                    pre_rename_stat,
                    restore=not replace,
                )
        elif IS_WINDOWS:
            with ExitStack() as stack:
                absolute_src, _ = stack.enter_context(
                    _windows_rooted_parent(src_path, root_path, expected_root_identity)
                )
                absolute_dest, _ = stack.enter_context(
                    _windows_rooted_parent(dest_path, root_path, expected_root_identity)
                )
                absolute_metadata = metadata_path
                if keep_target_permissions or keep_target_xattrs:
                    absolute_metadata, _ = stack.enter_context(
                        _windows_rooted_parent(
                            metadata_path, root_path, expected_root_identity
                        )
                    )

                source_handle, source_info = _open_windows_handle(
                    absolute_src,
                    delete_access=True,
                    read_access=True,
                    share_write=False,
                )
                stack.callback(_close_windows_handle, source_handle)
                source_stat = os.lstat(absolute_src)
                _validate_source_identity(
                    source_stat,
                    expected_source_identity,
                    absolute_src,
                )
                if replace and expected_source_identity is not None:
                    _require_same_replace_target(source_stat, absolute_dest)

                source_is_reparse = bool(
                    source_info.file_attributes & _FILE_ATTRIBUTE_REPARSE_POINT
                )
                if (
                    keep_target_permissions or keep_target_xattrs
                ) and not source_is_reparse:
                    try:
                        metadata_stat = os.lstat(absolute_metadata)
                    except (FileNotFoundError, NotADirectoryError):
                        pass
                    else:
                        if _same_stat_identity(source_stat, metadata_stat):
                            metadata_info = source_info
                        else:
                            metadata_handle, metadata_info = _open_windows_handle(
                                absolute_metadata,
                                read_access=True,
                            )
                            stack.callback(_close_windows_handle, metadata_handle)
                        if not metadata_info.file_attributes & (
                            _FILE_ATTRIBUTE_REPARSE_POINT
                        ):
                            _copy_move_metadata(
                                absolute_src,
                                absolute_metadata,
                                keep_target_permissions=keep_target_permissions,
                                keep_target_xattrs=keep_target_xattrs,
                            )

                pre_rename_stat = os.lstat(absolute_src)
                _validate_stable_move_source(
                    source_stat,
                    pre_rename_stat,
                    expected_source_identity,
                    absolute_src,
                    keep_target_permissions=keep_target_permissions,
                    keep_target_xattrs=keep_target_xattrs,
                )
                _windows_rename_opened(
                    absolute_src,
                    source_handle,
                    absolute_dest,
                    replace=replace,
                )
                moved_stat = os.lstat(absolute_dest)
                if (
                    _snapshot_identity(pre_rename_stat)[:5]
                    != _snapshot_identity(moved_stat)[:5]
                ):
                    raise OSError(
                        errno.ESTALE,
                        "Source changed during move",
                        absolute_src,
                        absolute_dest,
                    )
        else:
            with ExitStack() as stack:
                src_parent_fd, src_name, absolute_src = stack.enter_context(
                    _posix_rooted_parent(src_path, root_path, expected_root_identity)
                )
                dest_parent_fd, dest_name, _ = stack.enter_context(
                    _posix_rooted_parent(dest_path, root_path, expected_root_identity)
                )
                metadata_parent_fd = dest_parent_fd
                metadata_name = dest_name
                if keep_target_permissions or keep_target_xattrs:
                    metadata_parent_fd, metadata_name, _ = stack.enter_context(
                        _posix_rooted_parent(
                            metadata_path, root_path, expected_root_identity
                        )
                    )

                source_stat = os.stat(
                    src_name,
                    dir_fd=src_parent_fd,
                    follow_symlinks=False,
                )
                _validate_source_identity(
                    source_stat,
                    expected_source_identity,
                    absolute_src,
                )
                if replace and expected_source_identity is not None:
                    try:
                        destination_stat = os.stat(
                            dest_name,
                            dir_fd=dest_parent_fd,
                            follow_symlinks=False,
                        )
                    except (FileNotFoundError, NotADirectoryError) as exc:
                        raise OSError(
                            errno.ESTALE,
                            "A replace move requires the same source and destination",
                            dest_name,
                        ) from exc
                    if not _same_stat_identity(source_stat, destination_stat):
                        raise OSError(
                            errno.ESTALE,
                            "A replace move requires the same source and destination",
                            dest_name,
                        )
                with _copy_move_metadata_at(
                    src_parent_fd,
                    src_name,
                    metadata_parent_fd,
                    metadata_name,
                    keep_target_permissions=keep_target_permissions,
                    keep_target_xattrs=keep_target_xattrs,
                ):
                    pre_rename_stat = os.stat(
                        src_name,
                        dir_fd=src_parent_fd,
                        follow_symlinks=False,
                    )
                    _validate_stable_move_source(
                        source_stat,
                        pre_rename_stat,
                        expected_source_identity,
                        absolute_src,
                        keep_target_permissions=keep_target_permissions,
                        keep_target_xattrs=keep_target_xattrs,
                    )
                    backup_name = None
                    if expected_source_identity is not None:
                        backup_name = _posix_link_move_source_at(
                            src_parent_fd,
                            src_name,
                            pre_rename_stat,
                        )
                    try:
                        if replace and expected_source_identity is not None:
                            _rename_no_replace_at(
                                src_parent_fd,
                                src_name,
                                dest_parent_fd,
                                dest_name,
                            )
                        elif replace:
                            os.rename(
                                src_name,
                                dest_name,
                                src_dir_fd=src_parent_fd,
                                dst_dir_fd=dest_parent_fd,
                            )
                        else:
                            _rename_no_replace_at(
                                src_parent_fd,
                                src_name,
                                dest_parent_fd,
                                dest_name,
                            )
                    except BaseException:
                        if backup_name is not None:
                            backup_stat = os.stat(
                                backup_name,
                                dir_fd=src_parent_fd,
                                follow_symlinks=False,
                            )
                            try:
                                current_source_stat = os.stat(
                                    src_name,
                                    dir_fd=src_parent_fd,
                                    follow_symlinks=False,
                                )
                            except (FileNotFoundError, NotADirectoryError):
                                current_source_stat = None
                            try:
                                current_destination_stat = os.stat(
                                    dest_name,
                                    dir_fd=dest_parent_fd,
                                    follow_symlinks=False,
                                )
                            except (FileNotFoundError, NotADirectoryError):
                                current_destination_stat = None

                            if current_source_stat is not None and _same_stat_identity(
                                backup_stat, current_source_stat
                            ):
                                os.unlink(backup_name, dir_fd=src_parent_fd)
                            elif (
                                current_destination_stat is not None
                                and _same_stat_identity(
                                    backup_stat, current_destination_stat
                                )
                            ):
                                os.unlink(backup_name, dir_fd=src_parent_fd)
                            else:
                                _posix_restore_move_source_at(
                                    src_parent_fd,
                                    backup_name,
                                    src_name,
                                    absolute_src,
                                    dest_path,
                                )
                        raise

                    if expected_source_identity is not None:
                        try:
                            moved_stat = os.stat(
                                dest_name,
                                dir_fd=dest_parent_fd,
                                follow_symlinks=False,
                            )
                        except (FileNotFoundError, NotADirectoryError) as exc:
                            if backup_name is not None:
                                _posix_restore_move_source_at(
                                    src_parent_fd,
                                    backup_name,
                                    src_name,
                                    absolute_src,
                                    dest_path,
                                )
                            raise OSError(
                                errno.ESTALE,
                                "Moved source disappeared before validation",
                                dest_path,
                            ) from exc
                        if (
                            _snapshot_identity(pre_rename_stat)[:5]
                            != _snapshot_identity(moved_stat)[:5]
                        ):
                            if backup_name is not None:
                                _posix_restore_move_source_at(
                                    src_parent_fd,
                                    backup_name,
                                    src_name,
                                    absolute_src,
                                    dest_path,
                                )
                            raise OSError(
                                errno.ESTALE,
                                "Source changed during move",
                                absolute_src,
                                dest_path,
                            )
                    if backup_name is not None:
                        os.unlink(backup_name, dir_fd=src_parent_fd)
    except FileNotFoundError:
        if raise_error:
            raise
    except OSError as exc:
        err = exc

    if raise_error and err:
        raise err
    else:
        return err


def _require_same_replace_target(
    source_stat: os.stat_result, destination_path: str
) -> None:
    try:
        destination_stat = os.lstat(destination_path)
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise OSError(
            errno.ESTALE,
            "A replace move requires the same source and destination",
            destination_path,
        ) from exc

    if not _same_stat_identity(source_stat, destination_stat):
        raise OSError(
            errno.ESTALE,
            "A replace move requires the same source and destination",
            destination_path,
        )


def _validate_moved_source_path(
    source_path: str,
    destination_path: str,
    source_stat: os.stat_result,
    *,
    restore: bool,
) -> None:
    try:
        moved_stat = os.lstat(destination_path)
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise OSError(
            errno.ESTALE,
            "Moved source disappeared before validation",
            destination_path,
        ) from exc

    if _snapshot_identity(source_stat)[:5] == _snapshot_identity(moved_stat)[:5]:
        return

    if restore:
        try:
            _rename_no_replace(destination_path, source_path)
        except FileExistsError as exc:
            raise OSError(
                errno.ESTALE,
                "Moved replacement remains at destination",
                source_path,
                destination_path,
            ) from exc

    raise OSError(
        errno.ESTALE,
        "Source changed during move",
        source_path,
        destination_path,
    )


def _validate_source_identity(
    stat_result: os.stat_result,
    expected_identity: tuple[int, ...] | None,
    path: str,
) -> None:
    if expected_identity is None:
        return

    actual_identity = _snapshot_identity(stat_result)
    if actual_identity[: len(expected_identity)] != expected_identity:
        raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), path)


def _validate_stable_move_source(
    initial_stat: os.stat_result,
    current_stat: os.stat_result,
    expected_identity: tuple[int, ...] | None,
    path: str,
    *,
    keep_target_permissions: bool,
    keep_target_xattrs: bool,
) -> None:
    """Verify stable source fields immediately before a rename."""
    if not _same_stat_identity(initial_stat, current_stat):
        raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), path)

    initial_identity = _snapshot_identity(initial_stat)
    current_identity = _snapshot_identity(current_stat)
    if initial_identity[3:5] != current_identity[3:5]:
        raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), path)

    if expected_identity is None:
        return

    changed_by_metadata = {6}
    if keep_target_permissions:
        changed_by_metadata.add(2)
    if keep_target_permissions or keep_target_xattrs:
        changed_by_metadata.add(5)

    for index, expected_value in enumerate(expected_identity):
        if (
            index not in changed_by_metadata
            and current_identity[index] != expected_value
        ):
            raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), path)


def _posix_link_move_source_at(
    parent_fd: int,
    name: str,
    source_stat: os.stat_result,
) -> str | None:
    """Hold a regular file or symlink under a private name through a rename."""
    if not (stat.S_ISREG(source_stat.st_mode) or S_ISLNK(source_stat.st_mode)):
        return None

    for _ in range(100):
        backup_name = f"{MOVE_TEMP_PREFIX}{secrets.token_hex(16)}"
        try:
            os.link(
                name,
                backup_name,
                src_dir_fd=parent_fd,
                dst_dir_fd=parent_fd,
                follow_symlinks=False,
            )
        except FileExistsError:
            continue
        backup_stat = os.stat(
            backup_name,
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
        if (
            not _same_stat_identity(source_stat, backup_stat)
            or _snapshot_identity(source_stat)[3:5]
            != _snapshot_identity(backup_stat)[3:5]
        ):
            os.unlink(backup_name, dir_fd=parent_fd)
            raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), name)
        return backup_name

    raise FileExistsError("Could not reserve a private move backup")


def _posix_restore_move_source_at(
    parent_fd: int,
    backup_name: str,
    source_name: str,
    absolute_source: str,
    destination_path: str,
) -> None:
    """Restore a held move source without displacing a concurrent replacement."""
    try:
        _rename_no_replace_at(
            parent_fd,
            backup_name,
            parent_fd,
            source_name,
        )
    except FileExistsError as exc:
        recovery_name = ""
        for _ in range(100):
            recovery_name = f"Maestral preserved {secrets.token_hex(8)}"
            try:
                _rename_no_replace_at(
                    parent_fd,
                    backup_name,
                    parent_fd,
                    recovery_name,
                )
            except FileExistsError:
                continue
            break
        else:
            raise OSError(
                errno.ESTALE,
                f"Source changed; original remains as '{backup_name}'",
                absolute_source,
                destination_path,
            ) from exc
        raise OSError(
            errno.ESTALE,
            f"Source changed; original preserved as '{recovery_name}'",
            absolute_source,
            destination_path,
        ) from exc

    raise OSError(
        errno.ESTALE,
        "Source changed during move; original restored",
        absolute_source,
        destination_path,
    )


def _copy_move_metadata(
    src_path: str,
    metadata_path: str,
    *,
    keep_target_permissions: bool,
    keep_target_xattrs: bool,
) -> None:
    """Copy requested destination metadata to a move source."""
    if keep_target_permissions:
        try:
            dest_mode = os.lstat(metadata_path).st_mode & 0o777
            follow_symlinks = os.chmod not in os.supports_follow_symlinks
            os.chmod(src_path, dest_mode, follow_symlinks=follow_symlinks)
        except (FileNotFoundError, NotADirectoryError):
            pass

    if keep_target_xattrs and xattr is not None:
        try:
            dest_attrs = xattr.xattr(metadata_path)
            for key, value in dest_attrs.iteritems():
                if key.startswith("user.") or not IS_LINUX:
                    xattr.setxattr(src_path, key, value)
        except OSError:
            # Fail gracefully if extended attributes are not supported by the system.
            pass


def _open_posix_metadata_item(
    parent_fd: int, name: str, item_stat: os.stat_result
) -> int:
    if S_ISDIR(item_stat.st_mode):
        flags = _DIRECTORY_OPEN_FLAGS
    else:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)

    file_descriptor = os.open(name, flags, dir_fd=parent_fd)
    opened_stat = os.fstat(file_descriptor)
    if not _same_stat_identity(item_stat, opened_stat):
        os.close(file_descriptor)
        raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), name)
    return file_descriptor


@contextmanager
def _copy_move_metadata_at(
    src_parent_fd: int,
    src_name: str,
    metadata_parent_fd: int,
    metadata_name: str,
    *,
    keep_target_permissions: bool,
    keep_target_xattrs: bool,
) -> Iterator[None]:
    """Copy metadata through held descriptors and keep the source bound."""
    if not keep_target_permissions and not keep_target_xattrs:
        yield
        return

    try:
        source_stat = os.stat(
            src_name,
            dir_fd=src_parent_fd,
            follow_symlinks=False,
        )
        metadata_stat = os.stat(
            metadata_name,
            dir_fd=metadata_parent_fd,
            follow_symlinks=False,
        )
    except (FileNotFoundError, NotADirectoryError):
        yield
        return

    safe_types = (stat.S_ISREG, stat.S_ISDIR)
    if not any(check(source_stat.st_mode) for check in safe_types) or not any(
        check(metadata_stat.st_mode) for check in safe_types
    ):
        yield
        return

    try:
        source_fd = _open_posix_metadata_item(src_parent_fd, src_name, source_stat)
        try:
            metadata_fd = _open_posix_metadata_item(
                metadata_parent_fd, metadata_name, metadata_stat
            )
        except BaseException:
            os.close(source_fd)
            raise
    except OSError:
        if keep_target_permissions:
            raise
        yield
        return

    try:
        if keep_target_permissions:
            os.fchmod(source_fd, os.fstat(metadata_fd).st_mode & 0o777)

        if keep_target_xattrs and xattr is not None:
            try:
                metadata_attrs = xattr.xattr(metadata_fd)
                for key, value in metadata_attrs.iteritems():
                    if key.startswith("user.") or not IS_LINUX:
                        xattr.setxattr(source_fd, key, value)
            except OSError:
                pass

        current_stat = os.stat(
            src_name,
            dir_fd=src_parent_fd,
            follow_symlinks=False,
        )
        opened_stat = os.fstat(source_fd)
        if (
            not _same_stat_identity(opened_stat, current_stat)
            or _snapshot_identity(source_stat)[3:5]
            != _snapshot_identity(opened_stat)[3:5]
        ):
            raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), src_name)

        yield
    finally:
        os.close(metadata_fd)
        os.close(source_fd)


def _rename_no_replace(src_path: str, dest_path: str) -> None:
    """Rename without replacing an existing destination."""
    if IS_WINDOWS:
        # os.rename already refuses to replace an existing destination on Windows.
        os.rename(src_path, dest_path)
        return

    libc = ctypes.CDLL(None, use_errno=True)
    src_bytes = os.fsencode(src_path)
    dest_bytes = os.fsencode(dest_path)

    if IS_LINUX:
        try:
            renameat2 = libc.renameat2
        except AttributeError as exc:
            raise OSError(
                errno.ENOTSUP,
                "Atomic no-replace rename is not supported",
                src_path,
                dest_path,
            ) from exc
        renameat2.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        renameat2.restype = ctypes.c_int
        result = renameat2(-100, src_bytes, -100, dest_bytes, 1)
    elif platform.system() == "Darwin":
        renamex_np = libc.renamex_np
        renamex_np.argtypes = [
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        renamex_np.restype = ctypes.c_int
        result = renamex_np(src_bytes, dest_bytes, 0x00000004)
    else:
        raise OSError(
            errno.ENOTSUP,
            "Atomic no-replace rename is not supported",
            src_path,
            dest_path,
        )

    if result != 0:
        error_number = ctypes.get_errno()
        raise OSError(
            error_number,
            os.strerror(error_number),
            src_path,
            dest_path,
        )


def _rename_no_replace_at(
    src_parent_fd: int,
    src_name: str,
    dest_parent_fd: int,
    dest_name: str,
) -> None:
    """Rename relative to held parent descriptors without replacing the destination."""
    libc = ctypes.CDLL(None, use_errno=True)
    src_bytes = os.fsencode(src_name)
    dest_bytes = os.fsencode(dest_name)

    if IS_LINUX:
        try:
            renameat2 = libc.renameat2
        except AttributeError as exc:
            raise OSError(
                errno.ENOTSUP,
                "Atomic no-replace rename is not supported",
                src_name,
                dest_name,
            ) from exc
        renameat2.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        renameat2.restype = ctypes.c_int
        result = renameat2(
            src_parent_fd,
            src_bytes,
            dest_parent_fd,
            dest_bytes,
            1,
        )
    elif platform.system() == "Darwin":
        try:
            renameatx_np = libc.renameatx_np
        except AttributeError as exc:
            raise OSError(
                errno.ENOTSUP,
                "Atomic no-replace rename is not supported",
                src_name,
                dest_name,
            ) from exc
        renameatx_np.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        renameatx_np.restype = ctypes.c_int
        result = renameatx_np(
            src_parent_fd,
            src_bytes,
            dest_parent_fd,
            dest_bytes,
            0x00000004,
        )
    else:
        raise OSError(
            errno.ENOTSUP,
            "Atomic no-replace rename is not supported",
            src_name,
            dest_name,
        )

    if result != 0:
        error_number = ctypes.get_errno()
        raise OSError(
            error_number,
            os.strerror(error_number),
            src_name,
            dest_name,
        )


def walk(
    root: str | bytes,
    listdir: Callable[[str], Iterable["os.DirEntry[str]"]] = os.scandir,
) -> Iterator[Tuple[str, os.stat_result]]:
    """
    Iterates recursively over the content of a folder.

    :param root: Root folder to walk.
    :param listdir: Function to call to get the folder content.
    :returns: Iterator over (path, stat) results.
    """
    try:
        entries = listdir(os.fsdecode(root))
    except OSError as exc:
        if exc.errno in (errno.ENOENT, errno.ENOTDIR, errno.EINVAL):
            return
        raise

    for entry in entries:
        try:
            path = entry.path
            # DirEntry may cache stale stat data on Windows. Query the path again so
            # that files removed after directory enumeration are not yielded.
            stat = os.lstat(path)

            yield path, stat

            if S_ISDIR(stat.st_mode) and not is_fs_link(stat):
                yield from walk(entry.path, listdir)

        except OSError as exc:
            # Directory may have been deleted between finding it in the directory
            # list of its parent and trying to list its contents. If this
            # happens we treat it as empty. Likewise, if the directory was replaced
            # with a file of the same name (less likely, but possible), it will be
            # treated as empty.
            if exc.errno in (errno.ENOENT, errno.ENOTDIR, errno.EINVAL):
                continue
            else:
                raise


# ==== miscellaneous utilities =========================================================


def content_hash(
    local_path: str, chunk_size: int = 65536
) -> Tuple[Optional[str], Optional[float]]:
    """
    Computes content hash of a local file.

    :param local_path: Absolute path on local drive.
    :param chunk_size: Size of chunks to hash in bytes.
    :returns: Content hash to compare with Dropbox's content hash and mtime just before
        the hash was computed.
    """
    hasher = DropboxContentHasher()

    try:
        mtime = os.lstat(local_path).st_mtime

        try:
            with open(local_path, "rb", opener=opener_no_symlink) as f:
                while True:
                    chunk = f.read(chunk_size)
                    if len(chunk) == 0:
                        break
                    hasher.update(chunk)

        except IsADirectoryError:
            return "folder", mtime

        except OSError as exc:
            if exc.errno == errno.ELOOP:
                hasher.update(b"")  # use empty file for symlinks
            else:
                raise exc

        return str(hasher.hexdigest()), mtime

    except FileNotFoundError:
        return None, None
    except NotADirectoryError:
        # a parent directory in the path refers to a file instead of a folder
        return None, None
    finally:
        del hasher


def fs_max_lengths_for_path(path: str = "/") -> Tuple[int, int]:
    """
    Return the maximum length of file names and paths allowed on a file system.

    :param path: Path to check. This can be specified because different paths may be
        residing on different file systems. If the given path does not exist, the first
        existing parent directory in the tree be taken.
    :returns: Tuple giving the maximum file name and total path lengths.
    """
    path = osp.abspath(path)
    dirname = osp.dirname(path)

    if not hasattr(os, "pathconf"):
        raise RuntimeError("Cannot get file length limits.")

    while True:
        try:
            max_char_name = os.pathconf(dirname, "PC_NAME_MAX")
            max_char_path = os.pathconf(dirname, "PC_PATH_MAX")
            return max_char_name, max_char_path
        except (FileNotFoundError, NotADirectoryError):
            dirname = osp.dirname(dirname)
        except ValueError:
            raise RuntimeError("Cannot get file length limits.")
        except OSError as exc:
            if exc.errno == errno.EINVAL:
                raise RuntimeError("Cannot get file length limits.")
            else:
                dirname = "/"


# ==== symlink-proof os methods ========================================================


def _posix_rooted_walk_directory(
    directory_fd: int,
    absolute_path: str,
    opened_stat: os.stat_result,
    should_recurse: Callable[[str, os.stat_result], bool] | None,
    include_root: bool = False,
) -> Iterator[Tuple[str, os.stat_result]]:
    if include_root:
        yield absolute_path, opened_stat

    with os.scandir(directory_fd) as entries:
        child_names = tuple(sorted(entry.name for entry in entries))

    for child_name in child_names:
        child_path = osp.join(absolute_path, child_name)
        try:
            child_stat = os.stat(
                child_name,
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except OSError as exc:
            raise OSError(
                errno.ESTALE,
                os.strerror(errno.ESTALE),
                child_path,
            ) from exc

        if S_ISDIR(child_stat.st_mode) and not S_ISLNK(child_stat.st_mode):
            try:
                child_fd = os.open(
                    child_name,
                    _DIRECTORY_OPEN_FLAGS,
                    dir_fd=directory_fd,
                )
            except OSError as exc:
                raise OSError(
                    errno.ESTALE,
                    os.strerror(errno.ESTALE),
                    child_path,
                ) from exc

            try:
                child_opened_stat = os.fstat(child_fd)
                if not _same_snapshot_stat(child_stat, child_opened_stat):
                    raise OSError(
                        errno.ESTALE,
                        os.strerror(errno.ESTALE),
                        child_path,
                    )
                yield child_path, child_opened_stat
                if should_recurse is None or should_recurse(
                    child_path, child_opened_stat
                ):
                    yield from _posix_rooted_walk_directory(
                        child_fd,
                        child_path,
                        child_opened_stat,
                        should_recurse,
                        False,
                    )
                try:
                    final_child_stat = os.stat(
                        child_name,
                        dir_fd=directory_fd,
                        follow_symlinks=False,
                    )
                except OSError as exc:
                    raise OSError(
                        errno.ESTALE,
                        os.strerror(errno.ESTALE),
                        child_path,
                    ) from exc
                if not _same_snapshot_stat(child_opened_stat, final_child_stat):
                    raise OSError(
                        errno.ESTALE,
                        os.strerror(errno.ESTALE),
                        child_path,
                    )
            finally:
                os.close(child_fd)
        else:
            yield child_path, child_stat
            try:
                final_child_stat = os.stat(
                    child_name,
                    dir_fd=directory_fd,
                    follow_symlinks=False,
                )
            except OSError as exc:
                raise OSError(
                    errno.ESTALE,
                    os.strerror(errno.ESTALE),
                    child_path,
                ) from exc
            if not _same_snapshot_stat(child_stat, final_child_stat):
                raise OSError(
                    errno.ESTALE,
                    os.strerror(errno.ESTALE),
                    child_path,
                )

    with os.scandir(directory_fd) as entries:
        final_child_names = tuple(sorted(entry.name for entry in entries))
    if final_child_names != child_names:
        raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), absolute_path)

    final_stat = os.fstat(directory_fd)
    if not _same_snapshot_stat(opened_stat, final_stat):
        raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), absolute_path)


def _windows_rooted_walk_opened(
    path: str,
    handle: int,
    info: _WindowsFileAttributeTagInfo,
    should_recurse: Callable[[str, os.stat_result], bool] | None,
    include_root: bool = False,
) -> Iterator[Tuple[str, os.stat_result]]:  # pragma: no cover - Windows only
    is_directory = bool(info.file_attributes & _FILE_ATTRIBUTE_DIRECTORY)
    is_reparse_point = bool(info.file_attributes & _FILE_ATTRIBUTE_REPARSE_POINT)
    opened_stat = os.lstat(path)
    if include_root:
        yield path, opened_stat
    if not is_directory or is_reparse_point:
        return

    child_names = _windows_directory_names(path)
    children: list[tuple[str, int, _WindowsFileAttributeTagInfo, os.stat_result]] = []
    try:
        for child_name in child_names:
            child_path = osp.join(path, child_name)
            try:
                child_handle, child_info = _open_windows_handle(
                    child_path,
                    read_access=True,
                    share_write=False,
                )
            except OSError as exc:
                raise OSError(
                    errno.ESTALE,
                    os.strerror(errno.ESTALE),
                    child_path,
                ) from exc
            children.append(
                (child_path, child_handle, child_info, os.lstat(child_path))
            )

        if _windows_directory_names(path) != child_names:
            raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), path)

        for child_path, child_handle, child_info, child_stat in children:
            yield child_path, child_stat
            child_is_directory = bool(
                child_info.file_attributes & _FILE_ATTRIBUTE_DIRECTORY
            )
            child_is_reparse = bool(
                child_info.file_attributes & _FILE_ATTRIBUTE_REPARSE_POINT
            )
            if (
                child_is_directory
                and not child_is_reparse
                and (should_recurse is None or should_recurse(child_path, child_stat))
            ):
                yield from _windows_rooted_walk_opened(
                    child_path,
                    child_handle,
                    child_info,
                    should_recurse,
                    False,
                )

            final_child_stat = os.lstat(child_path)
            if not _same_snapshot_stat(child_stat, final_child_stat):
                raise OSError(
                    errno.ESTALE,
                    os.strerror(errno.ESTALE),
                    child_path,
                )

        if _windows_directory_names(path) != child_names:
            raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), path)
    finally:
        for _, child_handle, _, _ in reversed(children):
            _close_windows_handle(child_handle)

    final_stat = os.lstat(path)
    if not _same_snapshot_stat(opened_stat, final_stat):
        raise OSError(errno.ESTALE, os.strerror(errno.ESTALE), path)


def rooted_walk(
    path: str,
    root_path: str,
    *,
    expected_root_identity: tuple[int, ...] | None = None,
    should_recurse: Callable[[str, os.stat_result], bool] | None = None,
    include_root: bool = False,
) -> Iterator[Tuple[str, os.stat_result]]:
    """Walk descendants through held, no-follow directory handles.

    ``should_recurse`` receives an anchored directory path and its opened stat. A false
    result keeps the directory in the output but prunes its descendants. Set
    ``include_root`` to include ``path`` itself in the buffered result.
    """
    try:
        if _rooted_path_is_root(path, root_path):
            absolute_root = osp.normpath(osp.abspath(root_path))
            if IS_WINDOWS:
                handle, info = _open_windows_handle(
                    absolute_root,
                    read_access=True,
                    share_write=False,
                )
                try:
                    if info.file_attributes & _FILE_ATTRIBUTE_REPARSE_POINT:
                        raise OSError(
                            errno.ELOOP,
                            os.strerror(errno.ELOOP),
                            absolute_root,
                        )
                    root_stat = os.lstat(absolute_root)
                    _validate_source_identity(
                        root_stat,
                        expected_root_identity,
                        absolute_root,
                    )
                    walked = tuple(
                        _windows_rooted_walk_opened(
                            absolute_root,
                            handle,
                            info,
                            should_recurse,
                            include_root,
                        )
                    )
                finally:
                    _close_windows_handle(handle)
            else:
                directory_fd = os.open(absolute_root, _DIRECTORY_OPEN_FLAGS)
                try:
                    root_stat = os.fstat(directory_fd)
                    _validate_source_identity(
                        root_stat,
                        expected_root_identity,
                        absolute_root,
                    )
                    walked = tuple(
                        _posix_rooted_walk_directory(
                            directory_fd,
                            absolute_root,
                            root_stat,
                            should_recurse,
                            include_root,
                        )
                    )
                    final_stat = os.lstat(absolute_root)
                    if not _same_snapshot_stat(root_stat, final_stat):
                        raise OSError(
                            errno.ESTALE,
                            os.strerror(errno.ESTALE),
                            absolute_root,
                        )
                finally:
                    os.close(directory_fd)
            yield from walked
            return

        if IS_WINDOWS:
            with _windows_rooted_parent(path, root_path, expected_root_identity) as (
                absolute_path,
                _,
            ):
                handle, info = _open_windows_handle(
                    absolute_path,
                    read_access=True,
                    share_write=False,
                )
                try:
                    walked = tuple(
                        _windows_rooted_walk_opened(
                            absolute_path,
                            handle,
                            info,
                            should_recurse,
                            include_root,
                        )
                    )
                finally:
                    _close_windows_handle(handle)
            yield from walked
            return

        with _posix_rooted_parent(path, root_path, expected_root_identity) as (
            parent_fd,
            name,
            absolute_path,
        ):
            item_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if not S_ISDIR(item_stat.st_mode) or S_ISLNK(item_stat.st_mode):
                if not include_root:
                    return
                final_stat = os.stat(
                    name,
                    dir_fd=parent_fd,
                    follow_symlinks=False,
                )
                if not _same_snapshot_stat(item_stat, final_stat):
                    raise OSError(
                        errno.ESTALE,
                        os.strerror(errno.ESTALE),
                        absolute_path,
                    )
                walked = ((absolute_path, final_stat),)
            else:
                directory_fd = os.open(
                    name,
                    _DIRECTORY_OPEN_FLAGS,
                    dir_fd=parent_fd,
                )
                try:
                    opened_stat = os.fstat(directory_fd)
                    if not _same_stat_identity(item_stat, opened_stat):
                        raise OSError(
                            errno.ESTALE,
                            os.strerror(errno.ESTALE),
                            absolute_path,
                        )
                    walked = tuple(
                        _posix_rooted_walk_directory(
                            directory_fd,
                            absolute_path,
                            opened_stat,
                            should_recurse,
                            include_root,
                        )
                    )
                    final_stat = os.stat(
                        name,
                        dir_fd=parent_fd,
                        follow_symlinks=False,
                    )
                    if not _same_snapshot_stat(opened_stat, final_stat):
                        raise OSError(
                            errno.ESTALE,
                            os.strerror(errno.ESTALE),
                            absolute_path,
                        )
                finally:
                    os.close(directory_fd)
        yield from walked
    except OSError as exc:
        if exc.errno in (errno.ENOENT, errno.ENOTDIR, errno.EINVAL):
            return
        raise


def opener_no_symlink(path: _AnyPath, flags: int) -> int:
    """
    Opener that does not follow symlinks. Uses :meth:`os.open` under the hood.

    :param path: Path to open.
    :param flags: Flags passed to :meth:`os.open`. O_NOFOLLOW will be added.
    :return: Open file descriptor.
    """
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    if no_follow:
        flags |= no_follow
    else:
        try:
            if is_fs_link(os.lstat(path)):
                raise OSError(errno.ELOOP, os.strerror(errno.ELOOP), path)
        except FileNotFoundError:
            pass
    return os.open(path, flags=flags)


def get_local_change_time(stat: os.stat_result) -> float:
    """Return the platform timestamp used to detect a local file change."""
    return stat.st_mtime if IS_WINDOWS else stat.st_ctime


def get_local_change_time_ns(stat: os.stat_result) -> int:
    """Return the nanosecond platform timestamp used during file transfers."""
    return stat.st_mtime_ns if IS_WINDOWS else stat.st_ctime_ns


def exists(path: _AnyPath) -> bool:
    """Returns whether an item exists at the path. Returns True for symlinks."""
    try:
        os.lstat(path)
    except (FileNotFoundError, NotADirectoryError):
        return False
    else:
        return True


def isfile(path: _AnyPath) -> bool:
    """Returns whether a file exists at the path. Returns True for symlinks."""
    try:
        stat = os.lstat(path)
        return is_fs_link(stat) or not S_ISDIR(stat.st_mode)
    except (FileNotFoundError, NotADirectoryError):
        return False


def isdir(path: _AnyPath) -> bool:
    """Returns whether a folder exists at the path. Returns False for symlinks."""
    try:
        stat = os.lstat(path)
        return S_ISDIR(stat.st_mode) and not is_fs_link(stat)
    except (FileNotFoundError, NotADirectoryError):
        return False


def getsize(path: _AnyPath) -> int:
    """Returns the size. Returns False for symlinks."""
    return os.lstat(path).st_size


def equal_but_for_unicode_norm(s0: str, s1: str) -> bool:
    return normalize_unicode(s0) == normalize_unicode(s1)


def get_symlink_target(local_path: str) -> Optional[str]:
    """
    Returns the symlink target of a file.

    :param local_path: Absolute path on local drive.
    :returns: Symlink target of local file. None if the local path does not refer to
        a symlink or does not exist.
    """
    try:
        return os.readlink(local_path)
    except (FileNotFoundError, NotADirectoryError):
        return None
    except OSError as err:
        if err.errno == errno.EINVAL:
            # File is not a symlink.
            return None

        if err.errno == errno.ENAMETOOLONG:
            # Path cannot exist on this filesystem.
            return None

        raise err
