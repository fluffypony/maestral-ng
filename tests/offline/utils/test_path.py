import os
import platform
import stat

import pytest

try:
    import xattr
except ImportError:
    xattr = None

from maestral.constants import IS_LINUX
from maestral.utils.appdirs import get_home_dir
from maestral.utils.path import (
    delete,
    fs_max_lengths_for_path,
    get_existing_equivalent_paths,
    is_child,
    is_equal_or_child,
    is_fs_case_sensitive,
    move,
    normalized_path_exists,
    walk,
)


def touch(path: str) -> None:
    open(path, "w").close()


def test_normalized_path_exists(tmp_path):
    # Assert that an existing path is found, even when a different casing is used.

    path = str(tmp_path)

    assert normalized_path_exists(path)
    assert normalized_path_exists(path.title())
    assert normalized_path_exists(path.upper())

    # Assert that a non-existent path is identified.
    path = str(tmp_path / "path_928")
    assert not normalized_path_exists(path)

    # Assert that specifying a non-existing root returns False.
    child_path = str(tmp_path / "path_928" / "content")
    assert not normalized_path_exists(child_path, root=path)


def test_get_existing_equivalent_paths(tmp_path):
    # Test that we can find a unique correctly cased path
    # starting from a candidate with scrambled casing.

    path = str(tmp_path)

    candidates = get_existing_equivalent_paths(path.upper())

    assert candidates == [path]

    candidates = get_existing_equivalent_paths("/test", root=path)

    assert len(candidates) == 0


@pytest.mark.skipif(
    not is_fs_case_sensitive(get_home_dir()),
    reason="requires case-sensitive file system",
)
def test_multiple_existing_equivalent_paths(tmp_path):
    # test that we can get multiple cased path
    # candidates on case-sensitive file systems

    # create two folders that differ only in casing

    dir0 = tmp_path / "TeSt foLder/subfolder"
    dir1 = tmp_path / "Test Folder/subfolder"

    dir0.mkdir(parents=True, exist_ok=True)
    dir1.mkdir(parents=True, exist_ok=True)

    dir0 = str(dir0)
    dir1 = str(dir1)

    # scramble the casing and check if we can find matches
    candidates = get_existing_equivalent_paths(dir0.lower())

    assert set(candidates) == {dir0, dir1}

    # find matches for children
    candidates = get_existing_equivalent_paths(
        "/test folder/subfolder", root=str(tmp_path)
    )

    assert set(candidates) == {dir0, dir1}


def test_is_child():
    assert is_child("/parent/path/child", "/parent/path/")
    assert is_child("/parent/path/child/", "/parent/path")
    assert not is_child("/parent/path", "/parent/path")
    assert not is_child("/path1", "/path2")


def test_is_equal_or_child_handles_root_and_trailing_separator():
    assert is_equal_or_child("/parent/path/", "/parent/path")
    assert is_equal_or_child("/child", "/")


def test_is_fs_case_sensitive_rejects_root():
    with pytest.raises(ValueError):
        is_fs_case_sensitive(os.path.sep)


def test_delete_missing_case_sensitive_path_honours_raise_error(tmp_path):
    missing_path = str(tmp_path / "missing")

    err = delete(missing_path, force_case_sensitive=True, raise_error=False)

    assert isinstance(err, FileNotFoundError)

    with pytest.raises(FileNotFoundError):
        delete(missing_path, force_case_sensitive=True, raise_error=True)


def test_walk_continues_after_entry_disappears(tmp_path):
    vanished_path = tmp_path / "a-vanished"
    remaining_path = tmp_path / "b-remaining"
    touch(str(vanished_path))
    touch(str(remaining_path))

    entries = {entry.name: entry for entry in os.scandir(tmp_path)}
    vanished_path.unlink()

    def listdir(path):
        return [entries["a-vanished"], entries["b-remaining"]]

    assert list(walk(str(tmp_path), listdir=listdir)) == [
        (str(remaining_path), os.lstat(remaining_path))
    ]


def test_fs_max_lengths_returns_name_before_path(monkeypatch, tmp_path):
    limits = {"PC_NAME_MAX": 255, "PC_PATH_MAX": 4096}
    monkeypatch.setattr(os, "pathconf", lambda path, name: limits[name], raising=False)

    assert fs_max_lengths_for_path(str(tmp_path)) == (255, 4096)


def test_move_preserves_permissions(tmp_path):
    if platform.system() == "Windows":
        pytest.skip("Windows does not expose POSIX execute permission bits")

    src_path = str(tmp_path / "source.txt")
    dest_path = str(tmp_path / "dest.txt")

    touch(src_path)
    touch(dest_path)

    os.chmod(dest_path, stat.S_IEXEC)

    move(src_path, dest_path, keep_target_permissions=True)

    assert bool(os.stat(dest_path).st_mode & stat.S_IEXEC)


def test_move_preserves_xattrs(tmp_path):
    if xattr is None:
        pytest.skip("Xattrs are not supported on this platform")

    src_path = str(tmp_path / "source.txt")
    dest_path = str(tmp_path / "dest.txt")

    # Extended attributes set by the user need to be prefixed with 'user.' in Linux.
    attr_name = "user.test" if IS_LINUX else "com.myapp.test"
    attr_value = "hello!".encode()

    touch(src_path)
    touch(dest_path)

    try:
        xattr.setxattr(dest_path, attr_name, attr_value)
    except OSError:
        pytest.skip("Setting Xattr is not supported on this system")

    move(src_path, dest_path, keep_target_xattrs=True)

    assert xattr.getxattr(dest_path, attr_name) == attr_value
