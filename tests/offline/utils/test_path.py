import errno
import os
import platform
import stat

import pytest

import maestral.utils.path as path_module

try:
    import xattr
except ImportError:
    xattr = None

from maestral.constants import IS_LINUX
from maestral.utils.appdirs import get_home_dir
from maestral.utils.path import (
    content_hash,
    create_rooted_tempfile,
    delete,
    fs_max_lengths_for_path,
    get_existing_equivalent_paths,
    get_symlink_target,
    is_child,
    is_equal_or_child,
    is_fs_case_sensitive,
    mkdir,
    move,
    normalized_path_exists,
    open_rooted_file,
    rmdir,
    rooted_item_snapshot,
    rooted_tree_snapshot,
    rooted_walk,
    symlink,
    unlink,
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
        "test folder/subfolder", root=str(tmp_path)
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


def test_strict_move_raises_for_missing_source(tmp_path):
    source = str(tmp_path / "missing.txt")
    destination = str(tmp_path / "destination.txt")

    with pytest.raises(FileNotFoundError):
        move(source, destination, raise_error=True)


def test_move_without_replace_preserves_existing_destination(tmp_path):
    source = tmp_path / "source.txt"
    destination = tmp_path / "destination.txt"
    source.write_text("source")
    destination.write_text("destination")

    with pytest.raises(FileExistsError):
        move(str(source), str(destination), replace=False, raise_error=True)

    assert source.read_text() == "source"
    assert destination.read_text() == "destination"


def test_rooted_move_without_replace_preserves_existing_destination(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    source = root / "source.txt"
    destination = root / "destination.txt"
    source.write_text("source")
    destination.write_text("destination")

    with pytest.raises(FileExistsError):
        move(
            str(source),
            str(destination),
            replace=False,
            raise_error=True,
            root_path=str(root),
        )

    assert source.read_text() == "source"
    assert destination.read_text() == "destination"


def test_rooted_move_checks_expected_source_identity(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    source = root / "source.txt"
    old_source = root / "old-source.txt"
    destination = root / "destination.txt"
    source.write_text("old")
    source_stat = os.lstat(source)
    expected_identity = (
        source_stat.st_dev,
        source_stat.st_ino,
        source_stat.st_mode,
    )

    source.rename(old_source)
    source.write_text("new")

    with pytest.raises(OSError) as exc_info:
        move(
            str(source),
            str(destination),
            raise_error=True,
            root_path=str(root),
            expected_source_identity=expected_identity,
        )

    assert exc_info.value.errno == errno.ESTALE
    assert source.read_text() == "new"
    assert not destination.exists()


def test_rooted_no_replace_move_restores_raced_source(monkeypatch, tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    source = root / "source.txt"
    original = root / "original.txt"
    destination = root / "destination.txt"
    source.write_text("original")
    source_stat = os.lstat(source)
    expected_identity = (
        source_stat.st_dev,
        source_stat.st_ino,
        source_stat.st_mode,
    )
    swapped = False

    if platform.system() == "Windows":
        real_rename_opened = path_module._windows_rename_opened

        def racing_rename_opened(
            source_path,
            source_handle,
            destination_path,
            *,
            replace,
        ):
            nonlocal swapped
            swapped = True
            with pytest.raises(OSError):
                os.rename(source, original)
            return real_rename_opened(
                source_path,
                source_handle,
                destination_path,
                replace=replace,
            )

        monkeypatch.setattr(
            path_module,
            "_windows_rename_opened",
            racing_rename_opened,
        )
    else:
        real_rename_at = path_module._rename_no_replace_at

        def racing_rename_at(
            source_parent_fd,
            source_name,
            destination_parent_fd,
            destination_name,
        ):
            nonlocal swapped
            if not swapped and source_name == source.name:
                swapped = True
                source.rename(original)
                source.write_text("replacement")
            return real_rename_at(
                source_parent_fd,
                source_name,
                destination_parent_fd,
                destination_name,
            )

        monkeypatch.setattr(path_module, "_rename_no_replace_at", racing_rename_at)

    if platform.system() == "Windows":
        move(
            str(source),
            str(destination),
            replace=False,
            raise_error=True,
            root_path=str(root),
            expected_source_identity=expected_identity,
        )

        assert swapped
        assert not source.exists()
        assert destination.read_text() == "original"
        assert not original.exists()
        return

    with pytest.raises(OSError) as exc_info:
        move(
            str(source),
            str(destination),
            replace=False,
            raise_error=True,
            root_path=str(root),
            expected_source_identity=expected_identity,
        )

    assert exc_info.value.errno == errno.ESTALE
    assert source.read_text() == "original"
    assert original.read_text() == "original"
    assert destination.read_text() == "replacement"


def test_rooted_move_detects_source_content_race(monkeypatch, tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    source = root / "source.txt"
    destination = root / "destination.txt"
    source.write_bytes(b"original")
    source_stat = os.lstat(source)
    expected_identity = (
        source_stat.st_dev,
        source_stat.st_ino,
        source_stat.st_mode,
        source_stat.st_size,
        source_stat.st_mtime_ns,
        source_stat.st_ctime_ns,
    )
    raced = False

    if platform.system() == "Windows":
        real_rename_opened = path_module._windows_rename_opened

        def racing_rename_opened(
            source_path,
            source_handle,
            destination_path,
            *,
            replace,
        ):
            nonlocal raced
            raced = True
            with pytest.raises(OSError):
                source.write_bytes(b"changed!")
            return real_rename_opened(
                source_path,
                source_handle,
                destination_path,
                replace=replace,
            )

        monkeypatch.setattr(
            path_module,
            "_windows_rename_opened",
            racing_rename_opened,
        )
    else:
        real_rename_no_replace_at = path_module._rename_no_replace_at

        def racing_rename_no_replace_at(
            source_parent_fd,
            source_name,
            destination_parent_fd,
            destination_name,
        ):
            nonlocal raced
            if not raced and source_name == source.name:
                raced = True
                source.write_bytes(b"changed!")
            return real_rename_no_replace_at(
                source_parent_fd,
                source_name,
                destination_parent_fd,
                destination_name,
            )

        monkeypatch.setattr(
            path_module,
            "_rename_no_replace_at",
            racing_rename_no_replace_at,
        )

    if platform.system() == "Windows":
        move(
            str(source),
            str(destination),
            replace=False,
            raise_error=True,
            root_path=str(root),
            expected_source_identity=expected_identity,
        )

        assert raced
        assert destination.read_bytes() == b"original"
        return

    with pytest.raises(OSError) as exc_info:
        move(
            str(source),
            str(destination),
            replace=False,
            raise_error=True,
            root_path=str(root),
            expected_source_identity=expected_identity,
        )

    assert raced
    assert exc_info.value.errno == errno.ESTALE
    assert source.read_bytes() == b"changed!"
    assert destination.read_bytes() == b"changed!"


@pytest.mark.skipif(platform.system() == "Windows", reason="requires POSIX rename")
def test_rooted_move_preserves_source_and_postrename_replacement(monkeypatch, tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    source = root / "source.txt"
    destination = root / "destination.txt"
    replacement = root / "replacement.txt"
    source.write_text("original")
    replacement.write_text("replacement")
    source_stat = os.lstat(source)
    expected_identity = (
        source_stat.st_dev,
        source_stat.st_ino,
        source_stat.st_mode,
        source_stat.st_size,
        source_stat.st_mtime_ns,
        source_stat.st_ctime_ns,
    )
    real_rename_no_replace_at = path_module._rename_no_replace_at
    raced = False

    def replace_after_rename(
        source_parent_fd,
        source_name,
        destination_parent_fd,
        destination_name,
    ):
        nonlocal raced
        real_rename_no_replace_at(
            source_parent_fd,
            source_name,
            destination_parent_fd,
            destination_name,
        )
        if not raced and source_name == source.name:
            raced = True
            os.replace(replacement, destination)

    monkeypatch.setattr(
        path_module,
        "_rename_no_replace_at",
        replace_after_rename,
    )

    with pytest.raises(OSError) as exc_info:
        move(
            str(source),
            str(destination),
            replace=False,
            raise_error=True,
            root_path=str(root),
            expected_source_identity=expected_identity,
        )

    assert raced
    assert exc_info.value.errno == errno.ESTALE
    assert source.read_text() == "original"
    assert destination.read_text() == "replacement"
    assert not replacement.exists()
    assert not any(path.name.startswith(".~maestral-move-") for path in root.iterdir())


@pytest.mark.skipif(platform.system() == "Windows", reason="requires POSIX rename")
def test_rooted_replace_move_rejects_distinct_hardlink_destination(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    source = root / "source.txt"
    destination = root / "destination.txt"
    source.write_text("original")
    os.link(source, destination)
    source_stat = os.lstat(source)
    expected_identity = (
        source_stat.st_dev,
        source_stat.st_ino,
        source_stat.st_mode,
        source_stat.st_size,
        source_stat.st_mtime_ns,
        source_stat.st_ctime_ns,
    )

    with pytest.raises(FileExistsError):
        move(
            str(source),
            str(destination),
            replace=True,
            raise_error=True,
            root_path=str(root),
            expected_source_identity=expected_identity,
        )

    assert source.read_text() == "original"
    assert destination.read_text() == "original"


@pytest.mark.skipif(platform.system() == "Windows", reason="requires POSIX rename")
def test_rooted_replace_move_preserves_raced_case_alias(monkeypatch, tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    source = root / "Source.txt"
    destination = root / "source.txt"
    saved_source = root / "saved-source.txt"
    source.write_text("original")
    if not destination.exists() or not os.path.samefile(source, destination):
        pytest.skip("requires a case-insensitive file system")

    source_stat = os.lstat(source)
    expected_identity = (
        source_stat.st_dev,
        source_stat.st_ino,
        source_stat.st_mode,
        source_stat.st_size,
        source_stat.st_mtime_ns,
        source_stat.st_ctime_ns,
    )
    real_rename_no_replace_at = path_module._rename_no_replace_at
    raced = False

    def replace_before_rename(
        source_parent_fd,
        source_name,
        destination_parent_fd,
        destination_name,
    ):
        nonlocal raced
        if not raced and source_name == source.name:
            raced = True
            source.rename(saved_source)
            destination.write_text("replacement")
        return real_rename_no_replace_at(
            source_parent_fd,
            source_name,
            destination_parent_fd,
            destination_name,
        )

    monkeypatch.setattr(
        path_module,
        "_rename_no_replace_at",
        replace_before_rename,
    )

    with pytest.raises(OSError) as exc_info:
        move(
            str(source),
            str(destination),
            replace=True,
            raise_error=True,
            root_path=str(root),
            expected_source_identity=expected_identity,
        )

    assert raced
    assert exc_info.value.errno == errno.ESTALE
    assert destination.read_text() == "replacement"
    assert saved_source.read_text() == "original"
    assert any(
        path.read_text() == "original"
        for path in root.iterdir()
        if path.name.startswith("Maestral preserved ")
    )


def test_rooted_replace_move_requires_same_destination(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    source = root / "source.txt"
    destination = root / "destination.txt"
    source.write_text("source")
    destination.write_text("destination")
    source_stat = os.lstat(source)
    expected_identity = (
        source_stat.st_dev,
        source_stat.st_ino,
        source_stat.st_mode,
    )

    with pytest.raises(OSError) as exc_info:
        move(
            str(source),
            str(destination),
            replace=True,
            raise_error=True,
            root_path=str(root),
            expected_source_identity=expected_identity,
        )

    assert exc_info.value.errno == errno.ESTALE
    assert source.read_text() == "source"
    assert destination.read_text() == "destination"


@pytest.mark.skipif(platform.system() == "Windows", reason="requires POSIX fchmod")
def test_rooted_move_applies_permissions_through_held_descriptor(monkeypatch, tmp_path):
    root = tmp_path / "root"
    source_parent = root / "source-parent"
    saved_parent = root / "saved-parent"
    outside = tmp_path / "outside"
    source_parent.mkdir(parents=True)
    outside.mkdir()
    source = source_parent / "source.txt"
    outside_source = outside / "source.txt"
    metadata = root / "metadata.txt"
    destination = root / "destination.txt"
    source.write_text("source")
    outside_source.write_text("outside")
    metadata.write_text("metadata")
    os.chmod(source, 0o600)
    os.chmod(outside_source, 0o600)
    os.chmod(metadata, 0o700)

    real_fchmod = os.fchmod
    swapped = False

    def racing_fchmod(file_descriptor, mode):
        nonlocal swapped
        if not swapped:
            swapped = True
            source_parent.rename(saved_parent)
            os.symlink(outside, source_parent, target_is_directory=True)
        real_fchmod(file_descriptor, mode)

    monkeypatch.setattr(path_module.os, "fchmod", racing_fchmod)

    move(
        str(source),
        str(destination),
        keep_target_permissions=True,
        metadata_source_path=str(metadata),
        root_path=str(root),
        raise_error=True,
    )

    assert stat.S_IMODE(os.stat(destination).st_mode) == 0o700
    assert stat.S_IMODE(os.stat(outside_source).st_mode) == 0o600


@pytest.mark.skipif(xattr is None, reason="xattrs are unavailable")
def test_rooted_move_applies_xattrs_through_held_descriptor(monkeypatch, tmp_path):
    if platform.system() == "Windows":
        pytest.skip("Windows does not provide the xattr package")

    root = tmp_path / "root"
    source_parent = root / "source-parent"
    saved_parent = root / "saved-parent"
    outside = tmp_path / "outside"
    source_parent.mkdir(parents=True)
    outside.mkdir()
    source = source_parent / "source.txt"
    outside_source = outside / "source.txt"
    metadata = root / "metadata.txt"
    destination = root / "destination.txt"
    source.write_text("source")
    outside_source.write_text("outside")
    metadata.write_text("metadata")
    attribute = "user.test" if IS_LINUX else "com.maestral.test"

    try:
        xattr.setxattr(metadata, attribute, b"metadata")
        xattr.setxattr(outside_source, attribute, b"outside")
    except OSError as exc:
        pytest.skip(f"xattrs are unavailable: {exc}")

    real_setxattr = xattr.setxattr
    swapped = False

    def racing_setxattr(item, key, value, *args, **kwargs):
        nonlocal swapped
        assert isinstance(item, int)
        if not swapped:
            swapped = True
            source_parent.rename(saved_parent)
            os.symlink(outside, source_parent, target_is_directory=True)
        return real_setxattr(item, key, value, *args, **kwargs)

    monkeypatch.setattr(path_module.xattr, "setxattr", racing_setxattr)

    move(
        str(source),
        str(destination),
        keep_target_xattrs=True,
        metadata_source_path=str(metadata),
        root_path=str(root),
        raise_error=True,
    )

    assert xattr.getxattr(destination, attribute) == b"metadata"
    assert xattr.getxattr(outside_source, attribute) == b"outside"


def _make_directory_symlink(target, link) -> None:
    try:
        os.symlink(target, link, target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Directory symlinks are unavailable: {exc}")


def test_rooted_mkdir_rejects_link_parent(tmp_path):
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    _make_directory_symlink(outside, root / "link")

    with pytest.raises(OSError):
        mkdir(str(root / "link" / "new"), root_path=str(root))

    assert not (outside / "new").exists()


@pytest.mark.skipif(platform.system() == "Windows", reason="requires POSIX dir_fd")
def test_rooted_mkdir_calls_posix_mkdir_once(monkeypatch, tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    real_mkdir = os.mkdir
    calls = 0

    def tracked_mkdir(path, mode=0o777, *, dir_fd=None):
        nonlocal calls
        if path == "new" and dir_fd is not None:
            calls += 1
        return real_mkdir(path, mode, dir_fd=dir_fd)

    monkeypatch.setattr(path_module.os, "mkdir", tracked_mkdir)

    mkdir(str(root / "new"), root_path=str(root))

    assert calls == 1


def test_rooted_symlink_rejects_link_parent(tmp_path):
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    _make_directory_symlink(outside, root / "link")

    with pytest.raises(OSError):
        symlink(
            "target",
            str(root / "link" / "new-link"),
            root_path=str(root),
        )

    assert not (outside / "new-link").exists()


def test_rooted_symlink_creates_link(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    link = root / "link"

    symlink("target", str(link), root_path=str(root))

    assert os.readlink(link) == "target"


def test_rooted_move_rejects_link_destination_parent(tmp_path):
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    source = root / "source.txt"
    source.write_text("source")
    _make_directory_symlink(outside, root / "link")

    with pytest.raises(OSError):
        move(
            str(source),
            str(root / "link" / "destination.txt"),
            raise_error=True,
            root_path=str(root),
        )

    assert source.read_text() == "source"
    assert not (outside / "destination.txt").exists()


def test_rooted_delete_does_not_follow_tree_link(tmp_path):
    root = tmp_path / "root"
    tree = root / "tree"
    outside = tmp_path / "outside"
    tree.mkdir(parents=True)
    outside.mkdir()
    (tree / "local.txt").write_text("local")
    (outside / "keep.txt").write_text("keep")
    _make_directory_symlink(outside, tree / "link")

    delete(str(tree), root_path=str(root), raise_error=True)

    assert not tree.exists()
    assert (outside / "keep.txt").read_text() == "keep"


@pytest.mark.parametrize("operation", ["delete", "unlink"])
def test_rooted_file_deletion_preserves_replacement(operation, tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    target = root / "target.txt"
    original = root / "original.txt"
    target.write_text("original")
    original_stat = os.lstat(target)
    expected_identity = (
        original_stat.st_dev,
        original_stat.st_ino,
        original_stat.st_mode,
    )
    target.rename(original)
    target.write_text("replacement")

    with pytest.raises(OSError) as exc_info:
        if operation == "delete":
            delete(
                str(target),
                root_path=str(root),
                expected_target_identity=expected_identity,
                raise_error=True,
            )
        else:
            unlink(
                str(target),
                root_path=str(root),
                expected_target_identity=expected_identity,
            )

    assert exc_info.value.errno == errno.ESTALE
    assert target.read_text() == "replacement"
    assert not any(
        path.name.startswith(".~maestral-remove-") for path in root.iterdir()
    )


def test_rooted_rmdir_preserves_replacement(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    target = root / "target"
    original = root / "original"
    target.mkdir()
    original_stat = os.lstat(target)
    expected_identity = (
        original_stat.st_dev,
        original_stat.st_ino,
        original_stat.st_mode,
    )
    target.rename(original)
    target.mkdir()

    with pytest.raises(OSError) as exc_info:
        rmdir(
            str(target),
            root_path=str(root),
            expected_target_identity=expected_identity,
        )

    assert exc_info.value.errno == errno.ESTALE
    assert target.is_dir()


def test_rooted_delete_removes_expected_target(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    target = root / "target.txt"
    target.write_text("target")
    expected_snapshot = rooted_tree_snapshot(str(target), str(root))
    expected_identity = expected_snapshot[str(target)][:6]

    delete(
        str(target),
        root_path=str(root),
        expected_target_identity=expected_identity,
        expected_tree_snapshot=expected_snapshot,
        raise_error=True,
    )

    assert not target.exists()


def test_rooted_delete_uses_deterministic_quarantine(monkeypatch, tmp_path):
    root = tmp_path / "root"
    tree = root / "tree"
    quarantine = root / "discard-token"
    tree.mkdir(parents=True)
    (tree / "child.txt").write_text("child")
    expected_snapshot = rooted_tree_snapshot(str(tree), str(root))
    snapshot_in_quarantine = False

    if platform.system() == "Windows":
        real_snapshot_opened = path_module._windows_snapshot_opened

        def checked_snapshot_opened(path, handle, info):
            nonlocal snapshot_in_quarantine
            if os.path.normcase(path) == os.path.normcase(str(quarantine)):
                snapshot_in_quarantine = True
                assert not tree.exists()
                assert quarantine.exists()
            return real_snapshot_opened(path, handle, info)

        monkeypatch.setattr(
            path_module,
            "_windows_snapshot_opened",
            checked_snapshot_opened,
        )
    else:
        real_snapshot_at = path_module._posix_snapshot_at

        def checked_snapshot_at(parent_fd, name, absolute_path):
            nonlocal snapshot_in_quarantine
            if name == quarantine.name:
                snapshot_in_quarantine = True
                assert not tree.exists()
                assert quarantine.exists()
            return real_snapshot_at(parent_fd, name, absolute_path)

        monkeypatch.setattr(
            path_module,
            "_posix_snapshot_at",
            checked_snapshot_at,
        )

    delete(
        str(tree),
        root_path=str(root),
        expected_tree_snapshot=expected_snapshot,
        quarantine_path=str(quarantine),
        raise_error=True,
    )

    assert snapshot_in_quarantine
    assert not tree.exists()
    assert not quarantine.exists()


def test_rooted_delete_restores_changed_tree_from_deterministic_quarantine(tmp_path):
    root = tmp_path / "root"
    tree = root / "tree"
    quarantine = root / "discard-token"
    tree.mkdir(parents=True)
    child = tree / "child.txt"
    child.write_text("first")
    expected_snapshot = rooted_tree_snapshot(str(tree), str(root))
    child.write_text("other")

    with pytest.raises(OSError) as exc_info:
        delete(
            str(tree),
            root_path=str(root),
            expected_tree_snapshot=expected_snapshot,
            quarantine_path=str(quarantine),
            raise_error=True,
        )

    assert exc_info.value.errno == errno.ESTALE
    assert child.read_text() == "other"
    assert not quarantine.exists()


def test_rooted_delete_does_not_replace_deterministic_quarantine(tmp_path):
    root = tmp_path / "root"
    tree = root / "tree"
    quarantine = root / "discard-token"
    tree.mkdir(parents=True)
    quarantine.mkdir()
    child = tree / "child.txt"
    child.write_text("child")
    quarantined_child = quarantine / "existing.txt"
    quarantined_child.write_text("existing")
    expected_snapshot = rooted_tree_snapshot(str(tree), str(root))

    with pytest.raises(OSError):
        delete(
            str(tree),
            root_path=str(root),
            expected_tree_snapshot=expected_snapshot,
            quarantine_path=str(quarantine),
            raise_error=True,
        )

    assert child.read_text() == "child"
    assert quarantined_child.read_text() == "existing"


def test_rooted_delete_recovers_known_quarantine_without_another_move(
    monkeypatch, tmp_path
):
    root = tmp_path / "root"
    quarantine = root / "discard-token"
    quarantine.mkdir(parents=True)
    (quarantine / "child.txt").write_text("child")
    expected_snapshot = rooted_tree_snapshot(str(quarantine), str(root))

    def unexpected_token(*args, **kwargs):
        raise AssertionError("Recovery created an unjournalled quarantine")

    monkeypatch.setattr(path_module.secrets, "token_hex", unexpected_token)

    delete(
        str(quarantine),
        root_path=str(root),
        expected_tree_snapshot=expected_snapshot,
        quarantine_path=str(quarantine),
        raise_error=True,
    )

    assert not quarantine.exists()


@pytest.mark.skipif(platform.system() == "Windows", reason="requires POSIX renameat")
def test_rooted_delete_crash_leaves_deterministic_quarantine(monkeypatch, tmp_path):
    root = tmp_path / "root"
    tree = root / "tree"
    quarantine = root / "discard-token"
    tree.mkdir(parents=True)
    (tree / "child.txt").write_text("child")
    expected_snapshot = rooted_tree_snapshot(str(tree), str(root))
    real_rename_no_replace_at = path_module._rename_no_replace_at

    class SimulatedCrash(BaseException):
        pass

    def crashing_rename_no_replace_at(
        source_parent_fd,
        source_name,
        destination_parent_fd,
        destination_name,
    ):
        result = real_rename_no_replace_at(
            source_parent_fd,
            source_name,
            destination_parent_fd,
            destination_name,
        )
        if destination_name == quarantine.name:
            raise SimulatedCrash
        return result

    monkeypatch.setattr(
        path_module,
        "_rename_no_replace_at",
        crashing_rename_no_replace_at,
    )

    with pytest.raises(SimulatedCrash):
        delete(
            str(tree),
            root_path=str(root),
            expected_tree_snapshot=expected_snapshot,
            quarantine_path=str(quarantine),
            raise_error=True,
        )

    assert not tree.exists()
    assert (quarantine / "child.txt").read_text() == "child"


def test_rooted_delete_preserves_changed_descendant(tmp_path):
    root = tmp_path / "root"
    tree = root / "tree"
    tree.mkdir(parents=True)
    child = tree / "child.txt"
    child.write_bytes(b"first")
    expected_snapshot = rooted_tree_snapshot(str(tree), str(root))
    child.write_bytes(b"other")

    with pytest.raises(OSError) as exc_info:
        delete(
            str(tree),
            root_path=str(root),
            expected_tree_snapshot=expected_snapshot,
            raise_error=True,
        )

    assert exc_info.value.errno == errno.ESTALE
    assert child.read_bytes() == b"other"


@pytest.mark.skipif(platform.system() == "Windows", reason="requires POSIX renameat")
def test_rooted_delete_snapshots_after_quarantine(monkeypatch, tmp_path):
    root = tmp_path / "root"
    tree = root / "tree"
    tree.mkdir(parents=True)
    child = tree / "child.txt"
    child.write_bytes(b"first")
    expected_snapshot = rooted_tree_snapshot(str(tree), str(root))
    real_rename_no_replace_at = path_module._rename_no_replace_at
    raced = False

    def racing_rename_no_replace_at(
        source_parent_fd,
        source_name,
        destination_parent_fd,
        destination_name,
    ):
        nonlocal raced
        result = real_rename_no_replace_at(
            source_parent_fd,
            source_name,
            destination_parent_fd,
            destination_name,
        )
        if not raced and destination_name == "item":
            raced = True
            child_fd = os.open(
                os.path.join("item", child.name),
                os.O_WRONLY | os.O_TRUNC,
                dir_fd=destination_parent_fd,
            )
            try:
                os.write(child_fd, b"other")
            finally:
                os.close(child_fd)
        return result

    monkeypatch.setattr(
        path_module,
        "_rename_no_replace_at",
        racing_rename_no_replace_at,
    )

    with pytest.raises(OSError) as exc_info:
        delete(
            str(tree),
            root_path=str(root),
            expected_tree_snapshot=expected_snapshot,
            raise_error=True,
        )

    assert raced
    assert exc_info.value.errno == errno.ESTALE
    assert child.read_bytes() == b"other"


@pytest.mark.skipif(platform.system() == "Windows", reason="requires POSIX renameat")
def test_rooted_delete_restores_final_component_replacement(monkeypatch, tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    target = root / "target.txt"
    original = root / "original.txt"
    target.write_text("original")
    target_stat = os.lstat(target)
    expected_identity = (
        target_stat.st_dev,
        target_stat.st_ino,
        target_stat.st_mode,
        target_stat.st_size,
        target_stat.st_mtime_ns,
        target_stat.st_ctime_ns,
    )
    real_rename_no_replace_at = path_module._rename_no_replace_at
    swapped = False

    def racing_rename_no_replace_at(
        source_parent_fd,
        source_name,
        destination_parent_fd,
        destination_name,
    ):
        nonlocal swapped
        if not swapped and source_name == target.name and destination_name == "item":
            swapped = True
            target.rename(original)
            target.write_text("replacement")
        return real_rename_no_replace_at(
            source_parent_fd,
            source_name,
            destination_parent_fd,
            destination_name,
        )

    monkeypatch.setattr(
        path_module,
        "_rename_no_replace_at",
        racing_rename_no_replace_at,
    )

    with pytest.raises(OSError) as exc_info:
        delete(
            str(target),
            root_path=str(root),
            expected_target_identity=expected_identity,
            raise_error=True,
        )

    assert exc_info.value.errno == errno.ESTALE
    assert target.read_text() == "replacement"
    assert original.read_text() == "original"


def test_rooted_unlink_and_rmdir(tmp_path):
    root = tmp_path / "root"
    folder = root / "folder"
    root.mkdir()
    mkdir(str(folder), root_path=str(root))
    file = folder / "file.txt"
    file.write_text("content")

    unlink(str(file), root_path=str(root))
    rmdir(str(folder), root_path=str(root))

    assert not folder.exists()


def test_rooted_tempfile_rejects_link_directory(tmp_path):
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    _make_directory_symlink(outside, root / "cache")

    with pytest.raises(OSError):
        create_rooted_tempfile(str(root / "cache"), str(root))

    assert list(outside.iterdir()) == []


def test_rooted_tempfile_writes_without_reopening_path(tmp_path):
    root = tmp_path / "root"
    cache = root / "cache"
    cache.mkdir(parents=True)

    with create_rooted_tempfile(str(cache), str(root)) as temporary:
        with temporary.open("wb") as file:
            file.write(b"download")

        assert os.lstat(temporary.path).st_ino == temporary.identity[1]
        assert open(temporary.path, "rb").read() == b"download"


@pytest.mark.skipif(platform.system() == "Windows", reason="requires POSIX rename")
def test_rooted_tempfile_write_survives_parent_symlink_swap(tmp_path):
    root = tmp_path / "root"
    cache = root / "cache"
    saved_cache = root / "saved-cache"
    outside = tmp_path / "outside"
    cache.mkdir(parents=True)
    outside.mkdir()

    with create_rooted_tempfile(str(cache), str(root)) as temporary:
        name = os.path.basename(temporary.path)
        cache.rename(saved_cache)
        os.symlink(outside, cache, target_is_directory=True)
        outside_file = outside / name
        outside_file.write_bytes(b"outside")

        with temporary.open("wb") as file:
            file.write(b"download")

        assert (saved_cache / name).read_bytes() == b"download"
        assert outside_file.read_bytes() == b"outside"


def test_open_rooted_file_returns_held_binary_reader(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    file = root / "file.txt"
    file.write_bytes(b"content")
    root_stat = os.lstat(root)
    file_stat = os.lstat(file)

    with open_rooted_file(
        str(file),
        str(root),
        expected_root_identity=(root_stat.st_dev, root_stat.st_ino, root_stat.st_mode),
        expected_file_identity=(
            file_stat.st_dev,
            file_stat.st_ino,
            file_stat.st_mode,
            file_stat.st_size,
            file_stat.st_mtime_ns,
            file_stat.st_ctime_ns,
        ),
    ) as opened_file:
        opened_stat = os.fstat(opened_file.fileno())

        assert opened_file.read() == b"content"
        assert (opened_stat.st_dev, opened_stat.st_ino, opened_stat.st_mode) == (
            file_stat.st_dev,
            file_stat.st_ino,
            file_stat.st_mode,
        )

    assert opened_file.closed


def test_open_rooted_file_stops_ancestor_swap(monkeypatch, tmp_path):
    root = tmp_path / "root"
    parent = root / "parent"
    saved_parent = root / "saved-parent"
    outside = tmp_path / "outside"
    parent.mkdir(parents=True)
    outside.mkdir()
    file = parent / "file.txt"
    outside_file = outside / "file.txt"
    file.write_bytes(b"inside")
    outside_file.write_bytes(b"outside")
    raced = False

    if platform.system() == "Windows":
        real_open_windows_handle = path_module._open_windows_handle

        def racing_open_windows_handle(path, **kwargs):
            nonlocal raced
            if os.path.normcase(path) == os.path.normcase(str(file)) and not raced:
                raced = True
                with pytest.raises(OSError):
                    parent.rename(saved_parent)
            return real_open_windows_handle(path, **kwargs)

        monkeypatch.setattr(
            path_module,
            "_open_windows_handle",
            racing_open_windows_handle,
        )
    else:
        real_open = os.open

        def racing_open(path, flags, mode=0o777, *, dir_fd=None):
            nonlocal raced
            if path == file.name and dir_fd is not None and not raced:
                raced = True
                parent.rename(saved_parent)
                os.symlink(outside, parent, target_is_directory=True)
            if dir_fd is None:
                return real_open(path, flags, mode)
            return real_open(path, flags, mode, dir_fd=dir_fd)

        monkeypatch.setattr(path_module.os, "open", racing_open)

    with open_rooted_file(str(file), str(root)) as opened_file:
        assert opened_file.read() == b"inside"

    assert raced
    assert outside_file.read_bytes() == b"outside"


def test_open_rooted_file_rejects_final_replacement(monkeypatch, tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    file = root / "file.txt"
    original = root / "original.txt"
    file.write_bytes(b"original")
    file_stat = os.lstat(file)
    expected_identity = (
        file_stat.st_dev,
        file_stat.st_ino,
        file_stat.st_mode,
        file_stat.st_size,
        file_stat.st_mtime_ns,
        file_stat.st_ctime_ns,
    )
    raced = False

    if platform.system() == "Windows":
        real_open_windows_handle = path_module._open_windows_handle

        def racing_open_windows_handle(path, **kwargs):
            nonlocal raced
            if os.path.normcase(path) == os.path.normcase(str(file)) and not raced:
                raced = True
                file.rename(original)
                file.write_bytes(b"replacement")
            return real_open_windows_handle(path, **kwargs)

        monkeypatch.setattr(
            path_module,
            "_open_windows_handle",
            racing_open_windows_handle,
        )
    else:
        real_open = os.open

        def racing_open(path, flags, mode=0o777, *, dir_fd=None):
            nonlocal raced
            if path == file.name and dir_fd is not None and not raced:
                raced = True
                file.rename(original)
                file.write_bytes(b"replacement")
            if dir_fd is None:
                return real_open(path, flags, mode)
            return real_open(path, flags, mode, dir_fd=dir_fd)

        monkeypatch.setattr(path_module.os, "open", racing_open)

    with pytest.raises(OSError) as exc_info:
        open_rooted_file(
            str(file),
            str(root),
            expected_file_identity=expected_identity,
        )

    assert exc_info.value.errno == errno.ESTALE
    assert file.read_bytes() == b"replacement"
    assert original.read_bytes() == b"original"


def test_rooted_mutation_rejects_path_outside_root(tmp_path):
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()

    with pytest.raises(ValueError):
        mkdir(str(outside), root_path=str(root))

    assert not outside.exists()


def test_rooted_operations_reject_replaced_root(tmp_path):
    root = tmp_path / "root"
    original_root = tmp_path / "original-root"
    root.mkdir()
    original_stat = os.lstat(root)
    expected_root_identity = (
        original_stat.st_dev,
        original_stat.st_ino,
        original_stat.st_mode,
    )

    root.rename(original_root)
    root.mkdir()
    replacement_file = root / "replacement.txt"
    replacement_file.write_text("replacement")

    with pytest.raises(OSError) as snapshot_error:
        rooted_tree_snapshot(
            str(replacement_file),
            str(root),
            expected_root_identity=expected_root_identity,
        )
    with pytest.raises(OSError) as mkdir_error:
        mkdir(
            str(root / "new"),
            root_path=str(root),
            expected_root_identity=expected_root_identity,
        )

    assert snapshot_error.value.errno == errno.ESTALE
    assert mkdir_error.value.errno == errno.ESTALE
    assert not (root / "new").exists()


def test_rooted_tree_snapshot_does_not_follow_links(tmp_path):
    root = tmp_path / "root"
    tree = root / "tree"
    outside = tmp_path / "outside"
    tree.mkdir(parents=True)
    outside.mkdir()
    file = tree / "file.txt"
    link = tree / "link"
    outside_file = outside / "outside.txt"
    file.write_text("inside")
    outside_file.write_text("outside")
    _make_directory_symlink(outside, link)

    snapshot = rooted_tree_snapshot(str(tree), str(root))

    assert set(snapshot) == {str(tree), str(file), str(link)}
    assert str(outside_file) not in snapshot
    assert snapshot[str(file)][5] == os.lstat(file).st_ctime_ns
    assert snapshot[str(file)][6] == content_hash(str(file))[0]
    assert snapshot[str(link)][6] == f"symlink:{get_symlink_target(str(link))}"


def test_rooted_tree_snapshot_hash_detects_same_size_content_change(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    file = root / "file.txt"
    file.write_text("first")

    first_hash = rooted_tree_snapshot(str(file), str(root))[str(file)][6]
    file.write_text("other")
    second_hash = rooted_tree_snapshot(str(file), str(root))[str(file)][6]

    assert first_hash != second_hash


def test_rooted_item_snapshot_does_not_recurse(tmp_path):
    root = tmp_path / "root"
    directory = root / "directory"
    directory.mkdir(parents=True)
    file = directory / "file.txt"
    link = root / "link"
    file.write_bytes(b"content")
    os.symlink("directory/file.txt", link)

    directory_identity = rooted_item_snapshot(str(directory), str(root))
    file_identity = rooted_item_snapshot(str(file), str(root))
    link_identity = rooted_item_snapshot(str(link), str(root))

    assert directory_identity[6] is None
    assert file_identity[6] == content_hash(str(file))[0]
    assert link_identity[6] == "symlink:directory/file.txt"


def test_rooted_item_snapshot_accepts_root(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    root_stat = os.lstat(root)

    root_identity = rooted_item_snapshot(
        str(root),
        str(root),
        expected_root_identity=(root_stat.st_dev, root_stat.st_ino, root_stat.st_mode),
    )

    assert root_identity[:3] == (
        root_stat.st_dev,
        root_stat.st_ino,
        root_stat.st_mode,
    )
    assert root_identity[6] is None


@pytest.mark.skipif(platform.system() == "Windows", reason="requires POSIX dir_fd")
def test_rooted_item_snapshot_rejects_directory_replacement(monkeypatch, tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    directory = root / "directory"
    saved_directory = root / "saved-directory"
    directory.mkdir()
    directory_inode = os.lstat(directory).st_ino
    real_fstat = os.fstat
    raced = False

    def racing_fstat(file_descriptor):
        nonlocal raced
        item_stat = real_fstat(file_descriptor)
        if item_stat.st_ino == directory_inode and not raced:
            raced = True
            directory.rename(saved_directory)
            directory.mkdir()
        return item_stat

    monkeypatch.setattr(path_module.os, "fstat", racing_fstat)

    with pytest.raises(OSError) as exc_info:
        rooted_item_snapshot(str(directory), str(root))

    assert raced
    assert exc_info.value.errno == errno.ESTALE
    assert directory.is_dir()
    assert saved_directory.is_dir()


@pytest.mark.skipif(platform.system() == "Windows", reason="requires POSIX dir_fd")
def test_rooted_item_snapshot_rejects_file_replacement(monkeypatch, tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    file = root / "file.txt"
    saved_file = root / "saved-file.txt"
    file.write_text("original")
    real_read = os.read
    raced = False

    def racing_read(file_descriptor, size):
        nonlocal raced
        data = real_read(file_descriptor, size)
        if not raced:
            raced = True
            file.rename(saved_file)
            file.write_text("replacement")
        return data

    monkeypatch.setattr(path_module.os, "read", racing_read)

    with pytest.raises(OSError) as exc_info:
        rooted_item_snapshot(str(file), str(root))

    assert raced
    assert exc_info.value.errno == errno.ESTALE
    assert file.read_text() == "replacement"
    assert saved_file.read_text() == "original"


def test_rooted_walk_yields_descendants(tmp_path):
    root = tmp_path / "root"
    tree = root / "tree"
    directory = tree / "directory"
    directory.mkdir(parents=True)
    file = tree / "file.txt"
    nested_file = directory / "nested.txt"
    file.write_text("file")
    nested_file.write_text("nested")

    walked = dict(rooted_walk(str(tree), str(root)))

    assert set(walked) == {str(directory), str(file), str(nested_file)}
    assert stat.S_ISDIR(walked[str(directory)].st_mode)


def test_rooted_walk_accepts_root(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    child = root / "child.txt"
    child.write_text("child")
    root_stat = os.lstat(root)

    walked = dict(
        rooted_walk(
            str(root),
            str(root),
            expected_root_identity=(
                root_stat.st_dev,
                root_stat.st_ino,
                root_stat.st_mode,
            ),
        )
    )

    assert set(walked) == {str(child)}


def test_rooted_walk_prunes_with_anchored_stat_callback(tmp_path):
    root = tmp_path / "root"
    tree = root / "tree"
    kept_directory = tree / "kept"
    pruned_directory = tree / "pruned"
    kept_directory.mkdir(parents=True)
    pruned_directory.mkdir()
    kept_file = kept_directory / "kept.txt"
    pruned_file = pruned_directory / "pruned.txt"
    kept_file.write_text("kept")
    pruned_file.write_text("pruned")
    callbacks = []

    def should_recurse(path, stat_result):
        callbacks.append((path, stat_result))
        return path != str(pruned_directory)

    walked = dict(
        rooted_walk(
            str(tree),
            str(root),
            should_recurse=should_recurse,
        )
    )

    assert set(walked) == {
        str(kept_directory),
        str(kept_file),
        str(pruned_directory),
    }
    assert {path for path, _ in callbacks} == {
        str(kept_directory),
        str(pruned_directory),
    }
    assert all(stat.S_ISDIR(item_stat.st_mode) for _, item_stat in callbacks)


def test_rooted_walk_does_not_follow_raced_directory_link(monkeypatch, tmp_path):
    root = tmp_path / "root"
    tree = root / "tree"
    child = tree / "child"
    saved_child = root / "saved-child"
    outside = tmp_path / "outside"
    child.mkdir(parents=True)
    outside.mkdir()
    (child / "inside.txt").write_text("inside")
    outside_file = outside / "outside.txt"
    outside_file.write_text("outside")
    raced = False
    race_blocked = False

    if platform.system() == "Windows":
        real_open_windows_handle = path_module._open_windows_handle

        def racing_open_windows_handle(path, **kwargs):
            nonlocal raced, race_blocked
            if os.path.normcase(path) == os.path.normcase(str(child)) and not raced:
                raced = True
                try:
                    child.rename(saved_child)
                    os.symlink(outside, child, target_is_directory=True)
                except OSError:
                    race_blocked = True
            return real_open_windows_handle(path, **kwargs)

        monkeypatch.setattr(
            path_module,
            "_open_windows_handle",
            racing_open_windows_handle,
        )
    else:
        real_open = os.open

        def racing_open(path, flags, mode=0o777, *, dir_fd=None):
            nonlocal raced
            if path == child.name and dir_fd is not None and not raced:
                raced = True
                child.rename(saved_child)
                os.symlink(outside, child, target_is_directory=True)
            if dir_fd is None:
                return real_open(path, flags, mode)
            return real_open(path, flags, mode, dir_fd=dir_fd)

        monkeypatch.setattr(path_module.os, "open", racing_open)

    walked = []
    walk_error = None
    try:
        walked.extend(rooted_walk(str(tree), str(root)))
    except OSError as exc:
        walk_error = exc

    assert raced
    assert str(outside_file) not in {path for path, _ in walked}
    if platform.system() != "Windows" and not race_blocked:
        assert walk_error is not None
        assert walk_error.errno == errno.ESTALE


@pytest.mark.skipif(platform.system() != "Windows", reason="requires Windows handles")
def test_rooted_windows_snapshot_requires_stable_child_set(monkeypatch, tmp_path):
    root = tmp_path / "root"
    tree = root / "tree"
    tree.mkdir(parents=True)
    (tree / "file.txt").write_text("content")
    real_directory_names = path_module._windows_directory_names
    tree_checks = 0

    def racing_directory_names(path):
        nonlocal tree_checks
        names = real_directory_names(path)
        if os.path.normcase(path) == os.path.normcase(str(tree)):
            tree_checks += 1
            if tree_checks == 2:
                return (*names, "late-entry")
        return names

    monkeypatch.setattr(
        path_module,
        "_windows_directory_names",
        racing_directory_names,
    )

    with pytest.raises(OSError) as exc_info:
        rooted_tree_snapshot(str(tree), str(root))

    assert exc_info.value.errno == errno.ESTALE


@pytest.mark.skipif(platform.system() == "Windows", reason="requires POSIX dir_fd")
def test_rooted_delete_stops_ancestor_symlink_swap(monkeypatch, tmp_path):
    root = tmp_path / "root"
    parent = root / "parent"
    saved_parent = root / "saved-parent"
    outside = tmp_path / "outside"
    parent.mkdir(parents=True)
    outside.mkdir()
    (parent / "victim.txt").write_text("inside")
    outside_victim = outside / "victim.txt"
    outside_victim.write_text("outside")

    real_open = os.open
    swapped = False

    def racing_open(path, flags, *, dir_fd=None):
        nonlocal swapped
        if path == "parent" and dir_fd is not None and not swapped:
            swapped = True
            parent.rename(saved_parent)
            os.symlink(outside, parent, target_is_directory=True)
        if dir_fd is None:
            return real_open(path, flags)
        return real_open(path, flags, dir_fd=dir_fd)

    monkeypatch.setattr(path_module.os, "open", racing_open)

    with pytest.raises(OSError):
        delete(
            str(parent / "victim.txt"),
            root_path=str(root),
            raise_error=True,
        )

    assert outside_victim.read_text() == "outside"
    assert (saved_parent / "victim.txt").read_text() == "inside"


@pytest.mark.skipif(platform.system() == "Windows", reason="requires POSIX dir_fd")
def test_rooted_snapshot_stops_ancestor_symlink_swap(monkeypatch, tmp_path):
    root = tmp_path / "root"
    parent = root / "parent"
    saved_parent = root / "saved-parent"
    outside = tmp_path / "outside"
    parent.mkdir(parents=True)
    outside.mkdir()
    (parent / "file.txt").write_text("inside")
    outside_file = outside / "file.txt"
    outside_file.write_text("outside")

    real_open = os.open
    swapped = False

    def racing_open(path, flags, *, dir_fd=None):
        nonlocal swapped
        if path == "parent" and dir_fd is not None and not swapped:
            swapped = True
            parent.rename(saved_parent)
            os.symlink(outside, parent, target_is_directory=True)
        if dir_fd is None:
            return real_open(path, flags)
        return real_open(path, flags, dir_fd=dir_fd)

    monkeypatch.setattr(path_module.os, "open", racing_open)

    with pytest.raises(OSError):
        rooted_tree_snapshot(str(parent / "file.txt"), str(root))

    assert outside_file.read_text() == "outside"


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
