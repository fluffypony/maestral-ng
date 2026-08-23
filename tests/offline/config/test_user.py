import configparser as cp
import copy
import os
import stat
from unittest import mock

import pytest
from packaging.version import Version

from maestral.config.user import ConfigLoadError, UserConfig

from .conftest import CONF_VERSION, DEFAULTS_CONFIG


def fresh_defaults():
    return {
        "auth": {"account_id": "default", "keyring": "automatic"},
        "sync": {"path": "", "selective_sync_paths": [], "upload": True},
    }


def test_config_creation(config):
    # Check that all config values have been set correctly.

    for section_name, section in DEFAULTS_CONFIG.items():
        for option, value in section.items():
            assert config.get(section_name, option) == value

    assert config.get_version() == CONF_VERSION


def test_get_failures(config):
    # Check getting non-existing config options.
    with pytest.raises(cp.NoOptionError):
        config.get("main", "invalid_option")

    with pytest.raises(cp.NoSectionError):
        config.get("invalid_section", "invalid_option")

    assert config.get("main", "invalid_option", "default") == "default"
    assert config.get("invalid_section", "invalid_option", "default") == "default"


def test_set_option(config):
    # Test setting valid config values of different types.
    config.set("sync", "path", "/test/path")
    config.set("sync", "selective_sync_paths", ["a", "b", "c"])
    config.set("new_section", "new_option", {"a", "b", "c"})

    assert config.get("sync", "path") == "/test/path"
    assert config.get("sync", "selective_sync_paths") == ["a", "b", "c"]
    assert config.get("new_section", "new_option") == {"a", "b", "c"}

    # Check setting invalid config values.
    with pytest.raises(ValueError):
        config.set("sync", "path", 1234)

    with pytest.raises(ValueError):
        config.set("sync", "selective_sync_paths", "path")


def test_update(config):
    old_version = CONF_VERSION

    # Modify some values.
    config.set("auth", "account_id", "my id")
    config.set("sync", "path", "/path/to/folder")

    # Remove a default config option.
    del DEFAULTS_CONFIG["sync"]["path"]

    # Add a default config option.
    DEFAULTS_CONFIG["sync"]["new_option"] = "brand new"

    # Modify some default config options.
    DEFAULTS_CONFIG["auth"]["account_id"] = "another id"
    DEFAULTS_CONFIG["sync"]["upload"] = False

    # Create a new instance with modified defaults.
    new_version = f"{old_version.major + 1}.{old_version.minor}.{old_version.micro}"

    for i in range(2):
        conf = UserConfig(
            str(config.config_path),
            defaults=DEFAULTS_CONFIG,
            version=Version(new_version),
            backup=True,
            remove_obsolete=True,
        )

        # Check that the config was updated properly.

        assert conf.get("auth", "account_id") == "my id"
        assert conf.get("sync", "new_option") == "brand new"

        with pytest.raises(cp.NoOptionError):
            conf.get("sync", "path")


def test_save_is_atomic_when_replace_fails(tmp_path):
    config_path = tmp_path / "atomic.ini"
    config = UserConfig(
        str(config_path),
        defaults=fresh_defaults(),
        version=CONF_VERSION,
        backup=True,
    )
    original_contents = config_path.read_bytes()
    original_replace = os.replace

    def fail_primary_replace(source, destination):
        if os.fspath(destination) == os.fspath(config_path):
            raise OSError("replace failed")
        return original_replace(source, destination)

    with mock.patch(
        "maestral.config.user.os.replace", side_effect=fail_primary_replace
    ):
        with pytest.raises(OSError, match="replace failed"):
            config.set("auth", "account_id", "new value")

    assert config_path.read_bytes() == original_contents
    assert list(tmp_path.glob(".atomic.ini.*")) == []


@pytest.mark.parametrize(
    "operation",
    [
        "set_existing",
        "set_new",
        "remove_option",
        "remove_section",
        "reset_to_defaults",
    ],
)
def test_precommit_save_failure_restores_all_in_memory_state(tmp_path, operation):
    config_path = tmp_path / "rollback.ini"
    config = UserConfig(
        str(config_path),
        defaults=fresh_defaults(),
        version=CONF_VERSION,
        backup=True,
    )
    config.set("auth", "account_id", "custom", save=False)
    sections_before = copy.deepcopy(config._sections)
    parser_defaults_before = copy.deepcopy(config._defaults)
    defaults_before = copy.deepcopy(config.default_config)
    contents_before = config_path.read_bytes()
    original_replace = os.replace

    def fail_primary_replace(source, destination):
        if os.fspath(destination) == os.fspath(config_path):
            raise OSError("replace failed")
        return original_replace(source, destination)

    with mock.patch(
        "maestral.config.user.os.replace", side_effect=fail_primary_replace
    ):
        with pytest.raises(OSError, match="replace failed"):
            if operation == "set_existing":
                config.set("auth", "account_id", "new value")
            elif operation == "set_new":
                config.set("new_section", "new_option", {"new value"})
            elif operation == "remove_option":
                config.remove_option("auth", "account_id")
            elif operation == "remove_section":
                config.remove_section("auth")
            else:
                config.reset_to_defaults()

    assert config._sections == sections_before
    assert config._defaults == parser_defaults_before
    assert config.default_config == defaults_before
    assert config_path.read_bytes() == contents_before
    assert list(tmp_path.glob(".rollback.ini.*")) == []


def test_directory_fsync_failure_keeps_committed_memory_and_disk(tmp_path):
    if os.name == "nt":
        pytest.skip("Windows does not fsync the configuration directory")

    config_path = tmp_path / "committed.ini"
    defaults = fresh_defaults()
    config = UserConfig(
        str(config_path),
        defaults=defaults,
        version=CONF_VERSION,
        backup=True,
    )
    original_fsync = os.fsync

    def fail_for_directory(file_descriptor):
        if stat.S_ISDIR(os.fstat(file_descriptor).st_mode):
            raise OSError("directory fsync failed")
        original_fsync(file_descriptor)

    with mock.patch("maestral.config.user.os.fsync", side_effect=fail_for_directory):
        config.set("auth", "account_id", "published")

    assert config.get("auth", "account_id") == "published"
    reloaded = UserConfig(
        str(config_path),
        defaults=defaults,
        version=CONF_VERSION,
        backup=False,
    )
    assert reloaded.get("auth", "account_id") == "published"


def test_postcommit_proof_is_not_reused_by_later_failed_save(tmp_path):
    config_path = tmp_path / "commit-proof.ini"
    defaults = fresh_defaults()
    config = UserConfig(
        str(config_path),
        defaults=defaults,
        version=CONF_VERSION,
        backup=True,
    )
    original_replace = os.replace

    def replace_then_interrupt(source, destination):
        original_replace(source, destination)
        if os.fspath(destination) == os.fspath(config_path):
            raise KeyboardInterrupt("after replace")

    def fail_primary_replace(source, destination):
        if os.fspath(destination) == os.fspath(config_path):
            raise OSError("before replace")
        return original_replace(source, destination)

    with mock.patch(
        "maestral.config.user.os.replace", side_effect=replace_then_interrupt
    ):
        with pytest.raises(KeyboardInterrupt, match="after replace"):
            config.set("auth", "account_id", "published")

    committed_contents = config_path.read_bytes()
    assert config.get("auth", "account_id") == "published"

    with mock.patch(
        "maestral.config.user.os.replace", side_effect=fail_primary_replace
    ):
        with pytest.raises(OSError, match="before replace"):
            config.set("auth", "account_id", "must roll back")

    assert config.get("auth", "account_id") == "published"
    assert config_path.read_bytes() == committed_contents


@pytest.mark.parametrize(
    "corrupt_contents",
    [
        "",
        "[sync]\npath = /partial/path\n",
        "[main]\nversion = invalid-version\n",
        "[main]\nversion = 1.0.0\n[auth]\naccount_id = first\n"
        "[auth]\naccount_id = second\n",
    ],
)
def test_load_recovers_from_backup(tmp_path, corrupt_contents):
    config_path = tmp_path / "recover.ini"
    defaults = fresh_defaults()
    config = UserConfig(
        str(config_path),
        defaults=defaults,
        version=CONF_VERSION,
        backup=True,
    )
    config.set("auth", "account_id", "from backup")
    config_path.write_text(corrupt_contents, encoding="utf-8")

    recovered = UserConfig(
        str(config_path),
        defaults=defaults,
        version=CONF_VERSION,
        backup=True,
    )

    assert recovered.get("auth", "account_id") == "from backup"
    assert recovered.get_version() == CONF_VERSION


def test_load_corrupt_config_without_backup_uses_defaults(tmp_path):
    config_path = tmp_path / "defaults.ini"
    config_path.write_text("[main]\nversion = invalid-version\n", encoding="utf-8")

    config = UserConfig(
        str(config_path),
        defaults=fresh_defaults(),
        version=CONF_VERSION,
        backup=True,
    )

    assert config.get("auth", "account_id") == "default"
    assert config.get_version() == CONF_VERSION


def test_corrupt_durable_state_does_not_load_a_stale_backup(tmp_path):
    state_path = tmp_path / "durable.state"
    defaults = fresh_defaults()
    state = UserConfig(
        str(state_path),
        defaults=defaults,
        version=CONF_VERSION,
        backup=True,
        recover_from_backup=False,
    )
    state.set("auth", "account_id", "current")
    UserConfig(
        str(state_path),
        defaults=defaults,
        version=CONF_VERSION,
        backup=True,
        recover_from_backup=False,
    )
    state_path.write_text("[main]\nversion = invalid-version\n", encoding="utf-8")

    with pytest.raises(ConfigLoadError, match="Cannot safely recover"):
        UserConfig(
            str(state_path),
            defaults=defaults,
            version=CONF_VERSION,
            backup=True,
            recover_from_backup=False,
        )


def test_cleanup_preserves_other_config_backups(tmp_path):
    work_path = tmp_path / "work.ini"
    work2_path = tmp_path / "work2.ini"
    defaults = fresh_defaults()

    work = UserConfig(
        str(work_path), defaults=defaults, version=CONF_VERSION, backup=True
    )
    UserConfig(str(work2_path), defaults=defaults, version=CONF_VERSION, backup=True)

    UserConfig(str(work_path), defaults=defaults, version=CONF_VERSION, backup=True)
    work2_reloaded = UserConfig(
        str(work2_path), defaults=defaults, version=CONF_VERSION, backup=True
    )
    work2_backup = work2_reloaded.backup_path_for_version(None)

    work.cleanup()

    assert work2_path.exists()
    assert os.path.exists(work2_backup)

    work2_reloaded.cleanup()
