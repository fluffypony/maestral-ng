import configparser as cp
import os
from unittest import mock

import pytest
from packaging.version import Version

from maestral.config.user import UserConfig

from .conftest import CONF_VERSION, DEFAULTS_CONFIG


def fresh_defaults():
    return {
        "auth": {"account_id": "default", "keyring": "automatic"},
        "sync": {"path": "", "excluded_items": [], "upload": True},
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
    config.set("sync", "excluded_items", ["a", "b", "c"])
    config.set("new_section", "new_option", {"a", "b", "c"})

    assert config.get("sync", "path") == "/test/path"
    assert config.get("sync", "excluded_items") == ["a", "b", "c"]
    assert config.get("new_section", "new_option") == {"a", "b", "c"}

    # Check setting invalid config values.
    with pytest.raises(ValueError):
        config.set("sync", "path", 1234)

    with pytest.raises(ValueError):
        config.set("sync", "excluded_items", "path")


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

    with mock.patch(
        "maestral.config.user.os.replace", side_effect=OSError("replace failed")
    ):
        with pytest.raises(OSError, match="replace failed"):
            config.set("auth", "account_id", "new value")

    assert config_path.read_bytes() == original_contents
    assert list(tmp_path.glob(".atomic.ini.*")) == []


@pytest.mark.parametrize(
    "corrupt_contents",
    [
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

    UserConfig(
        str(config_path),
        defaults=defaults,
        version=CONF_VERSION,
        backup=True,
    )
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
