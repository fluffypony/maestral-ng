#
# Copyright © Spyder Project Contributors
# Licensed under the terms of the MIT License
# (see spyder/__init__.py for details)

"""
This module provides user configuration file management and is mostly copied from the
config module of the Spyder IDE.
"""

from __future__ import annotations

import ast
import configparser as cp
import copy
import logging
import os
import os.path as osp
import shutil
import tempfile
from threading import RLock
from typing import Any, Dict, Iterable, Iterator, MutableSet, TypeVar

from packaging.version import InvalidVersion, Version

logger = logging.getLogger(__name__)

_DefaultsType = Dict[str, Dict[str, Any]]
_T = TypeVar("_T")

# =============================================================================
# Auxiliary classes
# =============================================================================


class NoDefault:
    pass


class ConfigLoadError(RuntimeError):
    """Raised when a persisted config cannot be loaded safely."""


# =============================================================================
# Defaults class
# =============================================================================


class DefaultsConfig(cp.ConfigParser):
    """
    Class used to save defaults to a file and as base class for UserConfig.
    """

    def __init__(self, path: str) -> None:
        super().__init__(interpolation=None)

        dirname, basename = osp.split(path)
        filename, ext = osp.splitext(basename)

        self._path = path
        self._dirname = dirname
        self._filename = filename
        self._suffix = ext
        self._save_generation = 0
        self._pending_save: tuple[str, tuple[int, int]] | None = None

    def _set(self, section: str, option: str, value: Any) -> None:
        """Private set method"""
        if not self.has_section(section):
            self.add_section(section)
        if not isinstance(value, str):
            value = repr(value)

        super().set(section, option, value)

    def _save_snapshot_to(self, path: str) -> None:
        """Atomically write the current parser state to an auxiliary path."""
        directory = osp.dirname(path)
        os.makedirs(directory, exist_ok=True)
        temp_path = ""
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=directory,
                prefix=f".{osp.basename(path)}.",
                delete=False,
            ) as configfile:
                temp_path = configfile.name
                self.write(configfile)
                configfile.flush()
                os.fsync(configfile.fileno())
            os.replace(temp_path, path)
            temp_path = ""
        finally:
            if temp_path:
                try:
                    os.unlink(temp_path)
                except FileNotFoundError:
                    pass

    def save(self) -> int:
        """Save config into the associated file."""
        os.makedirs(self._dirname, exist_ok=True)

        temp_path = ""

        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self._dirname,
                prefix=f".{osp.basename(self.config_path)}.",
                delete=False,
            ) as configfile:
                temp_path = configfile.name
                self.write(configfile)
                configfile.flush()
                os.fsync(configfile.fileno())
                temp_stat = os.fstat(configfile.fileno())

            self._pending_save = (
                temp_path,
                (temp_stat.st_dev, temp_stat.st_ino),
            )
            os.replace(temp_path, self.config_path)
            temp_path = ""
            self._save_generation += 1
            self._pending_save = None
            if os.name != "nt":
                try:
                    directory_fd = os.open(
                        self._dirname,
                        os.O_RDONLY
                        | getattr(os, "O_DIRECTORY", 0)
                        | getattr(os, "O_CLOEXEC", 0),
                    )
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
                except OSError:
                    # The replace is the commit point. Do not report a published
                    # configuration as uncommitted when directory fsync is unsupported.
                    pass
        finally:
            if temp_path:
                try:
                    os.unlink(temp_path)
                except FileNotFoundError:
                    pass

        return self._save_generation

    @property
    def save_generation(self) -> int:
        """Return the number of file replacements completed by this instance."""
        return self._save_generation

    def save_committed_since(self, generation: int) -> bool:
        """Return whether a save committed after ``generation`` was observed."""
        if self._save_generation != generation:
            self._pending_save = None
            return True
        if self._pending_save is None:
            return False
        temp_path, expected_identity = self._pending_save
        if osp.lexists(temp_path):
            return False
        try:
            target_stat = os.stat(self.config_path, follow_symlinks=False)
        except OSError:
            self._pending_save = None
            return False
        if (target_stat.st_dev, target_stat.st_ino) != expected_identity:
            self._pending_save = None
            return False

        # A BaseException can land after os.replace but before save() records its
        # generation. Consume that commit proof once so it cannot validate a later,
        # failed save.
        self._save_generation += 1
        self._pending_save = None
        return True

    @property
    def config_path(self) -> str:
        """The ini file where this configuration is stored."""
        return self._path


# =============================================================================
# User config class
# =============================================================================


class UserConfig(DefaultsConfig):
    """
    UserConfig class, based on ConfigParser. This class is safe to use from different
    threads but must not be used from different processes!

    :param path: Configuration file will be saved to this path.
    :param defaults: Dictionary containing options.
    :param version: Version of the configuration file.
    :param backup: Whether to create a backup on version changes and on initial setup.
    :param remove_obsolete: If `True`, values that were removed from the configuration
        on version change, are removed from the saved configuration file.

    .. note:: The ``get`` and ``set`` arguments number and type differ from the
        reimplemented methods.
    """

    DEFAULT_SECTION_NAME = "main"

    def __init__(
        self,
        path: str,
        defaults: _DefaultsType | None = None,
        load: bool = True,
        version: Version = Version("0.0.0"),
        backup: bool = False,
        remove_obsolete: bool = False,
        recover_from_backup: bool = True,
    ) -> None:
        super().__init__(path=path)

        self._lock = RLock()

        self._load = load
        self._backup = backup
        self._remove_obsolete = remove_obsolete

        self.default_config = self._set_defaults(version, defaults)

        # Set all values to defaults. They may be overwritten later
        # when loading form file.
        self.reset_to_defaults(save=False)

        self._backup_folder = "backups"
        self._backup_suffix = "bak"

        if load:
            # If config file already exists, it overrides Default options.
            primary_exists = osp.lexists(self.config_path)
            loaded_from_primary = self._load_from_ini(self.config_path)

            if not loaded_from_primary:
                backup_path = self.backup_path_for_version(None)
                if not recover_from_backup and (
                    primary_exists or osp.lexists(backup_path)
                ):
                    raise ConfigLoadError(
                        f"Cannot safely recover persisted state from {self.config_path}"
                    )
                if recover_from_backup:
                    loaded_from_backup = self._load_from_ini(backup_path)

                    if loaded_from_backup:
                        logger.warning("Restored config from backup: %s", backup_path)
            elif backup:
                self._make_backup()

            old_version = self.get_version()

            # Updating defaults only if major/minor version is different.

            if version != old_version:
                if backup and loaded_from_primary:
                    self._make_backup(old_version)

                self.apply_configuration_patches(old_version)

                # Remove deprecated options if major version has changed.
                if remove_obsolete and version.major > old_version.major:
                    self.remove_deprecated_options(save=False)

                # Set new version number.
                self.set_version(version, save=False)

            # Save any changes back to file.
            self.save()

    # --- Helpers and checkers ---------------------------------------------------------

    def _set_defaults(
        self, version: Version, defaults: _DefaultsType | None
    ) -> _DefaultsType:
        """
        Check if defaults are valid and update defaults values.

        :param version: The config version.
        :param defaults: New default config values.
        """
        if defaults:
            defaults = copy.deepcopy(defaults)
        else:
            defaults = {}

        if UserConfig.DEFAULT_SECTION_NAME not in defaults:
            defaults[UserConfig.DEFAULT_SECTION_NAME] = {}

        self.default_config = defaults
        self.default_config[UserConfig.DEFAULT_SECTION_NAME]["version"] = str(version)

        return self.default_config

    def _make_backup(self, version: Version | None = None) -> None:
        """
        Make a backup of the configuration file.

        :param version: If a version is provided, it will be appended to the backup
            file name.
        """
        backup_path = self.backup_path_for_version(version)
        os.makedirs(osp.dirname(backup_path), exist_ok=True)

        try:
            shutil.copyfile(self.config_path, backup_path)
        except OSError:
            pass

    def _load_from_ini(self, path: str) -> bool:
        """
        Loads the configuration from the given path. Overwrites any current values
        stored in memory.

        :param path: Path of config file to load.
        :returns: Whether a valid config file was loaded.
        """
        with self._lock:
            parsed = cp.ConfigParser(interpolation=None)

            try:
                loaded_paths = parsed.read(path, encoding="utf-8")
                if not loaded_paths:
                    return False

                Version(
                    parsed.get(
                        UserConfig.DEFAULT_SECTION_NAME,
                        "version",
                        raw=True,
                    )
                )
            except (cp.Error, InvalidVersion, UnicodeError):
                logger.error("Could not load config file: %s", path, exc_info=True)
                return False

            merged = cp.ConfigParser(interpolation=None)
            self._copy_parser_state(self, merged)
            self._overlay_parser_state(parsed, merged)
            self._copy_parser_state(merged, self)
            return True

    @staticmethod
    def _copy_parser_state(
        source: cp.ConfigParser,
        target: cp.ConfigParser,
    ) -> None:
        """Replace one parser's values without calling overridden mutators."""
        for section in target.sections():
            cp.RawConfigParser.remove_section(target, section)
        target._defaults.clear()  # type: ignore[attr-defined]

        UserConfig._overlay_parser_state(source, target)

    @staticmethod
    def _overlay_parser_state(
        source: cp.ConfigParser,
        target: cp.ConfigParser,
    ) -> None:
        """Overlay explicit parser values without calling overridden mutators."""

        for option, value in source.defaults().items():
            cp.RawConfigParser.set(
                target,
                target.default_section,
                option,
                value,
            )

        for section in source.sections():
            if not target.has_section(section):
                cp.RawConfigParser.add_section(target, section)
            for option, value in source._sections[section].items():  # type: ignore[attr-defined]
                cp.RawConfigParser.set(target, section, option, value)

    def save(self) -> int:
        """Atomically save the config into its associated file."""
        with self._lock:
            if self._backup:
                self._save_snapshot_to(self.backup_path_for_version(None))
            return super().save()

    def remove_deprecated_options(self, save: bool = True) -> None:
        """
        Remove options which are present in the file but not in defaults.
        """
        for section in self.sections():
            for option, _ in self.items(section, raw=True):
                if self.get_default(section, option) is NoDefault:
                    try:
                        self.remove_option(section, option, save)
                        if len(self.items(section, raw=True)) == 0:
                            self.remove_section(section)
                    except cp.NoSectionError:
                        self.remove_section(section, save)

    # --- Compatibility API ------------------------------------------------------------

    def backup_path_for_version(self, version: Version | None) -> str:
        """
        Get backup location based on version.

        :param version: The version of the backup, if any.
        :returns: The back for the backup file.
        """
        directory = osp.join(self._dirname, self._backup_folder)

        if version:
            filename = f"{self._filename}-{str(version)}"
        else:
            filename = self._filename

        name = f"{filename}.{self._suffix}.{self._backup_suffix}"

        return osp.join(directory, name)

    def apply_configuration_patches(self, old_version: Version) -> None:
        """
        Apply any patch to configuration values on version changes.

        To be reimplemented if patches to configuration values are needed.

        :param old_version: Old config version to patch.
        """
        pass

    # --- Public API -------------------------------------------------------------------

    def get_version(self) -> Version:
        """
        Get the current config version.

        :returns: Configuration (not application!) version.
        """
        with self._lock:
            version_str = self.get(UserConfig.DEFAULT_SECTION_NAME, "version")
            return Version(version_str)

    def set_version(self, version: Version, save: bool = True) -> None:
        """
        Set configuration (not application!) version.

        :param version: New version to set.
        :param save: Whether to save changes to drive.
        """
        with self._lock:
            self.set(
                UserConfig.DEFAULT_SECTION_NAME, "version", str(version), save=save
            )

    def reset_to_defaults(
        self,
        section: str | None = None,
        save: bool = True,
    ) -> None:
        """
        Reset config to default values.

        :param section: The section to reset. If not given, reset all sections.
        :param save: Whether to save the changes to the drive.
        """
        with self._lock:
            snapshot = self._configuration_snapshot() if save else None
            save_generation = self.save_generation
            try:
                for sec, options in self.default_config.items():
                    if section is None or section == sec:
                        for option in options:
                            value = options[option]
                            self._set(sec, option, value)
                if save:
                    self.save()
            except BaseException:
                if snapshot is not None and not self.save_committed_since(
                    save_generation
                ):
                    self._restore_configuration_snapshot(snapshot)
                raise

    def _configuration_snapshot(
        self,
    ) -> tuple[dict[str, Any], dict[str, Any], _DefaultsType]:
        """Return parser state which can be restored after a failed save."""
        return (
            copy.deepcopy(self._sections),
            copy.deepcopy(self._defaults),
            copy.deepcopy(self.default_config),
        )

    def _restore_configuration_snapshot(
        self,
        snapshot: tuple[dict[str, Any], dict[str, Any], _DefaultsType],
    ) -> None:
        """Restore parser state after a save failed before its commit point."""
        self._sections, self._defaults, self.default_config = snapshot

    def get_default(self, section: str, option: str) -> Any:
        """
        Get default value for a given ``section`` and ``option``.

        This is useful for type checking in `get` method.

        :param section: Section to search for option.
        :param option: Config option.
        :returns: Config value or None if section / option do not exist.
        """
        with self._lock:
            secdict = self.default_config.get(section, {})
            return secdict.get(option, NoDefault)

    def get(self, section: str, option: str, default: Any = NoDefault) -> Any:  # type: ignore
        """
        Get an option.

        :param section: Config section to search in.
        :param option: Config option to get.
        :param default: Default value to fall back to if not present.
        :returns: Config value.
        :raises cp.NoSectionError: if the section does not exist.
        :raises cp.NoOptionError: if the option does not exist and no default is given.
        """
        with self._lock:
            if not self.has_section(section):
                if default is NoDefault:
                    raise cp.NoSectionError(section)
                else:
                    self.add_section(section)

            if not self.has_option(section, option):
                if default is NoDefault:
                    raise cp.NoOptionError(option, section)
                else:
                    self.set(section, option, default)
                    return default

            raw_value: str = super().get(section, option, raw=True)
            default_value = self.get_default(section, option)
            value: Any

            if isinstance(default_value, str):
                value = raw_value
            else:
                try:
                    value = ast.literal_eval(raw_value)
                except (SyntaxError, ValueError):
                    value = raw_value

            if default_value is not NoDefault:
                if type(default_value) is not type(value):
                    logger.error(
                        f"Inconsistent config type for [{section}][{option}]. "
                        f"Expected {default_value.__class__.__name__} but "
                        f"got {value.__class__.__name__}."
                    )

            return value

    def set_default(self, section: str, option: str, default_value: Any) -> None:
        """
        Set Default value for a given `section`, `option`.

        If the section or option does not exist, it will be created.
        """
        with self._lock:
            if section not in self.default_config:
                self.default_config[section] = {}

            self.default_config[section][option] = default_value

    def set(self, section: str, option: str, value: Any, save: bool = True) -> None:  # type: ignore
        """
        Set an ``option` on a given ``section``.

        If section is None, the ``option`` is added to the default section.

        :param section: Config section to search in.
        :param option: Config option to set.
        :param value: Config value.
        :param save: Whether to save the changes to the drive.
        """
        with self._lock:
            snapshot = self._configuration_snapshot() if save else None
            save_generation = self.save_generation
            default_value = self.get_default(section, option)
            try:
                if default_value is NoDefault:
                    default_value = value
                    self.set_default(section, option, default_value)

                if isinstance(default_value, float) and isinstance(value, int):
                    value = float(value)

                if type(default_value) is not type(value):
                    raise ValueError(
                        f"Inconsistent type for config value [{section}][{option}]. "
                        f"Expected {default_value.__class__.__name__} but "
                        f"got {value.__class__.__name__}."
                    )

                self._set(section, option, value)

                if save:
                    self.save()
            except BaseException:
                if snapshot is not None and not self.save_committed_since(
                    save_generation
                ):
                    self._restore_configuration_snapshot(snapshot)
                raise

    def remove_section(self, section: str, save: bool = True) -> bool:
        """
        Remove ``section`` and all options within it.

        :param section: Section to remove from the config file.
        :param save: Whether to save the changes to the drive.
        :returns: Whether the section was removed successfully.
        """
        with self._lock:
            snapshot = self._configuration_snapshot() if save else None
            save_generation = self.save_generation
            try:
                res = super().remove_section(section)
                if save:
                    self.save()
                return res
            except BaseException:
                if snapshot is not None and not self.save_committed_since(
                    save_generation
                ):
                    self._restore_configuration_snapshot(snapshot)
                raise

    def remove_option(self, section: str, option: str, save: bool = True) -> bool:
        """
        Remove ``option`` from ``section``.

        :param section: Section to look for the option.
        :param option: Option to remove from the config file.
        :param save: Whether to save the changes to the drive.
        :returns: Whether the section was removed successfully.
        """
        with self._lock:
            snapshot = self._configuration_snapshot() if save else None
            save_generation = self.save_generation
            try:
                res = super().remove_option(section, option)
                if save:
                    self.save()
                return res
            except BaseException:
                if snapshot is not None and not self.save_committed_since(
                    save_generation
                ):
                    self._restore_configuration_snapshot(snapshot)
                raise

    def cleanup(self) -> None:
        """Remove files associated with config and reset to defaults."""
        with self._lock:
            self.reset_to_defaults(save=False)
            backup_path = osp.join(self._dirname, self._backup_folder)

            # remove config file
            try:
                os.remove(self.config_path)
            except FileNotFoundError:
                pass

            # remove saved backups
            if osp.isdir(backup_path):
                for file in os.scandir(backup_path):
                    backup_ending = f".{self._suffix}.{self._backup_suffix}"
                    unversioned_name = f"{self._filename}{backup_ending}"
                    version_prefix = f"{self._filename}-"
                    version_str = file.name[len(version_prefix) : -len(backup_ending)]

                    is_unversioned = file.name == unversioned_name
                    is_versioned = file.name.startswith(
                        version_prefix
                    ) and file.name.endswith(backup_ending)

                    if is_versioned:
                        try:
                            Version(version_str)
                        except InvalidVersion:
                            is_versioned = False

                    if is_unversioned or is_versioned:
                        try:
                            os.remove(file.path)
                        except FileNotFoundError:
                            pass


# ======================================================================================
# Wrapper classes
# ======================================================================================


class PersistentMutableSet(MutableSet[_T]):
    """Wraps a list in our state file as a Mapping

    :param conf: UserConfig instance to store the set.
    :param section: Section name in state file.
    :param option: Option name in state file.
    """

    def __init__(self, conf: UserConfig, section: str, option: str) -> None:
        super().__init__()
        self.section = section
        self.option = option
        self._conf = conf
        self._lock = RLock()

    def __iter__(self) -> Iterator[_T]:
        with self._lock:
            return iter(self._conf.get(self.section, self.option))

    def __contains__(self, entry: Any) -> bool:
        with self._lock:
            return entry in self._conf.get(self.section, self.option)

    def __len__(self) -> int:
        with self._lock:
            return len(self._conf.get(self.section, self.option))

    def _save_entries(self, entries: Iterable[_T]) -> None:
        with self._conf._lock:
            previous = self._conf.get(self.section, self.option)
            updated = list(entries)
            if set(previous) == set(updated):
                return
            self._conf.set(self.section, self.option, updated)

    def add(self, entry: _T) -> None:
        with self._lock:
            entries = set(self._conf.get(self.section, self.option))
            entries.add(entry)
            self._save_entries(entries)

    def discard(self, entry: _T) -> None:
        with self._lock:
            entries = set(self._conf.get(self.section, self.option))
            entries.discard(entry)
            self._save_entries(entries)

    def update(self, *others: Iterable[_T]) -> None:
        with self._lock:
            entries = set(self._conf.get(self.section, self.option))
            entries.update(*others)
            self._save_entries(entries)

    def difference_update(self, *others: Iterable[_T]) -> None:
        with self._lock:
            entries = set(self._conf.get(self.section, self.option))
            entries.difference_update(*others)
            self._save_entries(entries)

    def clear(self) -> None:
        """Clears all elements."""
        with self._lock:
            self._save_entries([])

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__}(section='{self.section}',"
            f"option='{self.option}', entries={list(self)})>"
        )
