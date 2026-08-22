import os
import platform
import re
from typing import List, TypeVar

from ..utils.appdirs import get_conf_path, get_data_path
from .main import MaestralConfig, MaestralState
from .user import PersistentMutableSet

__all__ = [
    "MaestralConfig",
    "MaestralState",
    "PersistentMutableSet",
    "list_configs",
    "remove_configuration",
    "validate_config_name",
]


_C = TypeVar("_C", bound=str)

_WINDOWS_RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}


def list_configs() -> List[str]:
    """
    Lists all maestral configs.

    :returns: A list of all currently existing config files.
    """
    configs = []
    for file in os.listdir(get_conf_path("maestral")):
        if file.endswith(".ini"):
            configs.append(os.path.splitext(os.path.basename(file))[0])

    return configs


def remove_configuration(config_name: str) -> None:
    """
    Removes all config and state files associated with the given configuration.

    :param config_name: The configuration to remove.
    """

    MaestralConfig(config_name).cleanup()
    MaestralState(config_name).cleanup()

    data_path = get_data_path("maestral")

    files = []

    for file_name in os.listdir(data_path):
        stem, extension = os.path.splitext(file_name)
        if extension and stem == config_name:
            files.append(os.path.join(data_path, file_name))

    for file in files:
        try:
            os.unlink(file)
        except OSError:
            pass


def validate_config_name(string: _C) -> _C:
    """
    Validates that the config name contains only safe filename characters.

    :param string: String to validate.
    :returns: The input value.
    :raises ValueError: if the config name contains unsupported characters.
    """
    if re.fullmatch(r"[A-Za-z0-9._-]+", string) is None or not string.strip("."):
        raise ValueError(
            "Config name may contain only letters, numbers, periods, underscores, "
            "and hyphens"
        )

    if platform.system() == "Windows" and (
        string.endswith(".")
        or string.split(".", 1)[0].upper() in _WINDOWS_RESERVED_NAMES
    ):
        raise ValueError("Config name is reserved on Windows")

    return string
