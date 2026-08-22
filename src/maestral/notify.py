"""Notification levels shared by the daemon, CLI, and desktop app."""

__all__ = [
    "NONE",
    "ERROR",
    "SYNCISSUE",
    "FILECHANGE",
    "level_name_to_number",
    "level_number_to_name",
]


NONE = 100
"""No desktop notifications"""
ERROR = 40
"""Notify only on fatal errors"""
SYNCISSUE = 30
"""Notify for sync issues and higher"""
FILECHANGE = 15
"""Notify for all remote file changes"""

_level_to_name = {
    NONE: "NONE",
    ERROR: "ERROR",
    SYNCISSUE: "SYNCISSUE",
    FILECHANGE: "FILECHANGE",
}

_name_to_level = {
    "NONE": 100,
    "ERROR": 40,
    "SYNCISSUE": 30,
    "FILECHANGE": 15,
}


def level_number_to_name(number: int) -> str:
    """
    Converts a Maestral notification level number to name.

    :param number: Level number.
    :returns: Level name.
    """
    try:
        return _level_to_name[number]
    except KeyError:
        return f"Level {number}"


def level_name_to_number(name: str) -> int:
    """
    Converts a Maestral notification level name to number.

    :param name: Level name.
    :returns: Level number.
    """
    try:
        return _name_to_level[name]
    except KeyError:
        raise ValueError("Invalid level name")
