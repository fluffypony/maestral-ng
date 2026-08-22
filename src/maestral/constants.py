"""
This module provides constants used throughout the maestral, the GUI and CLI. It should
be kept free of memory heavy imports.
"""

import platform

# system imports
import sys
from enum import Enum

FROZEN = getattr(sys, "frozen", False)

# app
BUNDLE_ID = "com.samschott.maestral"
ENV = {"PYTHONOPTIMIZE": "2", "LC_CTYPE": "UTF-8"}
DEFAULT_CONFIG_NAME = "maestral"

# sync
OLD_REV_FILE = ".maestral"
MIGNORE_FILE = ".mignore"
FILE_CACHE = ".maestral.cache"
ROOT_MARKER_FILE = ".maestral-root"

EXCLUDED_FILE_NAMES = frozenset(
    [
        "desktop.ini",
        "Thumbs.db",
        "thumbs.db",
        ".DS_Store",
        ".ds_store",
        ".Spotlight-V100",
        ".Trashes",
        ".fseventsd",
        ".localized",
        ".TemporaryItems",
        "Icon\r",
        "icon\r",
        ".com.apple.timemachine.supported",
        ".dropbox",
        ".dropbox.attr",
        ".dropbox.cache",
        FILE_CACHE,
        OLD_REV_FILE,
        ROOT_MARKER_FILE,
    ]
)

EXCLUDED_DIR_NAMES = frozenset([".dropbox.cache", FILE_CACHE])

# state messages
IDLE = "Up to date"
SYNCING = "Syncing..."
PAUSED = "Paused"
CONNECTED = "Connected"
DISCONNECTED = "Connection lost"
CONNECTING = "Connecting..."
SYNC_ERROR = "Sync error"
ERROR = "Fatal error"


# file status enum
class FileStatus(Enum):
    """Enumeration of sync status"""

    Unwatched = "unwatched"
    Uploading = "uploading"
    Downloading = "downloading"
    Error = "error"
    Synced = "up to date"


# platform detection
IS_MACOS = platform.system() == "Darwin"
IS_LINUX = platform.system() == "Linux"

# keys
DROPBOX_APP_KEY = "2jmbq42w7vof78h"

# urls
GITHUB_RELEASES_API = "https://api.github.com/repos/samschott/maestral/releases"
