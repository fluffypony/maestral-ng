import sqlite3

from maestral.database.core import Database
from maestral.database.orm import Manager
from maestral.models import HashCacheEntry


def test_manager_update_refreshes_cached_row() -> None:
    db = Database(sqlite3.connect(":memory:"))
    manager = Manager(db, HashCacheEntry)

    old_entry = HashCacheEntry(
        inode=1,
        local_path="/old",
        hash_str="old-hash",
        mtime=1.0,
    )
    manager.save(old_entry)

    new_entry = HashCacheEntry(
        inode=1,
        local_path="/new",
        hash_str="new-hash",
        mtime=2.0,
    )
    manager.update(new_entry)

    assert manager.get(1) is new_entry
