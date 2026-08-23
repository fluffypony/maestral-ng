"""Transactional migrations for Maestral's SQLite database."""

from __future__ import annotations

import sqlite3


class IndexMigrationError(RuntimeError):
    """Raised when the sync index cannot be migrated safely."""


_COMMON_INDEX_COLUMNS = {
    "dbx_path_lower": ("BLOB", 1),
    "dbx_path_cased": ("BLOB", 1),
    "item_type": ("TEXT", 1),
    "last_sync": ("REAL", 0),
    "rev": ("TEXT", 1),
    "content_hash": ("TEXT", 0),
    "symlink_target": ("BLOB", 0),
}

_CREATE_INDEX_TABLE = """
CREATE TABLE "_maestral_index_new" (
    provider_id TEXT NOT NULL PRIMARY KEY,
    dbx_path_lower BLOB NOT NULL UNIQUE,
    dbx_path_cased BLOB NOT NULL,
    item_type TEXT NOT NULL CHECK(item_type IN ('File', 'Folder', 'Unknown')),
    last_sync REAL,
    rev TEXT NOT NULL,
    content_hash TEXT,
    symlink_target BLOB
)
"""


def _index_schema_kind(connection: sqlite3.Connection) -> str:
    columns = {
        row[1]: (str(row[2]).upper(), int(row[3]), int(row[5]))
        for row in connection.execute("PRAGMA table_info('index')")
    }

    old_columns = {
        **{
            name: (*properties, 0) for name, properties in _COMMON_INDEX_COLUMNS.items()
        },
        "dbx_id": ("TEXT", 1, 0),
    }
    old_columns["dbx_path_lower"] = ("BLOB", 1, 1)

    new_columns = {
        **{
            name: (*properties, 0) for name, properties in _COMMON_INDEX_COLUMNS.items()
        },
        "provider_id": ("TEXT", 1, 1),
    }

    old_id = columns.get("dbx_id")
    old_id_is_known = old_id in {("TEXT", 0, 0), ("TEXT", 1, 0)}
    if old_id_is_known and {
        name: properties for name, properties in columns.items() if name != "dbx_id"
    } == {
        name: properties for name, properties in old_columns.items() if name != "dbx_id"
    }:
        return "old"
    if columns == new_columns and _has_unique_path_index(connection):
        return "new"
    return "unknown"


def _has_unique_path_index(connection: sqlite3.Connection) -> bool:
    indexes = connection.execute("PRAGMA index_list('index')")
    for index in indexes:
        if not index[2]:
            continue
        index_columns = connection.execute(
            "SELECT name FROM pragma_index_info(?) ORDER BY seqno",
            (index[1],),
        )
        if tuple(row[0] for row in index_columns) == ("dbx_path_lower",):
            return True
    return False


def _validate_provider_ids(connection: sqlite3.Connection, source_column: str) -> None:
    invalid_id = connection.execute(
        f'SELECT 1 FROM "index" '
        f"WHERE typeof(\"{source_column}\") != 'text' "
        f'OR length("{source_column}") = 0 LIMIT 1'
    ).fetchone()
    if invalid_id is not None:
        raise IndexMigrationError("Sync index contains a null or empty provider ID")

    duplicate_id = connection.execute(
        f'SELECT 1 FROM "index" GROUP BY "{source_column}" '
        "HAVING count(*) > 1 LIMIT 1"
    ).fetchone()
    if duplicate_id is not None:
        raise IndexMigrationError("Sync index contains a duplicate provider ID")


def migrate_index_table(connection: sqlite3.Connection) -> None:
    """Migrate the path-keyed sync index to stable provider IDs.

    The schema is the migration marker. Any unsupported schema or invalid identity
    aborts the transaction without changing the database.
    """
    try:
        connection.execute("BEGIN IMMEDIATE")

        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'index'"
        ).fetchone()
        if table is None:
            connection.commit()
            return

        schema_kind = _index_schema_kind(connection)
        if schema_kind == "unknown":
            raise IndexMigrationError("Unsupported sync index schema")

        source_id = "dbx_id" if schema_kind == "old" else "provider_id"
        _validate_provider_ids(connection, source_id)

        if schema_kind == "new":
            connection.commit()
            return

        source_row_count = connection.execute(
            'SELECT count(*) FROM "index"'
        ).fetchone()[0]
        connection.execute(_CREATE_INDEX_TABLE)
        connection.execute("""
            INSERT INTO "_maestral_index_new" (
                provider_id,
                dbx_path_lower,
                dbx_path_cased,
                item_type,
                last_sync,
                rev,
                content_hash,
                symlink_target
            )
            SELECT
                dbx_id,
                dbx_path_lower,
                dbx_path_cased,
                item_type,
                last_sync,
                rev,
                content_hash,
                symlink_target
            FROM "index"
            """)
        copied_row_count = connection.execute(
            'SELECT count(*) FROM "_maestral_index_new"'
        ).fetchone()[0]
        if copied_row_count != source_row_count:
            raise IndexMigrationError("Sync index migration lost rows")
        connection.execute('DROP TABLE "index"')
        connection.execute('ALTER TABLE "_maestral_index_new" RENAME TO "index"')
        connection.execute(
            'CREATE INDEX "idx_index_dbx_path_cased" ON "index" (dbx_path_cased)'
        )
        connection.commit()
    except BaseException as exc:
        connection.rollback()
        if isinstance(exc, IndexMigrationError):
            raise
        if not isinstance(exc, Exception):
            raise
        raise IndexMigrationError("Could not migrate the sync index") from exc
