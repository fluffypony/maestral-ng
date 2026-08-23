import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from maestral.client import DropboxClient
from maestral.config import MaestralConfig, MaestralState
from maestral.database.core import Database
from maestral.database.migrations import IndexMigrationError, migrate_index_table
from maestral.database.orm import Manager
from maestral.keyring import CredentialStorage
from maestral.models import IndexEntry
from maestral.sync import SyncEngine
from maestral.utils.appdirs import get_data_path


def create_old_index(
    connection: sqlite3.Connection, *, nullable_id: bool = False
) -> None:
    id_constraint = "" if nullable_id else " NOT NULL"
    connection.execute(f"""
        CREATE TABLE "index" (
            dbx_path_lower BLOB NOT NULL PRIMARY KEY,
            dbx_path_cased BLOB NOT NULL,
            dbx_id TEXT{id_constraint},
            item_type TEXT NOT NULL,
            last_sync REAL,
            rev TEXT NOT NULL,
            content_hash TEXT,
            symlink_target BLOB
        )
        """)
    connection.execute(
        'CREATE INDEX "idx_index_dbx_path_cased" ' 'ON "index" (dbx_path_cased)'
    )
    connection.commit()


def insert_old_index_row(
    connection: sqlite3.Connection,
    *,
    dbx_path_lower: bytes,
    dbx_path_cased: bytes,
    dbx_id: str | None,
    item_type: str = "File",
    last_sync: float | None = 1.5,
    rev: str = "rev",
    content_hash: str | None = "hash",
    symlink_target: bytes | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO "index" (
            dbx_path_lower,
            dbx_path_cased,
            dbx_id,
            item_type,
            last_sync,
            rev,
            content_hash,
            symlink_target
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            dbx_path_lower,
            dbx_path_cased,
            dbx_id,
            item_type,
            last_sync,
            rev,
            content_hash,
            symlink_target,
        ),
    )
    connection.commit()


@pytest.fixture
def old_index_connection() -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(":memory:")
    create_old_index(connection)
    yield connection
    connection.close()


def index_schema(connection: sqlite3.Connection) -> str:
    row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'index'"
    ).fetchone()
    assert row is not None
    return row[0]


def index_rows(connection: sqlite3.Connection) -> list[tuple[object, ...]]:
    return connection.execute('SELECT * FROM "index" ORDER BY 1').fetchall()


def test_index_migration_preserves_rows_and_blob_paths(
    old_index_connection: sqlite3.Connection,
) -> None:
    lower_path = b"/raw-\xff-path"
    cased_path = b"/Raw-\xff-Path"
    symlink_target = b"../target-\xfe"
    insert_old_index_row(
        old_index_connection,
        dbx_path_lower=lower_path,
        dbx_path_cased=cased_path,
        dbx_id="id:stable",
        last_sync=7.25,
        rev="revision",
        content_hash="content-hash",
        symlink_target=symlink_target,
    )

    migrate_index_table(old_index_connection)
    migrate_index_table(old_index_connection)

    columns = {
        row[1]: row
        for row in old_index_connection.execute("PRAGMA table_info('index')")
    }
    assert "dbx_id" not in columns
    assert columns["provider_id"][3] == 1
    assert columns["provider_id"][5] == 1
    assert columns["dbx_path_lower"][3] == 1
    assert columns["dbx_path_lower"][5] == 0

    unique_indexes = [
        row[1]
        for row in old_index_connection.execute("PRAGMA index_list('index')")
        if row[2]
    ]
    assert any(
        [
            row[2]
            for row in old_index_connection.execute(
                f'PRAGMA index_info("{index_name}")'
            )
        ]
        == ["dbx_path_lower"]
        for index_name in unique_indexes
    )

    row = old_index_connection.execute("""
        SELECT
            provider_id,
            dbx_path_lower,
            dbx_path_cased,
            item_type,
            last_sync,
            rev,
            content_hash,
            symlink_target,
            typeof(dbx_path_lower),
            typeof(dbx_path_cased)
        FROM "index"
        """).fetchone()
    assert row == (
        "id:stable",
        lower_path,
        cased_path,
        "File",
        7.25,
        "revision",
        "content-hash",
        symlink_target,
        "blob",
        "blob",
    )

    with pytest.raises(sqlite3.IntegrityError):
        old_index_connection.execute(
            """
            INSERT INTO "index" (
                provider_id, dbx_path_lower, dbx_path_cased, item_type, rev
            ) VALUES (?, ?, ?, ?, ?)
            """,
            ("id:other", lower_path, cased_path, "File", "other-rev"),
        )
    old_index_connection.rollback()


def test_empty_old_index_table_migrates(
    old_index_connection: sqlite3.Connection,
) -> None:
    migrate_index_table(old_index_connection)

    assert "provider_id" in {
        row[1] for row in old_index_connection.execute("PRAGMA table_info('index')")
    }
    assert index_rows(old_index_connection) == []


def test_current_index_model_schema_is_accepted() -> None:
    connection = sqlite3.connect(":memory:")
    Manager(Database(connection), IndexEntry)

    migrate_index_table(connection)

    assert "provider_id" in {
        row[1] for row in connection.execute("PRAGMA table_info('index')")
    }
    connection.close()


@pytest.mark.parametrize(
    ("provider_ids", "nullable_id", "message"),
    [
        ([None], True, "null or empty"),
        ([""], False, "null or empty"),
        (["id:duplicate", "id:duplicate"], False, "duplicate"),
    ],
)
def test_index_migration_rejects_invalid_provider_ids_without_changes(
    provider_ids: list[str | None], nullable_id: bool, message: str
) -> None:
    connection = sqlite3.connect(":memory:")
    create_old_index(connection, nullable_id=nullable_id)
    for index, provider_id in enumerate(provider_ids):
        insert_old_index_row(
            connection,
            dbx_path_lower=f"/path-{index}".encode(),
            dbx_path_cased=f"/Path-{index}".encode(),
            dbx_id=provider_id,
        )
    schema_before = index_schema(connection)
    rows_before = index_rows(connection)

    with pytest.raises(IndexMigrationError, match=message):
        migrate_index_table(connection)

    assert index_schema(connection) == schema_before
    assert index_rows(connection) == rows_before
    assert (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name = '_maestral_index_new'"
        ).fetchone()
        is None
    )
    connection.close()


def test_index_migration_rejects_unknown_schema_without_changes() -> None:
    connection = sqlite3.connect(":memory:")
    connection.execute(
        'CREATE TABLE "index" (dbx_path_lower BLOB PRIMARY KEY, unknown TEXT)'
    )
    connection.execute('INSERT INTO "index" VALUES (?, ?)', (b"/path", "value"))
    connection.commit()
    schema_before = index_schema(connection)
    rows_before = index_rows(connection)

    with pytest.raises(IndexMigrationError, match="Unsupported"):
        migrate_index_table(connection)

    assert index_schema(connection) == schema_before
    assert index_rows(connection) == rows_before
    connection.close()


def test_index_migration_rolls_back_ddl_failure(
    old_index_connection: sqlite3.Connection,
) -> None:
    insert_old_index_row(
        old_index_connection,
        dbx_path_lower=b"/path",
        dbx_path_cased=b"/Path",
        dbx_id="id:stable",
    )
    schema_before = index_schema(old_index_connection)
    rows_before = index_rows(old_index_connection)

    def deny_table_rename(
        action: int,
        _arg1: str | None,
        _arg2: str | None,
        _database: str | None,
        _trigger: str | None,
    ) -> int:
        if action == sqlite3.SQLITE_ALTER_TABLE:
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    old_index_connection.set_authorizer(deny_table_rename)
    with pytest.raises(IndexMigrationError, match="Could not migrate"):
        migrate_index_table(old_index_connection)
    old_index_connection.set_authorizer(None)

    assert index_schema(old_index_connection) == schema_before
    assert index_rows(old_index_connection) == rows_before
    assert (
        old_index_connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name = '_maestral_index_new'"
        ).fetchone()
        is None
    )


def test_sync_engine_migrates_before_manager_and_preserves_cursor(
    config_name: str, tmp_path: Path
) -> None:
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    MaestralConfig(config_name).set("sync", "path", str(dropbox_path))
    state = MaestralState(config_name)
    opaque_cursor = "opaque:cursor/+==:do-not-parse"
    state.set("sync", "cursor", opaque_cursor)

    database_path = Path(get_data_path("maestral", f"{config_name}.db"))
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path)
    create_old_index(connection)
    insert_old_index_row(
        connection,
        dbx_path_lower=b"/path",
        dbx_path_cased=b"/Path",
        dbx_id="id:stable",
    )
    connection.close()

    sync = SyncEngine(DropboxClient(config_name, CredentialStorage(config_name)))
    try:
        entry = sync.get_index_entry("/path")
        assert entry is not None
        assert entry.provider_id == "id:stable"
        assert sync.remote_cursor == opaque_cursor
    finally:
        sync._connection.close()


def test_sync_engine_does_not_reset_unknown_index_or_cursor(
    config_name: str, tmp_path: Path
) -> None:
    dropbox_path = tmp_path / "Dropbox"
    dropbox_path.mkdir()
    MaestralConfig(config_name).set("sync", "path", str(dropbox_path))
    state = MaestralState(config_name)
    opaque_cursor = "opaque:cursor:must-survive"
    state.set("sync", "cursor", opaque_cursor)

    database_path = Path(get_data_path("maestral", f"{config_name}.db"))
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path)
    connection.execute('CREATE TABLE "index" (unknown TEXT)')
    connection.execute('INSERT INTO "index" VALUES (?)', ("keep-me",))
    connection.commit()
    schema_before = index_schema(connection)
    rows_before = index_rows(connection)
    connection.close()

    with pytest.raises(IndexMigrationError, match="Unsupported"):
        SyncEngine(DropboxClient(config_name, CredentialStorage(config_name)))

    connection = sqlite3.connect(database_path)
    assert index_schema(connection) == schema_before
    assert index_rows(connection) == rows_before
    connection.close()
    assert MaestralState(config_name).get("sync", "cursor") == opaque_cursor
