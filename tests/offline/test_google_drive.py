from __future__ import annotations

import base64
import hashlib
import io
import json
import os
from collections.abc import Mapping
from dataclasses import replace
from datetime import timezone
from types import SimpleNamespace
from typing import Any
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

import pytest
import requests

from maestral.config import MaestralConfig, remove_configuration
from maestral.core import DeletedMetadata, FileMetadata, FolderMetadata, WriteMode
from maestral.exceptions import UnsupportedProviderOperationError
from maestral.providers.google_drive import (
    GOOGLE_DRIVE_SCOPE,
    DriveChange,
    DriveItem,
    DriveProjection,
    GoogleAbout,
    GoogleDriveClient,
    GoogleDriveError,
    GoogleDriveProvider,
    GoogleOAuth,
    GoogleOAuthLoopback,
    GoogleTokens,
)
from maestral.sync import SyncEngine


class FakeSession:
    def __init__(self, *responses: requests.Response) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        self.calls.append((method, url, kwargs))
        if not self.responses:
            raise AssertionError("No fake response remains")
        return self.responses.pop(0)

    def post(self, url: str, **kwargs: Any) -> requests.Response:
        return self.request("POST", url, **kwargs)


def response(
    status: int,
    payload: Mapping[str, Any] | bytes | None = None,
    *,
    headers: Mapping[str, str] | None = None,
) -> requests.Response:
    value = requests.Response()
    value.status_code = status
    if payload is None:
        value._content = b""
    elif isinstance(payload, bytes):
        value._content = payload
    else:
        value._content = json.dumps(payload).encode()
        value.headers["Content-Type"] = "application/json"
    setattr(value, "_content_consumed", True)
    if headers:
        value.headers.update(headers)
    return value


def item_payload(
    item_id: str,
    name: str,
    *,
    parent: str | None = "root-id",
    mime_type: str = "text/plain",
    version: str = "1",
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": item_id,
        "name": name,
        "mimeType": mime_type,
        "modifiedTime": "2026-08-23T10:20:30.123Z",
        "size": "5",
        "version": version,
        "trashed": False,
        "capabilities": {"canDownload": True, "canEdit": True},
    }
    if parent is not None:
        payload["parents"] = [parent]
    payload.update(extra)
    return payload


def drive_item(
    item_id: str,
    name: str,
    *,
    parent: str | None = "root-id",
    mime_type: str = "text/plain",
    **extra: Any,
) -> DriveItem:
    return DriveItem.from_api(
        item_payload(
            item_id,
            name,
            parent=parent,
            mime_type=mime_type,
            **extra,
        )
    )


def tokens(*, expires_at: float = 10_000.0) -> GoogleTokens:
    return GoogleTokens("access", "refresh", expires_at, GOOGLE_DRIVE_SCOPE)


def client_with_responses(
    *responses: requests.Response,
    expires_at: float = 10_000.0,
    clock: float = 1_000.0,
) -> tuple[GoogleDriveClient, FakeSession, list[GoogleTokens]]:
    session = FakeSession(*responses)
    oauth = GoogleOAuth("desktop-client", session=session, clock=lambda: clock)
    saved: list[GoogleTokens] = []
    client = GoogleDriveClient(
        oauth,
        tokens(expires_at=expires_at),
        saved.append,
        session=session,
        clock=lambda: clock,
        sleep=lambda _delay: None,
    )
    return client, session, saved


def test_oauth_builds_an_exact_pkce_loopback_request() -> None:
    oauth = GoogleOAuth("desktop-client")
    auth = oauth.authorization_request("http://127.0.0.1:49321/oauth2/callback")
    query = parse_qs(urlparse(auth.url).query)

    assert query["client_id"] == ["desktop-client"]
    assert query["scope"] == [GOOGLE_DRIVE_SCOPE]
    assert query["redirect_uri"] == [auth.redirect_uri]
    assert query["state"] == [auth.state]
    assert query["code_challenge_method"] == ["S256"]
    expected = base64.urlsafe_b64encode(
        hashlib.sha256(auth.verifier.encode()).digest()
    ).rstrip(b"=")
    assert query["code_challenge"] == [expected.decode()]

    with pytest.raises(ValueError):
        oauth.authorization_request("https://127.0.0.1:49321/oauth2/callback")
    with pytest.raises(ValueError):
        oauth.authorization_request("http://localhost:49321/oauth2/callback")


def test_oauth_exchanges_and_refreshes_tokens_without_a_client_secret() -> None:
    session = FakeSession(
        response(
            200,
            {
                "access_token": "first",
                "refresh_token": "saved-refresh",
                "expires_in": 3600,
                "scope": GOOGLE_DRIVE_SCOPE,
                "token_type": "Bearer",
            },
        ),
        response(
            200,
            {
                "access_token": "second",
                "expires_in": 1800,
                "scope": GOOGLE_DRIVE_SCOPE,
                "token_type": "Bearer",
            },
        ),
    )
    oauth = GoogleOAuth("desktop-client", session=session, clock=lambda: 100.0)
    auth = oauth.authorization_request("http://127.0.0.1:49321/oauth2/callback")

    first = oauth.exchange_code(auth, "authorization-code")
    second = oauth.refresh(first)

    assert first == GoogleTokens("first", "saved-refresh", 3700.0, GOOGLE_DRIVE_SCOPE)
    assert second == GoogleTokens("second", "saved-refresh", 1900.0, GOOGLE_DRIVE_SCOPE)
    assert session.calls[0][2]["data"]["code_verifier"] == auth.verifier
    assert "client_secret" not in session.calls[0][2]["data"]


def test_oauth_loopback_rejects_wrong_state_then_accepts_one_code() -> None:
    oauth = GoogleOAuth("desktop-client")
    with GoogleOAuthLoopback() as receiver:
        auth = receiver.authorization_request(oauth)
        with pytest.raises(HTTPError) as exc_info:
            urlopen(f"{receiver.redirect_uri}?state=wrong&code=bad", timeout=2)
        assert exc_info.value.code == 400

        with urlopen(
            f"{receiver.redirect_uri}?state={auth.state}&code=accepted", timeout=2
        ) as callback:
            assert callback.status == 200
            assert callback.headers["Cache-Control"] == "no-store"
            assert b"Return to Maestral" in callback.read()

        assert receiver.wait_for_code(timeout=1) == "accepted"


def test_tokens_have_a_strict_keyring_format() -> None:
    original = tokens()
    assert GoogleTokens.from_json(original.to_json()) == original

    with pytest.raises(GoogleDriveError):
        GoogleTokens.from_json('{"accessToken":"x"}')
    with pytest.raises(GoogleDriveError):
        GoogleTokens.from_json("not-json")


def test_drive_items_preserve_stable_identity_and_reject_multiple_parents() -> None:
    item = drive_item("stable-id", "Notes.txt", md5Checksum="a" * 32)
    assert item.id == "stable-id"
    assert item.modified_time is not None
    assert item.modified_time.tzinfo == timezone.utc
    assert item.size == 5

    payload = item_payload("id", "Name")
    payload["parents"] = ["one", "two"]
    with pytest.raises(GoogleDriveError):
        DriveItem.from_api(payload)


def test_projection_uses_id_suffixes_for_case_and_unicode_collisions() -> None:
    folder = drive_item(
        "folder", "Projects", mime_type="application/vnd.google-apps.folder"
    )
    items = [
        folder,
        drive_item("a", "Résumé.txt", parent="folder"),
        drive_item("b", "Re\u0301sume\u0301.TXT", parent="folder"),
        drive_item("c", "plain.txt", parent="folder"),
    ]
    projection = DriveProjection.from_items("root-id", items)

    first = projection.path_for_id("a")
    second = projection.path_for_id("b")
    plain = projection.path_for_id("c")
    assert first is not None and "(Drive " in first
    assert second is not None and "(Drive " in second
    assert first.casefold() != second.casefold()
    assert plain == "/Projects/plain.txt"
    assert projection.item_for_path("/projects/PLAIN.TXT").id == "c"  # type: ignore[union-attr]


def test_projection_escapes_cross_platform_names_and_reserved_devices() -> None:
    items = [
        drive_item("slash", "a/b"),
        drive_item("percent", "100%"),
        drive_item("device", "CON.txt"),
        drive_item("trail", "trail. "),
    ]
    projection = DriveProjection.from_items("root-id", items)

    assert projection.path_for_id("slash") == "/a%2Fb"
    assert projection.path_for_id("percent") == "/100%25"
    assert projection.path_for_id("device") == "/%43ON.txt"
    assert projection.path_for_id("trail") == "/trail.%20"


def test_projection_uses_read_only_stubs_for_native_items_and_shortcuts() -> None:
    document = drive_item(
        "doc",
        "Plan",
        mime_type="application/vnd.google-apps.document",
        webViewLink="https://docs.google.com/document/d/doc/edit",
    )
    shortcut = drive_item(
        "shortcut",
        "Plan link",
        mime_type="application/vnd.google-apps.shortcut",
        shortcutDetails={
            "targetId": "doc",
            "targetMimeType": "application/vnd.google-apps.document",
            "targetResourceKey": "key",
        },
    )
    projection = DriveProjection.from_items("root-id", [document, shortcut])

    assert projection.path_for_id("doc") == "/Plan.gdoc"
    assert projection.path_for_id("shortcut") == "/Plan link.gshortcut"
    doc_stub = json.loads(projection.native_stub("doc"))
    shortcut_stub = json.loads(projection.native_stub("shortcut"))
    assert doc_stub["readOnly"] is True
    assert doc_stub["url"].startswith("https://docs.google.com/")
    assert shortcut_stub["targetId"] == "doc"
    assert shortcut_stub["targetResourceKey"] == "key"


def test_projection_applies_moves_and_removals_by_file_id() -> None:
    one = drive_item("one", "One", mime_type="application/vnd.google-apps.folder")
    two = drive_item("two", "Two", mime_type="application/vnd.google-apps.folder")
    note = drive_item("note", "Note.txt", parent="one", version="1")
    projection = DriveProjection.from_items("root-id", [one, two, note])
    assert projection.path_for_id("note") == "/One/Note.txt"

    moved = drive_item("note", "Moved.txt", parent="two", version="2")
    projection.apply_changes([DriveChange("note", False, moved, None)])
    assert projection.path_for_id("note") == "/Two/Moved.txt"

    projection.apply_changes([DriveChange("note", True, None, None)])
    assert projection.path_for_id("note") is None


def test_projection_ignores_orphans_and_rejects_cycles_atomically() -> None:
    orphan = drive_item("orphan", "Lost.txt", parent="missing")
    projection = DriveProjection.from_items("root-id", [orphan])
    assert projection.projected_items() == []

    first = drive_item(
        "first",
        "First",
        parent="root-id",
        mime_type="application/vnd.google-apps.folder",
    )
    second = drive_item(
        "second",
        "Second",
        parent="first",
        mime_type="application/vnd.google-apps.folder",
    )
    projection = DriveProjection.from_items("root-id", [first, second])
    changed = drive_item(
        "first",
        "First",
        parent="second",
        mime_type="application/vnd.google-apps.folder",
    )
    with pytest.raises(GoogleDriveError):
        projection.apply_changes([DriveChange("first", False, changed, None)])
    assert projection.path_for_id("second") == "/First/Second"


def test_client_lists_all_pages_and_keeps_page_tokens_opaque() -> None:
    client, session, _saved = client_with_responses(
        response(
            200,
            {"files": [item_payload("one", "One")], "nextPageToken": "a+/= token"},
        ),
        response(200, {"files": [item_payload("two", "Two")]}),
    )

    assert [item.id for item in client.list_all_items()] == ["one", "two"]
    assert session.calls[1][2]["params"]["pageToken"] == "a+/= token"


def test_client_reads_changes_and_requires_exact_token_shape() -> None:
    client, _session, _saved = client_with_responses(
        response(
            200,
            {
                "changes": [
                    {"fileId": "gone", "removed": True, "time": "2026-08-23T10:00:00Z"},
                    {
                        "fileId": "live",
                        "removed": False,
                        "file": item_payload("live", "Live"),
                    },
                ],
                "newStartPageToken": "opaque-final",
            },
        )
    )

    page = client.list_changes("opaque-start")
    assert [change.file_id for change in page.changes] == ["gone", "live"]
    assert page.next_page_token is None
    assert page.new_start_page_token == "opaque-final"


def test_client_refreshes_before_expiry_and_after_one_unauthorized_response() -> None:
    refresh_payload = {
        "access_token": "new-access",
        "expires_in": 3600,
        "scope": GOOGLE_DRIVE_SCOPE,
        "token_type": "Bearer",
    }
    session = FakeSession(
        response(200, refresh_payload),
        response(401, {"error": {"code": 401, "message": "expired"}}),
        response(200, refresh_payload),
        response(200, item_payload("id", "Name")),
    )
    oauth = GoogleOAuth("desktop-client", session=session, clock=lambda: 1000.0)
    saved: list[GoogleTokens] = []
    client = GoogleDriveClient(
        oauth,
        tokens(expires_at=1001.0),
        saved.append,
        session=session,
        clock=lambda: 1000.0,
        sleep=lambda _delay: None,
    )

    assert client.get_item("id").id == "id"
    assert len(saved) == 2
    assert session.calls[-1][2]["headers"]["Authorization"] == "Bearer new-access"


def test_client_retries_rate_limits_and_preserves_custom_upload_headers() -> None:
    client, session, _saved = client_with_responses(
        response(200, {"ids": ["generated-id"], "space": "drive"}),
        response(
            403,
            {
                "error": {
                    "message": "slow down",
                    "errors": [{"reason": "rateLimitExceeded"}],
                }
            },
        ),
        response(
            200,
            None,
            headers={
                "Location": "https://www.googleapis.com/upload/drive/v3/files?id=x"
            },
        ),
    )

    session_url = client.start_resumable_upload(
        name="File.txt",
        parent_id="root-id",
        mime_type="text/plain",
        size=5,
    )
    assert session_url.endswith("id=x")
    assert session.calls[1][2]["headers"]["X-Upload-Content-Length"] == "5"
    assert session.calls[2][2]["headers"]["X-Upload-Content-Length"] == "5"
    assert session.calls[1][2]["json"]["id"] == "generated-id"


def test_folder_create_recovers_a_matching_pre_generated_id() -> None:
    folder = item_payload(
        "generated-id",
        "Folder",
        mime_type="application/vnd.google-apps.folder",
        size=None,
    )
    folder.pop("size")
    client, session, _saved = client_with_responses(
        response(200, {"ids": ["generated-id"], "space": "drive"}),
        response(409, {"error": {"code": 409, "message": "already exists"}}),
        response(200, folder),
    )

    created = client.create_folder("Folder", "root-id")

    assert created.id == "generated-id"
    assert session.calls[1][2]["json"]["id"] == "generated-id"


def test_resumable_upload_tracks_server_ranges_and_returns_stable_id() -> None:
    first = response(308, None, headers={"Range": "bytes=0-262143"})
    final_payload = item_payload("uploaded", "File.bin", size="300000")
    client, session, _saved = client_with_responses(first, response(200, final_payload))
    source = io.BytesIO(b"x" * 300000)
    progress: list[int] = []

    item = client.upload_resumable(
        "https://www.googleapis.com/upload/drive/v3/files?upload_id=one",
        source,
        300000,
        chunk_size=262144,
        progress=progress.append,
    )

    assert item.id == "uploaded"
    assert progress == [262144, 300000]
    assert session.calls[0][2]["headers"]["Content-Range"] == "bytes 0-262143/300000"
    assert (
        session.calls[1][2]["headers"]["Content-Range"] == "bytes 262144-299999/300000"
    )


def test_client_rejects_non_google_resumable_urls() -> None:
    client, _session, _saved = client_with_responses()
    with pytest.raises(ValueError):
        client.upload_resumable("https://example.com/upload/file", io.BytesIO(), 0)


class FakeDriveAPI:
    """Stateful fake for provider-level and sync-level Drive tests."""

    def __init__(self, items: list[DriveItem], blobs: Mapping[str, bytes]) -> None:
        self.root = drive_item(
            "root-id",
            "My Drive",
            parent=None,
            mime_type="application/vnd.google-apps.folder",
        )
        self.items = {item.id: item for item in items}
        self.blobs = dict(blobs)
        self.changes: list[DriveChange] = []
        self.tokens: dict[str, int] = {}
        self.generated_ids: list[str] = []
        self.uploads: dict[str, tuple[str, str, str, int]] = {}
        self.fail_upload = False
        self._next_id = 1

    def _token(self, offset: int) -> str:
        token = f"opaque/{offset}+= cursor"
        self.tokens[token] = offset
        return token

    def _record(self, change: DriveChange) -> None:
        self.changes.append(change)

    def get_root(self) -> DriveItem:
        return self.root

    def get_about(self) -> GoogleAbout:
        return GoogleAbout(
            "account-id",
            "Drive User",
            "drive@example.com",
            None,
            12,
            100,
        )

    def list_all_items(self) -> list[DriveItem]:
        return list(self.items.values())

    def get_start_page_token(self) -> str:
        return self._token(len(self.changes))

    def list_changes(self, page_token: str):
        from maestral.providers.google_drive import DriveChangePage

        offset = self.tokens[page_token]
        return DriveChangePage(
            tuple(self.changes[offset:]),
            None,
            self._token(len(self.changes)),
        )

    def download_blob(
        self,
        file_id: str,
        destination: Any,
        progress: Any = None,
    ) -> DriveItem:
        destination.write(self.blobs[file_id])
        if progress:
            progress(destination.tell())
        return self.items[file_id]

    def generate_file_id(self) -> str:
        file_id = f"generated-{self._next_id}"
        self._next_id += 1
        self.generated_ids.append(file_id)
        return file_id

    def create_folder(self, name: str, parent_id: str) -> DriveItem:
        file_id = self.generate_file_id()
        item = drive_item(
            file_id,
            name,
            parent=parent_id,
            mime_type="application/vnd.google-apps.folder",
        )
        self.items[file_id] = item
        self._record(DriveChange(file_id, False, item, None))
        return item

    def delete_item(self, file_id: str) -> None:
        self.items.pop(file_id)
        self.blobs.pop(file_id, None)
        self._record(DriveChange(file_id, True, None, None))

    def move_item(
        self,
        file_id: str,
        *,
        old_parent_id: str,
        new_parent_id: str,
        new_name: str,
    ) -> DriveItem:
        item = self.items[file_id]
        assert item.parent_id == old_parent_id
        moved = replace(
            item,
            name=new_name,
            parent_id=new_parent_id,
            version=str(int(item.version) + 1),
        )
        self.items[file_id] = moved
        self._record(DriveChange(file_id, False, moved, None))
        return moved

    def start_resumable_upload(
        self,
        *,
        name: str,
        parent_id: str,
        mime_type: str,
        size: int,
        file_id: str | None = None,
    ) -> str:
        file_id = file_id or self.generate_file_id()
        session = f"fake-session-{len(self.uploads)}"
        self.uploads[session] = (file_id, name, parent_id, size)
        return session

    def upload_resumable(
        self,
        session_url: str,
        source: Any,
        size: int,
        *,
        offset: int = 0,
        chunk_size: int = 8 * 1024 * 1024,
        progress: Any = None,
    ) -> DriveItem:
        del offset, chunk_size
        if self.fail_upload:
            raise GoogleDriveError("Fake upload failed", "The fake rejected the data.")
        file_id, name, parent_id, expected_size = self.uploads[session_url]
        assert size == expected_size
        data = source.read()
        assert len(data) == size
        old = self.items.get(file_id)
        if old is not None:
            parent_id = old.parent_id or parent_id
            version = str(int(old.version) + 1)
        else:
            version = "1"
        item = drive_item(
            file_id,
            name,
            parent=parent_id,
            version=version,
            size=str(size),
            md5Checksum=hashlib.md5(data, usedforsecurity=False).hexdigest(),
        )
        self.items[file_id] = item
        self.blobs[file_id] = data
        self._record(DriveChange(file_id, False, item, None))
        if progress:
            progress(size)
        return item

    def external_move(self, file_id: str, parent_id: str, name: str) -> DriveItem:
        item = self.items[file_id]
        return self.move_item(
            file_id,
            old_parent_id=item.parent_id or "root-id",
            new_parent_id=parent_id,
            new_name=name,
        )

    def external_update(self, file_id: str, data: bytes) -> DriveItem:
        item = self.items[file_id]
        updated = replace(
            item,
            size=len(data),
            md5_checksum=hashlib.md5(data, usedforsecurity=False).hexdigest(),
            version=str(int(item.version) + 1),
        )
        self.items[file_id] = updated
        self.blobs[file_id] = data
        self._record(DriveChange(file_id, False, updated, None))
        return updated


def provider_with_fake_api(
    config_name: str,
    api: FakeDriveAPI,
) -> GoogleDriveProvider:
    credentials = SimpleNamespace(account_id="account-id", token=None)
    return GoogleDriveProvider(
        config_name,
        credentials,  # type: ignore[arg-type]
        drive_client=api,  # type: ignore[arg-type]
        client_id="desktop-client",
        sleep=lambda _delay: None,
    )


def test_provider_initial_listing_projects_duplicates_and_native_stubs(
    config_name: str,
) -> None:
    first = drive_item("first", "Same.txt", md5Checksum="a" * 32)
    second = drive_item("second", "same.TXT", md5Checksum="b" * 32)
    native = drive_item(
        "native",
        "Plan",
        mime_type="application/vnd.google-apps.document",
        webViewLink="https://docs.google.com/document/d/native/edit",
    )
    api = FakeDriveAPI([first, second, native], {})
    provider = provider_with_fake_api(config_name, api)

    result = provider.list_folder("/", recursive=True)
    paths = {entry.id: entry.path_display for entry in result.entries}  # type: ignore[attr-defined]
    assert "(Drive " in paths["first"]
    assert "(Drive " in paths["second"]
    assert paths["native"] == "/Plan.gdoc"
    assert result.cursor.startswith("opaque/")

    destination = io.BytesIO()
    metadata = provider.download("/Plan.gdoc", destination)
    assert json.loads(destination.getvalue())["readOnly"] is True
    assert (
        metadata.content_hash
        == hashlib.md5(destination.getvalue(), usedforsecurity=False).hexdigest()
    )


def test_provider_upload_download_move_delete_and_failure_rollback(
    config_name: str,
) -> None:
    api = FakeDriveAPI([], {})
    provider = provider_with_fake_api(config_name, api)
    provider.list_folder("/", recursive=True)

    uploaded = provider.upload(io.BytesIO(b"hello"), "/New.txt")
    assert uploaded.id == api.generated_ids[-1]
    assert (
        uploaded.content_hash
        == hashlib.md5(b"hello", usedforsecurity=False).hexdigest()
    )
    downloaded = io.BytesIO()
    assert provider.download("/New.txt", downloaded).id == uploaded.id
    assert downloaded.getvalue() == b"hello"

    moved = provider.move("/New.txt", "/Moved.txt")
    assert moved.id == uploaded.id
    assert moved.path_display == "/Moved.txt"
    removed = provider.remove("/Moved.txt", parent_rev=moved.rev)
    assert removed.id == uploaded.id
    assert provider.get_metadata("/Moved.txt") is None

    before = provider._ensure_projection().projected_items()
    api.fail_upload = True
    with pytest.raises(GoogleDriveError, match="Fake upload failed"):
        provider.upload(io.BytesIO(b"failed"), "/Failed.txt", WriteMode.Add)
    assert provider._ensure_projection().projected_items() == before


def test_provider_resumes_opaque_cursor_and_emits_identity_move(
    config_name: str,
) -> None:
    note = drive_item("note", "Note.txt", md5Checksum="a" * 32)
    folder = drive_item(
        "folder",
        "Folder",
        mime_type="application/vnd.google-apps.folder",
    )
    api = FakeDriveAPI([note, folder], {"note": b"note"})
    provider = provider_with_fake_api(config_name, api)
    initial = provider.list_folder("/", recursive=True)
    indexed = {
        entry.id: entry.path_display
        for entry in initial.entries
        if isinstance(entry, (FileMetadata, FolderMetadata))
    }

    api.external_move("note", "folder", "Moved.txt")
    resumed_provider = provider_with_fake_api(config_name, api)
    pages = list(resumed_provider.list_remote_changes_iterator(initial.cursor, indexed))
    assert len(pages) == 1
    assert pages[0].cursor != initial.cursor
    assert any(
        isinstance(entry, DeletedMetadata) and entry.path_display == "/Note.txt"
        for entry in pages[0].entries
    )
    moved = next(entry for entry in pages[0].entries if isinstance(entry, FileMetadata))
    assert moved.id == "note"
    assert moved.path_display == "/Folder/Moved.txt"


def test_provider_folder_move_reprojects_the_full_subtree(config_name: str) -> None:
    folder = drive_item(
        "folder",
        "Old",
        mime_type="application/vnd.google-apps.folder",
    )
    child = drive_item(
        "child",
        "Child.txt",
        parent="folder",
        md5Checksum="a" * 32,
    )
    api = FakeDriveAPI([folder, child], {"child": b"child"})
    provider = provider_with_fake_api(config_name, api)
    initial = provider.list_folder("/", recursive=True)
    indexed = {
        entry.id: entry.path_display
        for entry in initial.entries
        if isinstance(entry, (FileMetadata, FolderMetadata))
    }

    api.external_move("folder", "root-id", "New")
    page = next(provider.list_remote_changes_iterator(initial.cursor, indexed))

    assert {
        entry.path_display
        for entry in page.entries
        if isinstance(entry, DeletedMetadata)
    } == {"/Old", "/Old/Child.txt"}
    assert {
        entry.path_display
        for entry in page.entries
        if isinstance(entry, (FileMetadata, FolderMetadata))
    } == {"/New", "/New/Child.txt"}


def test_provider_native_stub_rejects_content_updates(config_name: str) -> None:
    native = drive_item(
        "native",
        "Plan",
        mime_type="application/vnd.google-apps.document",
    )
    provider = provider_with_fake_api(config_name, FakeDriveAPI([native], {}))
    provider.list_folder("/", recursive=True)

    with pytest.raises(Exception, match="read-only"):
        provider.upload(
            io.BytesIO(b"changed"),
            "/Plan.gdoc",
            WriteMode.Update,
            update_rev="1",
        )


def test_provider_saves_refreshed_tokens_through_scoped_storage(
    config_name: str,
) -> None:
    credentials = SimpleNamespace(account_id="account-id", token=None)
    saved: list[tuple[str, str, bool]] = []

    def save_creds(account_id: str, token: str, allow_plaintext: bool = False) -> None:
        saved.append((account_id, token, allow_plaintext))

    credentials.save_creds = save_creds  # type: ignore[attr-defined]
    provider = GoogleDriveProvider(
        config_name,
        credentials,  # type: ignore[arg-type]
        drive_client=FakeDriveAPI([], {}),  # type: ignore[arg-type]
        client_id="desktop-client",
    )
    refreshed = GoogleTokens("new-access", "saved-refresh", 9999, GOOGLE_DRIVE_SCOPE)

    provider._save_refreshed_tokens(refreshed)

    assert saved == [("account-id", refreshed.to_json(), True)]


def test_provider_account_space_and_unsupported_optional_apis(
    config_name: str,
) -> None:
    provider = provider_with_fake_api(config_name, FakeDriveAPI([], {}))

    account = provider.get_account_info()
    usage = provider.get_space_usage()

    assert account.account_id == "account-id"
    assert account.email == "drive@example.com"
    assert account.root_info.root_namespace_id == "root-id"
    assert usage.used == 12
    assert usage.allocated == 100
    with pytest.raises(UnsupportedProviderOperationError):
        provider.list_revisions("/File.txt")
    with pytest.raises(UnsupportedProviderOperationError):
        provider.create_shared_link("/File.txt")


def test_provider_requires_explicit_google_client_id(
    config_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MAESTRAL_GOOGLE_CLIENT_ID", raising=False)
    credentials = SimpleNamespace(account_id=None, token=None)
    provider = GoogleDriveProvider(config_name, credentials)  # type: ignore[arg-type]

    with pytest.raises(GoogleDriveError, match="MAESTRAL_GOOGLE_CLIENT_ID"):
        provider.get_auth_url()


def test_sync_engine_initial_index_and_local_upload_use_drive_md5(
    config_name: str,
    tmp_path: Any,
) -> None:
    data = b"remote data"
    remote = drive_item(
        "remote",
        "Remote.txt",
        size=str(len(data)),
        md5Checksum=hashlib.md5(data, usedforsecurity=False).hexdigest(),
    )
    api = FakeDriveAPI([remote], {"remote": data})
    MaestralConfig(config_name).set("auth", "provider", "google_drive")
    provider = provider_with_fake_api(config_name, api)
    sync = SyncEngine(provider)
    root = tmp_path / "Drive"
    root.mkdir()
    sync.dropbox_path = os.fspath(root)
    sync.create_root_marker()
    try:
        sync.download_sync_cycle()
        assert (root / "Remote.txt").read_bytes() == data
        assert sync._state.get("sync", "cursors") == {
            "google_drive": sync.remote_cursor
        }
        assert sync._state.get("sync", "cursor") == ""
        entry = sync.get_index_entry("/remote.txt")
        assert entry is not None
        assert entry.provider_id == "remote"
        assert (
            entry.content_hash == hashlib.md5(data, usedforsecurity=False).hexdigest()
        )

        (root / "Local.txt").write_bytes(b"local data")
        sync.upload_local_changes_while_inactive()
        uploaded = provider.get_metadata("/Local.txt")
        assert isinstance(uploaded, FileMetadata)
        assert (
            uploaded.content_hash
            == hashlib.md5(b"local data", usedforsecurity=False).hexdigest()
        )

        api.external_move("remote", "root-id", "Moved remote.txt")
        api.external_update("remote", b"updated remote data")
        sync.download_sync_cycle()
        assert not (root / "Remote.txt").exists()
        assert (root / "Moved remote.txt").read_bytes() == b"updated remote data"
        assert sync.get_index_entry("/moved remote.txt").provider_id == "remote"  # type: ignore[union-attr]

        api.delete_item("remote")
        sync.download_sync_cycle()
        assert not (root / "Moved remote.txt").exists()
        assert sync.get_index_entry("/moved remote.txt") is None
    finally:
        sync._connection.close()
        remove_configuration(config_name)
