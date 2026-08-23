from __future__ import annotations

import base64
import hashlib
import io
import json
from collections.abc import Mapping
from datetime import timezone
from typing import Any
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

import pytest
import requests

from maestral.providers.google_drive import (
    GOOGLE_DRIVE_SCOPE,
    DriveChange,
    DriveItem,
    DriveProjection,
    GoogleDriveClient,
    GoogleDriveError,
    GoogleOAuth,
    GoogleOAuthLoopback,
    GoogleTokens,
)


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
