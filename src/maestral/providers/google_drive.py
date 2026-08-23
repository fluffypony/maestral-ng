"""Google Drive v3 primitives for Maestral.

This module uses the REST API directly. It keeps Drive page tokens opaque, treats file
IDs as identity, and projects Drive's non-unique names to deterministic local paths.
"""

from __future__ import annotations

import base64
import hashlib
import html
import json
import mimetypes
import os
import posixpath
import random
import re
import secrets
import threading
import time
import unicodedata
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import PurePosixPath
from queue import Empty, Queue
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Final,
    Protocol,
    TypeVar,
    cast,
    overload,
)
from urllib.parse import parse_qs, urlencode, urlparse

import requests

from maestral import __version__
from maestral.config import MaestralState
from maestral.core import (
    Account,
    AccountType,
    DeletedMetadata,
    FileMetadata,
    FolderMetadata,
    FullAccount,
    LinkAccessLevel,
    LinkAudience,
    ListFolderResult,
    Metadata,
    PersonalSpaceUsage,
    RootInfo,
    SharedLinkMetadata,
    UserRootInfo,
    WriteMode,
)
from maestral.exceptions import (
    DataChangedError,
    DataCorruptionError,
    FileConflictError,
    FolderConflictError,
    InvalidDbidError,
    IsAFolderError,
    MaestralApiError,
    NotAFolderError,
    NotFoundError,
    NotLinkedError,
    ProviderConnectionError,
    ProviderServerError,
    UnsupportedFileError,
    UnsupportedProviderOperationError,
)
from maestral.keyring import CredentialStorage
from maestral.logging import scoped_logger
from maestral.utils import natural_size
from maestral.utils.hashing import ContentHasherFactory, md5_content_hasher
from maestral.utils.path import normalize, opener_no_symlink

if TYPE_CHECKING:
    from maestral.models import SyncEvent

GOOGLE_DRIVE_SCOPE: Final = "https://www.googleapis.com/auth/drive"
GOOGLE_AUTH_URL: Final = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL: Final = "https://oauth2.googleapis.com/token"
GOOGLE_DRIVE_API: Final = "https://www.googleapis.com/drive/v3"
GOOGLE_DRIVE_UPLOAD_API: Final = "https://www.googleapis.com/upload/drive/v3"
GOOGLE_DESKTOP_CLIENT_ID: Final = ""
GOOGLE_CLIENT_ID_ENV: Final = "MAESTRAL_GOOGLE_CLIENT_ID"

FOLDER_MIME: Final = "application/vnd.google-apps.folder"
SHORTCUT_MIME: Final = "application/vnd.google-apps.shortcut"
GOOGLE_MIME_PREFIX: Final = "application/vnd.google-apps."
_MAX_JSON_BYTES: Final = 8 * 1024 * 1024
_MAX_NAME_BYTES: Final = 240
_MAX_RETRIES: Final = 5
_RESUMABLE_CHUNK_GRANULARITY: Final = 256 * 1024
_DEFAULT_RESUMABLE_CHUNK: Final = 8 * 1024 * 1024
_RETRY_STATUSES: Final = frozenset({429, 500, 502, 503, 504})
_RETRY_REASONS: Final = frozenset(
    {"rateLimitExceeded", "userRateLimitExceeded", "backendError"}
)
_FORBIDDEN_WINDOWS_CHARS: Final = frozenset('<>:"/\\|?*')
_DOS_NAMES: Final = frozenset(
    {"CON", "PRN", "AUX", "NUL"}
    | {f"COM{index}" for index in range(1, 10)}
    | {f"LPT{index}" for index in range(1, 10)}
)
_NATIVE_EXTENSIONS: Final[Mapping[str, str]] = {
    "application/vnd.google-apps.document": ".gdoc",
    "application/vnd.google-apps.spreadsheet": ".gsheet",
    "application/vnd.google-apps.presentation": ".gslides",
    "application/vnd.google-apps.drawing": ".gdraw",
    "application/vnd.google-apps.form": ".gform",
    "application/vnd.google-apps.map": ".gmap",
    "application/vnd.google-apps.site": ".gsite",
    "application/vnd.google-apps.script": ".gscript",
    "application/vnd.google-apps.vid": ".gvid",
}
_FILE_FIELDS: Final = (
    "id,name,mimeType,parents,modifiedTime,size,md5Checksum,version,trashed,"
    "webViewLink,resourceKey,capabilities(canDownload,canEdit),"
    "shortcutDetails(targetId,targetMimeType,targetResourceKey)"
)


class GoogleDriveError(MaestralApiError):
    """A Google OAuth or Drive API error."""

    def __init__(
        self,
        title: str,
        message: str,
        *,
        status: int | None = None,
        reason: str | None = None,
    ) -> None:
        super().__init__(title, message)
        self.status = status
        self.reason = reason


class GoogleDriveConnectionError(GoogleDriveError, ProviderConnectionError):
    """A connection failure after Drive request retries."""


class GoogleDriveServerError(GoogleDriveError, ProviderServerError):
    """A temporary Drive service failure after request retries."""


class HttpSession(Protocol):
    """The requests session methods used by this provider."""

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response: ...

    def post(self, url: str, **kwargs: Any) -> requests.Response: ...


@dataclass(frozen=True)
class GoogleTokens:
    """OAuth tokens and their absolute expiry time."""

    access_token: str
    refresh_token: str
    expires_at: float
    scope: str

    def to_json(self) -> str:
        """Return a stable keyring representation."""
        return json.dumps(
            {
                "accessToken": self.access_token,
                "refreshToken": self.refresh_token,
                "expiresAt": self.expires_at,
                "scope": self.scope,
            },
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, value: str) -> GoogleTokens:
        """Load and validate a keyring representation."""
        try:
            payload = json.loads(value)
        except (TypeError, json.JSONDecodeError) as exc:
            raise GoogleDriveError(
                "Cannot load Google credentials", "The saved token is not valid JSON."
            ) from exc
        if not isinstance(payload, dict) or set(payload) != {
            "accessToken",
            "refreshToken",
            "expiresAt",
            "scope",
        }:
            raise GoogleDriveError(
                "Cannot load Google credentials",
                "The saved token has an unknown format.",
            )
        access_token = payload["accessToken"]
        refresh_token = payload["refreshToken"]
        expires_at = payload["expiresAt"]
        scope = payload["scope"]
        if (
            not isinstance(access_token, str)
            or not access_token
            or not isinstance(refresh_token, str)
            or not refresh_token
            or not isinstance(expires_at, (int, float))
            or isinstance(expires_at, bool)
            or not isinstance(scope, str)
            or GOOGLE_DRIVE_SCOPE not in scope.split()
        ):
            raise GoogleDriveError(
                "Cannot load Google credentials",
                "The saved token fields are not valid.",
            )
        return cls(access_token, refresh_token, float(expires_at), scope)


@dataclass(frozen=True)
class GoogleOAuthRequest:
    """One PKCE authorization request."""

    url: str
    state: str
    verifier: str
    redirect_uri: str


class GoogleOAuth:
    """Google's installed-app OAuth flow with PKCE."""

    def __init__(
        self,
        client_id: str,
        *,
        session: HttpSession | None = None,
        clock: Callable[[], float] = time.time,
        timeout: float = 30.0,
    ) -> None:
        if not client_id or any(char.isspace() for char in client_id):
            raise ValueError("A Google OAuth client ID is required")
        self.client_id = client_id
        self._session = cast(HttpSession, session or requests.Session())
        self._clock = clock
        self._timeout = timeout

    def authorization_request(self, redirect_uri: str) -> GoogleOAuthRequest:
        """Create a PKCE request for an exact loopback redirect URI."""
        parsed = urlparse(redirect_uri)
        if (
            parsed.scheme != "http"
            or parsed.hostname not in {"127.0.0.1", "::1"}
            or parsed.port is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("The OAuth redirect must use an exact loopback address")

        verifier = secrets.token_urlsafe(72).rstrip("=")
        if not 43 <= len(verifier) <= 128:
            raise RuntimeError("The PKCE verifier length is not valid")
        challenge = _base64url(hashlib.sha256(verifier.encode("ascii")).digest())
        state = secrets.token_urlsafe(32).rstrip("=")
        params = {
            "access_type": "offline",
            "client_id": self.client_id,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
            "include_granted_scopes": "false",
            "prompt": "consent",
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": GOOGLE_DRIVE_SCOPE,
            "state": state,
        }
        return GoogleOAuthRequest(
            url=f"{GOOGLE_AUTH_URL}?{urlencode(params)}",
            state=state,
            verifier=verifier,
            redirect_uri=redirect_uri,
        )

    def exchange_code(self, request: GoogleOAuthRequest, code: str) -> GoogleTokens:
        """Exchange one authorization code and require a refresh token."""
        if not code or len(code) > 4096 or any(char.isspace() for char in code):
            raise GoogleDriveError(
                "Google authorization failed", "The authorization code is not valid."
            )
        payload = self._post_token(
            {
                "client_id": self.client_id,
                "code": code,
                "code_verifier": request.verifier,
                "grant_type": "authorization_code",
                "redirect_uri": request.redirect_uri,
            }
        )
        return _tokens_from_payload(payload, self._clock(), require_refresh=True)

    def refresh(self, tokens: GoogleTokens) -> GoogleTokens:
        """Refresh an access token and keep the saved refresh token."""
        payload = self._post_token(
            {
                "client_id": self.client_id,
                "grant_type": "refresh_token",
                "refresh_token": tokens.refresh_token,
            }
        )
        refreshed = _tokens_from_payload(payload, self._clock(), require_refresh=False)
        return replace(
            refreshed,
            refresh_token=refreshed.refresh_token or tokens.refresh_token,
        )

    def _post_token(self, data: Mapping[str, str]) -> Mapping[str, Any]:
        try:
            response = self._session.post(
                GOOGLE_TOKEN_URL,
                data=data,
                headers={"Accept": "application/json"},
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            raise GoogleDriveError(
                "Google authorization failed", "The token service is not available."
            ) from exc
        status = response.status_code
        try:
            payload = _response_json(response)
        finally:
            response.close()
        if status != 200:
            error = payload.get("error")
            description = payload.get("error_description")
            reason = error if isinstance(error, str) else None
            message = (
                description
                if isinstance(description, str) and description
                else "Google rejected the token request."
            )
            raise GoogleDriveError(
                "Google authorization failed",
                message,
                status=status,
                reason=reason,
            )
        return payload


class GoogleOAuthLoopback:
    """Receive one OAuth callback on an IPv4 loopback port."""

    def __init__(self, path: str = "/oauth2/callback") -> None:
        if not path.startswith("/") or "?" in path or "#" in path:
            raise ValueError("The OAuth callback path is not valid")
        self.path = path
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._expected_state: str | None = None
        self._result: Queue[Mapping[str, list[str]]] = Queue(maxsize=1)

    @property
    def redirect_uri(self) -> str:
        server = self._server
        if server is None:
            raise RuntimeError("The OAuth loopback receiver is not active")
        return f"http://127.0.0.1:{server.server_port}{self.path}"

    def __enter__(self) -> GoogleOAuthLoopback:
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                receiver._handle_callback(self)

            def log_message(self, _format: str, *args: Any) -> None:
                del args

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        self._server = server
        self._thread = threading.Thread(
            target=server.serve_forever,
            name="maestral-google-oauth",
            daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(self, *_exc_info: object) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._server = None
        self._thread = None
        self._expected_state = None

    def authorization_request(self, oauth: GoogleOAuth) -> GoogleOAuthRequest:
        """Create and bind one request to this callback receiver."""
        if self._expected_state is not None:
            raise RuntimeError("An OAuth request is already active")
        request = oauth.authorization_request(self.redirect_uri)
        self._expected_state = request.state
        return request

    def wait_for_code(self, timeout: float = 300.0) -> str:
        """Wait for one state-checked code or authorization error."""
        if self._expected_state is None:
            raise RuntimeError("No OAuth request is active")
        try:
            query = self._result.get(timeout=timeout)
        except Empty as exc:
            raise GoogleDriveError(
                "Google authorization timed out",
                "No authorization response arrived on the local callback.",
            ) from exc
        errors = query.get("error", [])
        codes = query.get("code", [])
        if len(errors) == 1 and not codes:
            description = query.get("error_description", [])
            message = description[0] if len(description) == 1 else errors[0]
            raise GoogleDriveError(
                "Google authorization failed", message, reason=errors[0]
            )
        if len(codes) != 1 or errors:
            raise GoogleDriveError(
                "Google authorization failed", "The callback response is not valid."
            )
        code = codes[0]
        if not code or len(code) > 4096 or any(char.isspace() for char in code):
            raise GoogleDriveError(
                "Google authorization failed", "The callback code is not valid."
            )
        return code

    def _handle_callback(self, handler: BaseHTTPRequestHandler) -> None:
        if handler.client_address[0] != "127.0.0.1" or len(handler.path) > 8192:
            self._send_callback_page(
                handler, 400, "The authorization response is invalid."
            )
            return
        parsed = urlparse(handler.path)
        if parsed.path != self.path or parsed.fragment:
            self._send_callback_page(
                handler, 404, "This callback path is not available."
            )
            return
        try:
            query = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
        except ValueError:
            self._send_callback_page(
                handler, 400, "The authorization query is invalid."
            )
            return
        state = query.get("state", [])
        if len(state) != 1 or not secrets.compare_digest(
            state[0], self._expected_state or ""
        ):
            self._send_callback_page(
                handler, 400, "The authorization state is invalid."
            )
            return
        if self._result.full():
            self._send_callback_page(
                handler, 409, "The authorization response was used."
            )
            return
        self._result.put_nowait(query)
        self._send_callback_page(
            handler,
            200,
            "Google Drive is connected. Return to Maestral to finish setup.",
        )

    @staticmethod
    def _send_callback_page(
        handler: BaseHTTPRequestHandler, status: int, message: str
    ) -> None:
        safe_message = html.escape(message)
        body = (
            '<!doctype html><html lang="en"><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            "<title>Maestral authorization</title>"
            "<style>body{margin:0;background:#f5f5f7;color:#1d1d1f;font:17px "
            "system-ui,sans-serif;display:grid;min-height:100vh;place-items:center}"
            "main{max-width:32rem;padding:2rem;text-align:center}h1{font-size:1.75rem}"
            "p{line-height:1.5}</style><main><h1>Maestral</h1>"
            f"<p>{safe_message}</p></main></html>"
        ).encode("utf-8")
        handler.send_response(status)
        handler.send_header("Cache-Control", "no-store")
        handler.send_header(
            "Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'"
        )
        handler.send_header("Content-Type", "text/html; charset=utf-8")
        handler.send_header("Content-Length", str(len(body)))
        handler.send_header("Referrer-Policy", "no-referrer")
        handler.send_header("X-Content-Type-Options", "nosniff")
        handler.end_headers()
        handler.wfile.write(body)


@dataclass(frozen=True)
class DriveItem:
    """The Drive fields required for sync and deterministic projection."""

    id: str
    name: str
    mime_type: str
    parent_id: str | None
    modified_time: datetime | None
    size: int | None
    md5_checksum: str | None
    version: str
    trashed: bool
    web_view_link: str | None
    resource_key: str | None
    can_download: bool
    can_edit: bool
    shortcut_target_id: str | None
    shortcut_target_mime_type: str | None
    shortcut_target_resource_key: str | None

    @property
    def is_folder(self) -> bool:
        return self.mime_type == FOLDER_MIME

    @property
    def is_shortcut(self) -> bool:
        return self.mime_type == SHORTCUT_MIME

    @property
    def is_google_native(self) -> bool:
        return self.mime_type.startswith(GOOGLE_MIME_PREFIX) and not (
            self.is_folder or self.is_shortcut
        )

    @property
    def stub_extension(self) -> str | None:
        if self.is_shortcut:
            return ".gshortcut"
        return (
            _NATIVE_EXTENSIONS.get(self.mime_type, ".gdrive")
            if self.is_google_native
            else None
        )

    @classmethod
    def from_api(cls, payload: Mapping[str, Any]) -> DriveItem:
        """Parse one strict but forward-compatible Drive file resource."""
        item_id = _required_string(payload, "id")
        name = _required_string(payload, "name")
        mime_type = _required_string(payload, "mimeType")
        if not name or "\x00" in name:
            raise GoogleDriveError(
                "Google Drive returned invalid data", "A file name is empty or unsafe."
            )

        parents_raw = payload.get("parents", [])
        if not isinstance(parents_raw, list) or any(
            not isinstance(parent, str) or not parent for parent in parents_raw
        ):
            raise GoogleDriveError(
                "Google Drive returned invalid data", "A file parent is not valid."
            )
        if len(parents_raw) > 1:
            raise GoogleDriveError(
                "Google Drive returned invalid data", "A file has more than one parent."
            )
        parent_id = parents_raw[0] if parents_raw else None

        modified_raw = payload.get("modifiedTime")
        modified_time = (
            _parse_google_datetime(modified_raw)
            if isinstance(modified_raw, str)
            else None
        )
        size = _optional_nonnegative_int(payload.get("size"), "file size")
        version_raw = payload.get("version", "0")
        if not isinstance(version_raw, str) or not version_raw.isdecimal():
            raise GoogleDriveError(
                "Google Drive returned invalid data", "A file version is not valid."
            )
        trashed = payload.get("trashed", False)
        if not isinstance(trashed, bool):
            raise GoogleDriveError(
                "Google Drive returned invalid data", "A trash marker is not valid."
            )

        capabilities = payload.get("capabilities", {})
        if not isinstance(capabilities, dict):
            raise GoogleDriveError(
                "Google Drive returned invalid data", "File capabilities are not valid."
            )
        can_download = capabilities.get("canDownload", True)
        can_edit = capabilities.get("canEdit", False)
        if not isinstance(can_download, bool) or not isinstance(can_edit, bool):
            raise GoogleDriveError(
                "Google Drive returned invalid data", "File capabilities are not valid."
            )

        shortcut = payload.get("shortcutDetails", {})
        if not isinstance(shortcut, dict):
            raise GoogleDriveError(
                "Google Drive returned invalid data", "Shortcut details are not valid."
            )
        target_id = _optional_string(shortcut.get("targetId"), "shortcut target")
        target_mime = _optional_string(
            shortcut.get("targetMimeType"), "shortcut target MIME type"
        )
        target_key = _optional_string(
            shortcut.get("targetResourceKey"), "shortcut resource key"
        )
        if mime_type == SHORTCUT_MIME and (not target_id or not target_mime):
            raise GoogleDriveError(
                "Google Drive returned invalid data", "A shortcut target is missing."
            )

        checksum = _optional_string(payload.get("md5Checksum"), "MD5 checksum")
        if checksum is not None and not re.fullmatch(r"[0-9a-f]{32}", checksum):
            raise GoogleDriveError(
                "Google Drive returned invalid data", "A file checksum is not valid."
            )

        return cls(
            id=item_id,
            name=name,
            mime_type=mime_type,
            parent_id=parent_id,
            modified_time=modified_time,
            size=size,
            md5_checksum=checksum,
            version=version_raw,
            trashed=trashed,
            web_view_link=_optional_string(payload.get("webViewLink"), "web link"),
            resource_key=_optional_string(payload.get("resourceKey"), "resource key"),
            can_download=can_download,
            can_edit=can_edit,
            shortcut_target_id=target_id,
            shortcut_target_mime_type=target_mime,
            shortcut_target_resource_key=target_key,
        )


@dataclass(frozen=True)
class DriveChange:
    """One change keyed by the stable Drive file ID."""

    file_id: str
    removed: bool
    item: DriveItem | None
    time: datetime | None

    @classmethod
    def from_api(cls, payload: Mapping[str, Any]) -> DriveChange:
        file_id = _required_string(payload, "fileId")
        removed = payload.get("removed", False)
        if not isinstance(removed, bool):
            raise GoogleDriveError(
                "Google Drive returned invalid data", "A removal marker is not valid."
            )
        item_payload = payload.get("file")
        if item_payload is not None and not isinstance(item_payload, dict):
            raise GoogleDriveError(
                "Google Drive returned invalid data", "A changed file is not valid."
            )
        item = (
            DriveItem.from_api(item_payload) if isinstance(item_payload, dict) else None
        )
        if item is not None and item.id != file_id:
            raise GoogleDriveError(
                "Google Drive returned invalid data",
                "A changed file ID does not match.",
            )
        if not removed and item is None:
            raise GoogleDriveError(
                "Google Drive returned invalid data", "A changed file is missing."
            )
        time_raw = payload.get("time")
        changed_at = (
            _parse_google_datetime(time_raw) if isinstance(time_raw, str) else None
        )
        return cls(file_id, removed, item, changed_at)


@dataclass(frozen=True)
class DriveChangePage:
    """A Drive change page with opaque continuation tokens."""

    changes: tuple[DriveChange, ...]
    next_page_token: str | None
    new_start_page_token: str | None


@dataclass(frozen=True)
class GoogleAbout:
    """Account and storage fields returned by Drive's about endpoint."""

    account_id: str
    display_name: str
    email: str
    profile_photo_url: str | None
    storage_used: int
    storage_limit: int | None


@dataclass(frozen=True)
class ProjectedDriveItem:
    """A Drive item and its deterministic local path."""

    item: DriveItem
    path: str


class DriveProjection:
    """Project a Drive parent graph to unique cross-platform paths."""

    def __init__(self, root_id: str, items: Iterable[DriveItem]) -> None:
        if not root_id:
            raise ValueError("A Drive root ID is required")
        self.root_id = root_id
        item_list = [item for item in items if not item.trashed]
        if len({item.id for item in item_list}) != len(item_list):
            raise GoogleDriveError(
                "Google Drive returned invalid data", "A file ID occurs more than once."
            )
        self._items = {item.id: item for item in item_list}
        self._validate_unique_ids()
        self._path_by_id: dict[str, str] = {}
        self._id_by_path_key: dict[str, str] = {}
        self._rebuild()

    @classmethod
    def from_items(cls, root_id: str, items: Iterable[DriveItem]) -> DriveProjection:
        """Build a projection while checking a one-shot iterable for duplicate IDs."""
        return cls(root_id, items)

    def projected_items(self) -> list[ProjectedDriveItem]:
        """Return reachable items sorted by local path."""
        return sorted(
            (
                ProjectedDriveItem(self._items[item_id], path)
                for item_id, path in self._path_by_id.items()
            ),
            key=lambda projected: _path_collision_key(projected.path),
        )

    def path_for_id(self, file_id: str) -> str | None:
        return self._path_by_id.get(file_id)

    def item_for_id(self, file_id: str) -> DriveItem | None:
        return self._items.get(file_id)

    def item_for_path(self, path: str) -> DriveItem | None:
        file_id = self._id_by_path_key.get(_path_collision_key(path))
        return self._items.get(file_id) if file_id else None

    def apply_changes(self, changes: Iterable[DriveChange]) -> None:
        """Apply changes by stable ID, then rebuild all affected paths."""
        items = self._items.copy()
        for change in changes:
            if change.removed or change.item is None or change.item.trashed:
                items.pop(change.file_id, None)
            else:
                items[change.file_id] = change.item
        replacement = DriveProjection(self.root_id, items.values())
        self._items = replacement._items
        self._path_by_id = replacement._path_by_id
        self._id_by_path_key = replacement._id_by_path_key

    def _validate_unique_ids(self) -> None:
        for item_id, item in self._items.items():
            if item_id != item.id or not item_id:
                raise GoogleDriveError(
                    "Google Drive returned invalid data", "A file ID is not valid."
                )

    def _rebuild(self) -> None:
        self._validate_parent_graph()
        children: dict[str, list[DriveItem]] = defaultdict(list)
        for item in self._items.values():
            if item.id == self.root_id:
                continue
            if item.parent_id is not None:
                children[item.parent_id].append(item)

        paths: dict[str, str] = {}
        path_keys: dict[str, str] = {}
        visiting: set[str] = set()

        def visit(parent_id: str, parent_path: str) -> None:
            if parent_id in visiting:
                raise GoogleDriveError(
                    "Google Drive returned invalid data",
                    "The Drive folder graph has a cycle.",
                )
            visiting.add(parent_id)
            projected_names = _unique_projected_names(children.get(parent_id, []))
            for child in sorted(children.get(parent_id, []), key=lambda item: item.id):
                name = projected_names[child.id]
                path = f"/{name}" if parent_path == "/" else f"{parent_path}/{name}"
                key = _path_collision_key(path)
                if key in path_keys:
                    raise GoogleDriveError(
                        "Google Drive returned invalid data",
                        "Two Drive items project to the same local path.",
                    )
                paths[child.id] = path
                path_keys[key] = child.id
                if child.is_folder:
                    visit(child.id, path)
            visiting.remove(parent_id)

        visit(self.root_id, "/")
        self._path_by_id = paths
        self._id_by_path_key = path_keys

    def _validate_parent_graph(self) -> None:
        for item in self._items.values():
            seen = {item.id}
            parent_id = item.parent_id
            while parent_id is not None and parent_id != self.root_id:
                if parent_id in seen:
                    raise GoogleDriveError(
                        "Google Drive returned invalid data",
                        "The Drive folder graph has a cycle.",
                    )
                seen.add(parent_id)
                parent = self._items.get(parent_id)
                if parent is None:
                    break
                if not parent.is_folder:
                    raise GoogleDriveError(
                        "Google Drive returned invalid data",
                        "A Drive item has a file as its parent.",
                    )
                parent_id = parent.parent_id

    def native_stub(self, file_id: str) -> bytes:
        """Return a read-only JSON link stub for a native document or shortcut."""
        item = self._items.get(file_id)
        if item is None or item.stub_extension is None:
            raise GoogleDriveError(
                "Cannot create Google link", "The Drive item is not a link document."
            )
        target_id = item.shortcut_target_id or item.id
        url = item.web_view_link or f"https://drive.google.com/open?id={target_id}"
        payload: dict[str, Any] = {
            "format": 1,
            "provider": "google_drive",
            "id": item.id,
            "mimeType": item.mime_type,
            "name": item.name,
            "readOnly": True,
            "url": url,
        }
        if item.is_shortcut:
            payload["targetId"] = cast(str, item.shortcut_target_id)
            payload["targetMimeType"] = cast(str, item.shortcut_target_mime_type)
            if item.shortcut_target_resource_key:
                payload["targetResourceKey"] = item.shortcut_target_resource_key
        return (
            json.dumps(
                payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
            ).encode("utf-8")
            + b"\n"
        )


T = TypeVar("T")


class GoogleDriveClient:
    """A small Drive v3 client with refresh, backoff, and resumable uploads."""

    def __init__(
        self,
        oauth: GoogleOAuth,
        tokens: GoogleTokens,
        save_tokens: Callable[[GoogleTokens], None],
        *,
        session: HttpSession | None = None,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
        random_source: random.Random | None = None,
        timeout: float = 100.0,
    ) -> None:
        if GOOGLE_DRIVE_SCOPE not in tokens.scope.split():
            raise GoogleDriveError(
                "Google Drive access is unavailable",
                "The required Drive scope is missing.",
            )
        self._oauth = oauth
        self._tokens = tokens
        self._save_tokens = save_tokens
        self._session = session or requests.Session()
        self._clock = clock
        self._sleep = sleep
        self._random = random_source or random.SystemRandom()
        self._timeout = timeout
        self._refresh_lock = threading.Lock()

    @property
    def tokens(self) -> GoogleTokens:
        return self._tokens

    def get_root(self) -> DriveItem:
        """Return My Drive's root resource and stable ID."""
        return self.get_item("root")

    def get_about(self) -> GoogleAbout:
        """Return account identity and storage quota data."""
        payload = self._json_request(
            "GET",
            f"{GOOGLE_DRIVE_API}/about",
            params={
                "fields": (
                    "user(displayName,emailAddress,permissionId,photoLink),"
                    "storageQuota(limit,usage)"
                )
            },
        )
        user = payload.get("user")
        quota = payload.get("storageQuota")
        if not isinstance(user, dict) or not isinstance(quota, dict):
            raise _invalid_response("The Drive account data is missing.")
        account_id = _required_string(user, "permissionId")
        display_name = _required_string(user, "displayName")
        email = _required_string(user, "emailAddress")
        storage_used = _optional_nonnegative_int(quota.get("usage"), "storage usage")
        storage_limit = _optional_nonnegative_int(quota.get("limit"), "storage limit")
        if storage_used is None:
            raise _invalid_response("The Drive storage usage is missing.")
        return GoogleAbout(
            account_id=account_id,
            display_name=display_name,
            email=email,
            profile_photo_url=_optional_string(user.get("photoLink"), "photo link"),
            storage_used=storage_used,
            storage_limit=storage_limit,
        )

    def get_item(self, file_id: str) -> DriveItem:
        """Return one current file resource by stable ID."""
        _validate_file_id(file_id)
        payload = self._json_request(
            "GET",
            f"{GOOGLE_DRIVE_API}/files/{file_id}",
            params={"fields": _FILE_FIELDS, "supportsAllDrives": "true"},
        )
        return DriveItem.from_api(payload)

    def list_all_items(self) -> list[DriveItem]:
        """List the user's current Drive corpus, without trashed items."""
        items: list[DriveItem] = []
        page_token: str | None = None
        seen_tokens: set[str] = set()
        while True:
            params = {
                "corpora": "user",
                "fields": f"nextPageToken,incompleteSearch,files({_FILE_FIELDS})",
                "pageSize": "1000",
                "q": "trashed = false",
                "spaces": "drive",
                "supportsAllDrives": "true",
            }
            if page_token:
                params["pageToken"] = page_token
            payload = self._json_request(
                "GET", f"{GOOGLE_DRIVE_API}/files", params=params
            )
            if payload.get("incompleteSearch", False) is not False:
                raise GoogleDriveError(
                    "Google Drive indexing failed",
                    "Google returned an incomplete file search.",
                )
            files = payload.get("files")
            if not isinstance(files, list):
                raise _invalid_response("The file list is missing.")
            for raw_item in files:
                if not isinstance(raw_item, dict):
                    raise _invalid_response("A listed file is not valid.")
                items.append(DriveItem.from_api(raw_item))
            page_token = _page_token(payload, "nextPageToken")
            if page_token is None:
                break
            if page_token in seen_tokens:
                raise _invalid_response("A file page token was repeated.")
            seen_tokens.add(page_token)
        return items

    def get_start_page_token(self) -> str:
        """Get an opaque token for future changes."""
        payload = self._json_request(
            "GET",
            f"{GOOGLE_DRIVE_API}/changes/startPageToken",
            params={"supportsAllDrives": "true"},
        )
        token = _page_token(payload, "startPageToken")
        if token is None:
            raise _invalid_response("The start page token is missing.")
        return token

    def list_changes(self, page_token: str) -> DriveChangePage:
        """Read one change page. The caller persists the returned token unchanged."""
        _validate_page_token(page_token)
        payload = self._json_request(
            "GET",
            f"{GOOGLE_DRIVE_API}/changes",
            params={
                "fields": (
                    "nextPageToken,newStartPageToken,changes("
                    f"fileId,removed,time,file({_FILE_FIELDS}))"
                ),
                "includeItemsFromAllDrives": "true",
                "pageSize": "1000",
                "pageToken": page_token,
                "restrictToMyDrive": "true",
                "spaces": "drive",
                "supportsAllDrives": "true",
            },
        )
        changes_raw = payload.get("changes")
        if not isinstance(changes_raw, list):
            raise _invalid_response("The change list is missing.")
        changes: list[DriveChange] = []
        for raw_change in changes_raw:
            if not isinstance(raw_change, dict):
                raise _invalid_response("A Drive change is not valid.")
            changes.append(DriveChange.from_api(raw_change))
        next_token = _page_token(payload, "nextPageToken")
        start_token = _page_token(payload, "newStartPageToken")
        if (next_token is None) == (start_token is None):
            raise _invalid_response(
                "A change page must have one continuation or start token."
            )
        return DriveChangePage(tuple(changes), next_token, start_token)

    def download_blob(
        self,
        file_id: str,
        destination: BinaryIO,
        progress: Callable[[int], None] | None = None,
    ) -> DriveItem:
        """Download one blob after checking its current capabilities."""
        item = self.get_item(file_id)
        if item.is_folder or item.is_google_native or item.is_shortcut:
            raise GoogleDriveError(
                "Cannot download Google Drive item",
                "Use a read-only link stub for this item.",
            )
        if not item.can_download:
            raise GoogleDriveError(
                "Cannot download Google Drive item",
                "The file owner does not permit downloads.",
            )
        response = self._request(
            "GET",
            f"{GOOGLE_DRIVE_API}/files/{file_id}",
            params={"alt": "media", "supportsAllDrives": "true"},
            stream=True,
        )
        try:
            for chunk in response.iter_content(1024 * 1024):
                if chunk:
                    destination.write(chunk)
                    if progress:
                        progress(destination.tell())
        except (OSError, requests.RequestException) as exc:
            raise GoogleDriveError(
                "Google Drive download failed", "The file transfer stopped early."
            ) from exc
        finally:
            response.close()
        return item

    def generate_file_id(self) -> str:
        """Reserve one ID so a create request can be retried without duplicates."""
        payload = self._json_request(
            "GET",
            f"{GOOGLE_DRIVE_API}/files/generateIds",
            params={"count": "1", "space": "drive", "type": "files"},
        )
        ids = payload.get("ids")
        if not isinstance(ids, list) or len(ids) != 1 or not isinstance(ids[0], str):
            raise _invalid_response("Google did not return one generated file ID.")
        file_id = ids[0]
        _validate_file_id(file_id)
        return file_id

    def create_folder(self, name: str, parent_id: str) -> DriveItem:
        """Create a folder under one stable parent ID."""
        _validate_remote_name(name)
        _validate_file_id(parent_id)
        file_id = self.generate_file_id()
        try:
            payload = self._json_request(
                "POST",
                f"{GOOGLE_DRIVE_API}/files",
                params={"fields": _FILE_FIELDS, "supportsAllDrives": "true"},
                json={
                    "id": file_id,
                    "mimeType": FOLDER_MIME,
                    "name": name,
                    "parents": [parent_id],
                },
            )
            return DriveItem.from_api(payload)
        except GoogleDriveError as exc:
            if exc.status != 409:
                raise
            item = self.get_item(file_id)
            if not item.is_folder or item.name != name or item.parent_id != parent_id:
                raise _invalid_response(
                    "A recovered folder does not match its create request."
                ) from exc
            return item

    def delete_item(self, file_id: str) -> None:
        """Permanently delete one item by stable ID."""
        _validate_file_id(file_id)
        response = self._request(
            "DELETE",
            f"{GOOGLE_DRIVE_API}/files/{file_id}",
            params={"supportsAllDrives": "true"},
        )
        response.close()

    def move_item(
        self,
        file_id: str,
        *,
        old_parent_id: str,
        new_parent_id: str,
        new_name: str,
    ) -> DriveItem:
        """Rename and move one item without changing its identity."""
        _validate_file_id(file_id)
        _validate_file_id(old_parent_id)
        _validate_file_id(new_parent_id)
        _validate_remote_name(new_name)
        payload = self._json_request(
            "PATCH",
            f"{GOOGLE_DRIVE_API}/files/{file_id}",
            params={
                "addParents": new_parent_id,
                "fields": _FILE_FIELDS,
                "removeParents": old_parent_id,
                "supportsAllDrives": "true",
            },
            json={"name": new_name},
        )
        return DriveItem.from_api(payload)

    def start_resumable_upload(
        self,
        *,
        name: str,
        parent_id: str,
        mime_type: str,
        size: int,
        file_id: str | None = None,
    ) -> str:
        """Create a resumable session and return its opaque Google URL."""
        _validate_remote_name(name)
        _validate_file_id(parent_id)
        if file_id is not None:
            _validate_file_id(file_id)
        if not mime_type or any(char in mime_type for char in "\r\n"):
            raise ValueError("The upload MIME type is not valid")
        if size < 0:
            raise ValueError("The upload size cannot be negative")

        creating = file_id is None
        if creating:
            file_id = self.generate_file_id()
            method = "POST"
            url = f"{GOOGLE_DRIVE_UPLOAD_API}/files"
            metadata: dict[str, Any] = {
                "id": file_id,
                "name": name,
                "parents": [parent_id],
            }
        else:
            assert file_id is not None
            method = "PATCH"
            url = f"{GOOGLE_DRIVE_UPLOAD_API}/files/{file_id}"
            metadata = {"name": name}

        request_kwargs: dict[str, Any] = {
            "params": {
                "fields": _FILE_FIELDS,
                "supportsAllDrives": "true",
                "uploadType": "resumable",
            },
            "headers": {
                "X-Upload-Content-Length": str(size),
                "X-Upload-Content-Type": mime_type,
            },
            "json": metadata,
        }
        try:
            response = self._request(method, url, **request_kwargs)
        except GoogleDriveError as exc:
            if not creating or exc.status != 409:
                raise
            assert file_id is not None
            request_kwargs["json"] = {"name": name}
            response = self._request(
                "PATCH",
                f"{GOOGLE_DRIVE_UPLOAD_API}/files/{file_id}",
                **request_kwargs,
            )
        session_url = response.headers.get("Location")
        response.close()
        if session_url is None or not _is_google_upload_url(session_url):
            raise _invalid_response("The resumable upload URL is not valid.")
        return session_url

    def upload_resumable(
        self,
        session_url: str,
        source: BinaryIO,
        size: int,
        *,
        offset: int = 0,
        chunk_size: int = _DEFAULT_RESUMABLE_CHUNK,
        progress: Callable[[int], None] | None = None,
    ) -> DriveItem:
        """Upload remaining chunks and return final metadata."""
        if not _is_google_upload_url(session_url):
            raise ValueError("The resumable upload URL is not valid")
        if size < 0 or offset < 0 or offset > size:
            raise ValueError("The upload range is not valid")
        if chunk_size <= 0 or chunk_size % _RESUMABLE_CHUNK_GRANULARITY:
            raise ValueError("The upload chunk must be a positive multiple of 256 KiB")
        if offset:
            source.seek(offset)

        uploaded = offset
        if size == 0:
            response = self._request(
                "PUT",
                session_url,
                headers={"Content-Length": "0", "Content-Range": "bytes */0"},
                allow_statuses={200, 201},
            )
            try:
                payload = _response_json(response)
            finally:
                response.close()
            return DriveItem.from_api(payload)

        while uploaded < size:
            count = min(chunk_size, size - uploaded)
            data = source.read(count)
            if not isinstance(data, bytes) or len(data) != count:
                raise GoogleDriveError(
                    "Google Drive upload failed", "The local upload source ended early."
                )
            end = uploaded + count - 1
            response = self._request(
                "PUT",
                session_url,
                headers={
                    "Content-Length": str(count),
                    "Content-Range": f"bytes {uploaded}-{end}/{size}",
                },
                data=data,
                allow_statuses={200, 201, 308},
            )
            if response.status_code == 308:
                acknowledged = _acknowledged_upload_offset(response, size)
                response.close()
                if acknowledged < uploaded or acknowledged > end + 1:
                    raise _invalid_response(
                        "Google acknowledged an invalid upload range."
                    )
                uploaded = acknowledged
                source.seek(uploaded)
                if progress:
                    progress(uploaded)
                continue

            try:
                payload = _response_json(response)
            finally:
                response.close()
            uploaded = size
            if progress:
                progress(uploaded)
            return DriveItem.from_api(payload)

        raise GoogleDriveError(
            "Google Drive upload failed",
            "Google did not return uploaded file metadata.",
        )

    def query_resumable_upload(self, session_url: str, size: int) -> int:
        """Return the next byte offset for an interrupted resumable upload."""
        if not _is_google_upload_url(session_url):
            raise ValueError("The resumable upload URL is not valid")
        if size < 0:
            raise ValueError("The upload size cannot be negative")
        response = self._request(
            "PUT",
            session_url,
            headers={"Content-Length": "0", "Content-Range": f"bytes */{size}"},
            allow_statuses={200, 201, 308},
        )
        if response.status_code in {200, 201}:
            response.close()
            return size
        offset = _acknowledged_upload_offset(response, size)
        response.close()
        return offset

    def _json_request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> Mapping[str, Any]:
        response = self._request(method, url, **kwargs)
        try:
            return _response_json(response)
        finally:
            response.close()

    def _request(
        self,
        method: str,
        url: str,
        *,
        allow_statuses: set[int] | None = None,
        **kwargs: Any,
    ) -> requests.Response:
        if not _is_allowed_google_url(url):
            raise ValueError("The Google API URL is not allowed")
        allowed = allow_statuses or set(range(200, 300))
        refreshed_after_unauthorized = False
        base_headers = dict(cast(Mapping[str, str], kwargs.pop("headers", {})))

        for attempt in range(_MAX_RETRIES + 1):
            self._ensure_fresh_token()
            access_token = self._tokens.access_token
            headers = base_headers.copy()
            headers.update(
                {
                    "Accept": "application/json",
                    "Authorization": f"Bearer {access_token}",
                    "User-Agent": f"Maestral/{__version__}",
                }
            )
            try:
                response = self._session.request(
                    method,
                    url,
                    headers=headers,
                    timeout=self._timeout,
                    **kwargs,
                )
            except requests.RequestException as exc:
                if attempt == _MAX_RETRIES:
                    raise GoogleDriveConnectionError(
                        "Google Drive request failed", "The service is not available."
                    ) from exc
                self._backoff(attempt, None)
                continue

            if response.status_code in allowed:
                return response

            reason, message = _drive_error(response)
            if response.status_code == 401 and not refreshed_after_unauthorized:
                response.close()
                self._refresh_tokens(expected_access_token=access_token)
                refreshed_after_unauthorized = True
                continue

            retryable = response.status_code in _RETRY_STATUSES or (
                response.status_code == 403 and reason in _RETRY_REASONS
            )
            if retryable and attempt < _MAX_RETRIES:
                retry_after = response.headers.get("Retry-After")
                response.close()
                self._backoff(attempt, retry_after)
                continue

            status = response.status_code
            response.close()
            if retryable:
                raise GoogleDriveServerError(
                    "Google Drive service failed",
                    message,
                    status=status,
                    reason=reason,
                )
            raise GoogleDriveError(
                "Google Drive request failed",
                message,
                status=status,
                reason=reason,
            )

        raise RuntimeError("The Google request retry loop ended unexpectedly")

    def _ensure_fresh_token(self) -> None:
        if self._tokens.expires_at <= self._clock() + 60:
            self._refresh_tokens()

    def _refresh_tokens(self, expected_access_token: str | None = None) -> None:
        with self._refresh_lock:
            if (
                expected_access_token is not None
                and self._tokens.access_token != expected_access_token
            ):
                return
            if (
                expected_access_token is None
                and self._tokens.expires_at > self._clock() + 60
            ):
                return
            tokens = self._oauth.refresh(self._tokens)
            self._save_tokens(tokens)
            self._tokens = tokens

    def _backoff(self, attempt: int, retry_after: str | None) -> None:
        delay: float | None = None
        if retry_after is not None:
            try:
                delay = float(retry_after)
            except ValueError:
                delay = None
        if delay is None or delay < 0 or delay > 120:
            delay = min(32.0, float(2**attempt)) + self._random.random()
        self._sleep(delay)


class GoogleDriveProvider:
    """Provider-neutral adapter for Google Drive's ID-based data model."""

    provider_id = "google_drive"
    api_url = GOOGLE_DRIVE_API
    content_hasher_factory: ContentHasherFactory

    def __init__(
        self,
        config_name: str,
        cred_storage: CredentialStorage,
        *,
        session: HttpSession | None = None,
        client_id: str | None = None,
        drive_client: GoogleDriveClient | None = None,
        bandwidth_limit_up: float = 0,
        bandwidth_limit_down: float = 0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config_name = config_name
        self._cred_storage = cred_storage
        self._state = MaestralState(config_name)
        self._logger = scoped_logger(__name__, config_name)
        self._session = cast(HttpSession, session or requests.Session())
        self._configured_client_id = client_id
        self._drive_client = drive_client
        self._oauth: GoogleOAuth | None = None
        self._oauth_loopback: GoogleOAuthLoopback | None = None
        self._oauth_request: GoogleOAuthRequest | None = None
        self._projection: DriveProjection | None = None
        self._projection_cursor: str | None = None
        self._root_id = self._state.get("account", "path_root_nsid")
        self._cached_account_info: FullAccount | None = None
        self._account_id = cred_storage.account_id
        self._pending_tokens: GoogleTokens | None = None
        self._allow_plaintext_keyring = False
        self._sleep = sleep
        self.content_hasher_factory = md5_content_hasher
        self.bandwidth_limit_up = bandwidth_limit_up
        self.bandwidth_limit_down = bandwidth_limit_down

    @property
    def linked(self) -> bool:
        return self._drive_client is not None or self._cred_storage.token is not None

    @property
    def drive(self) -> GoogleDriveClient:
        if self._drive_client is None:
            token = self._cred_storage.token
            if token is None:
                raise NotLinkedError(
                    "No Google credentials set",
                    "Link a Google Drive account first.",
                )
            tokens = GoogleTokens.from_json(token)
            self._drive_client = GoogleDriveClient(
                self._oauth_client(),
                tokens,
                self._save_refreshed_tokens,
                session=self._session,
            )
        return self._drive_client

    def _client_id(self) -> str:
        client_id = (
            self._configured_client_id
            or os.environ.get(GOOGLE_CLIENT_ID_ENV, "")
            or GOOGLE_DESKTOP_CLIENT_ID
        )
        if not client_id:
            raise GoogleDriveError(
                "Google Drive client ID is missing",
                f"Set {GOOGLE_CLIENT_ID_ENV} to a Google desktop OAuth client ID.",
            )
        return client_id

    def _oauth_client(self) -> GoogleOAuth:
        if self._oauth is None:
            self._oauth = GoogleOAuth(self._client_id(), session=self._session)
        return self._oauth

    def _save_refreshed_tokens(self, tokens: GoogleTokens) -> None:
        if self._account_id is None:
            self._pending_tokens = tokens
            return
        self._cred_storage.save_creds(
            self._account_id,
            tokens.to_json(),
            allow_plaintext=True,
        )

    def get_auth_url(self) -> str:
        self._close_auth_flow()
        loopback = GoogleOAuthLoopback()
        loopback.__enter__()
        try:
            request = loopback.authorization_request(self._oauth_client())
        except BaseException:
            loopback.__exit__(None, None, None)
            raise
        self._oauth_loopback = loopback
        self._oauth_request = request
        return request.url

    def link(
        self,
        code: str | None = None,
        refresh_token: str | None = None,
        access_token: str | None = None,
        allow_plaintext_keyring: bool = False,
    ) -> int:
        self._allow_plaintext_keyring = allow_plaintext_keyring
        try:
            if refresh_token is not None:
                tokens = GoogleTokens(
                    access_token=access_token or "refresh-required",
                    refresh_token=refresh_token,
                    expires_at=0,
                    scope=GOOGLE_DRIVE_SCOPE,
                )
            elif access_token is not None:
                raise GoogleDriveError(
                    "Google authorization failed",
                    "Google Drive requires a long-lived refresh token.",
                    reason="invalid_token",
                )
            else:
                request = self._oauth_request
                loopback = self._oauth_loopback
                if request is None or loopback is None:
                    raise RuntimeError(
                        "Start the Google authorization flow with get_auth_url first."
                    )
                auth_code = code or loopback.wait_for_code()
                tokens = self._oauth_client().exchange_code(request, auth_code)

            self._drive_client = GoogleDriveClient(
                self._oauth_client(),
                tokens,
                self._save_refreshed_tokens,
                session=self._session,
            )
            about = self.drive.get_about()
            root = self.drive.get_root()
            saved_tokens = self._pending_tokens or self.drive.tokens
            self._account_id = about.account_id
            self._cred_storage.save_creds(
                about.account_id,
                saved_tokens.to_json(),
                allow_plaintext=allow_plaintext_keyring,
            )
            self._pending_tokens = None
            self._root_id = root.id
            self._save_account_state(about, root.id)
            self._cached_account_info = self._account_from_about(about, root.id)
            return 0
        except GoogleDriveError as exc:
            self._drive_client = None
            if exc.title == "Google Drive client ID is missing":
                raise
            if exc.reason in {"invalid_grant", "invalid_token"} or exc.status in {
                400,
                401,
            }:
                return 1
            self._logger.debug("Google Drive link failed", exc_info=True)
            return 2
        finally:
            self._close_auth_flow()

    def _close_auth_flow(self) -> None:
        if self._oauth_loopback is not None:
            self._oauth_loopback.__exit__(None, None, None)
        self._oauth_loopback = None
        self._oauth_request = None

    def unlink(self) -> None:
        try:
            self._cred_storage.delete_creds()
        finally:
            self._drive_client = None
            self._projection = None
            self._projection_cursor = None
            self._cached_account_info = None
            self._account_id = None
            self._pending_tokens = None
            self._close_auth_flow()

    def close(self) -> None:
        self._close_auth_flow()
        close = getattr(self._session, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> GoogleDriveProvider:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    @property
    def account_info(self) -> FullAccount:
        if self._cached_account_info is None:
            return self.get_account_info()
        return self._cached_account_info

    @property
    def namespace_id(self) -> str:
        if not self._root_id:
            self._root_id = self.drive.get_root().id
        return self._root_id

    @property
    def is_team_space(self) -> bool:
        return False

    @overload
    def get_account_info(self, dbid: None = None) -> FullAccount: ...

    @overload
    def get_account_info(self, dbid: str) -> Account: ...

    def get_account_info(self, dbid: str | None = None) -> Account:
        if (
            dbid is not None
            and self._account_id is not None
            and dbid != self._account_id
        ):
            raise InvalidDbidError(
                "Google account not found", "The account ID is not linked."
            )
        about = self.drive.get_about()
        if dbid is not None and dbid != about.account_id:
            raise InvalidDbidError(
                "Google account not found", "The account ID is not linked."
            )
        root_id = self.namespace_id
        self._account_id = about.account_id
        self._save_account_state(about, root_id)
        account = self._account_from_about(about, root_id)
        self._cached_account_info = account
        if dbid is None:
            return account
        return Account(
            account.account_id,
            account.display_name,
            account.email,
            account.email_verified,
            account.profile_photo_url,
            account.disabled,
        )

    def _account_from_about(self, about: GoogleAbout, root_id: str) -> FullAccount:
        return FullAccount(
            account_id=about.account_id,
            display_name=about.display_name,
            email=about.email,
            email_verified=True,
            profile_photo_url=about.profile_photo_url,
            disabled=False,
            country=None,
            locale="en",
            team=None,
            team_member_id=None,
            account_type=AccountType.Other,
            root_info=UserRootInfo(root_id, root_id),
        )

    def _save_account_state(self, about: GoogleAbout, root_id: str) -> None:
        self._state.set("account", "email", about.email)
        self._state.set("account", "display_name", about.display_name)
        self._state.set("account", "abbreviated_name", about.display_name[:2])
        self._state.set("account", "type", AccountType.Other.value)
        self._state.set("account", "path_root_type", "user")
        self._state.set("account", "path_root_nsid", root_id)
        self._state.set("account", "home_path", "")

    def get_space_usage(self) -> PersonalSpaceUsage:
        about = self.drive.get_about()
        allocated = about.storage_limit or about.storage_used
        usage = PersonalSpaceUsage(about.storage_used, allocated, None)
        if about.storage_limit:
            percent = about.storage_used / about.storage_limit
            space_usage = f"{percent:.1%} of {natural_size(allocated)} used"
        else:
            space_usage = f"{natural_size(about.storage_used)} used"
        self._state.set("account", "usage", space_usage)
        self._state.set("account", "usage_type", "individual")
        self._state.set("account", "usage_used", usage.used)
        self._state.set("account", "usage_allocated", usage.allocated)
        return usage

    def update_path_root(self, root_info: RootInfo) -> None:
        if not isinstance(root_info, UserRootInfo):
            raise GoogleDriveError(
                "Google Drive root is invalid",
                "Google Drive does not support a team-space path root.",
            )
        self._root_id = root_info.root_namespace_id
        self._state.set("account", "path_root_type", "user")
        self._state.set("account", "path_root_nsid", self._root_id)
        self._state.set("account", "home_path", "")

    def _refresh_projection(self) -> DriveProjection:
        root = self.drive.get_root()
        items = [item for item in self.drive.list_all_items() if item.id != root.id]
        projection = DriveProjection(root.id, [root, *items])
        self._root_id = root.id
        self._projection = projection
        return projection

    def _ensure_projection(self) -> DriveProjection:
        return self._projection or self._refresh_projection()

    @staticmethod
    def _normalise_remote_path(path: str) -> str:
        if not isinstance(path, str) or "\x00" in path:
            raise ValueError("The remote path is not valid")
        if path in {"", "/"}:
            return "/"
        if not path.startswith("/"):
            raise ValueError("The remote path must start with '/'")
        normalised = posixpath.normpath(path)
        if normalised == "/" or normalised.startswith("/../"):
            raise ValueError("The remote path is not valid")
        return normalised

    @staticmethod
    def _path_lower(path: str) -> str:
        return normalize(path)

    def _metadata_for(self, item: DriveItem, path: str) -> Metadata:
        path = self._normalise_remote_path(path)
        name = posixpath.basename(path)
        path_lower = self._path_lower(path)
        if item.is_folder:
            return FolderMetadata(name, path_lower, path, item.id, False)

        modified = item.modified_time or datetime.fromtimestamp(0, timezone.utc)
        projection = self._ensure_projection()
        if item.stub_extension is not None:
            content = projection.native_stub(item.id)
            hasher = self.content_hasher_factory()
            hasher.update(content)
            size = len(content)
            checksum = hasher.hexdigest()
        else:
            if item.md5_checksum is None:
                raise GoogleDriveError(
                    "Google Drive returned invalid data",
                    "A downloadable file has no MD5 checksum.",
                )
            size = item.size or 0
            checksum = item.md5_checksum
        return FileMetadata(
            name=name,
            path_lower=path_lower,
            path_display=path,
            id=item.id,
            client_modified=modified,
            server_modified=modified,
            rev=item.version,
            size=size,
            symlink_target=None,
            shared=False,
            modified_by=self._account_id,
            is_downloadable=True,
            content_hash=checksum,
        )

    def _metadata_for_id(self, file_id: str) -> Metadata | None:
        projection = self._ensure_projection()
        item = projection.item_for_id(file_id)
        path = projection.path_for_id(file_id)
        if item is None or path is None:
            return None
        return self._metadata_for(item, path)

    def get_metadata(
        self, remote_path: str, include_deleted: bool = False
    ) -> Metadata | None:
        if remote_path.startswith("rev:"):
            raise self._unsupported("revision metadata")
        del include_deleted
        remote_path = self._normalise_remote_path(remote_path)
        if remote_path == "/":
            root = self.drive.get_root()
            return FolderMetadata("", "/", "/", root.id, False)
        projection = self._ensure_projection()
        item = projection.item_for_path(remote_path)
        if item is None:
            return None
        projected_path = projection.path_for_id(item.id)
        if projected_path is None:
            return None
        return self._metadata_for(item, projected_path)

    def _listed_metadata(self, remote_path: str, recursive: bool) -> list[Metadata]:
        projection = self._ensure_projection()
        prefix = remote_path.rstrip("/") + "/"
        entries: list[Metadata] = []
        for projected in projection.projected_items():
            path = projected.path
            if remote_path == "/":
                relative = path[1:]
            elif path.startswith(prefix):
                relative = path[len(prefix) :]
            else:
                continue
            if not recursive and "/" in relative:
                continue
            entries.append(self._metadata_for(projected.item, path))
        return entries

    def list_folder(
        self,
        remote_path: str,
        recursive: bool = False,
        include_deleted: bool = False,
        include_mounted_folders: bool = True,
        include_non_downloadable_files: bool = False,
    ) -> ListFolderResult:
        return next(
            self.list_folder_iterator(
                remote_path,
                recursive,
                include_deleted,
                include_mounted_folders,
                include_non_downloadable_files=include_non_downloadable_files,
            )
        )

    def list_folder_iterator(
        self,
        remote_path: str,
        recursive: bool = False,
        include_deleted: bool = False,
        include_mounted_folders: bool = True,
        limit: int | None = None,
        include_non_downloadable_files: bool = False,
    ) -> Iterator[ListFolderResult]:
        del include_deleted, include_mounted_folders, limit
        del include_non_downloadable_files
        remote_path = self._normalise_remote_path(remote_path)
        cursor = self.drive.get_start_page_token()
        self._refresh_projection()
        self._projection_cursor = cursor
        yield ListFolderResult(
            self._listed_metadata(remote_path, recursive),
            False,
            cursor,
        )

    def list_remote_changes_iterator(
        self,
        last_cursor: str,
        indexed_paths: Mapping[str, str] | None = None,
    ) -> Iterator[ListFolderResult]:
        _validate_page_token(last_cursor)
        known_paths = dict(indexed_paths or {})
        apply_pages = (
            self._projection is not None and self._projection_cursor == last_cursor
        )
        if not apply_pages:
            self._refresh_projection()

        token = last_cursor
        seen_tokens = {token}
        while True:
            page = self.drive.list_changes(token)
            projection = self._ensure_projection()
            if apply_pages:
                projection.apply_changes(page.changes)
            changed_ids = {change.file_id for change in page.changes}
            current_paths = {
                projected.item.id: projected.path
                for projected in projection.projected_items()
            }
            entries: list[Metadata] = []
            for file_id, old_path in sorted(known_paths.items()):
                new_path = current_paths.get(file_id)
                if new_path is None or self._path_lower(new_path) != self._path_lower(
                    old_path
                ):
                    entries.append(
                        DeletedMetadata(
                            posixpath.basename(old_path),
                            self._path_lower(old_path),
                            old_path,
                        )
                    )
            for file_id, new_path in sorted(current_paths.items()):
                known_path = known_paths.get(file_id)
                path_changed = known_path is not None and self._path_lower(
                    known_path
                ) != self._path_lower(new_path)
                if file_id in changed_ids or path_changed:
                    metadata = self._metadata_for_id(file_id)
                    if metadata is not None:
                        entries.append(metadata)

            for file_id in tuple(known_paths):
                if file_id not in current_paths:
                    known_paths.pop(file_id, None)
            for entry in entries:
                if not isinstance(entry, DeletedMetadata):
                    item_id = cast(FileMetadata | FolderMetadata, entry).id
                    known_paths[item_id] = entry.path_display

            next_token = page.next_page_token or page.new_start_page_token
            assert next_token is not None
            self._projection_cursor = next_token
            yield ListFolderResult(
                entries, page.next_page_token is not None, next_token
            )
            if page.next_page_token is None:
                break
            if next_token in seen_tokens:
                raise GoogleDriveError(
                    "Google Drive returned invalid data",
                    "A change page token was repeated.",
                )
            seen_tokens.add(next_token)
            token = next_token

    def wait_for_remote_changes(self, last_cursor: str, timeout: int = 40) -> bool:
        if timeout <= 0:
            raise ValueError("Timeout must be positive")
        self._sleep(min(float(timeout), 5.0))
        return bool(self.drive.list_changes(last_cursor).changes)

    def download(
        self,
        remote_path: str,
        local_path: str | BinaryIO,
        sync_event: SyncEvent | None = None,
        *,
        rev: str | None = None,
        provider_id: str | None = None,
    ) -> FileMetadata:
        if remote_path.startswith("rev:"):
            raise self._unsupported("revision download")
        remote_path = self._normalise_remote_path(remote_path)
        projection = self._ensure_projection()
        item = (
            projection.item_for_id(provider_id)
            if provider_id is not None
            else projection.item_for_path(remote_path)
        )
        if item is None:
            raise NotFoundError("Google Drive item not found", dbx_path=remote_path)
        if item.is_folder:
            raise IsAFolderError("Cannot download folder", dbx_path=remote_path)
        if rev is not None and item.version != rev:
            raise DataChangedError(
                "Google Drive file changed",
                "The remote version changed before download.",
                dbx_path=remote_path,
            )
        projected_path = projection.path_for_id(item.id)
        if projected_path is None:
            raise NotFoundError("Google Drive item not found", dbx_path=remote_path)
        destination: AbstractContextManager[BinaryIO]
        if isinstance(local_path, str):
            destination = open(local_path, "wb", opener=opener_no_symlink)
        else:
            destination = nullcontext(local_path)

        with destination as file:
            file.seek(0)
            file.truncate()
            hasher = self.content_hasher_factory()

            class HashingWriter:
                def write(inner_self, data: bytes) -> int:
                    hasher.update(data)
                    result = file.write(data)
                    if sync_event is not None:
                        sync_event.completed = file.tell()
                    return result

                def tell(inner_self) -> int:
                    return file.tell()

            if item.stub_extension is not None:
                data = projection.native_stub(item.id)
                HashingWriter().write(data)
            else:
                self.drive.download_blob(item.id, cast(BinaryIO, HashingWriter()))
            metadata = cast(FileMetadata, self._metadata_for(item, projected_path))
            if hasher.hexdigest() != metadata.content_hash:
                raise DataCorruptionError(
                    "Data corrupted", "Retry the Google Drive download."
                )
            file.flush()
            try:
                timestamp = metadata.client_modified.timestamp()
                os.utime(file.fileno(), (time.time(), timestamp))
            except (AttributeError, OSError):
                pass
        return metadata

    def upload(
        self,
        local_file: str | os.PathLike[str] | BinaryIO,
        remote_path: str,
        write_mode: WriteMode = WriteMode.Add,
        update_rev: str | None = None,
        autorename: bool = False,
        sync_event: SyncEvent | None = None,
        *,
        local_path: str | None = None,
    ) -> FileMetadata:
        remote_path = self._normalise_remote_path(remote_path)
        parent_path, name = posixpath.split(remote_path)
        parent_path = parent_path or "/"
        projection = self._ensure_projection()
        parent = self._item_for_folder(parent_path)
        existing = projection.item_for_path(remote_path)
        if existing is not None and existing.is_folder:
            raise IsAFolderError("Cannot upload over folder", dbx_path=remote_path)
        if existing is not None and existing.stub_extension is not None:
            raise UnsupportedFileError(
                "Google link documents are read-only",
                "Edit the native document in Google Drive.",
                dbx_path=remote_path,
            )
        if write_mode is WriteMode.Add and existing is not None and not autorename:
            raise FileConflictError("File already exists", dbx_path=remote_path)
        if write_mode is WriteMode.Update:
            if existing is None or update_rev is None or existing.version != update_rev:
                raise FileConflictError(
                    "File changed on Google Drive", dbx_path=remote_path
                )

        source_path: str | None
        source_context: AbstractContextManager[BinaryIO]
        if isinstance(local_file, (str, bytes, os.PathLike)):
            source_path = os.fsdecode(local_file)
            source_context = open(source_path, "rb", opener=opener_no_symlink)
        else:
            source_path = local_path
            source_context = nullcontext(local_file)

        with source_context as source:
            source.seek(0)
            try:
                initial_stat = os.fstat(source.fileno())
                size = initial_stat.st_size
            except (AttributeError, OSError):
                source.seek(0, os.SEEK_END)
                size = source.tell()
                source.seek(0)
                initial_stat = None

            hasher = self.content_hasher_factory()
            while chunk := source.read(1024 * 1024):
                hasher.update(chunk)
            checksum = hasher.hexdigest()
            if initial_stat is not None and os.fstat(source.fileno()) != initial_stat:
                raise DataChangedError(
                    "File changed during hashing", local_path=source_path
                )
            source.seek(0)
            mime_type = mimetypes.guess_type(name)[0] or "application/octet-stream"
            update_id = (
                existing.id
                if existing is not None and write_mode is not WriteMode.Add
                else None
            )
            session_url = self.drive.start_resumable_upload(
                name=name,
                parent_id=parent.id,
                mime_type=mime_type,
                size=size,
                file_id=update_id,
            )
            item = self.drive.upload_resumable(
                session_url,
                source,
                size,
                progress=(
                    (lambda completed: setattr(sync_event, "completed", completed))
                    if sync_event is not None
                    else None
                ),
            )
            if initial_stat is not None and os.fstat(source.fileno()) != initial_stat:
                raise DataChangedError(
                    "File changed during upload", local_path=source_path
                )
        if item.md5_checksum != checksum:
            raise DataCorruptionError(
                "Data corrupted", "Retry the Google Drive upload."
            )
        projection.apply_changes((DriveChange(item.id, False, item, None),))
        metadata = self._metadata_for_id(item.id)
        if not isinstance(metadata, FileMetadata):
            raise GoogleDriveError(
                "Google Drive returned invalid data",
                "The uploaded file has no projected path.",
            )
        return metadata

    def _item_for_folder(self, path: str) -> DriveItem:
        path = self._normalise_remote_path(path)
        item: DriveItem | None
        if path == "/":
            item = self.drive.get_root()
        else:
            item = self._ensure_projection().item_for_path(path)
        if item is None:
            raise NotFoundError("Folder not found", dbx_path=path)
        if not item.is_folder:
            raise NotAFolderError("Not a folder", dbx_path=path)
        return item

    def make_dir(self, remote_path: str, autorename: bool = False) -> FolderMetadata:
        remote_path = self._normalise_remote_path(remote_path)
        parent_path, name = posixpath.split(remote_path)
        projection = self._ensure_projection()
        existing = projection.item_for_path(remote_path)
        if existing is not None and not autorename:
            raise FolderConflictError("Folder already exists", dbx_path=remote_path)
        parent = self._item_for_folder(parent_path or "/")
        item = self.drive.create_folder(name, parent.id)
        projection.apply_changes((DriveChange(item.id, False, item, None),))
        metadata = self._metadata_for_id(item.id)
        if not isinstance(metadata, FolderMetadata):
            raise GoogleDriveError(
                "Google Drive returned invalid data",
                "The created folder has no projected path.",
            )
        return metadata

    def move(
        self, remote_path: str, new_path: str, autorename: bool = False
    ) -> FileMetadata | FolderMetadata:
        remote_path = self._normalise_remote_path(remote_path)
        new_path = self._normalise_remote_path(new_path)
        projection = self._ensure_projection()
        item = projection.item_for_path(remote_path)
        if item is None:
            raise NotFoundError("Item not found", dbx_path=remote_path)
        destination = projection.item_for_path(new_path)
        if destination is not None and destination.id != item.id and not autorename:
            error_type = (
                FolderConflictError if destination.is_folder else FileConflictError
            )
            raise error_type("Destination already exists", dbx_path=new_path)
        parent_path, new_name = posixpath.split(new_path)
        new_parent = self._item_for_folder(parent_path or "/")
        old_parent_id = item.parent_id or self.namespace_id
        old_projected_name = posixpath.basename(remote_path)
        if new_name == old_projected_name:
            new_name = item.name
        elif item.stub_extension and new_name.endswith(item.stub_extension):
            new_name = new_name[: -len(item.stub_extension)]
        moved = self.drive.move_item(
            item.id,
            old_parent_id=old_parent_id,
            new_parent_id=new_parent.id,
            new_name=new_name,
        )
        projection.apply_changes((DriveChange(moved.id, False, moved, None),))
        metadata = self._metadata_for_id(moved.id)
        if not isinstance(metadata, (FileMetadata, FolderMetadata)):
            raise GoogleDriveError(
                "Google Drive returned invalid data", "The moved item is unreachable."
            )
        return metadata

    def remove(
        self, remote_path: str, parent_rev: str | None = None
    ) -> FileMetadata | FolderMetadata:
        remote_path = self._normalise_remote_path(remote_path)
        projection = self._ensure_projection()
        item = projection.item_for_path(remote_path)
        if item is None:
            raise NotFoundError("Item not found", dbx_path=remote_path)
        if parent_rev is not None and item.version != parent_rev:
            raise FileConflictError(
                "Item changed on Google Drive", dbx_path=remote_path
            )
        metadata = self._metadata_for(item, remote_path)
        if not isinstance(metadata, (FileMetadata, FolderMetadata)):
            raise GoogleDriveError(
                "Google Drive returned invalid data", "The item metadata is invalid."
            )
        self.drive.delete_item(item.id)
        projection.apply_changes((DriveChange(item.id, True, None, None),))
        return metadata

    def _unsupported(self, operation: str) -> UnsupportedProviderOperationError:
        return UnsupportedProviderOperationError(
            f"Google Drive does not support {operation}",
            "This operation is available only for providers which expose it.",
        )

    def share_dir(
        self, remote_path: str, force_async: bool = False
    ) -> FolderMetadata | None:
        del remote_path, force_async
        raise self._unsupported("shared folders")

    def list_revisions(
        self, remote_path: str, mode: str = "path", limit: int = 10
    ) -> list[FileMetadata]:
        del remote_path, mode, limit
        raise self._unsupported("revision history")

    def restore(self, remote_path: str, rev: str) -> FileMetadata:
        del remote_path, rev
        raise self._unsupported("revision restore")

    def create_shared_link(
        self,
        remote_path: str,
        visibility: LinkAudience = LinkAudience.Public,
        access_level: LinkAccessLevel = LinkAccessLevel.Viewer,
        allow_download: bool | None = None,
        password: str | None = None,
        expires: datetime | None = None,
    ) -> SharedLinkMetadata:
        del remote_path, visibility, access_level, allow_download, password, expires
        raise self._unsupported("shared links")

    def revoke_shared_link(self, url: str) -> None:
        del url
        raise self._unsupported("shared links")

    def list_shared_links(
        self, remote_path: str | None = None, *, direct_only: bool = False
    ) -> list[SharedLinkMetadata]:
        del remote_path, direct_only
        raise self._unsupported("shared links")


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _tokens_from_payload(
    payload: Mapping[str, Any], now: float, *, require_refresh: bool
) -> GoogleTokens:
    access_token = payload.get("access_token")
    refresh_token = payload.get("refresh_token", "")
    expires_in = payload.get("expires_in")
    scope = payload.get("scope", GOOGLE_DRIVE_SCOPE)
    token_type = payload.get("token_type")
    if (
        not isinstance(access_token, str)
        or not access_token
        or not isinstance(refresh_token, str)
        or (require_refresh and not refresh_token)
        or not isinstance(expires_in, (int, float))
        or isinstance(expires_in, bool)
        or expires_in <= 0
        or not isinstance(scope, str)
        or GOOGLE_DRIVE_SCOPE not in scope.split()
        or token_type != "Bearer"
    ):
        raise _invalid_response("The OAuth token response is incomplete.")
    return GoogleTokens(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=now + float(expires_in),
        scope=scope,
    )


def _response_json(response: requests.Response) -> Mapping[str, Any]:
    content = response.content
    if len(content) > _MAX_JSON_BYTES:
        raise _invalid_response("A Google response is too large.")
    try:
        payload = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _invalid_response("Google returned invalid JSON.") from exc
    if not isinstance(payload, dict):
        raise _invalid_response("Google returned an invalid JSON object.")
    return payload


def _drive_error(response: requests.Response) -> tuple[str | None, str]:
    try:
        payload = _response_json(response)
    except GoogleDriveError:
        return None, f"Google returned HTTP {response.status_code}."
    raw_error = payload.get("error")
    if not isinstance(raw_error, dict):
        return None, f"Google returned HTTP {response.status_code}."
    message = raw_error.get("message")
    safe_message = (
        message
        if isinstance(message, str) and message
        else f"Google returned HTTP {response.status_code}."
    )
    errors = raw_error.get("errors", [])
    if not isinstance(errors, list):
        return None, safe_message
    for item in errors:
        if isinstance(item, dict) and isinstance(item.get("reason"), str):
            return cast(str, item["reason"]), safe_message
    return None, safe_message


def _required_string(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise _invalid_response(f"The {key} field is missing.")
    return value


def _optional_string(value: Any, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise _invalid_response(f"The {field} field is not valid.")
    return value


def _optional_nonnegative_int(value: Any, field: str) -> int | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.isdecimal():
        raise _invalid_response(f"The {field} field is not valid.")
    return int(value)


def _parse_google_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise _invalid_response("A Google timestamp is not valid.") from exc
    if parsed.tzinfo is None:
        raise _invalid_response("A Google timestamp has no timezone.")
    return parsed.astimezone(timezone.utc)


def _page_token(payload: Mapping[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise _invalid_response(f"The {key} field is not valid.")
    _validate_page_token(value)
    return value


def _validate_page_token(value: str) -> None:
    if not value or len(value) > 8192 or any(ord(char) < 0x20 for char in value):
        raise ValueError("The Drive page token is not valid")


def _validate_file_id(value: str) -> None:
    if (
        not value
        or len(value) > 1024
        or any(char.isspace() or ord(char) < 0x20 for char in value)
    ):
        raise ValueError("The Drive file ID is not valid")


def _validate_remote_name(value: str) -> None:
    if not value or len(value.encode("utf-8")) > 32768 or "\x00" in value:
        raise ValueError("The Drive file name is not valid")


def _is_allowed_google_url(value: str) -> bool:
    parsed = urlparse(value)
    return (
        parsed.scheme == "https"
        and parsed.username is None
        and parsed.password is None
        and parsed.port in {None, 443}
        and parsed.hostname is not None
        and (
            parsed.hostname == "www.googleapis.com"
            or parsed.hostname.endswith(".googleapis.com")
        )
    )


def _is_google_upload_url(value: str) -> bool:
    parsed = urlparse(value)
    return _is_allowed_google_url(value) and parsed.path.startswith("/upload/")


def _acknowledged_upload_offset(response: requests.Response, size: int) -> int:
    range_value = response.headers.get("Range")
    if range_value is None:
        return 0
    match = re.fullmatch(r"bytes=0-(\d+)", range_value)
    if match is None:
        raise _invalid_response("The resumable upload range is not valid.")
    offset = int(match.group(1)) + 1
    if offset > size:
        raise _invalid_response("The resumable upload range exceeds the file size.")
    return offset


def _invalid_response(message: str) -> GoogleDriveError:
    return GoogleDriveError("Google Drive returned invalid data", message)


def _escape_drive_name(item: DriveItem) -> str:
    name = unicodedata.normalize("NFC", item.name)
    encoded: list[str] = []
    for char in name:
        if char == "%" or char in _FORBIDDEN_WINDOWS_CHARS or ord(char) < 0x20:
            encoded.extend(f"%{byte:02X}" for byte in char.encode("utf-8"))
        else:
            encoded.append(char)
    escaped = "".join(encoded)
    while escaped.endswith((" ", ".")):
        last = escaped[-1]
        escaped = escaped[:-1] + "".join(
            f"%{byte:02X}" for byte in last.encode("utf-8")
        )
    if escaped in {"", ".", ".."}:
        escaped = "".join(f"%{byte:02X}" for byte in name.encode("utf-8"))
    stem = escaped.split(".", 1)[0].upper()
    if stem in _DOS_NAMES:
        first = escaped[0]
        escaped = f"%{ord(first):02X}{escaped[1:]}"
    extension = item.stub_extension
    if extension and not escaped.casefold().endswith(extension.casefold()):
        escaped += extension
    return _truncate_utf8_name(escaped, _MAX_NAME_BYTES)


def _unique_projected_names(items: Iterable[DriveItem]) -> dict[str, str]:
    item_list = list(items)
    base_names = {item.id: _escape_drive_name(item) for item in item_list}
    groups: dict[str, list[DriveItem]] = defaultdict(list)
    for item in item_list:
        groups[_name_collision_key(base_names[item.id])].append(item)

    result: dict[str, str] = {}
    used: set[str] = set()
    for item in sorted(item_list, key=lambda candidate: candidate.id):
        base_name = base_names[item.id]
        collision = len(groups[_name_collision_key(base_name)]) > 1
        token_length = 8
        while True:
            if collision or token_length > 8:
                token = hashlib.sha256(item.id.encode("utf-8")).hexdigest()[
                    :token_length
                ]
                candidate = _append_id_suffix(base_name, token)
            else:
                candidate = base_name
            key = _name_collision_key(candidate)
            if key not in used:
                result[item.id] = candidate
                used.add(key)
                break
            collision = True
            token_length += 4
            if token_length > 64:
                raise _invalid_response("Drive names cannot be projected uniquely.")
    return result


def _append_id_suffix(name: str, token: str) -> str:
    extension = PurePosixPath(name).suffix
    stem = name[: -len(extension)] if extension else name
    suffix = f" (Drive {token})"
    available = _MAX_NAME_BYTES - len((suffix + extension).encode("utf-8"))
    stem = _truncate_utf8_name(stem, max(1, available))
    return f"{stem}{suffix}{extension}"


def _truncate_utf8_name(value: str, maximum: int) -> str:
    encoded = value.encode("utf-8")
    if len(encoded) <= maximum:
        return value
    shortened = encoded[:maximum]
    while shortened:
        try:
            return shortened.decode("utf-8")
        except UnicodeDecodeError:
            shortened = shortened[:-1]
    raise _invalid_response("A Drive name cannot be projected.")


def _name_collision_key(value: str) -> str:
    return unicodedata.normalize("NFC", value).casefold()


def _path_collision_key(value: str) -> str:
    return "/".join(_name_collision_key(part) for part in value.split("/"))
