from pathlib import Path
from unittest.mock import Mock

import pytest

import maestral.config.main as config_module
import maestral.main as main_module
from maestral.client import DropboxClient
from maestral.config import MaestralConfig
from maestral.exceptions import MaestralApiError, NotLinkedError
from maestral.main import Maestral
from maestral.providers.encrypted import EncryptedRemoteProvider
from maestral.providers.google_drive import GoogleDriveProvider
from maestral.rpc import JsonRpcDispatcher


@pytest.fixture
def unselected_maestral(config_name: str):
    maestral = Maestral(config_name)
    yield maestral
    maestral.manager.shutdown()
    maestral.sync._connection.close()
    maestral.client.close()


def rpc_request(
    method: str, params: dict[str, object] | None = None
) -> dict[str, object]:
    request: dict[str, object] = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
    }
    if params is not None:
        request["params"] = params
    return request


class FakeCryptomatorClient:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.terminated = False
        self.shutdown_called = False

    def shutdown(self) -> None:
        self.shutdown_called = True

    def terminate(self) -> None:
        self.terminated = True


def test_provider_must_be_selected_before_authentication(
    unselected_maestral: Maestral,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    auth_url = Mock(return_value="https://example.test/authorize")
    link = Mock(return_value=0)
    monkeypatch.setattr(unselected_maestral.client, "get_auth_url", auth_url)
    monkeypatch.setattr(unselected_maestral.client, "link", link)

    with pytest.raises(MaestralApiError, match="No storage provider selected"):
        unselected_maestral.get_auth_url()
    with pytest.raises(MaestralApiError, match="No storage provider selected"):
        unselected_maestral.link(code="code")

    auth_url.assert_not_called()
    link.assert_not_called()

    unselected_maestral.set_provider("dropbox")

    assert unselected_maestral.get_auth_url() == "https://example.test/authorize"
    assert unselected_maestral.link(code="code") == 0
    auth_url.assert_called_once_with()
    link.assert_called_once_with(
        code="code",
        refresh_token=None,
        access_token=None,
        allow_plaintext_keyring=False,
    )


def test_provider_must_be_selected_before_local_root(
    unselected_maestral: Maestral, tmp_path: Path
) -> None:
    with pytest.raises(MaestralApiError, match="No storage provider selected"):
        unselected_maestral.create_dropbox_directory(str(tmp_path / "sync"))


def test_provider_selection_persists_and_rebuilds_client(config_name: str) -> None:
    first = Maestral(config_name)
    second = None
    try:
        assert isinstance(first.client, DropboxClient)

        first.set_provider("google-drive")

        config = MaestralConfig(config_name)
        assert first.provider == "google_drive"
        assert isinstance(first.client, GoogleDriveProvider)
        assert first.sync.client is first.client
        assert config.get("auth", "provider") == "google_drive"
        assert config.get("auth", "provider_selected") is True

        first.manager.shutdown()
        first.sync._connection.close()
        first.client.close()
        with config_module._config_lock:
            config_module._config_instances.pop(config_name)
        with config_module._state_lock:
            config_module._state_instances.pop(config_name)
        second = Maestral(config_name)

        assert second.provider == "google_drive"
        assert isinstance(second.client, GoogleDriveProvider)
        assert second.get_app_snapshot()["provider"] == "google_drive"
    finally:
        if second is not None:
            second.manager.shutdown()
            second.sync._connection.close()
            second.client.close()
        else:
            first.manager.shutdown()
            first.sync._connection.close()
            first.client.close()


def test_unlink_reset_keeps_provider_for_the_next_selection(
    unselected_maestral: Maestral,
) -> None:
    unselected_maestral.set_provider("google_drive")
    unselected_maestral._state.set(
        "recovery",
        "sync_reset",
        {
            "kind": "unlink",
            "phase": "config",
            "provider": "google_drive",
            "account_id": "linked-account",
            "keyring": "automatic",
            "credentials_deleted": True,
            "root_path": "",
            "root_marker_id": "",
        },
    )

    unselected_maestral.sync._complete_pending_sync_reset()

    config = MaestralConfig(unselected_maestral.config_name)
    assert config.get("auth", "provider") == "google_drive"
    assert config.get("auth", "provider_selected") is False


def test_rpc_exposes_provider_selection_and_snapshot(
    unselected_maestral: Maestral,
) -> None:
    dispatcher = JsonRpcDispatcher(unselected_maestral)

    handshake = dispatcher.dispatch(rpc_request("rpc.handshake"))["result"]
    response = dispatcher.dispatch(
        rpc_request("set_provider", {"provider": "google_drive"})
    )
    snapshot = dispatcher.dispatch(rpc_request("get_app_snapshot"))["result"]

    assert "set_provider" in handshake["methods"]
    assert response["result"] is None
    assert snapshot["provider"] == "google_drive"


def test_encryption_configuration_rebuilds_provider_stack(
    unselected_maestral: Maestral,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_path = tmp_path / "ciphertext"
    monkeypatch.setattr(main_module, "CryptomatorClient", FakeCryptomatorClient)
    monkeypatch.setattr(
        main_module, "resolve_sidecar_path", lambda: Path("/fake/cryptomator")
    )

    unselected_maestral.configure_encryption("/Maestral Vault", str(cache_path))

    config = MaestralConfig(unselected_maestral.config_name)
    assert isinstance(unselected_maestral.client, EncryptedRemoteProvider)
    assert unselected_maestral.sync.client is unselected_maestral.client
    assert unselected_maestral.encryption_enabled is True
    assert unselected_maestral.encryption_vault_ready is False
    assert unselected_maestral.encryption_vault_path == "/Maestral Vault"
    assert unselected_maestral.encryption_cache_path == str(cache_path)
    assert unselected_maestral.encryption_vault_open is False
    assert config.get("encryption", "enabled") is True
    assert config.get("encryption", "vault_ready") is False
    assert config.get("encryption", "remote_path") == "/Maestral Vault"
    assert config.get("encryption", "cache_path") == str(cache_path)

    initialise = Mock(return_value=None)
    monkeypatch.setattr(unselected_maestral.client, "initialise_vault", initialise)
    unselected_maestral.initialise_encrypted_vault("vault password")
    initialise.assert_called_once_with("vault password")
    assert unselected_maestral.encryption_vault_ready is True
    assert config.get("encryption", "vault_ready") is True

    unselected_maestral.set_provider("google_drive")
    assert isinstance(unselected_maestral.client, EncryptedRemoteProvider)
    assert unselected_maestral.provider == "google_drive"
    assert unselected_maestral.client.provider_id == "cryptomator_google_drive"

    unselected_maestral.disable_encryption()
    assert isinstance(unselected_maestral.client, GoogleDriveProvider)
    assert unselected_maestral.encryption_enabled is False
    assert unselected_maestral.encryption_vault_ready is False
    assert unselected_maestral.encryption_vault_path == ""
    assert unselected_maestral.encryption_cache_path == ""


def test_encryption_configuration_is_exposed_over_rpc(
    unselected_maestral: Maestral,
) -> None:
    dispatcher = JsonRpcDispatcher(unselected_maestral)
    handshake = dispatcher.dispatch(rpc_request("rpc.handshake"))["result"]
    snapshot = dispatcher.dispatch(rpc_request("get_app_snapshot"))["result"]

    assert "configure_encryption" in handshake["methods"]
    assert "disable_encryption" in handshake["methods"]
    assert "initialise_encrypted_vault" in handshake["methods"]
    assert "attach_encrypted_vault" in handshake["methods"]
    assert "unlock_encrypted_vault" in handshake["methods"]
    assert "lock_encrypted_vault" in handshake["methods"]
    assert "encryption_enabled" in handshake["properties"]["read"]
    assert "encryption_vault_ready" in handshake["properties"]["read"]
    assert "encryption_vault_open" in handshake["properties"]["read"]
    assert snapshot["encryption_enabled"] is False
    assert snapshot["encryption_vault_ready"] is False
    assert snapshot["encryption_vault_path"] == ""
    assert snapshot["encryption_cache_path"] == ""
    assert snapshot["encryption_vault_open"] is False


def test_encryption_configuration_rejects_unsafe_transitions(
    unselected_maestral: Maestral,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(main_module, "CryptomatorClient", FakeCryptomatorClient)
    monkeypatch.setattr(
        main_module, "resolve_sidecar_path", lambda: Path("/fake/cryptomator")
    )

    with pytest.raises(ValueError, match="cache path must be absolute"):
        unselected_maestral.configure_encryption("/Vault", "relative/cache")

    unselected_maestral._conf.set("auth", "account_id", "linked-account")
    with pytest.raises(MaestralApiError, match="Unlink the current account"):
        unselected_maestral.configure_encryption("/Vault", str(tmp_path / "ciphertext"))
    unselected_maestral._conf.set("auth", "account_id", "")

    unselected_maestral.sync.dropbox_path = str(tmp_path / "sync")
    with pytest.raises(MaestralApiError, match="local sync folder"):
        unselected_maestral.configure_encryption("/Vault", str(tmp_path / "ciphertext"))

    with pytest.raises(ValueError, match="remote encryption API"):
        unselected_maestral.set_conf("encryption", "enabled", True)


@pytest.mark.parametrize("provider", ["onedrive", "", "dropbox_v2"])
def test_invalid_provider_does_not_change_selection(
    unselected_maestral: Maestral, provider: str
) -> None:
    with pytest.raises(ValueError, match="Unknown remote provider"):
        unselected_maestral.set_provider(provider)

    config = MaestralConfig(unselected_maestral.config_name)
    assert unselected_maestral.provider == "dropbox"
    assert config.get("auth", "provider") == "dropbox"
    assert config.get("auth", "provider_selected") is False


def test_provider_change_is_rejected_after_link(
    unselected_maestral: Maestral,
) -> None:
    unselected_maestral.set_provider("dropbox")
    unselected_maestral._conf.set("auth", "account_id", "linked-account")

    with pytest.raises(MaestralApiError, match="Unlink the current account"):
        unselected_maestral.set_provider("google_drive")

    assert unselected_maestral.provider == "dropbox"
    assert isinstance(unselected_maestral.client, DropboxClient)


def test_provider_change_is_rejected_after_root_selection(
    unselected_maestral: Maestral, tmp_path: Path
) -> None:
    unselected_maestral.set_provider("dropbox")
    unselected_maestral.sync.dropbox_path = str(tmp_path / "sync")

    with pytest.raises(MaestralApiError, match="local sync folder"):
        unselected_maestral.set_provider("google_drive")

    assert unselected_maestral.provider == "dropbox"


def test_direct_provider_config_write_is_rejected(
    unselected_maestral: Maestral,
) -> None:
    with pytest.raises(ValueError, match="provider selection API"):
        unselected_maestral.set_conf("auth", "provider", "google_drive")
    with pytest.raises(ValueError, match="provider selection API"):
        unselected_maestral.set_conf("auth", "provider_selected", True)


def test_unselected_profile_still_uses_legacy_dropbox_default(
    unselected_maestral: Maestral,
) -> None:
    assert unselected_maestral.provider == "dropbox"
    assert isinstance(unselected_maestral.client, DropboxClient)
    with pytest.raises(NotLinkedError):
        unselected_maestral.get_account_info()
