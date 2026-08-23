from pathlib import Path
from unittest.mock import Mock

import pytest

import maestral.config.main as config_module
import maestral.main as main_module
from maestral.client import DropboxClient
from maestral.config import MaestralConfig, MaestralState
from maestral.exceptions import KeyringAccessError, MaestralApiError, NotLinkedError
from maestral.main import Maestral
from maestral.providers.encrypted import EncryptedRemoteProvider
from maestral.providers.google_drive import GoogleDriveProvider
from maestral.rpc import JsonRpcDispatcher

from .virtual_files_fakes import FakeVirtualFileBackend


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


def configure_fake_encryption(
    maestral: Maestral,
    cache_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    vault_secrets: Mock | None = None,
) -> Mock:
    monkeypatch.setattr(main_module, "CryptomatorClient", FakeCryptomatorClient)
    monkeypatch.setattr(
        main_module, "resolve_sidecar_path", lambda: Path("/fake/cryptomator")
    )
    secrets = vault_secrets or Mock()
    monkeypatch.setattr(main_module, "VaultSecretStorage", Mock(return_value=secrets))
    maestral.configure_encryption("/Maestral Vault", str(cache_path))
    maestral.set_provider("dropbox")
    return secrets


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
    unselected_maestral._conf.set("encryption", "enabled", True)
    unselected_maestral._conf.set("encryption", "vault_ready", True)
    unselected_maestral._conf.set("encryption", "remote_path", "/Vault")
    unselected_maestral._conf.set("encryption", "cache_path", "/private/cache")
    unselected_maestral._conf.set("sync", "mode", "virtual")
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
            "vault_password_deleted": True,
            "virtual_files_reset": False,
            "root_path": "",
            "root_marker_id": "",
        },
    )

    unselected_maestral.sync._complete_pending_sync_reset()

    config = MaestralConfig(unselected_maestral.config_name)
    assert config.get("auth", "provider") == "google_drive"
    assert config.get("auth", "provider_selected") is False
    assert config.get("encryption", "enabled") is True
    assert config.get("encryption", "vault_ready") is False
    assert config.get("encryption", "remote_path") == "/Vault"
    assert config.get("encryption", "cache_path") == "/private/cache"
    assert config.get("sync", "mode") == "virtual"


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
    vault_secrets = Mock()
    monkeypatch.setattr(
        main_module, "VaultSecretStorage", Mock(return_value=vault_secrets)
    )

    unselected_maestral.configure_encryption("/Maestral Vault", str(cache_path))

    config = MaestralConfig(unselected_maestral.config_name)
    assert isinstance(unselected_maestral.client, EncryptedRemoteProvider)
    assert unselected_maestral.sync.client is unselected_maestral.client
    assert unselected_maestral.virtual_files.provider is unselected_maestral.client
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
    assert unselected_maestral.virtual_files.provider is unselected_maestral.client

    unselected_maestral.disable_encryption()
    assert isinstance(unselected_maestral.client, GoogleDriveProvider)
    assert unselected_maestral.virtual_files.provider is unselected_maestral.client
    assert unselected_maestral.encryption_enabled is False
    assert unselected_maestral.encryption_vault_ready is False
    assert unselected_maestral.encryption_vault_path == ""
    assert unselected_maestral.encryption_cache_path == ""
    vault_secrets.delete_password.assert_called_once_with()


def test_interrupted_unlink_preserves_protection_and_virtual_mode(
    config_name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_path = tmp_path / "ciphertext"
    config = MaestralConfig(config_name)
    state = MaestralState(config_name)
    config.set("auth", "provider_selected", True)
    config.set("auth", "account_id", "old-account")
    config.set("encryption", "enabled", True)
    config.set("encryption", "vault_ready", True)
    config.set("encryption", "remote_path", "/Maestral Vault")
    config.set("encryption", "cache_path", str(cache_path))
    config.set("sync", "mode", "virtual")
    config.set("sync", "path", str(tmp_path / "old-root"))
    state.set(
        "recovery",
        "sync_reset",
        {
            "kind": "unlink",
            "phase": "pending",
            "provider": "dropbox",
            "account_id": "old-account",
            "keyring": "automatic",
            "credentials_deleted": True,
            "vault_password_deleted": True,
            "virtual_files_reset": False,
            "root_path": str(tmp_path / "old-root"),
            "root_marker_id": "a" * 32,
        },
    )
    monkeypatch.setattr(main_module, "CryptomatorClient", FakeCryptomatorClient)
    monkeypatch.setattr(
        main_module, "resolve_sidecar_path", lambda: Path("/fake/cryptomator")
    )

    maestral = Maestral(
        config_name,
        virtual_file_backend=FakeVirtualFileBackend(),
        virtual_file_remote_polling=False,
    )
    try:
        assert isinstance(maestral.client, EncryptedRemoteProvider)
        assert maestral.sync.client is maestral.client
        assert maestral.virtual_files.provider is maestral.client
        assert maestral.sync_mode == "virtual"
        assert maestral.virtual_file_backend == "fake_native"
        assert maestral.encryption_enabled is True
        assert maestral.encryption_vault_ready is False
        assert maestral.encryption_vault_path == "/Maestral Vault"
        assert maestral.encryption_cache_path == str(cache_path)
        assert maestral.virtual_files.cursor == ""
        assert maestral.dropbox_path == ""
        assert state.get("recovery", "sync_reset") == {}
    finally:
        maestral.virtual_files.close()
        maestral.manager.shutdown()
        maestral.sync._connection.close()
        maestral.client.close()


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


@pytest.mark.parametrize("root_inside_cache", [False, True])
def test_encryption_rejects_overlapping_plaintext_and_ciphertext_roots(
    unselected_maestral: Maestral,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    root_inside_cache: bool,
) -> None:
    monkeypatch.setattr(main_module, "CryptomatorClient", FakeCryptomatorClient)
    monkeypatch.setattr(
        main_module, "resolve_sidecar_path", lambda: Path("/fake/cryptomator")
    )
    if root_inside_cache:
        cache_path = tmp_path / "ciphertext"
        root_path = cache_path / "plaintext"
    else:
        root_path = tmp_path / "plaintext"
        cache_path = root_path / "ciphertext"

    unselected_maestral.configure_encryption("/Vault", str(cache_path))
    unselected_maestral.set_provider("dropbox")
    monkeypatch.setattr(unselected_maestral, "_check_linked", Mock())

    with pytest.raises(MaestralApiError, match="cannot contain each other"):
        unselected_maestral.create_dropbox_directory(str(root_path))

    assert not root_path.exists()


def test_encryption_rechecks_root_separation_before_sync(
    unselected_maestral: Maestral,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_path = tmp_path / "plaintext"
    cache_path = root_path / "ciphertext"
    monkeypatch.setattr(main_module, "CryptomatorClient", FakeCryptomatorClient)
    monkeypatch.setattr(
        main_module, "resolve_sidecar_path", lambda: Path("/fake/cryptomator")
    )
    unselected_maestral.configure_encryption("/Vault", str(cache_path))
    unselected_maestral.sync._dropbox_path = str(root_path)
    monkeypatch.setattr(unselected_maestral, "_check_linked", Mock())
    monkeypatch.setattr(unselected_maestral, "_check_dropbox_dir", Mock())
    mirror_start = Mock()
    monkeypatch.setattr(unselected_maestral.manager, "start", mirror_start)

    with pytest.raises(MaestralApiError, match="cannot contain each other"):
        unselected_maestral.start_sync()

    mirror_start.assert_not_called()


@pytest.mark.parametrize(
    "protected_name",
    ["transfers", "owner", "manifest", "transaction"],
)
def test_encryption_rejects_every_private_cache_path(
    unselected_maestral: Maestral,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    protected_name: str,
) -> None:
    cache_path = tmp_path / "ciphertext"
    configure_fake_encryption(unselected_maestral, cache_path, monkeypatch)
    monkeypatch.setattr(unselected_maestral, "_check_linked", Mock())
    protected_paths = {
        "transfers": tmp_path / ".plaintext-transfers",
        "owner": tmp_path / ".ciphertext.maestral-cryptomator-cache",
        "manifest": tmp_path / ".ciphertext.maestral-cryptomator-manifest.json",
        "transaction": tmp_path / ".ciphertext.maestral-cryptomator-transaction.json",
    }
    root_path = protected_paths[protected_name]

    with pytest.raises(MaestralApiError, match="cannot contain each other"):
        unselected_maestral.create_dropbox_directory(str(root_path))

    assert not root_path.exists()


def test_encryption_rejects_a_linked_cache_alias(
    unselected_maestral: Maestral,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_path = tmp_path / "ciphertext"
    cache_path.mkdir()
    alias_path = tmp_path / "cache-alias"
    alias_path.symlink_to(cache_path, target_is_directory=True)
    configure_fake_encryption(unselected_maestral, cache_path, monkeypatch)
    monkeypatch.setattr(unselected_maestral, "_check_linked", Mock())

    with pytest.raises(MaestralApiError, match="unsafe"):
        unselected_maestral.create_dropbox_directory(str(alias_path))


def test_disable_encryption_rolls_back_before_config_commit(
    unselected_maestral: Maestral,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_fake_encryption(unselected_maestral, tmp_path / "ciphertext", monkeypatch)
    old_client = unselected_maestral.client
    old_close = Mock()
    monkeypatch.setattr(old_client, "close", old_close)
    new_client = Mock(linked=False)
    monkeypatch.setattr(
        unselected_maestral, "_create_client", Mock(return_value=new_client)
    )
    monkeypatch.setattr(
        unselected_maestral,
        "_save_encryption_settings",
        Mock(side_effect=OSError("save failed")),
    )

    with pytest.raises(OSError, match="save failed"):
        unselected_maestral.disable_encryption()

    assert unselected_maestral.client is old_client
    assert unselected_maestral.sync.client is old_client
    assert unselected_maestral.virtual_files.provider is old_client
    assert unselected_maestral.encryption_enabled is True
    assert unselected_maestral._state.get("recovery", "vault_secret_cleanup") == {}
    new_client.close.assert_called_once_with()
    old_close.assert_not_called()


def test_configure_encryption_finishes_after_config_commit_error(
    unselected_maestral: Maestral,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_path = tmp_path / "ciphertext"
    old_client = unselected_maestral.client
    old_close = Mock()
    monkeypatch.setattr(old_client, "close", old_close)
    new_client = Mock(linked=False)
    create_client = Mock(return_value=new_client)
    monkeypatch.setattr(unselected_maestral, "_create_client", create_client)
    save_settings = unselected_maestral._save_encryption_settings

    def commit_then_fail(*args: object) -> None:
        save_settings(*args)  # type: ignore[arg-type]
        raise OSError("after commit")

    monkeypatch.setattr(
        unselected_maestral, "_save_encryption_settings", commit_then_fail
    )

    with pytest.raises(MaestralApiError, match="encrypted provider was saved"):
        unselected_maestral.configure_encryption("/Vault", str(cache_path))

    assert unselected_maestral.client is new_client
    assert unselected_maestral.sync.client is new_client
    assert unselected_maestral.virtual_files.provider is new_client
    assert unselected_maestral.encryption_enabled is True
    assert unselected_maestral.encryption_vault_path == "/Vault"
    assert unselected_maestral.encryption_cache_path == str(cache_path)
    old_close.assert_called_once_with()
    new_client.close.assert_not_called()

    unselected_maestral.configure_encryption("/Vault", str(cache_path))

    create_client.assert_called_once()


def test_disable_encryption_finishes_after_config_commit_error(
    unselected_maestral: Maestral,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vault_secrets = configure_fake_encryption(
        unselected_maestral, tmp_path / "ciphertext", monkeypatch
    )
    old_client = unselected_maestral.client
    old_close = Mock()
    monkeypatch.setattr(old_client, "close", old_close)
    new_client = Mock(linked=False)
    monkeypatch.setattr(
        unselected_maestral, "_create_client", Mock(return_value=new_client)
    )
    save_settings = unselected_maestral._save_encryption_settings

    def commit_then_fail(*args: object) -> None:
        save_settings(*args)  # type: ignore[arg-type]
        raise OSError("after commit")

    monkeypatch.setattr(
        unselected_maestral, "_save_encryption_settings", commit_then_fail
    )

    with pytest.raises(MaestralApiError, match="plain provider was saved"):
        unselected_maestral.disable_encryption()

    assert unselected_maestral.client is new_client
    assert unselected_maestral.sync.client is new_client
    assert unselected_maestral.virtual_files.provider is new_client
    assert unselected_maestral.encryption_enabled is False
    assert unselected_maestral._state.get("recovery", "vault_secret_cleanup") == {}
    old_close.assert_called_once_with()
    new_client.close.assert_not_called()
    vault_secrets.delete_password.assert_called_once_with()


def test_disable_encryption_retries_keyring_cleanup(
    unselected_maestral: Maestral,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vault_secrets = Mock()
    vault_secrets.delete_password.side_effect = KeyringAccessError(
        "Keyring unavailable", "Try again"
    )
    configure_fake_encryption(
        unselected_maestral,
        tmp_path / "ciphertext",
        monkeypatch,
        vault_secrets,
    )

    unselected_maestral.disable_encryption()

    assert unselected_maestral.encryption_enabled is False
    assert unselected_maestral._state.get("recovery", "vault_secret_cleanup") == {
        "kind": "disable_encryption"
    }

    vault_secrets.delete_password.side_effect = None
    unselected_maestral.disable_encryption()

    assert unselected_maestral._state.get("recovery", "vault_secret_cleanup") == {}
    assert vault_secrets.delete_password.call_count == 2


def test_constructor_retries_vault_secret_cleanup(
    config_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = MaestralConfig(config_name)
    state = MaestralState(config_name)
    config.set("encryption", "enabled", False)
    state.set(
        "recovery",
        "vault_secret_cleanup",
        {"kind": "disable_encryption"},
    )
    vault_secrets = Mock()
    monkeypatch.setattr(
        main_module, "VaultSecretStorage", Mock(return_value=vault_secrets)
    )

    maestral = Maestral(config_name)
    try:
        vault_secrets.delete_password.assert_called_once_with()
        assert state.get("recovery", "vault_secret_cleanup") == {}
    finally:
        maestral._close_resources()


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
