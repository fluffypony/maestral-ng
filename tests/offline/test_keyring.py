from unittest import mock

import pytest
from keyring.backend import KeyringBackend
from keyring.backends.SecretService import Keyring as SecretServiceKeyring
from keyring.errors import InitError, KeyringLocked, NoKeyringError, PasswordDeleteError
from keyrings.alt.file import PlaintextKeyring

from maestral.config import MaestralConfig, remove_configuration
from maestral.exceptions import KeyringAccessError
from maestral.keyring import CredentialStorage


@pytest.fixture
def cred_storage():
    storage = CredentialStorage("test-config")

    yield storage

    storage.delete_creds()
    remove_configuration("test-config")


def test_unlinked_state(cred_storage: CredentialStorage) -> None:
    """Test unlinked state"""
    conf = MaestralConfig("test-config")

    assert not cred_storage.loaded
    assert cred_storage.account_id is None
    assert cred_storage.token is None
    assert cred_storage.keyring is None

    assert conf.get("auth", "account_id") == ""
    assert conf.get("auth", "keyring") == "automatic"


def test_save_creds(cred_storage: CredentialStorage) -> None:
    """Test linked state"""
    conf = MaestralConfig("test-config")
    cred_storage.set_keyring_backend(PlaintextKeyring())

    cred_storage.save_creds("account_id", "token", allow_plaintext=True)

    assert cred_storage.loaded
    assert cred_storage.account_id == "account_id"
    assert cred_storage.token == "token"
    assert isinstance(cred_storage.keyring, KeyringBackend)

    assert conf.get("auth", "account_id") == "account_id"
    assert conf.get("auth", "keyring") != "automatic"


def test_load_creds(cred_storage: CredentialStorage) -> None:
    """Test linked state"""

    cred_storage.set_keyring_backend(PlaintextKeyring())
    cred_storage.save_creds("account_id", "token", allow_plaintext=True)

    cred_storage2 = CredentialStorage("test-config")
    cred_storage2.load_creds()

    assert cred_storage2.loaded
    assert cred_storage2.account_id == "account_id"
    assert cred_storage2.token == "token"
    assert isinstance(cred_storage2.keyring, KeyringBackend)


def test_delete_creds(cred_storage: CredentialStorage) -> None:
    """Test resetting state on `delete_creds`"""
    conf = MaestralConfig("test-config")
    cred_storage.set_keyring_backend(PlaintextKeyring())

    cred_storage.save_creds("account_id", "token", allow_plaintext=True)
    cred_storage.delete_creds()

    assert not cred_storage.loaded
    assert cred_storage.account_id is None
    assert cred_storage.token is None
    assert cred_storage.keyring is None

    assert conf.get("auth", "account_id") == ""
    assert conf.get("auth", "keyring") == "automatic"


def test_delete_creds_publishes_live_clear_after_config_commit(
    cred_storage: CredentialStorage,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conf = MaestralConfig("test-config")
    cred_storage.set_keyring_backend(PlaintextKeyring())
    cred_storage.save_creds("account_id", "token", allow_plaintext=True)
    original_save = cred_storage._conf.save

    def save_then_interrupt() -> None:
        original_save()
        raise KeyboardInterrupt("after auth config commit")

    monkeypatch.setattr(cred_storage._conf, "save", save_then_interrupt)

    with pytest.raises(KeyboardInterrupt, match="after auth config commit"):
        cred_storage.delete_creds()

    assert not cred_storage.loaded
    assert cred_storage.token is None
    assert cred_storage.keyring is None
    assert conf.get("auth", "account_id") == ""


def test_delete_creds_keeps_live_token_before_config_commit(
    cred_storage: CredentialStorage,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cred_storage.set_keyring_backend(PlaintextKeyring())
    cred_storage.save_creds("account_id", "token", allow_plaintext=True)
    old_keyring = cred_storage.keyring

    def fail_save() -> None:
        raise OSError("before auth config commit")

    monkeypatch.setattr(cred_storage._conf, "save", fail_save)

    with pytest.raises(OSError, match="before auth config commit"):
        cred_storage.delete_creds()

    assert cred_storage.loaded
    assert cred_storage.token == "token"
    assert cred_storage.keyring is old_keyring


def test_locked_keyring_does_not_fall_back(cred_storage: CredentialStorage) -> None:
    conf = MaestralConfig("test-config")

    cred_storage.set_keyring_backend(SecretServiceKeyring())

    with mock.patch.object(
        cred_storage.keyring, "set_password", side_effect=KeyringLocked("")
    ):
        with pytest.raises(KeyringAccessError):
            cred_storage.save_creds("account_id", "token", allow_plaintext=True)

    assert isinstance(cred_storage.keyring, SecretServiceKeyring)
    assert conf.get("auth", "account_id") == ""
    assert conf.get("auth", "keyring") != "keyrings.alt.file.PlaintextKeyring"


def test_plaintext_fallback_requires_consent(
    cred_storage: CredentialStorage,
) -> None:
    conf = MaestralConfig("test-config")

    cred_storage.set_keyring_backend(SecretServiceKeyring())

    with mock.patch.object(
        cred_storage.keyring, "set_password", side_effect=NoKeyringError()
    ):
        with pytest.raises(KeyringAccessError):
            cred_storage.save_creds("account_id", "token")

    assert isinstance(cred_storage.keyring, SecretServiceKeyring)
    assert conf.get("auth", "account_id") == ""

    with mock.patch.object(
        cred_storage.keyring, "set_password", side_effect=NoKeyringError()
    ):
        cred_storage.save_creds("account_id", "token", allow_plaintext=True)

    assert isinstance(cred_storage.keyring, PlaintextKeyring)
    assert conf.get("auth", "keyring") == "keyrings.alt.file.PlaintextKeyring"


def test_keyring_initialisation_error_does_not_fall_back(
    cred_storage: CredentialStorage,
) -> None:
    with mock.patch.object(
        cred_storage, "_best_keyring_backend", side_effect=InitError()
    ):
        with pytest.raises(KeyringAccessError):
            cred_storage.save_creds("account_id", "token", allow_plaintext=True)

    assert cred_storage.keyring is None
    assert cred_storage.account_id is None


def test_configured_plaintext_keyring_requires_consent(
    cred_storage: CredentialStorage,
) -> None:
    plaintext_keyring = PlaintextKeyring()
    cred_storage.set_keyring_backend(plaintext_keyring)

    with mock.patch.object(plaintext_keyring, "set_password") as set_password:
        with pytest.raises(KeyringAccessError):
            cred_storage.save_creds("account_id", "token")

    set_password.assert_not_called()


def test_keyring_errors_without_args(cred_storage: CredentialStorage) -> None:
    conf = MaestralConfig("test-config")
    keyring = PlaintextKeyring()
    cred_storage.set_keyring_backend(keyring)
    conf.set("auth", "account_id", "account_id")

    with mock.patch.object(keyring, "get_password", side_effect=RuntimeError()):
        with pytest.raises(KeyringAccessError):
            cred_storage.load_creds()

    with mock.patch.object(keyring, "delete_password", side_effect=RuntimeError()):
        with pytest.raises(KeyringAccessError):
            cred_storage.delete_creds()

    with mock.patch.object(
        keyring, "delete_password", side_effect=PasswordDeleteError()
    ):
        cred_storage.delete_creds()


def test_keyring_logger_scope() -> None:
    with mock.patch("maestral.keyring.scoped_logger") as scoped_logger:
        storage = CredentialStorage("logger-test-config")

    scoped_logger.assert_called_once_with("maestral.keyring", "logger-test-config")
    storage.delete_creds()
    remove_configuration("logger-test-config")


def test_load_error(cred_storage: CredentialStorage) -> None:
    """Test loading state from config file and keyring"""

    cred_storage.set_keyring_backend(PlaintextKeyring())
    cred_storage.save_creds("account_id", "token", allow_plaintext=True)

    cred_storage2 = CredentialStorage("test-config")
    cred_storage2.set_keyring_backend(PlaintextKeyring())

    with mock.patch.object(
        cred_storage2.keyring, "get_password", side_effect=KeyringLocked("")
    ):
        with pytest.raises(KeyringAccessError):
            cred_storage2.token


def test_delete_error(cred_storage: CredentialStorage) -> None:
    """Test loading state from config file and keyring"""

    cred_storage.set_keyring_backend(PlaintextKeyring())
    cred_storage.save_creds("account_id", "token", allow_plaintext=True)

    with mock.patch.object(
        cred_storage.keyring, "delete_password", side_effect=KeyringLocked("")
    ):
        with pytest.raises(KeyringAccessError):
            cred_storage.delete_creds()


def test_provider_scopes_keyring_accessors() -> None:
    dropbox = CredentialStorage("scoped-config", "dropbox")
    google = CredentialStorage("scoped-config", "google_drive")

    assert dropbox._get_accessor("account") == ("config:scoped-config:dropbox:account")
    assert google._get_accessor("account") == (
        "config:scoped-config:google_drive:account"
    )
    remove_configuration("scoped-config")


def test_load_migrates_legacy_dropbox_accessor() -> None:
    storage = CredentialStorage("legacy-config", "dropbox")
    ring = PlaintextKeyring()
    ring.get_password = mock.Mock(  # type: ignore[method-assign]
        side_effect=[None, "legacy-token"]
    )
    ring.set_password = mock.Mock()  # type: ignore[method-assign]
    ring.delete_password = mock.Mock()  # type: ignore[method-assign]
    storage.set_keyring_backend(ring)
    MaestralConfig("legacy-config").set("auth", "account_id", "account")

    storage.load_creds()

    assert storage.token == "legacy-token"
    assert ring.get_password.call_args_list == [
        mock.call("Maestral", "config:legacy-config:dropbox:account"),
        mock.call("Maestral", "config:legacy-config:account"),
    ]
    ring.set_password.assert_called_once_with(
        "Maestral", "config:legacy-config:dropbox:account", "legacy-token"
    )
    ring.delete_password.assert_called_once_with(
        "Maestral", "config:legacy-config:account"
    )
    remove_configuration("legacy-config")
