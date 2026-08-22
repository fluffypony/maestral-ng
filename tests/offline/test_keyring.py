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
