"""
This module is responsible for authorization and token store in the system keyring.
"""

from __future__ import annotations

# system imports
from threading import RLock

import keyring.backends
import keyring.backends.kwallet
import keyring.backends.macOS
import keyring.backends.SecretService
import keyrings.alt.file

# external imports
import requests
from keyring.backend import KeyringBackend
from keyring.core import load_keyring
from keyring.errors import (
    InitError,
    KeyringLocked,
    NoKeyringError,
    PasswordDeleteError,
)

from .cli import output

# local imports
from .config import MaestralConfig
from .exceptions import KeyringAccessError
from .logging import scoped_logger
from .utils import exc_info_tuple

__all__ = ["CredentialStorage"]


supported_keyring_backends = (
    keyring.backends.macOS.Keyring,
    keyring.backends.SecretService.Keyring,
    keyring.backends.kwallet.DBusKeyring,
    keyring.backends.kwallet.DBusKeyringKWallet4,
    keyrings.alt.file.PlaintextKeyring,
)

CONNECTION_ERRORS = (
    requests.exceptions.Timeout,
    requests.exceptions.RetryError,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    ConnectionError,
)


class CredentialStorage:
    """Provides a threadsafe interface to store credentials in a system keyring

    CredentialStorage provides token store in the preferred system keyring. Supported
    keyring backends are, in order of preference:

        * macOS Keychain
        * Any keyring implementing the SecretService Dbus specification
        * KWallet
        * Plain text storage

    .. note:: Once the token has been stored with a keyring backend, that backend will
        be saved in the config file and remembered until deleting the credentials.

    .. warning:: Unlike macOS Keychain, Gnome Keyring and KWallet do not support
        app-specific access to passwords. If the user unlocks those keyrings, we and any
        other application in the same user session get access to *all* saved passwords.

    :param config_name: Name of maestral config.
    """

    _lock = RLock()

    def __init__(self, config_name: str) -> None:
        self._config_name = config_name
        self._logger = scoped_logger(__name__, config_name)

        self._conf = MaestralConfig(config_name)

        # defer keyring access until token requested by user
        self._token: str | None = None
        self._loaded = False
        self._keyring = self._keyring_from_config()

    @property
    def keyring(self) -> KeyringBackend | None:
        """
        The keyring backend currently being used to store auth tokens. Will be None if
        we are not linked.
        """
        return self._keyring

    def set_keyring_backend(self, ring: KeyringBackend | None) -> None:
        """
        Enforce usage of a particular keyring backend. If not called, the best backend
        will be selected depending on the platform. Do not change backends after saving
        credentials.

        :param ring: Keyring backend to use.
        """
        if not ring:
            self._conf.set("auth", "keyring", "automatic")
        else:
            self._conf.set(
                "auth",
                "keyring",
                f"{ring.__class__.__module__}.{ring.__class__.__name__}",
            )

        self._keyring = ring

    def _keyring_from_config(self) -> KeyringBackend | None:
        """Initialise keyring specified in config."""
        keyring_class: str = self._conf.get("auth", "keyring").strip()

        if keyring_class == "automatic":
            return None

        try:
            return load_keyring(keyring_class)
        except Exception as exc:
            # Bomb out with an exception.

            title = f"Cannot load keyring {keyring_class}"
            message = "Please relink Maestral to get a new access token."
            new_exc = KeyringAccessError(title, message).with_traceback(
                exc.__traceback__
            )
            raise new_exc

    def _best_keyring_backend(self) -> KeyringBackend:
        """Find and initialise the most secure of the available and supported keyring
        backends.
        """
        import keyring.backends

        # Get preferred keyring backends for platform.
        available_rings = keyring.backend.get_all_keyring()
        supported_rings = [
            k for k in available_rings if isinstance(k, supported_keyring_backends)
        ]

        if not supported_rings:
            raise NoKeyringError("No supported keyring backend found")

        ring = max(supported_rings, key=lambda x: x.priority)

        return ring

    def _get_accessor(self, account_id: str) -> str:
        return f"config:{self._config_name}:{account_id}"

    @property
    def loaded(self) -> bool:
        """Whether we have already loaded the credentials. This will be true after
        calling :meth:`load_creds` or accessing the any of the auth credentials through
        instance properties."""
        return self._loaded

    @property
    def token(self) -> str | None:
        """The saved token (read only). This call will block until the keyring is
        unlocked."""
        with self._lock:
            if not self._loaded:
                self.load_creds()
            return self._token

    @property
    def account_id(self) -> str | None:
        """The saved account id (read only)."""
        with self._lock:
            return self._conf.get("auth", "account_id") or None

    def load_creds(self) -> None:
        """
        Loads auth token from system keyring. This will be called automatically when
        accessing the :attr:`token` property. This call will block until the keyring is
        unlocked or unlocking is declined by the user.

        :raises KeyringAccessError: if the system keyring is locked or otherwise cannot
            be accessed (for example if the app bundle signature has been invalidated).
        """
        if not (self.keyring and self.account_id):
            return

        self._logger.debug(f"Using keyring: {self.keyring}")

        accessor = self._get_accessor(self.account_id)

        try:
            token = self.keyring.get_password("Maestral", accessor)
        except (KeyringLocked, InitError):
            title = "Could not load auth token"
            msg = (
                f"{self.keyring.name} is locked. Please unlock the keyring "
                "and try again."
            )
            new_exc = KeyringAccessError(title, msg)
            self._logger.error(title, exc_info=exc_info_tuple(new_exc))
            raise new_exc
        except Exception as e:
            title = "Could not load auth token"
            new_exc = KeyringAccessError(title, str(e))
            self._logger.error(title, exc_info=exc_info_tuple(new_exc))
            raise new_exc

        if token:
            self._token = token
            self._loaded = True

    def save_creds(
        self, account_id: str, token: str, allow_plaintext: bool = False
    ) -> None:
        """
        Saves the auth token to the system keyring. Falls back to plain text storage
        only if no keyring is available and the caller has explicitly allowed it.

        :param account_id: The account ID.
        :param token: The access token.
        :param allow_plaintext: Whether to allow plain text credential storage when no
            secure keyring is available.
        :raises KeyringAccessError: if the keyring cannot be accessed or plain text
            storage is required but has not been allowed.
        """
        with self._lock:
            try:
                if self._keyring:
                    keyring = self._keyring
                else:
                    keyring = self._best_keyring_backend()
            except NoKeyringError as exc:
                if not allow_plaintext:
                    raise KeyringAccessError(
                        "No secure keyring available",
                        "Relink with explicit permission to store the auth token "
                        "in plain text.",
                    ) from exc

                keyring = keyrings.alt.file.PlaintextKeyring()
            except (KeyringLocked, InitError) as exc:
                raise KeyringAccessError(
                    "Could not save auth token",
                    "The system keyring is locked or unavailable. Please try again.",
                ) from exc
            except Exception as exc:
                raise KeyringAccessError("Could not save auth token", str(exc)) from exc

            accessor = self._get_accessor(account_id)

            if isinstance(keyring, keyrings.alt.file.PlaintextKeyring):
                if not allow_plaintext:
                    raise KeyringAccessError(
                        "No secure keyring available",
                        "Relink with explicit permission to store the auth token "
                        "in plain text.",
                    )

            try:
                keyring.set_password("Maestral", accessor, token)
            except NoKeyringError as exc:
                if not allow_plaintext:
                    raise KeyringAccessError(
                        "No secure keyring available",
                        "Relink with explicit permission to store the auth token "
                        "in plain text.",
                    ) from exc

                keyring = keyrings.alt.file.PlaintextKeyring()
                try:
                    keyring.set_password("Maestral", accessor, token)
                except Exception as fallback_exc:
                    raise KeyringAccessError(
                        "Could not save auth token", str(fallback_exc)
                    ) from fallback_exc
            except (KeyringLocked, InitError) as exc:
                name = getattr(keyring, "name", keyring.__class__.__name__)
                raise KeyringAccessError(
                    "Could not save auth token",
                    f"{name} is locked or unavailable. Please try again.",
                ) from exc
            except Exception as exc:
                raise KeyringAccessError("Could not save auth token", str(exc)) from exc

            self.set_keyring_backend(keyring)

            self._conf.set("auth", "account_id", account_id)

            self._token = token
            self._loaded = True

            if isinstance(keyring, keyrings.alt.file.PlaintextKeyring):
                output.warn("No keyring found, credentials stored in plain text")

            output.ok("Credentials written")

    def delete_creds(
        self,
        account_id: str | None = None,
        keyring_name: str | None = None,
    ) -> None:
        """
        Deletes auth token from system keyring.

        :param account_id: Account ID to delete. Defaults to the configured account.
        :param keyring_name: Configured keyring class to use for reset recovery.
        :raises KeyringAccessError: if the system keyring is locked or otherwise cannot
            be accessed (for example if the app bundle signature has been invalidated).
        """
        with self._lock:
            configured_account_id = self.account_id
            target_account_id = account_id or configured_account_id
            ring = self.keyring
            if keyring_name and keyring_name != "automatic":
                current_name = (
                    f"{ring.__class__.__module__}.{ring.__class__.__name__}"
                    if ring
                    else ""
                )
                if current_name != keyring_name:
                    try:
                        ring = load_keyring(keyring_name)
                    except Exception as exc:
                        raise KeyringAccessError(
                            f"Cannot load keyring {keyring_name}",
                            "Unlock or restore the configured keyring and try again.",
                        ) from exc

            if ring and target_account_id:
                accessor = self._get_accessor(target_account_id)

                try:
                    ring.delete_password("Maestral", accessor)
                except (KeyringLocked, InitError):
                    title = "Could not delete auth token"
                    msg = (
                        f"{ring.name} is locked. Please unlock the keyring "
                        "and try again."
                    )
                    error = KeyringAccessError(title, msg)
                    self._logger.error(title, exc_info=exc_info_tuple(error))
                    raise error
                except PasswordDeleteError as exc:
                    # password does not exist in keyring
                    self._logger.info(str(exc))
                except Exception as e:
                    title = "Could not delete auth token"
                    new_exc = KeyringAccessError(title, str(e))
                    self._logger.error(title, exc_info=exc_info_tuple(new_exc))
                    raise new_exc
                else:
                    output.ok("Credentials removed")

            if configured_account_id == target_account_id:
                with self._conf._lock:
                    snapshot = self._conf._configuration_snapshot()
                    save_generation = self._conf.save_generation
                    try:
                        self._conf.set("auth", "keyring", "automatic", save=False)
                        self._conf.set("auth", "account_id", "", save=False)
                        self._conf.set("auth", "token_access_type", "", save=False)
                        self._conf.save()
                    except BaseException:
                        if self._conf.save_committed_since(save_generation):
                            self._keyring = None
                            self._token = None
                            self._loaded = False
                        else:
                            self._conf._restore_configuration_snapshot(snapshot)
                        raise
                self._keyring = None
                self._token = None
                self._loaded = False

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(config={self._config_name!r})>"
