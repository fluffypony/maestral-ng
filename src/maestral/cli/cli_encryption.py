from __future__ import annotations

from typing import TYPE_CHECKING

import click

from .common import convert_api_errors, inject_client
from .core import DropboxPath
from .dialogs import prompt_password, select
from .output import echo, info, ok

if TYPE_CHECKING:
    from ..daemon import MaestralClient
    from ..main import Maestral


DEFAULT_VAULT_PATH = "/Maestral Vault"


class VaultPath(DropboxPath):
    """A non-root absolute path for a remote Cryptomator vault."""

    name = "vault path"

    def convert(
        self,
        value: str | None,
        param: click.Parameter | None,
        ctx: click.Context | None,
    ) -> str | None:
        converted = super().convert(value, param, ctx)
        if converted is None:
            return None

        parts = converted.split("/")[1:]
        invalid = (
            converted == "/"
            or "\\" in converted
            or any(part in {"", ".", ".."} for part in parts)
            or any(character < " " or character == "\x7f" for character in converted)
        )
        if invalid:
            self.fail(
                "use a non-root absolute remote path such as /Maestral Vault",
                param,
                ctx,
            )
        return converted


def configure_encryption_dialog(m: MaestralClient | Maestral) -> None:
    """Choose remote file protection before account authorization."""
    choice = select(
        "How should Maestral store remote files?",
        options=[
            "Standard remote storage",
            "Cryptomator encryption",
        ],
        hint="Local files stay plaintext in both modes.",
    )

    if choice == 0:
        m.disable_encryption()
        return

    m.configure_encryption(DEFAULT_VAULT_PATH)
    info(f"Cryptomator vault path: {DEFAULT_VAULT_PATH}")


def setup_encrypted_vault_dialog(m: MaestralClient | Maestral) -> None:
    """Create or attach the configured vault with a hidden password prompt."""
    info(
        "Local files stay plaintext. The vault password is stored only in your "
        "system keyring."
    )
    choice = select(
        "How should Maestral set up the Cryptomator vault?",
        options=[
            "Create a new format-8 vault",
            "Attach an existing format-8 vault",
        ],
    )

    if choice == 0:
        password = prompt_password("Vault password", confirmation=True)
        try:
            m.initialise_encrypted_vault(password)
        finally:
            del password
        ok("Created and opened the encrypted vault.")
    else:
        password = prompt_password("Vault password")
        try:
            m.attach_encrypted_vault(password)
        finally:
            del password
        ok("Attached and opened the encrypted vault.")


@click.group(help="Configure and control remote Cryptomator encryption.")
def encryption() -> None:
    pass


@encryption.command(name="configure", help="Configure a remote Cryptomator vault.")
@click.argument(
    "remote_path",
    type=VaultPath(file_okay=False),
    required=False,
    default=DEFAULT_VAULT_PATH,
)
@click.option(
    "--cache-path",
    type=click.Path(file_okay=False, path_type=str, resolve_path=True),
    help="Use this private local ciphertext cache directory.",
)
@inject_client(fallback=True, existing_config=False)
@convert_api_errors
def encryption_configure(
    m: Maestral,
    remote_path: str,
    cache_path: str | None,
) -> None:
    m.configure_encryption(remote_path, cache_path)
    ok(f"Cryptomator encryption configured at {remote_path}.")


@encryption.command(
    name="create", help="Create and open the configured format-8 vault."
)
@inject_client(fallback=True, existing_config=True)
@convert_api_errors
def encryption_create(m: Maestral) -> None:
    info(
        "Local files stay plaintext. The vault password is stored only in your "
        "system keyring."
    )
    password = prompt_password("Vault password", confirmation=True)
    try:
        m.initialise_encrypted_vault(password)
    finally:
        del password
    ok("Created and opened the encrypted vault.")


@encryption.command(name="attach", help="Attach and open a format-8 vault.")
@inject_client(fallback=True, existing_config=True)
@convert_api_errors
def encryption_attach(m: Maestral) -> None:
    info(
        "Local files stay plaintext. The vault password is stored only in your "
        "system keyring."
    )
    password = prompt_password("Vault password")
    try:
        m.attach_encrypted_vault(password)
    finally:
        del password
    ok("Attached and opened the encrypted vault.")


@encryption.command(name="unlock", help="Open the vault with its keyring password.")
@inject_client(fallback=True, existing_config=True)
@convert_api_errors
def encryption_unlock(m: Maestral) -> None:
    m.unlock_encrypted_vault()
    ok("Encrypted vault unlocked.")


@encryption.command(
    name="lock", help="Close the vault and clear its in-memory password."
)
@inject_client(fallback=True, existing_config=True)
@convert_api_errors
def encryption_lock(m: Maestral) -> None:
    m.lock_encrypted_vault()
    ok("Encrypted vault locked.")


@encryption.command(name="disable", help="Disable remote encryption for this profile.")
@inject_client(fallback=True, existing_config=True)
@convert_api_errors
def encryption_disable(m: Maestral) -> None:
    m.disable_encryption()
    ok("Remote encryption disabled.")


@encryption.command(name="status", help="Show the encrypted vault state.")
@inject_client(fallback=True, existing_config=True)
@convert_api_errors
def encryption_status(m: Maestral) -> None:
    echo("")
    echo(f"Enabled:           {_yes_no(m.encryption_enabled)}")
    echo(f"Vault ready:       {_yes_no(m.encryption_vault_ready)}")
    echo(f"Vault open:        {_yes_no(m.encryption_vault_open)}")
    echo(f"Remote vault path: {m.encryption_vault_path or '-'}")
    echo(f"Ciphertext cache:  {m.encryption_cache_path or '-'}")
    echo("")


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"
