from __future__ import annotations

from unittest.mock import MagicMock, Mock

from click.testing import CliRunner

import maestral.cli.cli_core as cli_core_module
import maestral.cli.cli_encryption as cli_encryption_module
import maestral.daemon as daemon_module
from maestral.cli import main
from maestral.config import MaestralConfig
from maestral.daemon import Start


def test_encryption_configure_uses_a_safe_default_path(
    config_name: str, monkeypatch
) -> None:
    client = _mock_client(monkeypatch)

    result = CliRunner().invoke(
        main,
        ["encryption", "configure", "-c", config_name],
    )

    assert result.exit_code == 0, result.output
    client.configure_encryption.assert_called_once_with("/Maestral Vault", None)
    assert "/Maestral Vault" in result.output


def test_encryption_configure_rejects_the_remote_root(
    config_name: str, monkeypatch
) -> None:
    client = _mock_client(monkeypatch)

    result = CliRunner().invoke(
        main,
        ["encryption", "configure", "/", "-c", config_name],
    )

    assert result.exit_code == 2
    assert "non-root absolute remote path" in result.output
    client.configure_encryption.assert_not_called()


def test_encryption_create_uses_only_a_hidden_confirmed_prompt(
    config_name: str, monkeypatch
) -> None:
    _ensure_config(config_name)
    client = _mock_client(monkeypatch)

    result = CliRunner().invoke(
        main,
        ["encryption", "create", "-c", config_name],
        input="first-secret\nwrong-secret\nfinal-secret\nfinal-secret\n",
        env={"MAESTRAL_VAULT_PASSWORD": "environment-secret"},
    )

    assert result.exit_code == 0, result.output
    assert "entered values do not match" in result.output
    assert "secret" not in result.output
    client.initialise_encrypted_vault.assert_called_once_with("final-secret")


def test_encryption_attach_uses_one_hidden_prompt(
    config_name: str, monkeypatch
) -> None:
    _ensure_config(config_name)
    client = _mock_client(monkeypatch)

    result = CliRunner().invoke(
        main,
        ["encryption", "attach", "-c", config_name],
        input="existing-secret\n",
    )

    assert result.exit_code == 0, result.output
    assert "existing-secret" not in result.output
    assert "Repeat for confirmation" not in result.output
    client.attach_encrypted_vault.assert_called_once_with("existing-secret")


def test_encryption_commands_do_not_accept_a_password_option() -> None:
    runner = CliRunner()

    for command in ("create", "attach"):
        result = runner.invoke(main, ["encryption", command, "--help"])

        assert result.exit_code == 0, result.output
        assert "--password" not in result.output
        assert "PASSWORD" not in result.output


def test_encryption_runtime_commands_call_the_public_api(
    config_name: str, monkeypatch
) -> None:
    _ensure_config(config_name)
    client = _mock_client(monkeypatch)

    for command, method_name in [
        ("unlock", "unlock_encrypted_vault"),
        ("lock", "lock_encrypted_vault"),
        ("disable", "disable_encryption"),
    ]:
        result = CliRunner().invoke(
            main,
            ["encryption", command, "-c", config_name],
        )
        assert result.exit_code == 0, result.output
        getattr(client, method_name).assert_called_once_with()


def test_encryption_status_reports_paths_and_state(
    config_name: str, monkeypatch
) -> None:
    _ensure_config(config_name)
    client = _mock_client(monkeypatch)
    client.encryption_enabled = True
    client.encryption_vault_ready = True
    client.encryption_vault_open = False
    client.encryption_vault_path = "/Maestral Vault"
    client.encryption_cache_path = "/private/ciphertext"

    result = CliRunner().invoke(
        main,
        ["encryption", "status", "-c", config_name],
    )

    assert result.exit_code == 0, result.output
    assert "Enabled:           yes" in result.output
    assert "Vault ready:       yes" in result.output
    assert "Vault open:        no" in result.output
    assert "Remote vault path: /Maestral Vault" in result.output
    assert "Ciphertext cache:  /private/ciphertext" in result.output


def test_setup_dialog_configures_protection_before_oauth(
    config_name: str, monkeypatch
) -> None:
    order: list[str] = []
    client = MagicMock()
    client.provider = "dropbox"
    client.pending_link = True
    client.pending_dropbox_folder = True
    client.encryption_enabled = True
    client.encryption_vault_ready = False
    client.encryption_vault_open = True
    client.set_provider.side_effect = lambda _provider: order.append("provider")
    client.create_dropbox_directory.side_effect = lambda _path: order.append(
        "local-folder"
    )
    client.start_sync.side_effect = lambda: order.append("sync")

    monkeypatch.setattr(daemon_module, "is_running", Mock(return_value=False))
    monkeypatch.setattr(
        daemon_module,
        "start_maestral_daemon_process",
        Mock(return_value=Start.Ok),
    )
    monkeypatch.setattr(daemon_module, "wait_for_startup", Mock())
    monkeypatch.setattr(daemon_module, "MaestralClient", Mock(return_value=client))
    monkeypatch.setattr(
        cli_core_module,
        "configure_encryption_dialog",
        Mock(side_effect=lambda _client: order.append("protection")),
    )
    monkeypatch.setattr(
        cli_core_module,
        "link_dialog",
        Mock(side_effect=lambda _client: order.append("oauth")),
    )
    monkeypatch.setattr(
        cli_core_module,
        "setup_encrypted_vault_dialog",
        Mock(side_effect=lambda _client: order.append("vault")),
    )
    monkeypatch.setattr(
        cli_core_module,
        "select_dbx_path_dialog",
        Mock(return_value="/tmp/Maestral-test"),
    )
    monkeypatch.setattr(cli_core_module, "confirm", Mock(return_value=True))

    result = CliRunner().invoke(main, ["start", "-c", config_name])

    assert result.exit_code == 0, result.output
    assert order == [
        "provider",
        "protection",
        "oauth",
        "vault",
        "local-folder",
        "sync",
    ]


def test_protection_dialog_selects_standard_or_cryptomator(monkeypatch) -> None:
    client = Mock()
    select = Mock(side_effect=[0, 1])
    monkeypatch.setattr(cli_encryption_module, "select", select)

    cli_encryption_module.configure_encryption_dialog(client)
    cli_encryption_module.configure_encryption_dialog(client)

    client.disable_encryption.assert_called_once_with()
    client.configure_encryption.assert_called_once_with("/Maestral Vault")


def test_vault_dialog_confirms_only_a_new_password(monkeypatch) -> None:
    client = Mock()
    select = Mock(side_effect=[0, 1])
    password = Mock(side_effect=["new-secret", "existing-secret"])
    monkeypatch.setattr(cli_encryption_module, "select", select)
    monkeypatch.setattr(cli_encryption_module, "prompt_password", password)

    cli_encryption_module.setup_encrypted_vault_dialog(client)
    cli_encryption_module.setup_encrypted_vault_dialog(client)

    assert password.call_args_list == [
        (("Vault password",), {"confirmation": True}),
        (("Vault password",), {}),
    ]
    client.initialise_encrypted_vault.assert_called_once_with("new-secret")
    client.attach_encrypted_vault.assert_called_once_with("existing-secret")


def _mock_client(monkeypatch) -> MagicMock:
    client = MagicMock()
    client.__enter__.return_value = client
    monkeypatch.setattr(daemon_module, "MaestralClient", Mock(return_value=client))
    return client


def _ensure_config(config_name: str) -> None:
    MaestralConfig(config_name).set("auth", "provider", "dropbox")
