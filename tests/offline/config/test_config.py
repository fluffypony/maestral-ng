from unittest import mock

import pytest

import maestral.config as config_module
from maestral.config import MaestralConfig, MaestralState
from maestral.config.user import ConfigLoadError


def test_remove_configuration_does_not_remove_prefix_matches(
    monkeypatch, tmp_path
) -> None:
    for file_name in (
        "work.state",
        "work.db",
        "work.db-wal",
        "work2.state",
        "workshop.db",
    ):
        (tmp_path / file_name).touch()

    config = mock.Mock()
    state = mock.Mock()
    monkeypatch.setattr(config_module, "MaestralConfig", lambda name: config)
    monkeypatch.setattr(config_module, "MaestralState", lambda name: state)
    monkeypatch.setattr(config_module, "get_data_path", lambda name: str(tmp_path))

    config_module.remove_configuration("work")

    config.cleanup.assert_called_once_with()
    state.cleanup.assert_called_once_with()
    assert not (tmp_path / "work.state").exists()
    assert not (tmp_path / "work.db").exists()
    assert not (tmp_path / "work.db-wal").exists()
    assert (tmp_path / "work2.state").exists()
    assert (tmp_path / "workshop.db").exists()


def test_missing_state_fails_closed_for_linked_profile(config_name: str) -> None:
    config = MaestralConfig(config_name)
    config.set("auth", "account_id", "linked-account")

    with pytest.raises(ConfigLoadError, match="Cannot safely recreate"):
        MaestralState(config_name)
