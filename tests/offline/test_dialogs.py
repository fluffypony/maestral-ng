import click
from click.testing import CliRunner

from maestral.cli.dialogs import select, select_multiple, select_path


def test_select_returns_zero_based_index() -> None:
    @click.command()
    def command() -> None:
        click.echo(select("Choose", ["First", "Second"]))

    result = CliRunner().invoke(command, input="2\n")

    assert result.exit_code == 0
    assert result.output.endswith("1\n")


def test_select_multiple_reprompts_for_invalid_input() -> None:
    @click.command()
    def command() -> None:
        click.echo(select_multiple("Choose", ["First", "Second", "Third"]))

    result = CliRunner().invoke(command, input="4\n1, 3, 1\n")

    assert result.exit_code == 0
    assert "Choose numbers from 1 to 3" in result.output
    assert result.output.endswith("[0, 2]\n")


def test_select_multiple_accepts_empty_input() -> None:
    @click.command()
    def command() -> None:
        click.echo(select_multiple("Choose", ["First"]))

    result = CliRunner().invoke(command, input="\n")

    assert result.exit_code == 0
    assert result.output.endswith("[]\n")


def test_select_path_reprompts_until_path_is_valid(tmp_path) -> None:
    folder = tmp_path / "folder"
    folder.mkdir()

    @click.command()
    def command() -> None:
        click.echo(
            select_path(
                "Folder",
                exists=True,
                files_allowed=False,
                dirs_allowed=True,
            )
        )

    result = CliRunner().invoke(command, input=f"{tmp_path / 'missing'}\n{folder}\n")

    assert result.exit_code == 0
    assert "does not exist" in result.output
    assert result.output.endswith(f"{folder}\n")
