"""Interactive command-line dialogs built on Click and Rich."""

from __future__ import annotations

import functools
import os
from typing import Callable, Sequence, TypeVar

import click
from rich.console import Console
from rich.text import Text
from typing_extensions import ParamSpec

P = ParamSpec("P")
T = TypeVar("T")


def _console() -> Console:
    """Return a console which follows the current standard output stream."""
    return Console(highlight=False, soft_wrap=True)


def _show_error(message: str) -> None:
    click.echo(click.style(message, fg="red"), err=True)


def _show_options(options: Sequence[str], hint: str | None = None) -> None:
    console = _console()

    if hint:
        console.print(hint, style="dim")

    for index, option in enumerate(options, start=1):
        line = Text()
        line.append(str(index), style="cyan")
        line.append(f"  {option}")
        console.print(line)


def exit_on_keyboard_interrupt(func: Callable[P, T]) -> Callable[P, T]:
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return func(*args, **kwargs)
        except (KeyboardInterrupt, click.Abort):
            raise SystemExit("Aborted") from None

    return wrapper


@exit_on_keyboard_interrupt
def prompt(
    message: str,
    validate: Callable[[str], bool] | None = None,
) -> str:
    while True:
        value = click.prompt(message, type=str, show_default=False)

        if validate is None or validate(value):
            return value

        _show_error(f"'{value}' is not allowed")


@exit_on_keyboard_interrupt
def confirm(message: str, default: bool | None = True) -> bool:
    return click.confirm(message, default=default)


@exit_on_keyboard_interrupt
def select(message: str, options: Sequence[str], hint: str | None = "") -> int:
    if not options:
        raise ValueError("Cannot select from an empty option list")

    _show_options(options, hint)
    choice = click.prompt(message, type=click.IntRange(1, len(options)))
    return choice - 1


def _parse_indices(value: str, option_count: int) -> list[int] | None:
    value = value.strip()

    if not value:
        return []

    if value.casefold() == "all":
        return list(range(option_count))

    try:
        indices = [int(part.strip()) - 1 for part in value.split(",")]
    except ValueError:
        return None

    if any(index < 0 or index >= option_count for index in indices):
        return None

    return list(dict.fromkeys(indices))


@exit_on_keyboard_interrupt
def select_multiple(
    message: str, options: Sequence[str], hint: str | None = None
) -> list[int]:
    if not options:
        return []

    hint = hint or "Enter comma-separated numbers, 'all', or leave empty for none."
    _show_options(options, hint)

    while True:
        value = click.prompt(message, default="", show_default=False, type=str)
        indices = _parse_indices(value, len(options))

        if indices is not None:
            return indices

        _show_error(f"Choose numbers from 1 to {len(options)}")


@exit_on_keyboard_interrupt
def select_path(
    message: str,
    default: str | None = None,
    validate: Callable[[str], bool] = lambda value: True,
    exists: bool = False,
    files_allowed: bool = True,
    dirs_allowed: bool = True,
) -> str:
    while True:
        value = click.prompt(
            message,
            default=default,
            show_default=default is not None,
            type=str,
        ).strip()

        full_path = os.path.expanduser(value)
        forbidden_dir = os.path.isdir(full_path) and not dirs_allowed
        forbidden_file = os.path.isfile(full_path) and not files_allowed

        if exists and not os.path.exists(full_path):
            _show_error(f"'{value}' does not exist")
        elif forbidden_dir:
            _show_error(f"'{value}' is not a file")
        elif forbidden_file:
            _show_error(f"'{value}' is not a folder")
        elif not validate(value):
            _show_error(f"'{value}' is not allowed")
        else:
            return value
