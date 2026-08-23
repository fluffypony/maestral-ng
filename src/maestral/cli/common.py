from __future__ import annotations

import functools
import sys
from typing import TYPE_CHECKING, Any, Callable, TypeVar

import click
from typing_extensions import ParamSpec

from ..constants import DEFAULT_CONFIG_NAME
from .core import ConfigName
from .output import warn
from .utils import get_term_size

if TYPE_CHECKING:
    from ..daemon import MaestralClient
    from ..main import Maestral


P = ParamSpec("P")
T = TypeVar("T")


def convert_api_errors(func: Callable[P, T]) -> Callable[P, T]:
    """
    Decorator that catches a MaestralApiError and prints a formatted error message to
    stdout before exiting. Calls ``sys.exit(1)`` after printing the error to stdout.
    """

    from ..exceptions import MaestralApiError

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return func(*args, **kwargs)
        except MaestralApiError as exc:
            warn(f"{exc.title}. {exc.message}")
            sys.exit(1)

    return wrapper


def check_for_fatal_errors(m: MaestralClient | Maestral) -> bool:
    """
    Checks the given Maestral instance for fatal errors such as revoked Dropbox access,
    deleted Dropbox folder etc. Prints a nice representation to the command line.

    :param m: Client for the Maestral daemon or an in-process Maestral instance.
    :returns: True in case of fatal errors, False otherwise.
    """

    import textwrap

    maestral_err_list = m.fatal_errors

    if len(maestral_err_list) > 0:
        size = get_term_size()

        err = maestral_err_list[0]
        wrapped_msg = textwrap.fill(err.message, width=size.columns)

        click.echo("")
        click.secho(err.title, fg="red")
        click.secho(wrapped_msg, fg="red")
        click.echo("")

        return True
    else:
        return False


config_option = click.option(
    "-c",
    "--config-name",
    default=DEFAULT_CONFIG_NAME,
    type=ConfigName(existing=False),
    is_eager=True,
    expose_value=True,
    help="Run command with the given configuration.",
)
existing_config_option = click.option(
    "-c",
    "--config-name",
    default=DEFAULT_CONFIG_NAME,
    type=ConfigName(),
    is_eager=True,
    expose_value=True,
    help="Run command with the given configuration.",
)


def inject_client(
    fallback: bool,
    existing_config: bool,
    *,
    select_provider: bool = False,
) -> Callable[[Callable[P, T]], Callable[P, Any]]:
    def decorator(f: Callable[P, T]) -> Callable[P, Any]:
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            from ..daemon import CommunicationError, MaestralClient

            ctx = click.get_current_context()

            config_name = ctx.params.pop("config_name", "maestral")
            kwargs.pop("config_name", None)

            requested_provider = None
            if select_provider:
                from ..providers.base import normalise_provider_name

                requested_provider = ctx.params.pop("provider", None)
                kwargs.pop("provider", None)
                if requested_provider is not None:
                    requested_provider = normalise_provider_name(requested_provider)

            try:
                client = ctx.with_resource(
                    MaestralClient(config_name, fallback=fallback)
                )
            except CommunicationError:
                click.echo("Maestral daemon is not running.")
                ctx.exit(1)
            else:
                if select_provider:
                    from ..exceptions import MaestralApiError

                    try:
                        client.set_provider(requested_provider or client.provider)
                    except MaestralApiError as exc:
                        warn(f"{exc.title}. {exc.message}")
                        ctx.exit(1)
                return ctx.invoke(f, client, *args, **kwargs)

        if existing_config:
            f = existing_config_option(f)
        else:
            f = config_option(f)

        return functools.update_wrapper(wrapper, f)

    return decorator
