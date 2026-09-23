"""Authentication providers.

Credentials are never accepted as command-line arguments and never written to
snapshots, reports or logs. Supported sources:

* no auth (local Spark UI, History Server on a trusted network)
* bearer token read from an environment variable (default ``SPARKMEDIC_TOKEN``)
* HTTP basic auth read from ``SPARKMEDIC_USERNAME`` / ``SPARKMEDIC_PASSWORD``
* Databricks unified client authentication through the official Databricks SDK
  (OAuth U2M/M2M, Azure CLI / managed identity, config profiles, env vars, ...)
* the short-lived notebook token when running inside a Databricks notebook
"""

from __future__ import annotations

import base64
import os
from typing import Callable, Optional


class AuthProvider:
    """Returns the HTTP headers to attach to each request."""

    name = "none"

    def headers(self) -> dict[str, str]:  # pragma: no cover - trivial
        return {}

    def __repr__(self) -> str:  # never print secrets
        return f"<{type(self).__name__}>"


class NoAuth(AuthProvider):
    name = "none"


class BearerTokenAuth(AuthProvider):
    name = "bearer"

    def __init__(self, token_supplier: Callable[[], str]):
        self._supplier = token_supplier

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._supplier()}"}


class BasicAuth(AuthProvider):
    name = "basic"

    def __init__(self, username: str, password: str):
        self._value = base64.b64encode(f"{username}:{password}".encode()).decode()

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Basic {self._value}"}


class DatabricksSdkAuth(AuthProvider):
    """Uses ``databricks.sdk.core.Config`` so every Databricks auth method works."""

    name = "databricks-sdk"

    def __init__(self, profile: Optional[str] = None, host: Optional[str] = None):
        try:
            from databricks.sdk.core import Config
        except ImportError as exc:  # pragma: no cover - depends on env
            raise RuntimeError(
                "Databricks targets need the Databricks SDK: pip install 'sparkmedic[databricks]'"
            ) from exc
        kwargs = {}
        if profile:
            kwargs["profile"] = profile
        if host:
            kwargs["host"] = host
        self.config = Config(**kwargs)

    @property
    def host(self) -> str:
        return self.config.host.rstrip("/")

    def headers(self) -> dict[str, str]:
        return dict(self.config.authenticate())


def auth_from_env(
    token_env: str = "SPARKMEDIC_TOKEN",
    user_env: str = "SPARKMEDIC_USERNAME",
    password_env: str = "SPARKMEDIC_PASSWORD",
) -> AuthProvider:
    """Pick an auth provider for generic Spark REST endpoints from the environment."""
    token = os.environ.get(token_env)
    if token:
        return BearerTokenAuth(lambda: os.environ.get(token_env, token))
    user, password = os.environ.get(user_env), os.environ.get(password_env)
    if user and password:
        return BasicAuth(user, password)
    return NoAuth()
