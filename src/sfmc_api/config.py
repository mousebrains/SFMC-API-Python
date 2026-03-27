"""Configuration loading for the SFMC API client.

Configuration can be loaded from a JSON credentials file
(``~/.config/sfmc/credentials.json`` by default) or constructed
directly via :meth:`SFMCConfig.from_dict` / keyword arguments.

The credentials file uses **per-host** entries, keyed by hostname::

    {
        "gliderfmc1.ceoas.oregonstate.edu": {
            "apiCredentials": {
                "clientId": "...",
                "secret": "..."
            },
            "tlsRejectUnauthorized": 0,
            "rootDownloadPath": "/tmp/downloads",
            "stompDebug": false
        },
        "sfmc-backup.example.com": {
            "apiCredentials": {
                "clientId": "...",
                "secret": "..."
            }
        }
    }
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .exceptions import ConfigError

__all__ = ["DEFAULT_CONFIG_PATH", "SFMCConfig"]

DEFAULT_CONFIG_PATH = Path.home() / ".config" / "sfmc" / "credentials.json"


@dataclass(frozen=True)
class SFMCConfig:
    """Immutable configuration for connecting to an SFMC server.

    Attributes:
        host: SFMC server hostname or IP address (no ``https://`` prefix).
        client_id: API credential — client identifier.
        secret: API credential — client secret.
        tls_verify: Whether to verify TLS certificates. Defaults to ``True``.
            The JSON config field ``tlsRejectUnauthorized`` follows the
            Node.js convention: only the value ``0`` disables verification.
            All other values (including ``false``, ``true``, absent) keep
            verification enabled.
        root_download_path: Local directory for file downloads. Optional.
        stomp_debug: Enable verbose STOMP protocol logging. Defaults to ``False``.
    """

    host: str
    client_id: str
    secret: str
    tls_verify: bool = True
    root_download_path: Path | None = None
    stomp_debug: bool = False

    @classmethod
    def from_file(
        cls,
        path: Path | str | None = None,
        host: str | None = None,
    ) -> SFMCConfig:
        """Load configuration from a JSON credentials file.

        Top-level keys are hostnames, each mapping to a per-host
        config dict.  Use *host* to select which entry to load.
        If *host* is ``None`` and the file contains exactly one
        host, that host is used automatically.

        Args:
            path: Path to the credentials JSON file.  When *None*,
                defaults to ``~/.config/sfmc/credentials.json``.
            host: Hostname to look up in a multi-host credentials
                file.  Optional when only one host is defined.

        Returns:
            A new :class:`SFMCConfig` instance.

        Raises:
            ConfigError: If the file cannot be read, is missing
                required fields, or *host* is ambiguous.
        """
        path = Path(path) if path is not None else DEFAULT_CONFIG_PATH

        try:
            text = path.read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            raise ConfigError(f"Config file not found: {path}") from exc
        except OSError as exc:
            raise ConfigError(f"Cannot read config file {path}: {exc}") from exc

        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"Invalid JSON in {path}: {exc}") from exc

        if not isinstance(data, dict):
            raise ConfigError(f"Expected JSON object in {path}")

        # Multi-host format: top-level keys are hostnames.
        return cls._from_multi_host(data, host, path)

    @classmethod
    def _from_multi_host(
        cls,
        data: dict[str, Any],
        host: str | None,
        path: Path,
    ) -> SFMCConfig:
        """Resolve a multi-host credentials file to a single SFMCConfig."""
        if not data:
            raise ConfigError(f"Credentials file is empty: {path}")

        if host is not None:
            if host not in data:
                available = ", ".join(sorted(data.keys()))
                raise ConfigError(
                    f"Host {host!r} not found in {path}. Available hosts: {available}"
                )
            host_data = data[host]
        elif len(data) == 1:
            host = next(iter(data))
            host_data = data[host]
        else:
            available = ", ".join(sorted(data.keys()))
            raise ConfigError(
                f"Multiple hosts in {path} — specify one with --host. Available: {available}"
            )

        if not isinstance(host_data, dict):
            raise ConfigError(
                f"Expected dict for host {host!r} in {path}, got {type(host_data).__name__}"
            )

        host_data_with_host: dict[str, Any] = {"host": host, **host_data}
        return cls.from_dict(host_data_with_host)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SFMCConfig:
        """Create configuration from a dictionary.

        Accepts the same key structure as a single-host credentials
        entry, with a ``"host"`` key and ``"apiCredentials"`` sub-dict.

        Args:
            data: Dictionary with keys ``host``,
                ``apiCredentials.clientId``, ``apiCredentials.secret``,
                and optionally ``tlsRejectUnauthorized``,
                ``rootDownloadPath``, ``stompDebug``.

        Returns:
            A new :class:`SFMCConfig` instance.

        Raises:
            ConfigError: If required keys are missing.
        """
        try:
            host = data["host"]
            creds = data["apiCredentials"]
            client_id = creds["clientId"]
            secret = creds["secret"]
        except (KeyError, TypeError) as exc:
            raise ConfigError(
                f"Missing required config key: {exc}. "
                "Expected 'host', 'apiCredentials.clientId', "
                "and 'apiCredentials.secret'."
            ) from exc

        # tlsRejectUnauthorized follows the Node.js convention:
        # only the integer 0 (or string "0") disables TLS verification.
        # All other values — including false, true, 1, absent — enable it.
        # This matches Node's process.env["NODE_TLS_REJECT_UNAUTHORIZED"]
        # where only the string "0" skips verification.
        tls_raw = data.get("tlsRejectUnauthorized", 1)
        tls_verify = str(tls_raw) != "0"

        root_path_raw = data.get("rootDownloadPath")
        root_download_path = Path(root_path_raw).expanduser() if root_path_raw else None

        stomp_debug = bool(data.get("stompDebug", False))

        return cls(
            host=host,
            client_id=client_id,
            secret=secret,
            tls_verify=tls_verify,
            root_download_path=root_download_path,
            stomp_debug=stomp_debug,
        )

    @property
    def base_url(self) -> str:
        """Full base URL for the SFMC API.

        Example:
            ``"https://gliderfmc1.example.com/sfmc/api"``
        """
        return f"https://{self.host}/sfmc/api"
