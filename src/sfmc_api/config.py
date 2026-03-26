"""Configuration loading for the SFMC API client.

Configuration can be loaded from a JSON credentials file
(``~/.config/sfmc/credentials.json`` by default) or constructed
directly via :meth:`SFMCConfig.from_dict` / keyword arguments.

The JSON file uses the same schema as the Node.js reference
implementation's ``local.json``::

    {
        "host": "sfmc.example.com",
        "apiCredentials": {
            "clientId": "...",
            "secret": "..."
        },
        "tlsRejectUnauthorized": 0,
        "rootDownloadPath": "/tmp/downloads",
        "stompDebug": false
    }
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .exceptions import ConfigError

DEFAULT_CONFIG_PATH = Path.home() / ".config" / "sfmc" / "credentials.json"


@dataclass(frozen=True)
class SFMCConfig:
    """Immutable configuration for connecting to an SFMC server.

    Attributes:
        host: SFMC server hostname or IP address (no ``https://`` prefix).
        client_id: API credential — client identifier.
        secret: API credential — client secret.
        tls_verify: Whether to verify TLS certificates. Defaults to ``True``.
            The JSON config field ``tlsRejectUnauthorized`` uses the inverse
            convention: ``0`` means *do not verify* (maps to ``tls_verify=False``).
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
    def from_file(cls, path: Path | str | None = None) -> SFMCConfig:
        """Load configuration from a JSON credentials file.

        Args:
            path: Path to the credentials JSON file.  When *None*,
                defaults to ``~/.config/sfmc/credentials.json``.

        Returns:
            A new :class:`SFMCConfig` instance.

        Raises:
            ConfigError: If the file cannot be read or is missing
                required fields.
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

        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SFMCConfig:
        """Create configuration from a dictionary.

        Accepts the same key structure as the JSON credentials file.

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

        # tlsRejectUnauthorized: 0 means skip verification (tls_verify=False)
        tls_raw = data.get("tlsRejectUnauthorized", 1)
        tls_verify = bool(tls_raw)

        root_path_raw = data.get("rootDownloadPath")
        root_download_path = Path(root_path_raw) if root_path_raw else None

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
