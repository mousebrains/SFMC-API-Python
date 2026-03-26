"""Tests for sfmc_api.config."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sfmc_api.config import SFMCConfig
from sfmc_api.exceptions import ConfigError


def _write_config(tmp_path: Path, data: dict) -> Path:  # type: ignore[type-arg]
    p = tmp_path / "credentials.json"
    p.write_text(json.dumps(data))
    return p


VALID_CONFIG = {
    "host": "sfmc.example.com",
    "apiCredentials": {"clientId": "cid", "secret": "s3cret"},
}


class TestFromFile:
    def test_load_minimal(self, tmp_path: Path) -> None:
        p = _write_config(tmp_path, VALID_CONFIG)
        cfg = SFMCConfig.from_file(p)
        assert cfg.host == "sfmc.example.com"
        assert cfg.client_id == "cid"
        assert cfg.secret == "s3cret"
        assert cfg.tls_verify is True
        assert cfg.root_download_path is None
        assert cfg.stomp_debug is False

    def test_load_full(self, tmp_path: Path) -> None:
        data = {
            **VALID_CONFIG,
            "tlsRejectUnauthorized": 0,
            "rootDownloadPath": "/tmp/dl",
            "stompDebug": True,
        }
        cfg = SFMCConfig.from_file(_write_config(tmp_path, data))
        assert cfg.tls_verify is False
        assert cfg.root_download_path == Path("/tmp/dl")
        assert cfg.stomp_debug is True

    def test_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="not found"):
            SFMCConfig.from_file(tmp_path / "nope.json")

    def test_invalid_json(self, tmp_path: Path) -> None:
        p = tmp_path / "bad.json"
        p.write_text("{invalid")
        with pytest.raises(ConfigError, match="Invalid JSON"):
            SFMCConfig.from_file(p)

    def test_missing_host(self, tmp_path: Path) -> None:
        data = {"apiCredentials": {"clientId": "c", "secret": "s"}}
        with pytest.raises(ConfigError, match="Missing required"):
            SFMCConfig.from_file(_write_config(tmp_path, data))

    def test_missing_credentials(self, tmp_path: Path) -> None:
        data = {"host": "h"}
        with pytest.raises(ConfigError, match="Missing required"):
            SFMCConfig.from_file(_write_config(tmp_path, data))


class TestFromDict:
    def test_valid(self) -> None:
        cfg = SFMCConfig.from_dict(VALID_CONFIG)
        assert cfg.host == "sfmc.example.com"
        assert cfg.client_id == "cid"

    def test_missing_key(self) -> None:
        with pytest.raises(ConfigError):
            SFMCConfig.from_dict({"host": "h"})


class TestBaseUrl:
    def test_base_url(self) -> None:
        cfg = SFMCConfig(host="my.server.com", client_id="c", secret="s")
        assert cfg.base_url == "https://my.server.com/sfmc/api"


class TestImmutable:
    def test_frozen(self) -> None:
        cfg = SFMCConfig(host="h", client_id="c", secret="s")
        with pytest.raises(AttributeError):
            cfg.host = "other"  # type: ignore[misc]
