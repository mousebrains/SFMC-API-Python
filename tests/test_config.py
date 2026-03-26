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


VALID_DICT = {
    "host": "sfmc.example.com",
    "apiCredentials": {"clientId": "cid", "secret": "s3cret"},
}

VALID_FILE = {
    "sfmc.example.com": {
        "apiCredentials": {"clientId": "cid", "secret": "s3cret"},
    },
}


class TestFromFile:
    def test_load_minimal(self, tmp_path: Path) -> None:
        p = _write_config(tmp_path, VALID_FILE)
        cfg = SFMCConfig.from_file(p)
        assert cfg.host == "sfmc.example.com"
        assert cfg.client_id == "cid"
        assert cfg.secret == "s3cret"
        assert cfg.tls_verify is True
        assert cfg.root_download_path is None
        assert cfg.stomp_debug is False

    def test_load_full(self, tmp_path: Path) -> None:
        data = {
            "sfmc.example.com": {
                "apiCredentials": {"clientId": "cid", "secret": "s3cret"},
                "tlsRejectUnauthorized": 0,
                "rootDownloadPath": "/tmp/dl",
                "stompDebug": True,
            },
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

    def test_missing_credentials_in_host_entry(self, tmp_path: Path) -> None:
        data = {"myhost.com": {"tlsRejectUnauthorized": 0}}
        with pytest.raises(ConfigError, match="Missing required"):
            SFMCConfig.from_file(_write_config(tmp_path, data))

    def test_unreadable_file(self, tmp_path: Path) -> None:
        p = tmp_path / "noperm.json"
        p.write_text("{}")
        p.chmod(0o000)
        try:
            with pytest.raises(ConfigError, match="Cannot read"):
                SFMCConfig.from_file(p)
        finally:
            p.chmod(0o644)  # restore for cleanup

    def test_non_dict_json(self, tmp_path: Path) -> None:
        p = tmp_path / "array.json"
        p.write_text('["not", "a", "dict"]')
        with pytest.raises(ConfigError, match="Expected JSON object"):
            SFMCConfig.from_file(p)

    def test_host_entry_not_dict(self, tmp_path: Path) -> None:
        p = tmp_path / "bad_host.json"
        p.write_text('{"myhost.com": "not-a-dict"}')
        with pytest.raises(ConfigError, match="Expected dict for host"):
            SFMCConfig.from_file(p)


class TestFromDict:
    def test_valid(self) -> None:
        cfg = SFMCConfig.from_dict(VALID_DICT)
        assert cfg.host == "sfmc.example.com"
        assert cfg.client_id == "cid"

    def test_missing_key(self) -> None:
        with pytest.raises(ConfigError):
            SFMCConfig.from_dict({"host": "h"})

    def test_missing_client_id(self) -> None:
        data = {"host": "h", "apiCredentials": {"secret": "s"}}
        with pytest.raises(ConfigError, match="Missing required"):
            SFMCConfig.from_dict(data)

    def test_empty_credentials_dict(self) -> None:
        data = {"host": "h", "apiCredentials": {}}
        with pytest.raises(ConfigError, match="Missing required"):
            SFMCConfig.from_dict(data)


class TestTlsVerify:
    """Test tlsRejectUnauthorized → tls_verify conversion."""

    def test_int_0_means_no_verify(self) -> None:
        cfg = SFMCConfig.from_dict({**VALID_DICT, "tlsRejectUnauthorized": 0})
        assert cfg.tls_verify is False

    def test_int_1_means_verify(self) -> None:
        cfg = SFMCConfig.from_dict({**VALID_DICT, "tlsRejectUnauthorized": 1})
        assert cfg.tls_verify is True

    def test_string_0_means_no_verify(self) -> None:
        cfg = SFMCConfig.from_dict({**VALID_DICT, "tlsRejectUnauthorized": "0"})
        assert cfg.tls_verify is False

    def test_string_false_means_no_verify(self) -> None:
        cfg = SFMCConfig.from_dict({**VALID_DICT, "tlsRejectUnauthorized": "false"})
        assert cfg.tls_verify is False

    def test_string_1_means_verify(self) -> None:
        cfg = SFMCConfig.from_dict({**VALID_DICT, "tlsRejectUnauthorized": "1"})
        assert cfg.tls_verify is True

    def test_absent_defaults_to_verify(self) -> None:
        cfg = SFMCConfig.from_dict(VALID_DICT)
        assert cfg.tls_verify is True

    def test_none_means_no_verify(self) -> None:
        cfg = SFMCConfig.from_dict({**VALID_DICT, "tlsRejectUnauthorized": None})
        assert cfg.tls_verify is False


class TestBaseUrl:
    def test_base_url(self) -> None:
        cfg = SFMCConfig(host="my.server.com", client_id="c", secret="s")
        assert cfg.base_url == "https://my.server.com/sfmc/api"


class TestImmutable:
    def test_frozen(self) -> None:
        cfg = SFMCConfig(host="h", client_id="c", secret="s")
        with pytest.raises(AttributeError):
            cfg.host = "other"  # type: ignore[misc]


class TestRootDownloadPath:
    def test_path_converted(self) -> None:
        cfg = SFMCConfig.from_dict({**VALID_DICT, "rootDownloadPath": "/tmp/dl"})
        assert cfg.root_download_path == Path("/tmp/dl")
        assert isinstance(cfg.root_download_path, Path)

    def test_null_means_none(self) -> None:
        cfg = SFMCConfig.from_dict({**VALID_DICT, "rootDownloadPath": None})
        assert cfg.root_download_path is None

    def test_empty_string_means_none(self) -> None:
        cfg = SFMCConfig.from_dict({**VALID_DICT, "rootDownloadPath": ""})
        assert cfg.root_download_path is None


MULTI_HOST_CONFIG = {
    "host-a.example.com": {
        "apiCredentials": {"clientId": "a_id", "secret": "a_sec"},
    },
    "host-b.example.com": {
        "apiCredentials": {"clientId": "b_id", "secret": "b_sec"},
        "tlsRejectUnauthorized": 0,
    },
}


class TestMultiHost:
    def test_select_by_host(self, tmp_path: Path) -> None:
        p = _write_config(tmp_path, MULTI_HOST_CONFIG)
        cfg = SFMCConfig.from_file(p, host="host-a.example.com")
        assert cfg.host == "host-a.example.com"
        assert cfg.client_id == "a_id"

    def test_select_second_host(self, tmp_path: Path) -> None:
        p = _write_config(tmp_path, MULTI_HOST_CONFIG)
        cfg = SFMCConfig.from_file(p, host="host-b.example.com")
        assert cfg.host == "host-b.example.com"
        assert cfg.client_id == "b_id"
        assert cfg.tls_verify is False

    def test_single_host_auto_selects(self, tmp_path: Path) -> None:
        single = {"only.example.com": {"apiCredentials": {"clientId": "c", "secret": "s"}}}
        p = _write_config(tmp_path, single)
        cfg = SFMCConfig.from_file(p)
        assert cfg.host == "only.example.com"

    def test_multi_host_without_selection_errors(self, tmp_path: Path) -> None:
        p = _write_config(tmp_path, MULTI_HOST_CONFIG)
        with pytest.raises(ConfigError, match=r"Multiple hosts.*specify one with --host"):
            SFMCConfig.from_file(p)

    def test_unknown_host_errors(self, tmp_path: Path) -> None:
        p = _write_config(tmp_path, MULTI_HOST_CONFIG)
        with pytest.raises(ConfigError, match="not found"):
            SFMCConfig.from_file(p, host="nonexistent.example.com")

    def test_empty_file_errors(self, tmp_path: Path) -> None:
        p = _write_config(tmp_path, {})
        with pytest.raises(ConfigError, match="empty"):
            SFMCConfig.from_file(p)
