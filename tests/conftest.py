"""Shared test fixtures."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from sfmc_api.config import SFMCConfig


@pytest.fixture()
def config() -> SFMCConfig:
    """A minimal SFMCConfig for testing."""
    return SFMCConfig(host="sfmc.test", client_id="cid", secret="sec", tls_verify=False)


def make_mock_response(
    status: int = 200,
    json_data: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    text: str = "",
) -> MagicMock:
    """Create a mock httpx.Response with common fields."""
    r = MagicMock(spec=httpx.Response)
    r.status_code = status
    r.is_success = 200 <= status < 300
    r.json.return_value = json_data or {}
    r.headers = headers or {}
    r.text = text
    return r
