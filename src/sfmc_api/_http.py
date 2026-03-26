"""Internal HTTP transport helpers.

This module is **not** part of the public API.  It provides a thin
wrapper around :mod:`httpx` for building configured HTTP clients and
translating non-success responses into typed exceptions.
"""

from __future__ import annotations

import logging

import httpx

from .config import SFMCConfig
from .exceptions import APIError, RateLimitError

logger = logging.getLogger(__name__)

__all__: list[str] = []  # internal module — no public exports


def build_http_client(config: SFMCConfig) -> httpx.Client:
    """Create an :class:`httpx.Client` pre-configured for an SFMC server.

    The client is set up with:

    * ``base_url`` pointing at the SFMC API root
      (e.g. ``https://host/sfmc/api``).
    * TLS verification according to :attr:`SFMCConfig.tls_verify`.
    * A 30-second read timeout and 10-second connect timeout.

    The caller is responsible for closing the client when done.
    """
    if not config.tls_verify:
        logger.warning(
            "TLS certificate verification is DISABLED for %s. "
            "This is insecure and should only be used for testing.",
            config.host,
        )
    return httpx.Client(
        base_url=config.base_url,
        verify=config.tls_verify,
        timeout=httpx.Timeout(30.0, connect=10.0),
    )


def check_response(response: httpx.Response) -> None:
    """Raise an appropriate exception for non-success HTTP responses.

    ===== ======================================
    Code  Behaviour
    ===== ======================================
    2xx   Returns without raising.
    429   Raises :class:`RateLimitError` with the
          retry delay from the
          ``x-rate-limit-retry-after-milliseconds``
          response header.
    other Raises :class:`APIError` with the status
          code and response body.
    ===== ======================================
    """
    if response.is_success:
        return

    if response.status_code == 429:
        ms = response.headers.get("x-rate-limit-retry-after-milliseconds", "0")
        try:
            retry_seconds = int(ms) / 1000
        except (ValueError, TypeError):
            retry_seconds = 0.0
        raise RateLimitError(retry_after_seconds=retry_seconds)

    raise APIError(response.status_code, response.text)
