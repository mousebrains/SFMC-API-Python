"""Authentication for the SFMC REST API.

The SFMC API uses a simple token-based scheme:

1. POST ``/sfmc/api/signin`` with ``{"clientId": "...", "secret": "..."}``
2. Receive ``{"token": "..."}`` on success.
3. Include ``Authorization: Bearer <token>`` on all subsequent requests.

See :doc:`/docs/authentication` for a detailed data-flow description.
"""

from __future__ import annotations

import httpx

from ._http import check_response
from .config import SFMCConfig
from .exceptions import AuthenticationError


def authenticate(http_client: httpx.Client, config: SFMCConfig) -> str:
    """Sign in to the SFMC API and return the bearer token.

    Args:
        http_client: An :class:`httpx.Client` whose *base_url* is already
            set to the SFMC API root (``https://host/sfmc/api``).
        config: SFMC configuration providing *client_id* and *secret*.

    Returns:
        The bearer-token string to use in ``Authorization`` headers.

    Raises:
        AuthenticationError: On any failure — bad credentials, network
            error, unexpected response shape, etc.
    """
    try:
        response = http_client.post(
            "/signin",
            json={"clientId": config.client_id, "secret": config.secret},
        )
        check_response(response)
        data = response.json()
        return str(data["token"])
    except KeyError as exc:
        raise AuthenticationError(
            f"Unexpected sign-in response (missing 'token' key): {exc}"
        ) from exc
    except httpx.HTTPError as exc:
        raise AuthenticationError(f"Authentication failed: {exc}") from exc
