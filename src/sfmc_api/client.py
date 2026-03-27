"""High-level SFMC REST API client.

:class:`SFMCClient` is the primary public interface for this library.
It manages authentication transparently and exposes one Python method
per API operation.

Quick start::

    from sfmc_api import SFMCClient

    with SFMCClient() as client:
        details = client.get_glider_details("my-glider")
        print(details)

See :doc:`/docs/getting_started` for installation and configuration.
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any, cast

import httpx

from ._http import build_http_client, check_response
from .auth import authenticate
from .config import SFMCConfig
from .exceptions import APIError, AuthenticationError
from .stomp import StompConnection, StompSubscription

__all__ = ["SFMCClient"]


def _validate_path_segment(value: str, name: str) -> str:
    """Raise *ValueError* if *value* is unsuitable for a URL path segment."""
    if not value or "/" in value or "\x00" in value or ".." in value:
        raise ValueError(f"Invalid {name}: {value!r}")
    return value


class SFMCClient:
    """Client for the Slocum Fleet Management Center REST API.

    Handles authentication automatically on the first API call.  The
    bearer token obtained from ``POST /sfmc/api/signin`` is cached and
    reused for the lifetime of the client.

    **Construction** — supply credentials in one of three ways:

    1. *Default* — loads ``~/.config/sfmc/credentials.json``::

           client = SFMCClient()

    2. *Explicit path*::

           client = SFMCClient(config_path="/etc/sfmc/creds.json")

    3. *Pre-built config object*::

           cfg = SFMCConfig(host="sfmc.example.com",
                            client_id="id", secret="s3cret")
           client = SFMCClient(config=cfg)

    **Resource management** — use as a context manager to ensure the
    underlying connection pool is closed::

        with SFMCClient() as client:
            ...
    """

    def __init__(
        self,
        config: SFMCConfig | None = None,
        config_path: Path | str | None = None,
        host: str | None = None,
        download_path: Path | str | None = None,
    ) -> None:
        """Initialise the SFMC client.

        Args:
            config: A pre-built :class:`SFMCConfig`.  Takes precedence
                over *config_path* and *host* when provided.
            config_path: Path to a credentials JSON file.  Ignored when
                *config* is provided.  Defaults to
                ``~/.config/sfmc/credentials.json``.
            host: Hostname to select from a multi-host credentials
                file.  Ignored when *config* is provided.
            download_path: Default directory for file downloads.
                Overrides ``rootDownloadPath`` from the credentials
                file.  When *None*, uses the config value or the
                current working directory.
        """
        if config is not None:
            self._config = config
        else:
            self._config = SFMCConfig.from_file(config_path, host=host)

        self._download_path: Path | None = (
            Path(download_path).expanduser() if download_path else None
        )
        self._http: httpx.Client = build_http_client(self._config)
        self._token: str | None = None

    # ── Context manager ──────────────────────────────────────────────

    def __enter__(self) -> SFMCClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        self._http.close()

    @property
    def download_dir(self) -> Path:
        """Default directory for file downloads.

        Resolution order:

        1. *download_path* passed to the constructor.
        2. ``rootDownloadPath`` from the credentials file.
        3. The current working directory.

        The directory is created if it does not exist.
        """
        d = self._download_path or self._config.root_download_path or Path.cwd()
        d.mkdir(parents=True, exist_ok=True)
        return d

    # ── Authentication ───────────────────────────────────────────────

    def authenticate(self) -> None:
        """Explicitly sign in and cache the bearer token.

        This is called automatically before the first API request.
        Call it explicitly only if you want to verify credentials
        eagerly or refresh a token.

        Raises:
            AuthenticationError: If sign-in fails.
        """
        self._token = authenticate(self._http, self._config)

    def _ensure_auth(self) -> None:
        """Sign in lazily — only if no token is cached yet."""
        if self._token is None:
            self.authenticate()

    def _auth_headers(self) -> dict[str, str]:
        """Return an ``Authorization: Bearer ...`` header dict."""
        self._ensure_auth()
        return {"Authorization": f"Bearer {self._token}"}

    # ── Internal request helper ──────────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send an authenticated request to the SFMC API.

        All public API methods delegate to this helper, which:

        1. Ensures the client is authenticated.
        2. Attaches the ``Authorization`` header.
        3. Sends the request via :mod:`httpx`.
        4. Checks the response status and raises on errors.

        Args:
            method: HTTP method (``GET``, ``POST``, ``PUT``, ``DELETE``).
            path: URL path relative to the API base URL
                (e.g. ``"/v1/gliders/myglider"``).
            **kwargs: Passed through to
                :meth:`httpx.Client.request` — use for ``json``,
                ``params``, ``data``, ``content``, ``headers``, etc.

        Returns:
            The :class:`httpx.Response` (status already verified).

        Raises:
            AuthenticationError: If sign-in has not been done and fails.
            RateLimitError: If the server returns HTTP 429.
            APIError: For other non-2xx responses or transport errors.
        """
        headers = kwargs.pop("headers", {})
        headers.update(self._auth_headers())
        try:
            response = self._http.request(method, path, headers=headers, **kwargs)
        except httpx.HTTPError as exc:
            raise APIError(0, str(exc)) from exc
        check_response(response)
        return response

    @staticmethod
    def _json_or_empty(response: httpx.Response) -> dict[str, Any]:
        """Parse a JSON response body, returning ``{}`` when the body is empty.

        Several SFMC endpoints return HTTP 200 with an empty body on
        success (e.g. deploy, script-control, and delete-rule operations).
        """
        body = response.content
        if not body or not body.strip():
            return {}
        return cast(dict[str, Any], response.json())

    # ── Glider Management ────────────────────────────────────────────

    def get_glider_details(self, glider_name: str) -> dict[str, Any]:
        """Retrieve details for a registered glider.

        Calls ``GET /v1/gliders/{glider_name}``.

        Args:
            glider_name: The registered name of the glider
                (e.g. ``"osu680"``).

        Returns:
            A dictionary with the full glider details as returned by
            the server.  The exact shape depends on the SFMC version;
            typical top-level keys include ``"data"`` containing
            ``"id"``, ``"name"``, and deployment information.

        Raises:
            APIError: If the glider is not found or the server returns
                a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.

        Example::

            >>> with SFMCClient() as client:
            ...     info = client.get_glider_details("osu680")
            ...     print(info)
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request("GET", f"/v1/gliders/{glider_name}")
        return self._json_or_empty(response)

    def get_active_deployment_details(self, glider_name: str) -> dict[str, Any]:
        """Retrieve the active deployment for a glider.

        Calls ``GET /v1/active-deployment/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            A dictionary with deployment details including
            ``"id"``, ``"gliderName"``, timestamps, and status.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request("GET", f"/v1/active-deployment/{glider_name}")
        return self._json_or_empty(response)

    def get_newest_mission_status(self, glider_name: str) -> dict[str, Any]:
        """Retrieve the newest mission status for a glider.

        Calls ``GET /v1/newest-mission-details/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            A dictionary with mission status details.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request("GET", f"/v1/newest-mission-details/{glider_name}")
        return self._json_or_empty(response)

    def get_surface_sensor_samples(
        self,
        glider_name: str,
        sensor_type_name: str,
        start_datetime: str,
        end_datetime: str,
    ) -> dict[str, Any]:
        """Retrieve surface sensor samples within a time range.

        Calls ``GET /v1/surface-sensor-samples/{glider_name}/{sensor_type_name}``.

        Args:
            glider_name: The registered name of the glider.
            sensor_type_name: The sensor type to query
                (e.g. ``"m_gps_lat"``).
            start_datetime: Start of the time range
                (format: ``"yyyyMMddHHmm"``).
            end_datetime: End of the time range
                (format: ``"yyyyMMddHHmm"``).

        Returns:
            A dictionary containing the sensor sample data.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        _validate_path_segment(sensor_type_name, "sensor_type_name")
        response = self._request(
            "GET",
            f"/v1/surface-sensor-samples/{glider_name}/{sensor_type_name}",
            params={"startDateTime": start_datetime, "endDateTime": end_datetime},
        )
        return self._json_or_empty(response)

    def get_folder_file_listing(
        self,
        glider_name: str,
        folder: str,
        *,
        page: int = 0,
        filter: str | None = None,
        last_modified_after: str | None = None,
    ) -> dict[str, Any]:
        """List files in a glider folder.

        Calls ``GET /v1/glider-folder-file-listing/{glider_name}/{folder}``.

        Args:
            glider_name: The registered name of the glider.
            folder: Folder name (e.g. ``"from-glider"``,
                ``"to-glider"``, ``"to-science"``).
            page: Page number for paginated results (default ``0``).
            filter: Wildcard filter for file names
                (e.g. ``"*.sbd"``).  Optional.
            last_modified_after: Only include files modified after
                this timestamp (format: ``"yyyyMMddHHmm"``).  Optional.

        Returns:
            A dictionary with the file listing and pagination info.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        _validate_path_segment(folder, "folder")
        params: dict[str, str | int] = {"page": page}
        if filter is not None:
            params["filter"] = filter
        if last_modified_after is not None:
            params["lastModifiedAfter"] = last_modified_after

        response = self._request(
            "GET",
            f"/v1/glider-folder-file-listing/{glider_name}/{folder}",
            params=params,
        )
        return self._json_or_empty(response)

    def get_available_scripts(self, glider_name: str) -> dict[str, Any]:
        """List available scripts for a glider.

        Calls ``GET /v1/scripts-for-glider/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            A dictionary listing available scripts and their types.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request("GET", f"/v1/scripts-for-glider/{glider_name}")
        return self._json_or_empty(response)

    def get_zmodem_transfers(self, connection_id: int | str) -> dict[str, Any]:
        """Retrieve Zmodem transfers for a connection.

        Calls ``GET /v1/zmodem-transfers/{connection_id}``.

        Args:
            connection_id: The connection identifier.

        Returns:
            A dictionary with the Zmodem transfer details.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(str(connection_id), "connection_id")
        response = self._request("GET", f"/v1/zmodem-transfers/{connection_id}")
        return self._json_or_empty(response)

    # ── Plans — Query ────────────────────────────────────────────────

    def get_mission_plan(self, glider_name: str) -> dict[str, Any]:
        """Retrieve the assigned mission plan for a glider.

        Calls ``GET /v1/glider-assigned-mission-plan/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            A dictionary describing the assigned mission plan.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request("GET", f"/v1/glider-assigned-mission-plan/{glider_name}")
        return self._json_or_empty(response)

    def get_waypoint_plan(self, glider_name: str) -> dict[str, Any]:
        """Retrieve the assigned waypoint plan for a glider.

        Calls ``GET /v1/glider-assigned-waypoint-plan/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            A dictionary describing the assigned waypoint plan,
            including waypoint coordinates and sequencing.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request("GET", f"/v1/glider-assigned-waypoint-plan/{glider_name}")
        return self._json_or_empty(response)

    def get_yo_plan(self, glider_name: str) -> dict[str, Any]:
        """Retrieve the assigned yo plan for a glider.

        Calls ``GET /v1/glider-assigned-yo-plan/{glider_name}``.

        A *yo plan* defines the glider's dive/climb profile
        (depth targets, pitch angles, etc.).

        Args:
            glider_name: The registered name of the glider.

        Returns:
            A dictionary describing the assigned yo plan.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request("GET", f"/v1/glider-assigned-yo-plan/{glider_name}")
        return self._json_or_empty(response)

    def get_surface_plan(self, glider_name: str) -> dict[str, Any]:
        """Retrieve the assigned surface plan for a glider.

        Calls ``GET /v1/glider-assigned-surface-plan/{glider_name}``.

        A *surface plan* controls when the glider surfaces for
        communication (time intervals, waypoint triggers, etc.).

        Args:
            glider_name: The registered name of the glider.

        Returns:
            A dictionary describing the assigned surface plan
            and its rules.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request("GET", f"/v1/glider-assigned-surface-plan/{glider_name}")
        return self._json_or_empty(response)

    def get_sampling_plan(self, glider_name: str) -> dict[str, Any]:
        """Retrieve the assigned sampling plan for a glider.

        Calls ``GET /v1/glider-assigned-sampling-plan/{glider_name}``.

        A *sampling plan* controls sensor sampling rates and
        conditions during a mission.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            A dictionary describing the assigned sampling plan
            and its rules.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request("GET", f"/v1/glider-assigned-sampling-plan/{glider_name}")
        return self._json_or_empty(response)

    def get_data_transmission_plan(self, glider_name: str) -> dict[str, Any]:
        """Retrieve the assigned data transmission plan for a glider.

        Calls ``GET /v1/glider-assigned-data-transmission-plan/{glider_name}``.

        Controls which data files are transmitted when the glider
        surfaces (SBD/TBD list configuration).

        Args:
            glider_name: The registered name of the glider.

        Returns:
            A dictionary describing the data transmission plan.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request(
            "GET",
            f"/v1/glider-assigned-data-transmission-plan/{glider_name}",
        )
        return self._json_or_empty(response)

    def get_mission_sensor_plan(self, glider_name: str) -> dict[str, Any]:
        """Retrieve the assigned mission sensor plan for a glider.

        Calls ``GET /v1/glider-assigned-mission-sensor-plan/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            A dictionary describing which sensors are active
            and their configuration.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request(
            "GET",
            f"/v1/glider-assigned-mission-sensor-plan/{glider_name}",
        )
        return self._json_or_empty(response)

    def get_abort_plan(self, glider_name: str) -> dict[str, Any]:
        """Retrieve the assigned abort plan for a glider.

        Calls ``GET /v1/glider-assigned-abort-plan/{glider_name}``.

        An *abort plan* defines conditions under which the glider
        will autonomously abort its mission and surface.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            A dictionary describing the abort plan and its triggers.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request("GET", f"/v1/glider-assigned-abort-plan/{glider_name}")
        return self._json_or_empty(response)

    # ── Plans — Update ───────────────────────────────────────────────

    def update_waypoint_plan(self, glider_name: str, goto_file_path: Path | str) -> dict[str, Any]:
        """Upload and apply a new waypoint plan from a goto file.

        Calls ``PUT /v1/update-glider-waypoint-plan/{glider_name}``
        with the file as multipart form data.

        Args:
            glider_name: The registered name of the glider.
            goto_file_path: Path to the ``.goto`` plan file.

        Returns:
            Server response confirming the update.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        return self._upload_plan_file(
            f"/v1/update-glider-waypoint-plan/{glider_name}",
            goto_file_path,
        )

    def update_yo_plan(self, glider_name: str, yo_file_path: Path | str) -> dict[str, Any]:
        """Upload and apply a new yo plan file.

        Calls ``PUT /v1/update-glider-yo-plan/{glider_name}``
        with the file as multipart form data.

        Args:
            glider_name: The registered name of the glider.
            yo_file_path: Path to the yo plan file.

        Returns:
            Server response confirming the update.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        return self._upload_plan_file(
            f"/v1/update-glider-yo-plan/{glider_name}",
            yo_file_path,
        )

    def update_surface_plan(
        self, glider_name: str, surface_file_path: Path | str
    ) -> dict[str, Any]:
        """Upload and apply a new surface plan file.

        Calls ``PUT /v1/update-glider-surface-plan/{glider_name}``
        with the file as multipart form data.

        Args:
            glider_name: The registered name of the glider.
            surface_file_path: Path to the surface plan file.

        Returns:
            Server response confirming the update.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        return self._upload_plan_file(
            f"/v1/update-glider-surface-plan/{glider_name}",
            surface_file_path,
        )

    def update_sampling_plan(
        self, glider_name: str, sampling_file_path: Path | str
    ) -> dict[str, Any]:
        """Upload and apply a new sampling plan file.

        Calls ``PUT /v1/update-glider-sampling-plan/{glider_name}``
        with the file as multipart form data.

        Args:
            glider_name: The registered name of the glider.
            sampling_file_path: Path to the sampling plan file.

        Returns:
            Server response confirming the update.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        return self._upload_plan_file(
            f"/v1/update-glider-sampling-plan/{glider_name}",
            sampling_file_path,
        )

    def update_flight_data_transmission_plan(
        self, glider_name: str, sbd_list_file_path: Path | str
    ) -> dict[str, Any]:
        """Upload and apply a new flight data transmission plan.

        Calls ``PUT /v1/update-glider-flight-data-transmission-plan/{glider_name}``
        with the SBD list file as multipart form data.

        Args:
            glider_name: The registered name of the glider.
            sbd_list_file_path: Path to the SBD list file.

        Returns:
            Server response confirming the update.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        return self._upload_plan_file(
            f"/v1/update-glider-flight-data-transmission-plan/{glider_name}",
            sbd_list_file_path,
        )

    def update_science_data_transmission_plan(
        self, glider_name: str, tbd_list_file_path: Path | str
    ) -> dict[str, Any]:
        """Upload and apply a new science data transmission plan.

        Calls ``PUT /v1/update-glider-science-data-transmission-plan/{glider_name}``
        with the TBD list file as multipart form data.

        Args:
            glider_name: The registered name of the glider.
            tbd_list_file_path: Path to the TBD list file.

        Returns:
            Server response confirming the update.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        return self._upload_plan_file(
            f"/v1/update-glider-science-data-transmission-plan/{glider_name}",
            tbd_list_file_path,
        )

    def _upload_plan_file(self, path: str, file_path: Path | str) -> dict[str, Any]:
        """Upload a single file as multipart form data via PUT.

        Used internally by all ``update_*_plan`` methods.

        Args:
            path: API path relative to the base URL.
            file_path: Local path to the file to upload.

        Returns:
            The parsed JSON response from the server.
        """
        file_path = Path(file_path)
        with contextlib.ExitStack() as stack:
            fobj = stack.enter_context(open(file_path, "rb"))
            files = {"file": (file_path.name, fobj)}
            response = self._request("PUT", path, files=files)
            return self._json_or_empty(response)

    # ── Plans — Delete Rules ─────────────────────────────────────────

    def delete_hit_waypoint_surface_plan_rule(self, glider_name: str) -> dict[str, Any]:
        """Delete the hit-waypoint surface plan rule for a glider.

        Calls ``DELETE /v1/delete-glider-hit-waypoint-surface-plan-rule/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            Server response confirming the deletion.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request(
            "DELETE",
            f"/v1/delete-glider-hit-waypoint-surface-plan-rule/{glider_name}",
        )
        return self._json_or_empty(response)

    def delete_every_secs_surface_plan_rules(self, glider_name: str) -> dict[str, Any]:
        """Delete all every-N-seconds surface plan rules for a glider.

        Calls ``DELETE /v1/delete-glider-every-secs-surface-plan-rules/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            Server response confirming the deletion.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request(
            "DELETE",
            f"/v1/delete-glider-every-secs-surface-plan-rules/{glider_name}",
        )
        return self._json_or_empty(response)

    def delete_at_utc_time_surface_plan_rules(self, glider_name: str) -> dict[str, Any]:
        """Delete all at-UTC-time surface plan rules for a glider.

        Calls ``DELETE /v1/delete-glider-at-utc-time-surface-plan-rules/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            Server response confirming the deletion.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request(
            "DELETE",
            f"/v1/delete-glider-at-utc-time-surface-plan-rules/{glider_name}",
        )
        return self._json_or_empty(response)

    def delete_sampling_plan_rules(self, glider_name: str) -> dict[str, Any]:
        """Delete all sampling plan rules for a glider.

        Calls ``DELETE /v1/delete-glider-sampling-plan-rules/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            Server response confirming the deletion.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request(
            "DELETE",
            f"/v1/delete-glider-sampling-plan-rules/{glider_name}",
        )
        return self._json_or_empty(response)

    # ── Glider Registration & Deployment ─────────────────────────────

    def register_glider(self, glider_name: str, group_name: str = "default") -> dict[str, Any]:
        """Register a glider with the SFMC server.

        Calls ``POST /v1/register-glider/{group_name}``.

        Args:
            glider_name: Name for the new glider.
            group_name: Group to register the glider under
                (default: ``"default"``).

        Returns:
            Server response confirming registration.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        _validate_path_segment(group_name, "group_name")
        response = self._request(
            "POST",
            f"/v1/register-glider/{group_name}",
            content=glider_name,
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        return self._json_or_empty(response)

    def obtain_or_create_active_deployment(self, glider_name: str) -> dict[str, Any]:
        """Get the active deployment for a glider, creating one if needed.

        Calls ``POST /v1/obtain-or-create-active-deployment/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            A dictionary with the active deployment details.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request(
            "POST",
            f"/v1/obtain-or-create-active-deployment/{glider_name}",
        )
        return self._json_or_empty(response)

    def update_active_deployment_start(
        self, glider_name: str, start_datetime: str
    ) -> dict[str, Any]:
        """Update the start time of the active deployment.

        Calls ``PUT /v1/update-active-deployment-start/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.
            start_datetime: New start time
                (format: ``"yyyyMMddHHmm"``).

        Returns:
            Server response confirming the update.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request(
            "PUT",
            f"/v1/update-active-deployment-start/{glider_name}",
            params={"startDateTime": start_datetime},
        )
        return self._json_or_empty(response)

    # ── Script Control ───────────────────────────────────────────────

    def set_assigned_script(
        self, glider_name: str, script_type: str, script_name: str
    ) -> dict[str, Any]:
        """Assign a script to a glider.

        Calls ``PUT /v1/set-assigned-script/{glider_name}/{script_type}/{script_name}``.

        Args:
            glider_name: The registered name of the glider.
            script_type: Type of script (e.g. ``"mission"``).
            script_name: Name of the script to assign.

        Returns:
            Server response confirming the assignment.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        _validate_path_segment(script_type, "script_type")
        _validate_path_segment(script_name, "script_name")
        response = self._request(
            "PUT",
            f"/v1/set-assigned-script/{glider_name}/{script_type}/{script_name}",
        )
        return self._json_or_empty(response)

    def clear_assigned_script(self, glider_name: str) -> dict[str, Any]:
        """Clear the currently assigned script for a glider.

        Calls ``PUT /v1/clear-assigned-script/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            Server response confirming the script was cleared.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request("PUT", f"/v1/clear-assigned-script/{glider_name}")
        return self._json_or_empty(response)

    def pause_assigned_script(self, glider_name: str) -> dict[str, Any]:
        """Pause the currently assigned script for a glider.

        Calls ``PUT /v1/pause-assigned-script/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            Server response confirming the script was paused.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request("PUT", f"/v1/pause-assigned-script/{glider_name}")
        return self._json_or_empty(response)

    def resume_assigned_script(self, glider_name: str) -> dict[str, Any]:
        """Resume a paused script for a glider.

        Calls ``PUT /v1/resume-assigned-script/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            Server response confirming the script was resumed.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request("PUT", f"/v1/resume-assigned-script/{glider_name}")
        return self._json_or_empty(response)

    def rewind_assigned_script(self, glider_name: str) -> dict[str, Any]:
        """Rewind the assigned script for a glider to the beginning.

        Calls ``PUT /v1/rewind-assigned-script/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            Server response confirming the script was rewound.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request("PUT", f"/v1/rewind-assigned-script/{glider_name}")
        return self._json_or_empty(response)

    # ── Commands ─────────────────────────────────────────────────────

    def send_command(self, glider_name: str, command: str) -> dict[str, Any]:
        """Send a command to a glider.

        Calls ``PUT /v1/submit-command/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.
            command: The command string to send.

        Returns:
            Server response confirming the command was submitted.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request(
            "PUT",
            f"/v1/submit-command/{glider_name}",
            content=command,
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        return self._json_or_empty(response)

    # ── Deploy Files ─────────────────────────────────────────────────

    def deploy_goto_file(self, glider_name: str) -> dict[str, Any]:
        """Generate and deploy a goto file for a glider.

        Calls ``PUT /v1/gen-and-deploy-glider-goto-file/{glider_name}``.

        Triggers server-side generation of the goto file from the
        current waypoint plan and deploys it to the glider.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            Server response confirming the deployment.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request("PUT", f"/v1/gen-and-deploy-glider-goto-file/{glider_name}")
        return self._json_or_empty(response)

    def deploy_yo_file(self, glider_name: str) -> dict[str, Any]:
        """Generate and deploy a yo file for a glider.

        Calls ``PUT /v1/gen-and-deploy-glider-yo-file/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            Server response confirming the deployment.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request("PUT", f"/v1/gen-and-deploy-glider-yo-file/{glider_name}")
        return self._json_or_empty(response)

    def deploy_surface_files(self, glider_name: str) -> dict[str, Any]:
        """Generate and deploy surface files for a glider.

        Calls ``PUT /v1/gen-and-deploy-glider-surface-files/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            Server response confirming the deployment.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request(
            "PUT",
            f"/v1/gen-and-deploy-glider-surface-files/{glider_name}",
        )
        return self._json_or_empty(response)

    def deploy_sample_files(self, glider_name: str) -> dict[str, Any]:
        """Generate and deploy sample files for a glider.

        Calls ``PUT /v1/gen-and-deploy-glider-sample-files/{glider_name}``.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            Server response confirming the deployment.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request(
            "PUT",
            f"/v1/gen-and-deploy-glider-sample-files/{glider_name}",
        )
        return self._json_or_empty(response)

    def deploy_sbd_list_file(self, glider_name: str) -> dict[str, Any]:
        """Generate and deploy an SBD list file for a glider.

        Calls ``PUT /v1/gen-and-deploy-glider-sbd-list-file/{glider_name}``.

        An SBD list file controls which flight data files are
        transmitted when the glider surfaces.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            Server response confirming the deployment.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request(
            "PUT",
            f"/v1/gen-and-deploy-glider-sbd-list-file/{glider_name}",
        )
        return self._json_or_empty(response)

    def deploy_tbd_list_file(self, glider_name: str) -> dict[str, Any]:
        """Generate and deploy a TBD list file for a glider.

        Calls ``PUT /v1/gen-and-deploy-glider-tbd-list-file/{glider_name}``.

        A TBD list file controls which science data files are
        transmitted when the glider surfaces.

        Args:
            glider_name: The registered name of the glider.

        Returns:
            Server response confirming the deployment.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        response = self._request(
            "PUT",
            f"/v1/gen-and-deploy-glider-tbd-list-file/{glider_name}",
        )
        return self._json_or_empty(response)

    # ── File Operations ──────────────────────────────────────────────

    def upload_glider_files(
        self,
        glider_name: str,
        folder: str,
        file_paths: list[Path | str],
    ) -> dict[str, Any]:
        """Upload files to a glider folder.

        Calls ``PUT /v1/upload-glider-files/{glider_name}/{folder}``
        with multipart form data.

        Args:
            glider_name: The registered name of the glider.
            folder: Target folder — must be one of ``"to-glider"``,
                ``"to-science"``, or ``"from-glider"``.
            file_paths: List of local file paths to upload.

        Returns:
            Server response confirming the upload.

        Raises:
            ValueError: If *folder* is not an allowed upload target.
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        _validate_path_segment(folder, "folder")
        allowed = ("to-glider", "to-science", "from-glider")
        if folder not in allowed:
            raise ValueError(f"Upload folder must be one of {allowed}, got {folder!r}")

        return self._upload_files(
            f"/v1/upload-glider-files/{glider_name}/{folder}",
            file_paths,
        )

    def upload_cache_files(
        self,
        group_name: str,
        file_paths: list[Path | str],
    ) -> dict[str, Any]:
        """Upload cache files for a group.

        Calls ``PUT /v1/upload-cache-files/{group_name}``
        with multipart form data.

        Args:
            group_name: The group to upload cache files for.
            file_paths: List of local file paths to upload.

        Returns:
            Server response confirming the upload.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(group_name, "group_name")
        return self._upload_files(
            f"/v1/upload-cache-files/{group_name}",
            file_paths,
        )

    def _upload_files(self, path: str, file_paths: list[Path | str]) -> dict[str, Any]:
        """Upload multiple files as multipart form data via PUT.

        Opens each file with a context manager to satisfy resource
        management requirements.
        """
        with contextlib.ExitStack() as stack:
            files = [
                ("files", (Path(fp).name, stack.enter_context(open(Path(fp), "rb"))))
                for fp in file_paths
            ]
            response = self._request("PUT", path, files=files)
            return self._json_or_empty(response)

    def download_glider_file(
        self,
        glider_name: str,
        folder: str,
        file_name: str,
        download_path: Path | str | None = None,
    ) -> Path:
        """Download a single file from a glider folder.

        Calls ``GET /v1/download-glider-file/{glider_name}/{folder}/{file_name}``
        and streams the response to a local file.

        Args:
            glider_name: The registered name of the glider.
            folder: Source folder (e.g. ``"from-glider"``).
            file_name: Name of the file to download.
            download_path: Local path where the file will be saved.
                Defaults to :attr:`download_dir` ``/ file_name``.

        Returns:
            The :class:`~pathlib.Path` to the downloaded file.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        _validate_path_segment(folder, "folder")
        _validate_path_segment(file_name, "file_name")
        download_path = Path(download_path) if download_path else self.download_dir / file_name
        headers = self._auth_headers()
        with self._http.stream(
            "GET",
            f"/v1/download-glider-file/{glider_name}/{folder}/{file_name}",
            headers=headers,
        ) as response:
            check_response(response)
            tmp_path = download_path.with_suffix(download_path.suffix + ".part")
            try:
                with open(tmp_path, "wb") as f:
                    for chunk in response.iter_bytes():
                        f.write(chunk)
                tmp_path.rename(download_path)
            except BaseException:
                tmp_path.unlink(missing_ok=True)
                raise
        return download_path

    def download_glider_files(
        self,
        glider_name: str,
        folder: str,
        download_path: Path | str | None = None,
        *,
        filter: str | None = None,
        last_modified_after: str | None = None,
    ) -> Path:
        """Download multiple files from a glider folder as a zip archive.

        Calls ``GET /v1/download-glider-files/{glider_name}/{folder}``
        and streams the zip response to a local file.

        Args:
            glider_name: The registered name of the glider.
            folder: Source folder (e.g. ``"from-glider"``).
            download_path: Local path for the downloaded zip file.
                Defaults to :attr:`download_dir` ``/ {glider_name}-{folder}.zip``.
            filter: Wildcard filter for file names
                (e.g. ``"*.sbd"``).  Optional.
            last_modified_after: Only include files modified after
                this timestamp (format: ``"yyyyMMddHHmm"``).  Optional.

        Returns:
            The :class:`~pathlib.Path` to the downloaded zip file.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        _validate_path_segment(folder, "folder")
        if download_path is not None:
            download_path = Path(download_path)
        else:
            download_path = self.download_dir / f"{glider_name}-{folder}.zip"
        params: dict[str, str] = {}
        if filter is not None:
            params["filter"] = filter
        if last_modified_after is not None:
            params["lastModifiedAfter"] = last_modified_after

        headers = self._auth_headers()
        with self._http.stream(
            "GET",
            f"/v1/download-glider-files/{glider_name}/{folder}",
            headers=headers,
            params=params,
        ) as response:
            check_response(response)
            tmp_path = download_path.with_suffix(download_path.suffix + ".part")
            try:
                with open(tmp_path, "wb") as f:
                    for chunk in response.iter_bytes():
                        f.write(chunk)
                tmp_path.rename(download_path)
            except BaseException:
                tmp_path.unlink(missing_ok=True)
                raise
        return download_path

    def delete_glider_file(self, glider_name: str, folder: str, file_name: str) -> dict[str, Any]:
        """Delete a file from a glider folder.

        Calls ``DELETE /v1/delete-glider-file/{glider_name}/{folder}/{file_name}``.

        Args:
            glider_name: The registered name of the glider.
            folder: Folder containing the file — must be one of
                ``"to-glider"``, ``"to-science"``, or ``"configuration"``.
            file_name: Name of the file to delete.

        Returns:
            Server response confirming the deletion.

        Raises:
            ValueError: If *folder* is not an allowed deletion target.
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        _validate_path_segment(folder, "folder")
        _validate_path_segment(file_name, "file_name")
        allowed = ("to-glider", "to-science", "configuration")
        if folder not in allowed:
            raise ValueError(f"Delete folder must be one of {allowed}, got {folder!r}")
        response = self._request(
            "DELETE",
            f"/v1/delete-glider-file/{glider_name}/{folder}/{file_name}",
        )
        return self._json_or_empty(response)

    # ── Real-Time Streaming (STOMP) ──────────────────────────────────

    def open_stream(self) -> StompConnection:
        """Open a STOMP-over-SockJS connection for real-time events.

        Authenticates if needed, then establishes a WebSocket
        connection to the SFMC STOMP endpoint.  The returned
        :class:`~sfmc_api.stomp.StompConnection` can be used to
        subscribe to event topics.

        Use as a context manager::

            with client.open_stream() as stomp:
                sub = stomp.subscribe("/topic/glider-connections-8")
                for event in sub:
                    print(event)

        Returns:
            A connected :class:`StompConnection`.

        Raises:
            AuthenticationError: If sign-in fails.
            StompError: If the WebSocket or STOMP handshake fails.
        """
        self._ensure_auth()
        if self._token is None:
            raise AuthenticationError("Authentication succeeded but no token was returned")
        conn = StompConnection(self._config, self._token)
        conn.connect()
        return conn

    def subscribe_connection_events(
        self, glider_name: str, stomp: StompConnection
    ) -> StompSubscription:
        """Subscribe to real-time connection events for a glider.

        Listens on STOMP topic ``/topic/glider-connections-{gliderId}``.

        Each message is a list of connection event dicts with keys:
        ``id``, ``gliderDeploymentId``, ``active`` (bool),
        ``logFilePath``.

        Args:
            glider_name: The registered name of the glider.
            stomp: An open :class:`StompConnection` from
                :meth:`open_stream`.

        Returns:
            A :class:`StompSubscription` yielding connection event
            messages.

        Example::

            with client.open_stream() as stomp:
                sub = client.subscribe_connection_events("osu684", stomp)
                for events in sub:
                    for evt in events:
                        status = "CONNECTED" if evt["active"] else "DISCONNECTED"
                        print(f"{status} id={evt['id']}")

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        glider_id = self._get_glider_id(glider_name)
        return stomp.subscribe(f"/topic/glider-connections-{glider_id}")

    def subscribe_glider_output(
        self, glider_name: str, stomp: StompConnection
    ) -> StompSubscription:
        """Subscribe to real-time dialog/output data for a glider.

        Listens on STOMP topic ``/topic/glider-link-output/{gliderId}``.

        Each message is a dict with ``sequenceNumber`` and ``data``
        (the output text).  Messages may arrive out of order — see
        ``sfmc_api.monitor_glider.ordered_dialog()`` for a
        reordering implementation.

        Args:
            glider_name: The registered name of the glider.
            stomp: An open :class:`StompConnection`.

        Returns:
            A :class:`StompSubscription` yielding glider output
            messages.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        glider_id = self._get_glider_id(glider_name)
        return stomp.subscribe(f"/topic/glider-link-output/{glider_id}")

    def subscribe_script_events(
        self, glider_name: str, stomp: StompConnection
    ) -> StompSubscription:
        """Subscribe to script assignment update events for a glider.

        Listens on STOMP topic
        ``/topic/glider-script-assignment-updates-{gliderId}``.

        Each message is a dict with keys: ``scriptType``,
        ``scriptName``, ``scriptState``, ``paused`` (bool).

        Args:
            glider_name: The registered name of the glider.
            stomp: An open :class:`StompConnection`.

        Returns:
            A :class:`StompSubscription` yielding script event
            messages.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        glider_id = self._get_glider_id(glider_name)
        return stomp.subscribe(f"/topic/glider-script-assignment-updates-{glider_id}")

    def subscribe_zmodem_transfer_events(
        self, glider_name: str, stomp: StompConnection
    ) -> StompSubscription:
        """Subscribe to Zmodem transfer events for a glider.

        Listens on STOMP topic
        ``/topic/new-and-updated-zmodem-transfers-{deploymentId}``.

        Uses the *deployment* ID (not the glider ID), obtained
        automatically from the active deployment.

        Args:
            glider_name: The registered name of the glider.
            stomp: An open :class:`StompConnection`.

        Returns:
            A :class:`StompSubscription` yielding Zmodem transfer
            event messages.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        deployment = self.get_active_deployment_details(glider_name)
        try:
            deployment_id = deployment["data"]["id"]
        except (KeyError, TypeError) as exc:
            raise APIError(
                0, "Unexpected response from get_active_deployment_details: missing data.id"
            ) from exc
        return stomp.subscribe(f"/topic/new-and-updated-zmodem-transfers-{deployment_id}")

    def subscribe_deployment_events(
        self, glider_name: str, stomp: StompConnection
    ) -> StompSubscription:
        """Subscribe to low-frequency deployment update events.

        Listens on STOMP topic
        ``/topic/low-freq-glider-deployment-updates-{deploymentId}``.

        Args:
            glider_name: The registered name of the glider.
            stomp: An open :class:`StompConnection`.

        Returns:
            A :class:`StompSubscription` yielding deployment update
            messages.

        Raises:
            APIError: If the server returns a non-success response.
            RateLimitError: If the server returns HTTP 429.
            AuthenticationError: If sign-in fails.
        """
        _validate_path_segment(glider_name, "glider_name")
        deployment = self.get_active_deployment_details(glider_name)
        try:
            deployment_id = deployment["data"]["id"]
        except (KeyError, TypeError) as exc:
            raise APIError(
                0, "Unexpected response from get_active_deployment_details: missing data.id"
            ) from exc
        return stomp.subscribe(f"/topic/low-freq-glider-deployment-updates-{deployment_id}")

    def _get_glider_id(self, glider_name: str) -> int:
        """Look up the numeric glider ID from the glider name."""
        _validate_path_segment(glider_name, "glider_name")
        details = self.get_glider_details(glider_name)
        try:
            return int(details["data"]["id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise APIError(
                0, "Unexpected response from get_glider_details: missing data.id"
            ) from exc
