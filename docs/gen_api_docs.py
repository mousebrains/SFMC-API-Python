#!/usr/bin/env python3
"""Generate `docs/openapi.yaml` and `docs/API_REFERENCE.md` from one spec.

This is the single source of truth for the SFMC HTTP/STOMP API reference.
The endpoint list below is derived from the `sfmc_api` client and the
Teledyne SFMC Node.js reference library, cross-checked against a live
server.  Verbs, paths, parameters, and the auth flow are verified against
working code; response bodies are documented only for the fields the
clients actually consume — a server may return more.

Run `python docs/gen_api_docs.py` after changing the spec; do not edit
the generated files by hand.

STOMP streaming is HTTP-agnostic and cannot be expressed in OpenAPI, so
the streaming topics live in the `x-stomp-streaming` vendor extension of
the spec and in a dedicated Markdown section.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

DOCS = Path(__file__).resolve().parent

GLIDER = ("gliderName", "The registered glider name (e.g. `osusim`).")


def op(
    method: str,
    path: str,
    tag: str,
    summary: str,
    *,
    path_params: list[tuple[str, str]] | None = None,
    query: list[tuple[str, bool, str]] | None = None,
    body: dict[str, Any] | None = None,
    responses: list[dict[str, Any]],
    public: bool = False,
) -> dict[str, Any]:
    """Build one REST operation record used by both emitters."""
    return {
        "method": method,
        "path": path,
        "tag": tag,
        "summary": summary,
        "path_params": path_params or [],
        "query": query or [],
        "body": body,
        "responses": responses,
        "public": public,
    }


def plan_query(name: str, path: str) -> dict[str, Any]:
    return op(
        "GET",
        path,
        "Plans — Query",
        f"Read the glider's assigned {name} plan.",
        path_params=[GLIDER],
        responses=[{"code": "200", "desc": f"The assigned {name} plan."}],
    )


def plan_update(name: str, path: str) -> dict[str, Any]:
    return op(
        "PUT",
        path,
        "Plans — Update",
        f"Replace the glider's {name} plan by uploading a plan file.",
        path_params=[GLIDER],
        body={
            "type": "multipart/form-data",
            "desc": "The plan file, under the form field `file`.",
            "field": "file",
            "multiple": False,
        },
        responses=[{"code": "200", "desc": "The updated plan."}],
    )


def plan_delete(name: str, path: str) -> dict[str, Any]:
    return op(
        "DELETE",
        path,
        "Plans — Delete Rules",
        f"Delete the {name} from the glider's plan.",
        path_params=[GLIDER],
        responses=[{"code": "200", "desc": "Deletion result.", "empty": True}],
    )


def deploy(name: str, path: str) -> dict[str, Any]:
    return op(
        "PUT",
        path,
        "Deploy Files",
        f"Generate and deploy the glider {name} to the vehicle's to-glider queue.",
        path_params=[GLIDER],
        responses=[{"code": "200", "desc": "Deployed.", "empty": True}],
    )


def script_ctl(verb: str, path: str) -> dict[str, Any]:
    return op(
        "PUT",
        path,
        "Script Control",
        f"{verb} the glider's assigned script.",
        path_params=[GLIDER],
        responses=[{"code": "200", "desc": f"{verb} result.", "empty": True}],
    )


# ── The spec ──────────────────────────────────────────────────────────

TAGS: list[dict[str, str]] = [
    {"name": "Authentication", "description": "Exchange credentials for a bearer token."},
    {
        "name": "Glider Management",
        "description": "Register gliders; read status, deployment, sensors.",
    },
    {"name": "File Operations", "description": "List, upload, download, and delete glider files."},
    {"name": "Plans — Query", "description": "Read a glider's assigned plans."},
    {"name": "Plans — Update", "description": "Replace an assigned plan by file upload."},
    {"name": "Plans — Delete Rules", "description": "Remove rule classes from a plan."},
    {"name": "Deploy Files", "description": "Generate and deploy glider files from plans."},
    {
        "name": "Script Control",
        "description": "Assign and steer the mission script; send commands.",
    },
    {"name": "Zmodem Transfers", "description": "Read per-connection file-transfer records."},
]

OPERATIONS: list[dict[str, Any]] = [
    op(
        "POST",
        "/signin",
        "Authentication",
        "Exchange API credentials for a bearer token. The only unauthenticated endpoint.",
        body={
            "type": "application/json",
            "desc": "Client credentials.",
            "schema": "SigninRequest",
            "example": {"clientId": "…", "secret": "…"},
        },
        responses=[
            {
                "code": "200",
                "desc": "A bearer token for the Authorization header.",
                "schema": "SigninResponse",
                "example": {"token": "…"},
            },
        ],
        public=True,
    ),
    # ── Glider Management ──
    op(
        "GET",
        "/v1/gliders/{gliderName}",
        "Glider Management",
        "Retrieve details for a registered glider — numeric id, name, and connection state.",
        path_params=[GLIDER],
        responses=[
            {
                "code": "200",
                "desc": "Glider record.",
                "schema": "GliderDetails",
                "example": {"data": {"id": 2, "name": "osusim", "state": "disconnected"}},
            }
        ],
    ),
    op(
        "GET",
        "/v1/active-deployment/{gliderName}",
        "Glider Management",
        "Read the glider's active deployment, including the current script and its run state.",
        path_params=[GLIDER],
        responses=[
            {
                "code": "200",
                "desc": "Active deployment.",
                "schema": "ActiveDeployment",
                "example": {
                    "data": {
                        "id": 41,
                        "currentScriptName": "riot.xml",
                        "currentScriptType": "user",
                        "isCurrentScriptRunning": True,
                    }
                },
            }
        ],
    ),
    op(
        "GET",
        "/v1/newest-mission-details/{gliderName}",
        "Glider Management",
        "Get the status of the glider's most recent mission.",
        path_params=[GLIDER],
        responses=[{"code": "200", "desc": "Newest mission status record."}],
    ),
    op(
        "GET",
        "/v1/surface-sensor-samples/{gliderName}/{sensorTypeName}",
        "Glider Management",
        "Return surface sensor samples of a given type over a time window.",
        path_params=[GLIDER, ("sensorTypeName", "The surface sensor type to sample.")],
        query=[
            ("startDateTime", True, "Start of the window (ISO-8601 UTC)."),
            ("endDateTime", True, "End of the window (ISO-8601 UTC)."),
        ],
        responses=[{"code": "200", "desc": "Sensor samples within the window."}],
    ),
    op(
        "POST",
        "/v1/register-glider/{groupName}",
        "Glider Management",
        "Register a new glider within a group.",
        path_params=[("groupName", "The group to register the glider under (e.g. `default`).")],
        body={
            "type": "application/json",
            "desc": "The glider name, as a bare JSON string.",
            "schema": "BareString",
            "example": "osusim",
        },
        responses=[{"code": "200", "desc": "Registration result."}],
    ),
    op(
        "POST",
        "/v1/obtain-or-create-active-deployment/{gliderName}",
        "Glider Management",
        "Return the glider's active deployment, creating one if none exists.",
        path_params=[GLIDER],
        responses=[{"code": "200", "desc": "The existing or newly created active deployment."}],
    ),
    op(
        "PUT",
        "/v1/update-active-deployment-start/{gliderName}",
        "Glider Management",
        "Update the start time of the glider's active deployment.",
        path_params=[GLIDER],
        query=[("startDateTime", True, "New deployment start time (ISO-8601 UTC).")],
        responses=[{"code": "200", "desc": "Updated deployment.", "empty": True}],
    ),
    # ── File Operations ──
    op(
        "GET",
        "/v1/glider-folder-file-listing/{gliderName}/{folder}",
        "File Operations",
        "List files in a glider folder, paginated and optionally filtered by name or mtime.",
        path_params=[GLIDER, ("folder", "Folder to list (e.g. `from-glider`).")],
        query=[
            ("page", False, "Zero-based page index (default 0); ~20 entries/page."),
            ("filter", False, "Wildcard filename filter."),
            (
                "lastModifiedAfter",
                False,
                "Only files modified at/after this cutoff (`yyyyMMddHHmm`).",
            ),
        ],
        responses=[
            {
                "code": "200",
                "desc": "A page of listing entries.",
                "schema": "FileListing",
                "example": {
                    "limit": 20,
                    "results": [
                        {
                            "fileName": "osusim-2026-191-0-1.sbd",
                            "dateTimeModified": "2026-07-17 05:43:32",
                            "fileSize": 6841,
                        }
                    ],
                },
            }
        ],
    ),
    op(
        "PUT",
        "/v1/upload-glider-files/{gliderName}/{folder}",
        "File Operations",
        "Upload one or more files into a glider folder.",
        path_params=[
            GLIDER,
            ("folder", "Target folder: `to-glider`, `to-science`, or `from-glider`."),
        ],
        body={
            "type": "multipart/form-data",
            "desc": "One or more files under the repeated form field `files`.",
            "field": "files",
            "multiple": True,
        },
        responses=[{"code": "200", "desc": "Upload result."}],
    ),
    op(
        "PUT",
        "/v1/upload-cache-files/{groupName}",
        "File Operations",
        "Upload Slocum cache (.cac) files for a group.",
        path_params=[("groupName", "The glider group the cache files belong to.")],
        body={
            "type": "multipart/form-data",
            "desc": "One or more files under the repeated form field `files`.",
            "field": "files",
            "multiple": True,
        },
        responses=[{"code": "200", "desc": "Upload result."}],
    ),
    op(
        "GET",
        "/v1/download-glider-file/{gliderName}/{folder}/{fileName}",
        "File Operations",
        "Download a single file. The response is the raw file stream, not JSON.",
        path_params=[
            GLIDER,
            ("folder", "Source folder."),
            ("fileName", "Exact file name to download."),
        ],
        responses=[
            {
                "code": "200",
                "desc": "The file contents (streamed).",
                "media": "application/octet-stream",
                "binary": True,
            }
        ],
    ),
    op(
        "GET",
        "/v1/download-glider-files/{gliderName}/{folder}",
        "File Operations",
        "Download all matching files in a folder as a single ZIP archive (streamed).",
        path_params=[GLIDER, ("folder", "Source folder (e.g. `from-glider`).")],
        query=[
            ("filter", False, "Wildcard filename filter."),
            (
                "lastModifiedAfter",
                False,
                "Only files modified at/after this cutoff (`yyyyMMddHHmm`).",
            ),
        ],
        responses=[
            {
                "code": "200",
                "desc": "A ZIP archive of the matching files (streamed).",
                "media": "application/zip",
                "binary": True,
            }
        ],
    ),
    op(
        "DELETE",
        "/v1/delete-glider-file/{gliderName}/{folder}/{fileName}",
        "File Operations",
        "Delete a single file from a glider folder.",
        path_params=[
            GLIDER,
            ("folder", "One of: `to-glider`, `to-science`, `configuration`."),
            ("fileName", "Exact file name to delete."),
        ],
        responses=[{"code": "200", "desc": "Deletion result.", "empty": True}],
    ),
    # ── Plans — Query ──
    plan_query("mission", "/v1/glider-assigned-mission-plan/{gliderName}"),
    plan_query("waypoint (goto)", "/v1/glider-assigned-waypoint-plan/{gliderName}"),
    plan_query("yo (dive profile)", "/v1/glider-assigned-yo-plan/{gliderName}"),
    plan_query("surface", "/v1/glider-assigned-surface-plan/{gliderName}"),
    plan_query("sampling", "/v1/glider-assigned-sampling-plan/{gliderName}"),
    plan_query("data-transmission", "/v1/glider-assigned-data-transmission-plan/{gliderName}"),
    plan_query("mission-sensor", "/v1/glider-assigned-mission-sensor-plan/{gliderName}"),
    plan_query("abort", "/v1/glider-assigned-abort-plan/{gliderName}"),
    # ── Plans — Update ──
    plan_update("waypoint", "/v1/update-glider-waypoint-plan/{gliderName}"),
    plan_update("yo", "/v1/update-glider-yo-plan/{gliderName}"),
    plan_update("surface", "/v1/update-glider-surface-plan/{gliderName}"),
    plan_update("sampling", "/v1/update-glider-sampling-plan/{gliderName}"),
    plan_update(
        "flight data-transmission", "/v1/update-glider-flight-data-transmission-plan/{gliderName}"
    ),
    plan_update(
        "science data-transmission",
        "/v1/update-glider-science-data-transmission-plan/{gliderName}",
    ),
    # ── Plans — Delete Rules ──
    plan_delete(
        "hit-waypoint surface-plan rule",
        "/v1/delete-glider-hit-waypoint-surface-plan-rule/{gliderName}",
    ),
    plan_delete(
        "every-N-seconds surface-plan rules",
        "/v1/delete-glider-every-secs-surface-plan-rules/{gliderName}",
    ),
    plan_delete(
        "at-UTC-time surface-plan rules",
        "/v1/delete-glider-at-utc-time-surface-plan-rules/{gliderName}",
    ),
    plan_delete("sampling-plan rules", "/v1/delete-glider-sampling-plan-rules/{gliderName}"),
    # ── Deploy Files ──
    deploy("goto (waypoint) file", "/v1/gen-and-deploy-glider-goto-file/{gliderName}"),
    deploy("yo file", "/v1/gen-and-deploy-glider-yo-file/{gliderName}"),
    deploy("surface files", "/v1/gen-and-deploy-glider-surface-files/{gliderName}"),
    deploy("sample files", "/v1/gen-and-deploy-glider-sample-files/{gliderName}"),
    deploy("SBD list file", "/v1/gen-and-deploy-glider-sbd-list-file/{gliderName}"),
    deploy("TBD list file", "/v1/gen-and-deploy-glider-tbd-list-file/{gliderName}"),
    # ── Script Control ──
    op(
        "GET",
        "/v1/scripts-for-glider/{gliderName}",
        "Script Control",
        "List the scripts available to assign to the glider.",
        path_params=[GLIDER],
        responses=[{"code": "200", "desc": "Available scripts."}],
    ),
    op(
        "PUT",
        "/v1/set-assigned-script/{gliderName}/{scriptType}/{scriptName}",
        "Script Control",
        "Assign a script of a given type to the glider.",
        path_params=[
            GLIDER,
            ("scriptType", "The script type (e.g. `mission`, `user`)."),
            ("scriptName", "The script file name (e.g. `riot.xml`)."),
        ],
        responses=[{"code": "200", "desc": "Assignment result.", "empty": True}],
    ),
    script_ctl("Clear", "/v1/clear-assigned-script/{gliderName}"),
    script_ctl("Pause", "/v1/pause-assigned-script/{gliderName}"),
    script_ctl("Resume", "/v1/resume-assigned-script/{gliderName}"),
    script_ctl("Rewind", "/v1/rewind-assigned-script/{gliderName}"),
    op(
        "PUT",
        "/v1/submit-command/{gliderName}",
        "Script Control",
        "Send a raw command to the glider over the dialog link.",
        path_params=[GLIDER],
        body={
            "type": "application/json",
            "desc": "The command, as a bare JSON string.",
            "schema": "BareString",
            "example": "put c_science_all_on(bool) 1",
        },
        responses=[{"code": "200", "desc": "Submission result.", "empty": True}],
    ),
    # ── Zmodem ──
    op(
        "GET",
        "/v1/zmodem-transfers/{connectionId}",
        "Zmodem Transfers",
        "List the downloads and uploads transferred over one glider connection.",
        path_params=[("connectionId", "The numeric connection id (from a connection event).")],
        responses=[
            {
                "code": "200",
                "desc": "Transfer summary for the connection.",
                "schema": "ZmodemTransfers",
                "example": {
                    "data": {
                        "downloads": [{"transferStatus": "Completed"}],
                        "uploads": [],
                        "totalDownloadBytes": 34284,
                    }
                },
            }
        ],
    ),
]

STREAMING: dict[str, Any] = {
    "protocol": "STOMP 1.2 over SockJS WebSocket",
    "connect": "wss://{host}/sfmc/api/sfmc-stomp/{serverId}/{sessionId}/websocket?access_token={token}",
    "notes": (
        "{serverId} is a random 3-digit number and {sessionId} a random string (SockJS "
        "convention). After the socket opens, send a STOMP CONNECT frame with "
        "accept-version: 1.2 and a heart-beat, then SUBSCRIBE to topics. {gliderId} is "
        "data.id from GET /v1/gliders/{gliderName}; {deploymentId} is data.id from "
        "GET /v1/active-deployment/{gliderName}."
    ),
    "topics": [
        {
            "topic": "/topic/glider-connections-{gliderId}",
            "summary": "Connection open/close events for a glider.",
            "example": {
                "active": True,
                "id": 40801,
                "startDateTime": "2026-07-17 13:51:47",
                "endDateTime": None,
            },
        },
        {
            "topic": "/topic/glider-link-output/{gliderId}",
            "summary": "Raw dialog output from the glider, line by line.",
            "example": {"sequenceNumber": 562080, "data": "GPS Location: …"},
        },
        {
            "topic": "/topic/glider-script-assignment-updates-{gliderId}",
            "summary": "Script assignment and run-state changes.",
            "example": {
                "scriptName": "riot.xml",
                "scriptType": "user",
                "scriptState": "T1WaitForSurfacing",
                "paused": False,
            },
        },
        {
            "topic": "/topic/new-and-updated-zmodem-transfers-{deploymentId}",
            "summary": "New and updated Zmodem transfer records for the deployment.",
        },
        {
            "topic": "/topic/low-freq-glider-deployment-updates-{deploymentId}",
            "summary": "Low-frequency deployment status updates.",
        },
    ],
}

SCHEMAS: dict[str, Any] = {
    "SigninRequest": {
        "type": "object",
        "required": ["clientId", "secret"],
        "properties": {"clientId": {"type": "string"}, "secret": {"type": "string"}},
    },
    "SigninResponse": {
        "type": "object",
        "properties": {"token": {"type": "string"}},
    },
    "BareString": {
        "type": "string",
        "description": "A bare JSON string (the request body is a quoted string, not an object).",
    },
    "GliderDetails": {
        "type": "object",
        "description": "Fields the client consumes; the server may return more.",
        "properties": {
            "data": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                    "state": {"type": "string", "description": "e.g. deployed, disconnected."},
                },
            }
        },
    },
    "ActiveDeployment": {
        "type": "object",
        "description": "Fields the client consumes; the server may return more.",
        "properties": {
            "data": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "id": {"type": "integer"},
                    "currentScriptName": {"type": ["string", "null"]},
                    "currentScriptType": {"type": "string"},
                    "isCurrentScriptRunning": {"type": "boolean"},
                },
            }
        },
    },
    "FileEntry": {
        "type": "object",
        "properties": {
            "fileName": {"type": "string"},
            "dateTimeModified": {
                "type": "string",
                "description": "`YYYY-MM-DD HH:MM:SS` (glider or dockserver clock).",
            },
            "fileSize": {"type": "integer"},
        },
    },
    "FileListing": {
        "type": "object",
        "properties": {
            "limit": {"type": "integer"},
            "results": {"type": "array", "items": {"$ref": "#/components/schemas/FileEntry"}},
        },
    },
    "ZmodemTransfers": {
        "type": "object",
        "description": "Fields the client consumes; the server may return more.",
        "properties": {
            "data": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "downloads": {"type": "array", "items": {"type": "object"}},
                    "uploads": {"type": "array", "items": {"type": "object"}},
                    "totalDownloadBytes": {"type": "integer"},
                },
            }
        },
    },
}

PROVENANCE = (
    "Derived from the `sfmc_api` Python client and the Teledyne SFMC Node.js reference "
    "library, cross-checked against a live server (`gliderfmc1.ceoas.oregonstate.edu`). "
    "Verbs, paths, parameters, and the auth flow are verified against working code. "
    "Response bodies are documented only for the fields the clients consume — a server "
    "may return more. Not affiliated with Teledyne Webb Research; consult Appendix E of "
    "the SFMC User Manual for the authoritative specification."
)


# ── OpenAPI emitter ───────────────────────────────────────────────────


def _ref_or_object(resp: dict[str, Any]) -> dict[str, Any]:
    if resp.get("empty"):
        return {"description": resp["desc"]}
    if resp.get("binary"):
        return {
            "description": resp["desc"],
            "content": {resp["media"]: {"schema": {"type": "string", "format": "binary"}}},
        }
    schema: dict[str, Any] = (
        {"$ref": f"#/components/schemas/{resp['schema']}"}
        if resp.get("schema")
        else {"type": "object", "description": "Shape not fully documented; see provenance."}
    )
    content: dict[str, Any] = {"schema": schema}
    if resp.get("example") is not None:
        content["example"] = resp["example"]
    return {"description": resp["desc"], "content": {"application/json": content}}


def _request_body(body: dict[str, Any]) -> dict[str, Any]:
    if body["type"] == "multipart/form-data":
        item = {"type": "string", "format": "binary"}
        prop: dict[str, Any] = {"type": "array", "items": item} if body.get("multiple") else item
        schema = {"type": "object", "properties": {body["field"]: prop}}
        return {
            "required": True,
            "description": body["desc"],
            "content": {"multipart/form-data": {"schema": schema}},
        }
    schema = (
        {"$ref": f"#/components/schemas/{body['schema']}"}
        if body.get("schema")
        else {"type": "object"}
    )
    content: dict[str, Any] = {"schema": schema}
    if "example" in body:
        content["example"] = body["example"]
    return {"required": True, "description": body["desc"], "content": {body["type"]: content}}


def build_openapi() -> dict[str, Any]:
    paths: dict[str, Any] = {}
    for o in OPERATIONS:
        params = [
            {
                "name": n,
                "in": "path",
                "required": True,
                "description": d,
                "schema": {"type": "string"},
            }
            for n, d in o["path_params"]
        ]
        params += [
            {
                "name": n,
                "in": "query",
                "required": req,
                "description": d,
                "schema": {"type": "string"},
            }
            for n, req, d in o["query"]
        ]
        responses = {r["code"]: _ref_or_object(r) for r in o["responses"]}
        if not o["public"]:
            responses["401"] = {"$ref": "#/components/responses/Unauthorized"}
            responses["429"] = {"$ref": "#/components/responses/RateLimited"}
        operation: dict[str, Any] = {
            "tags": [o["tag"]],
            "summary": o["summary"],
            "operationId": (
                o["method"].lower() + "".join(c if c.isalnum() else "_" for c in o["path"])
            ).strip("_"),
            "responses": responses,
        }
        if o["public"]:
            operation["security"] = []
        if params:
            operation["parameters"] = params
        if o["body"]:
            operation["requestBody"] = _request_body(o["body"])
        paths.setdefault(o["path"], {})[o["method"].lower()] = operation

    return {
        "openapi": "3.1.0",
        "info": {
            "title": "SFMC REST & Streaming API",
            "version": "v1",
            "summary": "Slocum Fleet Mission Control HTTP and STOMP interface.",
            "description": PROVENANCE,
        },
        "servers": [
            {
                "url": "https://{host}/sfmc/api",
                "variables": {
                    "host": {
                        "default": "gliderfmc1.ceoas.oregonstate.edu",
                        "description": "SFMC server hostname or IP.",
                    }
                },
            }
        ],
        "tags": TAGS,
        "security": [{"bearerAuth": []}],
        "x-stomp-streaming": STREAMING,
        "paths": paths,
        "components": {
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "description": "Token from POST /signin.",
                },
            },
            "responses": {
                "Unauthorized": {"description": "Token missing or expired; re-run POST /signin."},
                "RateLimited": {
                    "description": (
                        "Rate limited. Wait the milliseconds in the "
                        "x-rate-limit-retry-after-milliseconds header, then retry."
                    )
                },
            },
            "schemas": SCHEMAS,
        },
    }


# ── Markdown emitter ──────────────────────────────────────────────────


def _fmt_example(ex: Any) -> str:
    import json

    if isinstance(ex, str):
        return f'"{ex}"'
    return json.dumps(ex, indent=2, ensure_ascii=False)


def build_markdown() -> str:
    lines: list[str] = []
    w = lines.append
    w("# SFMC REST & Streaming API Reference")
    w("")
    w("<!-- GENERATED by docs/gen_api_docs.py — do not edit by hand. -->")
    w("")
    w(f"> **Provenance.** {PROVENANCE}")
    w("")
    w(
        "**Base URL:** `https://{host}/sfmc/api` — all REST paths are relative to it. "
        "Values in `{braces}` are path parameters."
    )
    w("")
    w("A machine-readable OpenAPI 3.1 spec is in [`openapi.yaml`](openapi.yaml).")
    w("")

    w("## Authentication")
    w("")
    w("Authentication is a single token exchange:")
    w("")
    w('1. `POST /signin` with `{"clientId": "…", "secret": "…"}` returns `{"token": "…"}`.')
    w("2. Send the token as `Authorization: Bearer {token}` on every other request.")
    w(
        "3. On `401`, re-run `POST /signin` and retry once. On `429`, wait the "
        "`x-rate-limit-retry-after-milliseconds` header, then retry."
    )
    w("")

    # Table of contents.
    w("## Endpoints")
    w("")
    tags_in_order = [t["name"] for t in TAGS]
    for tag in tags_in_order:
        anchor = tag.lower().replace(" — ", "--").replace(" ", "-")
        w(f"- [{tag}](#{anchor})")
    w("- [Realtime Streaming (STOMP)](#realtime-streaming-stomp)")
    w("- [Status Codes](#status-codes)")
    w("")

    for tag in tags_in_order:
        w(f"## {tag}")
        w("")
        for o in [x for x in OPERATIONS if x["tag"] == tag]:
            w(f"### `{o['method']} {o['path']}`")
            w("")
            w(o["summary"])
            w("")
            if o["public"]:
                w("*Authentication: none — this endpoint issues the token.*")
                w("")
            if o["path_params"]:
                w("**Path parameters**")
                w("")
                w("| Name | Description |")
                w("| --- | --- |")
                for n, d in o["path_params"]:
                    w(f"| `{n}` | {d} |")
                w("")
            if o["query"]:
                w("**Query parameters**")
                w("")
                w("| Name | Required | Description |")
                w("| --- | --- | --- |")
                for n, req, d in o["query"]:
                    w(f"| `{n}` | {'yes' if req else 'no'} | {d} |")
                w("")
            if o["body"]:
                b = o["body"]
                w(f"**Request body** — `{b['type']}`: {b['desc']}")
                w("")
                if "example" in b:
                    w("``json")
                    w(_fmt_example(b["example"]))
                    w("``")
                    w("")
            w("**Responses**")
            w("")
            for r in o["responses"]:
                suffix = (
                    " *(empty body)*"
                    if r.get("empty")
                    else (f" *(`{r['media']}` stream)*" if r.get("binary") else "")
                )
                w(f"- `{r['code']}` — {r['desc']}{suffix}")
            w("")
            examples = [r for r in o["responses"] if r.get("example") is not None]
            if examples:
                w("``json")
                w(_fmt_example(examples[0]["example"]))
                w("``")
                w("")

    # Streaming.
    w("## Realtime Streaming (STOMP)")
    w("")
    w(
        f"Live events are delivered over **{STREAMING['protocol']}**, not REST. "
        "Open one connection and subscribe to the topics you need."
    )
    w("")
    w("**Connect**")
    w("")
    w("``")
    w(STREAMING["connect"])
    w("``")
    w("")
    w(STREAMING["notes"])
    w("")
    w("**Topics**")
    w("")
    for t in STREAMING["topics"]:
        w(f"### `SUBSCRIBE {t['topic']}`")
        w("")
        w(t["summary"])
        w("")
        if t.get("example") is not None:
            w("``json")
            w(_fmt_example(t["example"]))
            w("``")
            w("")

    # Status codes.
    w("## Status Codes")
    w("")
    w("| Status | Meaning | Client action |")
    w("| --- | --- | --- |")
    w(
        "| `200` | Success — JSON, a file/ZIP stream, or an empty body (deploy, "
        "script-control, delete-rule). | Parse per endpoint. |"
    )
    w("| `401` | Token missing or expired. | Re-run `POST /signin`, then retry once. |")
    w("| `429` | Rate limited. | Wait `x-rate-limit-retry-after-milliseconds`, then retry. |")
    w("| `4xx` | Client error — unknown glider, bad folder, malformed body. | Fix the request. |")
    w("| `5xx` | Server error. | Transient; retry with backoff. |")
    w("")
    w(
        "Non-idempotent calls (`POST`, `PUT`, `DELETE`) must not be blindly retried after an "
        "ambiguous network failure — the server may already have applied them."
    )
    w("")
    return "\n".join(lines) + "\n"


def validate(spec: dict[str, Any]) -> None:
    """Structural self-check: every local $ref resolves."""
    named: set[str] = set()
    for group in ("schemas", "responses"):
        named |= {f"#/components/{group}/{k}" for k in spec["components"][group]}

    refs: list[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                if k == "$ref" and isinstance(v, str):
                    refs.append(v)
                else:
                    walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(spec)
    missing = sorted({r for r in refs if r not in named})
    if missing:
        raise SystemExit(f"unresolved $ref(s): {missing}")


def main() -> None:
    spec = build_openapi()
    validate(spec)
    (DOCS / "openapi.yaml").write_text(
        "# GENERATED by docs/gen_api_docs.py — do not edit by hand.\n"
        + yaml.safe_dump(spec, sort_keys=False, width=100, allow_unicode=True),
        encoding="utf-8",
    )
    (DOCS / "API_REFERENCE.md").write_text(build_markdown(), encoding="utf-8")
    n_paths = len(spec["paths"])
    n_ops = sum(len(v) for v in spec["paths"].values())
    print(f"wrote openapi.yaml ({n_paths} paths, {n_ops} operations) and API_REFERENCE.md")
    print(f"plus {len(STREAMING['topics'])} STOMP topics in x-stomp-streaming / the Markdown")


if __name__ == "__main__":
    main()
