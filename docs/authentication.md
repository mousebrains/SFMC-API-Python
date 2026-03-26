# Authentication Data Flow

## Overview

The SFMC REST API uses bearer-token authentication.  Clients obtain a
token by posting credentials to the sign-in endpoint, then include that
token in the `Authorization` header of every subsequent request.

## Sequence Diagram

```
┌──────────┐                         ┌──────────────┐
│  Caller  │                         │  SFMC Server  │
└────┬─────┘                         └──────┬───────┘
     │                                      │
     │  SFMCClient()                        │
     │  ─ load config                       │
     │  ─ create httpx.Client               │
     │  ─ token = None                      │
     │                                      │
     │  client.get_glider_details("g1")     │
     │  ─ _ensure_auth() sees token is None │
     │  ─ calls authenticate()              │
     │                                      │
     │  POST /sfmc/api/signin               │
     │  {"clientId": "...", "secret": "..."}│
     │ ────────────────────────────────────► │
     │                                      │
     │  200 OK                              │
     │  {"token": "eyJ..."}                 │
     │ ◄──────────────────────────────────── │
     │                                      │
     │  ─ cache token in self._token        │
     │                                      │
     │  GET /v1/gliders/g1                  │
     │  Authorization: Bearer eyJ...        │
     │ ────────────────────────────────────► │
     │                                      │
     │  200 OK                              │
     │  { glider details JSON }             │
     │ ◄──────────────────────────────────── │
     │                                      │
     │  return response.json()              │
     │                                      │
```

## Detailed Steps

### 1. Client Construction

```python
client = SFMCClient()
```

- `SFMCConfig.from_file()` reads `~/.config/sfmc/credentials.json`.
- `build_http_client(config)` creates an `httpx.Client` with:
  - `base_url = "https://{host}/sfmc/api"`
  - `verify = config.tls_verify`
  - 30 s read timeout, 10 s connect timeout
- `self._token` is set to `None`.  **No network I/O occurs.**

### 2. Lazy Authentication (`_ensure_auth`)

On the first API call, `_ensure_auth()` checks `self._token`.
Since it is `None`, it calls `authenticate()`.

### 3. Sign-In Request

`authenticate()` sends:

```
POST https://{host}/sfmc/api/signin
Content-Type: application/json

{"clientId": "<client_id>", "secret": "<secret>"}
```

### 4. Sign-In Response

On success the server returns:

```
HTTP/1.1 200 OK
Content-Type: application/json

{"token": "<bearer_token>"}
```

The token string is extracted from the JSON body and stored in
`self._token`.

### 5. Error Cases

| Scenario                  | Exception raised         |
|---------------------------|--------------------------|
| Bad credentials (401/403) | `AuthenticationError`    |
| Server unreachable        | `AuthenticationError`    |
| Unexpected response shape | `AuthenticationError`    |
| Rate limited (429)        | `RateLimitError`         |

All errors are wrapped in `AuthenticationError` (or its parent
`SFMCError`) so callers need only catch one type.

### 6. Subsequent Requests

After authentication, `_auth_headers()` returns
`{"Authorization": "Bearer <token>"}` and the cached token is reused
without re-authenticating.

## Code Path

```
SFMCClient.get_glider_details(name)
  └─► _request("GET", "/v1/gliders/{name}")
        ├─► _auth_headers()
        │     └─► _ensure_auth()
        │           └─► authenticate(http_client, config)   [auth.py]
        │                 ├─► http_client.post("/signin", json=...)
        │                 └─► check_response(response)      [_http.py]
        ├─► http_client.request("GET", path, headers=...)
        └─► check_response(response)                        [_http.py]
```
