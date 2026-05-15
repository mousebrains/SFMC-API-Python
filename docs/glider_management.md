# Glider Management Data Flow

## Overview

Glider management endpoints handle registration, deployment lifecycle,
status queries, and sensor data retrieval.

> **A note on response shape.**  The SFMC server returns JSON objects
> whose exact field set depends on server version and configuration.
> The examples below show fields the library actively relies on; your
> server may include additional fields.  Treat unknown extras as
> opaque rather than asserting they exist.

## Endpoint Summary

| Method | Python method | API path |
|--------|--------------|----------|
| GET | `get_glider_details(name)` | `/v1/gliders/{name}` |
| GET | `get_active_deployment_details(name)` | `/v1/active-deployment/{name}` |
| GET | `get_newest_mission_status(name)` | `/v1/newest-mission-details/{name}` |
| GET | `get_surface_sensor_samples(name, sensor, start, end)` | `/v1/surface-sensor-samples/{name}/{sensor}` |
| GET | `get_folder_file_listing(name, folder, ...)` | `/v1/glider-folder-file-listing/{name}/{folder}` |
| GET | `get_available_scripts(name)` | `/v1/scripts-for-glider/{name}` |
| GET | `get_zmodem_transfers(conn_id)` | `/v1/zmodem-transfers/{conn_id}` |
| POST | `register_glider(name, group)` | `/v1/register-glider/{group}` |
| POST | `obtain_or_create_active_deployment(name)` | `/v1/obtain-or-create-active-deployment/{name}` |
| PUT | `update_active_deployment_start(name, dt)` | `/v1/update-active-deployment-start/{name}` |

## Data Flow: Query a Glider

```
┌──────────┐                           ┌──────────────┐
│  Caller  │                           │  SFMC Server  │
└────┬─────┘                           └──────┬───────┘
     │                                        │
     │  client.get_glider_details("osu684")   │
     │                                        │
     │  GET /v1/gliders/osu684               │
     │  Authorization: Bearer {token}         │
     │ ──────────────────────────────────────► │
     │                                        │
     │  200 OK                                │
     │  {                                     │
     │    "data": {                           │
     │      "id": 8,                          │
     │      "name": "osu684",                 │
     │      "state": "disconnected"           │
     │    },                                  │
     │    "links": {                          │
     │      "self": "/sfmc/api/v1/gliders/..."│
     │    }                                   │
     │  }                                     │
     │ ◄────────────────────────────────────── │
     │                                        │
     │  → returns dict                        │
```

## Typical response: `get_active_deployment_details(name)`

```json
{
  "data": {
    "id": 1234,
    "gliderName": "osu684",
    "currentScriptName": "sfmc.xml",
    "currentScriptType": "factory",
    "isCurrentScriptRunning": true
  }
}
```

The library reads `data.id` (used as the deployment ID for STOMP
topics), `data.currentScriptName`, `data.currentScriptType`, and
`data.isCurrentScriptRunning`.  Treat anything else as informational.

## Typical response: `get_folder_file_listing(...)`

Paginated.  The list of file names lives somewhere under `data`;
inspect the response from your server with `--compact | jq .` once to
see the exact shape, then extract accordingly.

```bash
sfmc-api --compact get-folder-file-listing osu684 from-glider \
    --filter "*.sbd" | jq .
```

## Data Flow: Register a New Glider

```
┌──────────┐                           ┌──────────────┐
│  Caller  │                           │  SFMC Server  │
└────┬─────┘                           └──────┬───────┘
     │                                        │
     │  client.register_glider("newglider",   │
     │                         "default")     │
     │                                        │
     │  POST /v1/register-glider/default      │
     │  Content-Type: application/json        │
     │  Body: "newglider"                     │
     │ ──────────────────────────────────────► │
     │                                        │
     │  200 OK  { confirmation }              │
     │ ◄────────────────────────────────────── │
```

## Data Flow: Sensor Sample Query

```
client.get_surface_sensor_samples(
    "osu684", "m_gps_lat",
    "202603200000", "202603260000"
)

  GET /v1/surface-sensor-samples/osu684/m_gps_lat
      ?startDateTime=202603200000
      &endDateTime=202603260000
  Authorization: Bearer {token}

  → 200 OK { sensor sample data }
```

## Data Flow: File Listing with Filtering

```
client.get_folder_file_listing(
    "osu684", "from-glider",
    filter="*.sbd",
    last_modified_after="202603200000",
    page=0,
)

  GET /v1/glider-folder-file-listing/osu684/from-glider
      ?page=0
      &filter=*.sbd
      &lastModifiedAfter=202603200000
  Authorization: Bearer {token}

  → 200 OK { paginated file listing }
```
