# Plans Data Flow

## Overview

Slocum gliders operate according to several configurable *plans* that
control navigation, diving, surfacing, sampling, and data transmission.
The SFMC API provides endpoints to query, update, and delete rules for
each plan type.

## Plan Types

| Plan | Controls | Query method | Update method |
|------|----------|-------------|---------------|
| **Mission** | Overall mission parameters | `get_mission_plan()` | — |
| **Waypoint** | Navigation path (goto file) | `get_waypoint_plan()` | `update_waypoint_plan()` |
| **Yo** | Dive/climb profile | `get_yo_plan()` | `update_yo_plan()` |
| **Surface** | When to surface for comms | `get_surface_plan()` | `update_surface_plan()` |
| **Sampling** | Sensor sampling rates | `get_sampling_plan()` | `update_sampling_plan()` |
| **Data Transmission** | Which files to transmit | `get_data_transmission_plan()` | `update_flight_data_transmission_plan()` / `update_science_data_transmission_plan()` |
| **Mission Sensor** | Active sensors | `get_mission_sensor_plan()` | — |
| **Abort** | Autonomous abort triggers | `get_abort_plan()` | — |

## Data Flow: Query a Plan

All plan queries follow an identical pattern — simple GET with
glider name:

```
┌──────────┐                           ┌──────────────┐
│  Caller  │                           │  SFMC Server  │
└────┬─────┘                           └──────┬───────┘
     │                                        │
     │  client.get_waypoint_plan("osu684")    │
     │                                        │
     │  GET /v1/glider-assigned-waypoint-plan/osu684
     │  Authorization: Bearer {token}         │
     │ ──────────────────────────────────────► │
     │                                        │
     │  200 OK { plan details + rules }       │
     │ ◄────────────────────────────────────── │
     │                                        │
     │  → returns dict                        │
```

## Data Flow: Update a Plan (File Upload)

Plan updates upload a configuration file via multipart form data:

```
┌──────────┐                           ┌──────────────┐
│  Caller  │                           │  SFMC Server  │
└────┬─────┘                           └──────┬───────┘
     │                                        │
     │  client.update_waypoint_plan(          │
     │      "osu684", "/path/to/plan.goto")   │
     │                                        │
     │  PUT /v1/update-glider-waypoint-plan/osu684
     │  Content-Type: multipart/form-data     │
     │  ┌─ file: plan.goto (binary) ─┐       │
     │ ──────────────────────────────────────► │
     │                                        │
     │  200 OK { confirmation }               │
     │ ◄────────────────────────────────────── │
```

### Code Path

```
client.update_waypoint_plan(name, path)
  └─► _upload_plan_file("/v1/update-glider-waypoint-plan/{name}", path)
        ├─► open(path, "rb")
        ├─► _request("PUT", path, files={"file": (name, fobj)})
        │     ├─► _auth_headers()
        │     ├─► httpx sends multipart/form-data
        │     └─► check_response()
        └─► close file handle
```

## Data Flow: Delete Plan Rules

Rule deletions remove specific rule types from surface or sampling
plans:

```
client.delete_every_secs_surface_plan_rules("osu684")

  DELETE /v1/delete-glider-every-secs-surface-plan-rules/osu684
  Authorization: Bearer {token}

  → 200 OK { confirmation }
```

### Surface Plan Rule Types

| Method | Rule type removed |
|--------|------------------|
| `delete_hit_waypoint_surface_plan_rule()` | Surface when hitting a waypoint |
| `delete_every_secs_surface_plan_rules()` | Surface every N seconds |
| `delete_at_utc_time_surface_plan_rules()` | Surface at specific UTC times |
| `delete_sampling_plan_rules()` | Sampling plan rules |

## Data Flow: Generate & Deploy

These endpoints trigger *server-side* generation of configuration
files from the current plans and deploy them to the glider.  No
file upload is needed — the server generates the files itself:

```
client.deploy_goto_file("osu684")

  PUT /v1/gen-and-deploy-glider-goto-file/osu684
  Authorization: Bearer {token}

  → 200 OK { confirmation }
```

| Method | File generated |
|--------|---------------|
| `deploy_goto_file()` | Navigation waypoints (from waypoint plan) |
| `deploy_yo_file()` | Dive/climb profile (from yo plan) |
| `deploy_surface_files()` | Surface behaviour (from surface plan) |
| `deploy_sample_files()` | Sampling config (from sampling plan) |
| `deploy_sbd_list_file()` | Flight data transmission list |
| `deploy_tbd_list_file()` | Science data transmission list |
