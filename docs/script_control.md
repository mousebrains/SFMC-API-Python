# Script Control & Commands Data Flow

## Overview

SFMC scripts automate glider operations. The API provides endpoints
to assign, pause, resume, rewind, and clear scripts, as well as send
direct commands to gliders.

## Endpoint Summary

| Method | Python method | API path |
|--------|--------------|----------|
| PUT | `set_assigned_script(name, type, script)` | `/v1/set-assigned-script/{name}/{type}/{script}` |
| PUT | `clear_assigned_script(name)` | `/v1/clear-assigned-script/{name}` |
| PUT | `pause_assigned_script(name)` | `/v1/pause-assigned-script/{name}` |
| PUT | `resume_assigned_script(name)` | `/v1/resume-assigned-script/{name}` |
| PUT | `rewind_assigned_script(name)` | `/v1/rewind-assigned-script/{name}` |
| PUT | `send_command(name, command)` | `/v1/submit-command/{name}` |

## Data Flow: Script Lifecycle

```
┌──────────┐                           ┌──────────────┐
│  Caller  │                           │  SFMC Server  │
└────┬─────┘                           └──────┬───────┘
     │                                        │
     │  ① Discover available scripts          │
     │  client.get_available_scripts("osu684")│
     │  GET /v1/scripts-for-glider/osu684     │
     │ ──────────────────────────────────────► │
     │  ◄── 200 { list of scripts }           │
     │                                        │
     │  ② Assign a script                     │
     │  client.set_assigned_script(           │
     │      "osu684", "mission", "dive10")    │
     │  PUT /v1/set-assigned-script/          │
     │      osu684/mission/dive10             │
     │ ──────────────────────────────────────► │
     │  ◄── 200 { confirmation }              │
     │                                        │
     │  ③ Pause if needed                     │
     │  client.pause_assigned_script("osu684")│
     │  PUT /v1/pause-assigned-script/osu684  │
     │ ──────────────────────────────────────► │
     │  ◄── 200 { confirmation }              │
     │                                        │
     │  ④ Resume                              │
     │  client.resume_assigned_script("osu684")
     │  PUT /v1/resume-assigned-script/osu684 │
     │ ──────────────────────────────────────► │
     │  ◄── 200 { confirmation }              │
     │                                        │
     │  ⑤ Rewind to start                     │
     │  client.rewind_assigned_script("osu684")
     │  PUT /v1/rewind-assigned-script/osu684 │
     │ ──────────────────────────────────────► │
     │  ◄── 200 { confirmation }              │
     │                                        │
     │  ⑥ Clear assignment                    │
     │  client.clear_assigned_script("osu684")│
     │  PUT /v1/clear-assigned-script/osu684  │
     │ ──────────────────────────────────────► │
     │  ◄── 200 { confirmation }              │
```

## Data Flow: Send a Command

Commands are sent as raw strings in the request body:

```
┌──────────┐                           ┌──────────────┐
│  Caller  │                           │  SFMC Server  │
└────┬─────┘                           └──────┬───────┘
     │                                        │
     │  client.send_command("osu684",         │
     │      "put c_science_on 0")             │
     │                                        │
     │  PUT /v1/submit-command/osu684         │
     │  Content-Type: application/json        │
     │  Body: "put c_science_on 0"            │
     │ ──────────────────────────────────────► │
     │                                        │
     │  200 OK { confirmation }               │
     │ ◄────────────────────────────────────── │
```

## Script State Transitions

```
  ┌───────────┐
  │ unassigned │
  └─────┬─────┘
        │ set_assigned_script()
        ▼
  ┌───────────┐  pause()   ┌────────┐
  │  running   │ ─────────► │ paused │
  └─────┬─────┘ ◄───────── └────────┘
        │         resume()
        │
        │ rewind()  → back to start of script
        │
        │ clear_assigned_script()
        ▼
  ┌───────────┐
  │ unassigned │
  └───────────┘
```
