# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Goal

Create a fully functional **Python** version of the SFMC REST API client. The existing Node.js implementation under `sfmc-toolbox/` is the reference — use it to understand endpoints, auth flow, and behavior, but write idiomatic Python.

Official documentation: Appendix E of the [SFMC User Manual (v8.7.0-1)](https://gliderfs2.ceoas.oregonstate.edu/gliderweb/docs/slocum_manuals/SFMC%20User%20Manual%20[M313834-NFC,%20Rev%20B]%20Software%20Ver%208.7.0-1.pdf).

## Reference Implementation (Node.js)

Everything lives under `sfmc-toolbox/`. The compressed library at `sfmc-nodejs-rest-lib/sfmc.tgz` can be extracted to `/tmp/sfmc-lib/` for inspection. The 48 CLI programs in `sfmc-nodejs-rest-programs/` each demonstrate one API operation.

## SFMC REST API Reference

Extracted from the Node.js library source. Base URL: `https://{host}/sfmc/api`

### Authentication

- **POST** `/sfmc/api/signin` — body: `{"clientId": "...", "secret": "..."}` → returns `{token: "..."}`. All other endpoints require `Authorization: Bearer {token}` header.

### Glider Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/v1/register-glider/{groupName}` | Register a glider (body: glider name string) |
| POST | `/v1/obtain-or-create-active-deployment/{gliderName}` | Get or create active deployment |
| PUT | `/v1/update-active-deployment-start/{gliderName}?startDateTime=` | Update deployment start time |
| GET | `/v1/gliders/{gliderName}` | Get glider details |
| GET | `/v1/active-deployment/{gliderName}` | Get active deployment details |
| GET | `/v1/newest-mission-details/{gliderName}` | Get newest mission status |
| GET | `/v1/surface-sensor-samples/{gliderName}/{sensorTypeName}?startDateTime=&endDateTime=` | Get surface sensor samples |

### File Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/glider-folder-file-listing/{gliderName}/{folder}?filter=&lastModifiedAfter=&page=` | List files in glider folder |
| PUT | `/v1/upload-glider-files/{gliderName}/{folder}` | Upload files (multipart form, folders: to-glider, to-science, from-glider) |
| PUT | `/v1/upload-cache-files/{groupName}` | Upload cache files (multipart form) |
| GET | `/v1/download-glider-file/{gliderName}/{folder}/{fileName}` | Download single file (stream response) |
| GET | `/v1/download-glider-files/{gliderName}/{folder}?filter=&lastModifiedAfter=` | Download files as zip (stream response) |
| DELETE | `/v1/delete-glider-file/{gliderName}/{folder}/{fileName}` | Delete file (folders: to-glider, to-science, configuration) |

### Plans — Query

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/glider-assigned-mission-plan/{gliderName}` | Mission plan |
| GET | `/v1/glider-assigned-waypoint-plan/{gliderName}` | Waypoint plan |
| GET | `/v1/glider-assigned-yo-plan/{gliderName}` | Yo plan |
| GET | `/v1/glider-assigned-surface-plan/{gliderName}` | Surface plan |
| GET | `/v1/glider-assigned-sampling-plan/{gliderName}` | Sampling plan |
| GET | `/v1/glider-assigned-data-transmission-plan/{gliderName}` | Data transmission plan |
| GET | `/v1/glider-assigned-mission-sensor-plan/{gliderName}` | Mission sensor plan |
| GET | `/v1/glider-assigned-abort-plan/{gliderName}` | Abort plan |

### Plans — Update (all PUT, body: JSON plan object)

| Endpoint | Description |
|----------|-------------|
| `/v1/update-glider-waypoint-plan/{gliderName}` | Update waypoint plan |
| `/v1/update-glider-yo-plan/{gliderName}` | Update yo plan |
| `/v1/update-glider-surface-plan/{gliderName}` | Update surface plan |
| `/v1/update-glider-sampling-plan/{gliderName}` | Update sampling plan |
| `/v1/update-glider-flight-data-transmission-plan/{gliderName}` | Update flight data transmission plan |
| `/v1/update-glider-science-data-transmission-plan/{gliderName}` | Update science data transmission plan |

### Plans — Delete Rules (all DELETE)

| Endpoint | Description |
|----------|-------------|
| `/v1/delete-glider-hit-waypoint-surface-plan-rule/{gliderName}` | Delete hit-waypoint surface plan rule |
| `/v1/delete-glider-every-secs-surface-plan-rules/{gliderName}` | Delete every-secs surface plan rules |
| `/v1/delete-glider-at-utc-time-surface-plan-rules/{gliderName}` | Delete at-UTC-time surface plan rules |
| `/v1/delete-glider-sampling-plan-rules/{gliderName}` | Delete sampling plan rules |

### Deploy Files (all POST)

| Endpoint | Description |
|----------|-------------|
| `/v1/gen-and-deploy-glider-goto-file/{gliderName}` | Generate & deploy goto file |
| `/v1/gen-and-deploy-glider-yo-file/{gliderName}` | Generate & deploy yo file |
| `/v1/gen-and-deploy-glider-surface-files/{gliderName}` | Generate & deploy surface files |
| `/v1/gen-and-deploy-glider-sample-files/{gliderName}` | Generate & deploy sample files |
| `/v1/gen-and-deploy-glider-sbd-list-file/{gliderName}` | Generate & deploy SBD list file |
| `/v1/gen-and-deploy-glider-tbd-list-file/{gliderName}` | Generate & deploy TBD list file |

### Script Control (all POST)

| Endpoint | Description |
|----------|-------------|
| `/v1/scripts-for-glider/{gliderName}` | Get available scripts (GET) |
| `/v1/set-assigned-script/{gliderName}/{scriptName}` | Set assigned script |
| `/v1/clear-assigned-script/{gliderName}` | Clear assigned script |
| `/v1/pause-assigned-script/{gliderName}` | Pause assigned script |
| `/v1/resume-assigned-script/{gliderName}` | Resume assigned script |
| `/v1/rewind-assigned-script/{gliderName}` | Rewind assigned script |
| `/v1/submit-command/{gliderName}` | Send command (body: command string) |

### Zmodem Transfers

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/zmodem-transfers/{connectionId}` | Get zmodem transfers for a connection |

### Real-Time Streaming (STOMP over SockJS)

Connect via SockJS: `https://{host}/sfmc/api/sfmc-stomp?access_token={token}`

STOMP subscription topics:
- `/topic/glider-connections-{gliderId}` — connection events
- `/topic/new-and-updated-zmodem-transfers-{deploymentId}` — zmodem transfer events
- `/topic/glider-script-assignment-updates-{gliderId}` — script assignment events
- `/topic/glider-link-output/{gliderId}` — glider dialog/output data
- `/topic/low-freq-glider-deployment-updates-{gliderDeploymentId}` — deployment update events

### Response Handling

- **200** — success, return response body
- **429** — rate limited; `x-rate-limit-retry-after-milliseconds` header indicates retry delay

## Configuration

`sfmc-toolbox/sfmc-nodejs-rest-config/local.json` is the config template:
- `host` — SFMC server hostname or IP
- `apiCredentials.clientId` / `apiCredentials.secret` — API credentials
- `rootDownloadPath` — local path for downloaded files
- `tlsRejectUnauthorized` — `0` to disable TLS verification
- `stompDebug` — enable STOMP protocol debug logging
