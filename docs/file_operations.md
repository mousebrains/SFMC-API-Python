# File Operations Data Flow

## Overview

The SFMC API supports uploading files to glider folders, downloading
files (individually or as zip archives), listing folder contents, and
deleting files.

## Endpoint Summary

| Method | Python method | API path |
|--------|--------------|----------|
| GET | `get_folder_file_listing(name, folder, ...)` | `/v1/glider-folder-file-listing/{name}/{folder}` |
| PUT | `upload_glider_files(name, folder, paths)` | `/v1/upload-glider-files/{name}/{folder}` |
| PUT | `upload_cache_files(group, paths)` | `/v1/upload-cache-files/{group}` |
| GET | `download_glider_file(name, folder, file, dest)` | `/v1/download-glider-file/{name}/{folder}/{file}` |
| GET | `download_glider_files(name, folder, dest, ...)` | `/v1/download-glider-files/{name}/{folder}` |
| DELETE | `delete_glider_file(name, folder, file)` | `/v1/delete-glider-file/{name}/{folder}/{file}` |

## Glider Folder Permissions

| Folder | Upload | Download | Delete |
|--------|--------|----------|--------|
| `to-glider` | yes | yes | yes |
| `to-science` | yes | yes | yes |
| `from-glider` | yes | yes | no |
| `configuration` | no | yes | yes |

## Data Flow: Upload Files

```
┌──────────┐                           ┌──────────────┐
│  Caller  │                           │  SFMC Server  │
└────┬─────┘                           └──────┬───────┘
     │                                        │
     │  client.upload_glider_files(           │
     │      "osu684", "to-glider",            │
     │      ["mission.mi", "goto_l10.ma"])    │
     │                                        │
     │  PUT /v1/upload-glider-files/          │
     │      osu684/to-glider                  │
     │  Content-Type: multipart/form-data     │
     │  ┌─ files: mission.mi (binary) ──┐    │
     │  ┌─ files: goto_l10.ma (binary) ─┐    │
     │ ──────────────────────────────────────► │
     │                                        │
     │  200 OK { confirmation }               │
     │ ◄────────────────────────────────────── │
```

## Data Flow: Download a Single File

Uses HTTP streaming to avoid loading the entire file into memory:

```
┌──────────┐                           ┌──────────────┐
│  Caller  │                           │  SFMC Server  │
└────┬─────┘                           └──────┬───────┘
     │                                        │
     │  client.download_glider_file(          │
     │      "osu684", "from-glider",          │
     │      "osu684_2026_069_0_0.sbd",        │
     │      "/tmp/osu684.sbd")                │
     │                                        │
     │  GET /v1/download-glider-file/         │
     │      osu684/from-glider/               │
     │      osu684_2026_069_0_0.sbd           │
     │ ──────────────────────────────────────► │
     │                                        │
     │  200 OK (streaming binary data)        │
     │  ┌─ chunk 1 ─┐                        │
     │  ┌─ chunk 2 ─┐  → written to file     │
     │  ┌─ chunk N ─┐                        │
     │ ◄────────────────────────────────────── │
     │                                        │
     │  → returns Path("/tmp/osu684.sbd")     │
```

### Code Path

```
client.download_glider_file(name, folder, file, dest)
  ├─► _auth_headers()
  ├─► httpx.stream("GET", url, headers=...)
  │     └─► check_response(response)
  └─► iterate response.iter_bytes()
        └─► write each chunk to dest file
```

## Data Flow: Download Multiple Files (Zip)

```
client.download_glider_files(
    "osu684", "from-glider",
    "/tmp/glider_data.zip",
    filter="*.sbd",
    last_modified_after="202603200000",
)

  GET /v1/download-glider-files/osu684/from-glider
      ?filter=*.sbd
      &lastModifiedAfter=202603200000
  Authorization: Bearer {token}

  → 200 OK (streaming zip archive)
  → written to /tmp/glider_data.zip
  → returns Path("/tmp/glider_data.zip")
```

## Data Flow: Delete a File

```
client.delete_glider_file(
    "osu684", "to-glider", "old_mission.mi"
)

  DELETE /v1/delete-glider-file/osu684/to-glider/old_mission.mi
  Authorization: Bearer {token}

  → 200 OK { confirmation }
```
