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

`from-glider` is read-only because it contains data the glider has
already uploaded — deleting would lose mission science.  Download a
copy first if you need to clear space.  `to-glider` and `to-science`
hold files you've queued for the glider; deletions are allowed.

A typical workflow:

1. Generate or fetch plan files (e.g. `goto_l30.ma`).
2. Upload them to `to-glider`.
3. The glider downloads them on its next surfacing.
4. The glider records data into `from-glider`.
5. You download data from `from-glider` for analysis.

See [glossary.md](glossary.md) for the file-type abbreviations
(`.ma`, `.mi`, `.sbd`, `.tbd`).

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

## Default Download Directory

When no explicit path is given, downloads go to the client's
`download_dir`, resolved as:

1. ``download_path=`` passed to ``SFMCClient()``
2. ``rootDownloadPath`` from the credentials file
3. The current working directory

```python
# Explicit path
client.download_glider_file("osu685", "from-glider", "data.sbd", "/tmp/data.sbd")

# Default: saves to download_dir/data.sbd
client.download_glider_file("osu685", "from-glider", "data.sbd")
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
