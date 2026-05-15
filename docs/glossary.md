# Glossary

Terms used throughout these docs.  Slocum firmware conventions are
marked **(Slocum)**; SFMC web service conventions are marked **(SFMC)**.

## General concepts

**Slocum glider** — An autonomous underwater vehicle made by Teledyne
Webb Research.  It moves through the water by changing buoyancy
(diving and climbing in a sawtooth pattern called a *yo*) and pitches
its body to steer.  It carries scientific sensors and surfaces
periodically to upload data and receive new commands.

**SFMC** — Slocum Fleet Management Center.  The web service and
backend that pilots use to monitor and command gliders.  This
repository is a client library that talks to the SFMC REST API.

**Pilot** — The person (or group) operating a glider.  In a research
group the pilot is often the chief scientist or a designated
glider-ops engineer.

## Mission lifecycle

**Mission** — A self-contained set of behaviors loaded onto the
glider, defined by a `.mi` file.  Examples: "fly a transect at 30 m
depth," "loiter and sample for 24 hours."

**Deployment (SFMC)** — One instance of a glider being put in the
water.  A deployment has a start time, an end time, and accumulates
all the data the glider sends home while in the water.  Most SFMC
endpoints scope queries to the active deployment.

**Surfacing** — When the glider rises to the surface, opens its
Iridium link, and exchanges data with SFMC.  Each surfacing is the
opportunity to send the glider new files and receive its science
data.

**Connection (SFMC)** — A single SFMC ↔ glider communication session
during a surfacing.  Connections have IDs, start/end times, and host
the dialog stream.

## Plans, files, and commands

**Plan** — A configuration that controls one aspect of glider
behavior.  Plans live on the SFMC side and are pushed to the glider
on its next surfacing.  See [plans.md](plans.md) for the full list.

**Waypoint plan** — The list of GPS coordinates the glider should
visit.  Pushed to the glider as a `goto_l{N}.ma` file.

**Yo plan** — The dive/climb profile: how deep, how fast, what angle.
Pushed as a `yo*.ma` file.

**Surface plan** — Rules for when the glider should surface (every
N seconds, at fixed UTC times, when it hits a waypoint).

**Sampling plan** — Which sensors are active and how often they
sample.

**Abort plan** — Conditions that make the glider give up its mission
and surface immediately (e.g. battery low, water leak, depth limit
exceeded).

**Script (SFMC)** — A high-level command sequence stored on the SFMC
server that controls glider operations.  Most are built into the
firmware (e.g. `dive10`, `surface`); some are custom.  See
[script_control.md](script_control.md).

**Command** — A single low-level directive sent to the glider during
a surfacing (e.g. `put c_science_on 0`).  Most users will not need
this — use scripts instead.

## Files

**`.ma` file (Slocum)** — *Mission Argument* file.  Supplies
parameters to one of the glider's built-in behaviors.  `goto_l30.ma`
is a waypoint plan; `yo15.ma` is a yo (dive) plan.  See
[`src/sfmc_api/ma_writer.py`](../src/sfmc_api/ma_writer.py) for the
format.

**`.mi` file (Slocum)** — *Mission* file.  Defines which behaviors
the glider runs and in what order.  Each behavior may have an
`args_from_file` directive that pulls parameters from a `.ma` file in
a specific slot.

**`.sbd` file (Slocum)** — *Short Binary Data*.  Compressed flight
data files (engineering telemetry — battery, depth, attitude, GPS).
Transmitted to shore between dives.  The "SBD list" controls which
flight data files are queued for transmission.

**`.tbd` file (Slocum)** — *Tiny Binary Data*.  Compressed science
data files (sensor readings: temperature, salinity, oxygen, etc.).
The "TBD list" controls science file transmission.

**Goto file (Slocum)** — A `goto_l{N}.ma` file containing waypoints
for the `goto_list` behavior.  See
[`src/sfmc_api/ma_writer.py`](../src/sfmc_api/ma_writer.py).

## Folders (server side)

**`to-glider`** — Files queued for delivery to the glider on its
next surfacing.  This is where your follower's `.ma` files end up.

**`from-glider`** — Files the glider has uploaded to SFMC (mostly
`.sbd` and `.tbd` data).  Read-only; you cannot delete from here.

**`to-science`** — Science-payload configuration files.

**`configuration`** — Static configuration files.  Deletable via
the API for cleanup; usually you should not touch these.

## Communications

**Iridium** — The satellite network the glider uses to talk to
shore.  Bandwidth is limited (~340 bytes/packet), expensive, and
session length is short — which is why files are compressed and why
the dialog stream arrives in fragments.

**Zmodem** — A file-transfer protocol used over the Iridium link
during a surfacing.  The SFMC tracks Zmodem transfer state per
connection.

**Dialog output** — The raw text stream from the glider during a
surfacing.  Contains everything: GPS fixes, sensor printouts, file
transfer status, error messages.  Dialog text arrives chopped into
fragments and out of order; sequence numbers let you reassemble it.

**STOMP** — Simple Text Oriented Messaging Protocol.  The
pub/sub protocol SFMC uses to push real-time events to clients (over
WebSocket / SockJS).  See [streaming.md](streaming.md).

**SockJS** — A WebSocket fallback protocol used by SFMC's STOMP
endpoint.  This client speaks raw SockJS framing over WebSocket.

## This library

**Client (`SFMCClient`)** — The main Python class for making REST
API calls.  Handles authentication, retries, and rate-limit handling
automatically.

**Follower** — A user-supplied Python class that watches a glider's
surfacings and generates new plan files.  See
[follow_glider.md](follow_glider.md).

**Dry-run** — A mode in which generated files are printed instead of
uploaded.  Always use for development; safe to run against a live
glider when you want to *see* what your follower would do without
risking the mission.

**Replay** — A mode in which the live dialog stream is replaced by a
recorded log file.  Combine with dry-run to iterate on a follower
entirely offline.
