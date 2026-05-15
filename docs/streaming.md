# Real-Time Streaming Data Flow

> **What is streaming for?**  Streaming lets you watch a glider while
> it is in the water without polling.  You subscribe to *topics* (e.g.
> connection events, dialog output) and receive updates as they
> happen.  Useful for live dashboards and follower plugins.  See
> [glossary.md](glossary.md) for vocabulary.

## Overview

The SFMC server provides real-time event streaming via **STOMP over
SockJS**.  The Python client handles both protocols transparently:

* **SockJS** provides the WebSocket transport layer with fallback
  support and session management.
* **STOMP** (Simple Text Oriented Messaging Protocol) provides
  publish/subscribe messaging on top of the transport.

## Architecture

```
┌────────────────────────────────────────────────────────┐
│                      SFMCClient                        │
│                                                        │
│  open_stream() ──► StompConnection                     │
│                      │                                 │
│                      ├── WebSocket (SockJS transport)  │
│                      ├── STOMP protocol framing        │
│                      └── Background receiver thread    │
│                                                        │
│  subscribe_*() ──► StompSubscription                   │
│                      │                                 │
│                      └── Queue[dict] ──► iterator      │
└────────────────────────────────────────────────────────┘
```

## Connection Sequence

```
┌──────────┐              ┌──────────────┐
│  Client  │              │  SFMC Server  │
└────┬─────┘              └──────┬───────┘
     │                           │
     │  ① WebSocket connect      │
     │  wss://{host}/sfmc/api/   │
     │    sfmc-stomp/{srv}/{sess}│
     │    /websocket             │
     │    ?access_token={token}  │
     │ ─────────────────────────►│
     │                           │
     │  ② SockJS open frame      │
     │  "o"                      │
     │ ◄─────────────────────────│
     │                           │
     │  ③ STOMP CONNECT          │
     │  a["CONNECT\n             │
     │     accept-version:1.2\n  │
     │     heart-beat:0,0\n      │
     │     \n\0"]                │
     │ ─────────────────────────►│
     │                           │
     │  ④ STOMP CONNECTED        │
     │  a["CONNECTED\n           │
     │     version:1.2\n         │
     │     \n\0"]                │
     │ ◄─────────────────────────│
     │                           │
     │  ⑤ STOMP SUBSCRIBE        │
     │  a["SUBSCRIBE\n           │
     │     id:sub-0\n            │
     │     destination:/topic/...│
     │     \n\0"]                │
     │ ─────────────────────────►│
     │                           │
     │  ⑥ STOMP MESSAGEs         │
     │  a["MESSAGE\n             │
     │     subscription:sub-0\n  │
     │     \n                    │
     │     {JSON payload}\0"]    │
     │ ◄─────────────────────────│
     │  (repeats for each event) │
     │                           │
     │  ⑦ STOMP DISCONNECT       │
     │ ─────────────────────────►│
     │                           │
     │  WebSocket close          │
     │ ◄────────────────────────►│
```

## SockJS Framing

SockJS wraps STOMP frames in a transport layer.  Messages from the
server are prefixed with a type character:

| Prefix | Meaning | Contains |
|--------|---------|----------|
| `o` | Open | Connection established |
| `h` | Heartbeat | Keep-alive |
| `a[...]` | Array | JSON array of STOMP frame strings |
| `c[...]` | Close | Close code and reason |

Messages *to* the server are sent as JSON arrays of STOMP frame
strings: ``["FRAME\nheader:value\n\nbody\0"]``

## STOMP Topics

| Python method | Topic pattern | ID source |
|--------------|---------------|-----------|
| `subscribe_connection_events()` | `/topic/glider-connections-{gliderId}` | `get_glider_details().data.id` |
| `subscribe_glider_output()` | `/topic/glider-link-output/{gliderId}` | `get_glider_details().data.id` |
| `subscribe_script_events()` | `/topic/glider-script-assignment-updates-{gliderId}` | `get_glider_details().data.id` |
| `subscribe_zmodem_transfer_events()` | `/topic/new-and-updated-zmodem-transfers-{deploymentId}` | `get_active_deployment_details().data.id` |
| `subscribe_deployment_events()` | `/topic/low-freq-glider-deployment-updates-{deploymentId}` | `get_active_deployment_details().data.id` |

Note: Zmodem and deployment subscriptions use the **deployment ID**,
not the glider ID.  The client resolves this automatically.

## Usage Patterns

### Basic: Stream connection events

```python
from sfmc_api import SFMCClient

with SFMCClient() as client:
    with client.open_stream() as stomp:
        sub = client.subscribe_connection_events("osu684", stomp)
        for event in sub:  # blocks until next event
            print(event)
```

### Multiple subscriptions on one connection

```python
with SFMCClient() as client:
    with client.open_stream() as stomp:
        conn_sub = client.subscribe_connection_events("osu684", stomp)
        script_sub = client.subscribe_script_events("osu684", stomp)

        # Process from either subscription using threads or polling:
        import threading

        def print_events(name, sub):
            for event in sub:
                print(f"[{name}] {event}")

        t1 = threading.Thread(target=print_events, args=("conn", conn_sub))
        t2 = threading.Thread(target=print_events, args=("script", script_sub))
        t1.start()
        t2.start()
```

### Non-blocking with timeout

```python
from queue import Empty

with SFMCClient() as client:
    with client.open_stream() as stomp:
        sub = client.subscribe_connection_events("osu684", stomp)
        while True:
            try:
                event = sub.get(timeout=5.0)
                if event is None:
                    break  # subscription closed
                print(event)
            except Empty:
                print("No event in 5 seconds, still waiting...")
```

## Code Path

```
SFMCClient.open_stream()
  ├─► _ensure_auth()
  └─► StompConnection(config, token)
        └─► .connect()
              ├─► _sockjs_url() → wss://host/.../websocket?access_token=...
              ├─► ws_connect(url)
              ├─► recv SockJS "o" frame
              ├─► send STOMP CONNECT (as JSON array)
              ├─► recv STOMP CONNECTED
              └─► start _receive_loop thread

SFMCClient.subscribe_connection_events(name, stomp)
  ├─► _get_glider_id(name) → GET /v1/gliders/{name}
  └─► stomp.subscribe("/topic/glider-connections-{id}")
        ├─► send STOMP SUBSCRIBE frame
        └─► return StompSubscription(queue)

StompConnection._receive_loop()  [background thread]
  └─► while not closing:
        ├─► ws.recv()
        ├─► _sockjs_decode() → list of STOMP frame strings
        └─► for each MESSAGE frame:
              ├─► parse subscription ID from headers
              ├─► json.loads(body)
              └─► put payload into subscription's queue
```

## Glider Output Ordering

The `subscribe_glider_output()` topic delivers dialog data with
``sequenceNumber`` fields.  Messages may arrive out of order over the
network.  The Node.js reference implementation queues out-of-order
messages and replays them when gaps are filled (with wraparound at
sequence 9007199254740991 → 0).

The Python client provides ``sfmc_api.monitor_glider.ordered_dialog()``
which implements this reordering.  The installed ``sfmc-monitor-glider``
script uses it to reassemble dialog output into complete lines.
