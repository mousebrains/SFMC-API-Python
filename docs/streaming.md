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

### Reconnection ownership

`StompConnection` reports closure to its subscriptions; it does not reconnect
itself. A dead connection cannot safely recreate application-specific topics
or state. Code using `open_stream()` directly must therefore open a new
connection and create new subscriptions after closure.

The long-running `sfmc-monitor-glider`, `sfmc-follow`, and
`sfmc-pull-new-downloads` commands implement that application-level recovery.
Their supervisors can preserve command state, refresh authentication, replace
all subscriptions together, and distinguish expected transport failures from
fatal processing errors. A new subscription receives future messages only;
the current SFMC topic API provides no cursor/history catch-up for the offline
interval.

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

The Python client implements this in `sfmc_api.dialog_stream`:

* `ordered_dialog(sub)` — reorder by sequence number.
* `LineAssembler` — reassemble fragments into complete lines
  (fragments are *not* aligned to line boundaries, and line endings
  are mixed CRLF/CR/LF).
* `dialog_lines(sub)` — both at once, for consumers that do not need
  to distinguish a clean shutdown from a dropped session.

```python
from sfmc_api.dialog_stream import dialog_lines

with client.open_stream() as stomp:
    sub = client.subscribe_glider_output("osu685", stomp)
    for line in dialog_lines(sub):
        print(line.text)
```

`ordered_dialog` is still importable from `sfmc_api.monitor_glider`
for existing code.

## Sessions: one stream, many consumers

A `StompSubscription` feeds one queue, so one consumer.  That is
enough when a program does one thing with a topic, but not when
several parts need the same stream at once — logging the dialog *while*
a command waits for its reply.  Subscribing twice would work but would
run the reordering buffer twice over duplicated server traffic.

`GliderSession` subscribes once per topic, runs the ordering and
reassembly pipeline once, and fans the result out:

```
             ┌──────────────────── GliderSession ────────────────────┐
             │                                                       │
STOMP topic  │  one subscription → ordered_dialog ──┬─► LineAssembler│
   ──────────┼─►                                    │        │       │
             │                                      ▼        ▼       │
             │                            ┌───────────┐ ┌─────────┐  │
             │                            │ raw b'cast│ │ b'cast  │  │
             │                            └─────┬─────┘ └─┬───┬───┘  │
             │  raw_dialog_listener()  ◄────────┘         │   │      │
             │  Listener (bounded queue) ◄────────────────┘   │      │
             │  on_line(callback)        ◄────────────────────┘      │
             │                                                       │
             │  supervised by StreamSupervisor: reconnects with      │
             │  backoff, so listeners stay valid across drops        │
             └───────────────────────────────────────────────────────┘
```

```python
with client.session("osu685", topics=["dialog", "connections"]) as session:
    session.on_line(lambda line: print(line.text))
    for event in session.listen("connections"):
        print(event)
```

### Lines or raw chunks

`dialog_listener()` / `on_line()` give reassembled lines, and are what
almost everything wants — logging, parsing surfacings, the follower.

`raw_dialog_listener()` / `on_raw_dialog()` give the ordered chunks
*before* line assembly, exactly as they arrived. Use them when you
match against the stream rather than against lines. The distinction
matters for one specific reason: a terminal prompt such as
`GliderDos N -1 >` carries no trailing newline, so it never completes a
line — it stays in the assembler's buffer and is discarded at the
session boundary, logged as `stream boundary discarded N-byte
unterminated fragment`. A line consumer therefore never sees an idle
prompt at all. `sfmc-xml-engine` reads the raw stream for exactly this
reason; see [xml_engine.md](xml_engine.md).

Listener queues are bounded and drop their **oldest** entry when a
consumer falls behind, counting the loss in `Listener.dropped`.  A
consumer that must not miss data checks that count rather than
trusting a silent stream.

`session.epoch` increments once per successfully subscribed session:
a consumer that captured an epoch and later sees a different one knows
the stream dropped and reconnected in between, and therefore that it
may have missed data.  That is how `CommandChannel` detects a drop
mid-capture.

## Reconnect supervision

All the long-running commands (`sfmc-monitor-glider`, `sfmc-follow`,
`sfmc-pull-new-downloads`) and `GliderSession` share one supervisor,
`sfmc_api.stream_reconnect.StreamSupervisor`.  It owns token refresh
before a retry, session numbering, `STREAM_BOUNDARY` logging,
connect/disconnect notification, offline accounting, worker-failure
classification, and the backed-off reconnect.  Callers supply only
what differs, through hooks (`setup`, `on_subscribed`, `on_idle`,
`on_session_end`).

Failure policy: a worker raising `StompError` — or a session raising
any `SFMCError` — is a transient loss and reconnects.  Any other
worker exception is a code fault and propagates, because running on
would look healthy while silently doing nothing.
