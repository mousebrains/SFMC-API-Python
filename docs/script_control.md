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

## Capturing a Command's Reply

> **A 200 from `submit-command` means SFMC queued the command, not
> that the glider ran it.**  A Slocum is underwater most of a mission;
> a command submitted then sits queued until the next surfacing, which
> may be hours away.  `send_command()` returns that acceptance and
> nothing more.

Whatever the glider says back arrives on a *different* channel — the
dialog topic `/topic/glider-link-output/{gliderId}` — and that topic
carries no correlation handle.  No request id, no framing, no
per-caller channel: it is one shared terminal that other pilots, the
SFMC script engine, and the glider's own unprompted chatter all write
to.  So matching a reply to a command is a heuristic, and
[`CommandChannel`](../src/sfmc_api/commands.py) reports that in its
result rather than hiding it.

```python
with client.command_channel("osu685") as chan:
    reply = chan.send("sensor m_battery")
    if reply.complete:
        print(reply.text)
    else:
        print(f"no reply: {reply.reason}")
```

### What the channel guarantees

```
┌──────────┐                          ┌──────────────┐        ┌────────┐
│  Caller  │                          │  SFMC Server │        │ Glider │
└────┬─────┘                          └──────┬───────┘        └───┬────┘
     │                                       │                    │
     │ ① subscribe to dialog FIRST           │                    │
     │   (a fast reply must not beat us)     │                    │
     │ ─────────────────────────────────────►│                    │
     │                                       │                    │
     │ ② PUT /v1/submit-command/osu685       │                    │
     │ ─────────────────────────────────────►│                    │
     │   ◄── 200 "accepted"  (NOT "ran")     │                    │
     │                                       │ ── if surfaced ──► │
     │                                       │                    │
     │ ③ collect dialog lines until          │  ◄── output ────── │
     │   terminator | quiet | timeout        │                    │
     │  ◄────────────────────────────────────│                    │
     │                                       │                    │
     │ ④ CommandReply(complete, reason,      │                    │
     │      correlated, dropped_lines)       │                    │
```

1. The dialog listener is attached **before** the command is
   submitted.  Subscribing afterwards races the reply.
2. One reply-capturing command runs at a time per glider — two
   overlapping captures would attribute each other's output.
3. A stream drop mid-capture returns `reason="disconnected"`.  The
   command is **never resubmitted**: SFMC may already have delivered
   it, and a repeated `put` on a live glider is a real hazard.

### Reading the reply honestly

| Field | Meaning |
|-------|---------|
| `complete` | Capture reached a defined stop **and heard something** (`terminator`, `quiet`, `max_lines`).  `False` means partial or absent — check `reason`. |
| `reason` | `terminator`, `quiet`, `max_lines`, `silent`, `timeout`, `disconnected`, `no_echo`. |
| `correlated` | `True` only when anchored to an echo of *this* command.  `False` means "what appeared on a shared terminal during the window". |
| `dropped_lines` | Non-zero means the capture lagged and `lines` has gaps. |
| `glider_connected` | Sampled only when no reply arrived, so you can tell "SFMC is broken" from "the glider is underwater". |
| `raw_response` | SFMC's acceptance body — again, not the glider's answer. |

**A missing reply is not an exception.**  Silence is the normal case
for a submerged glider, so `send()` returns `complete=False` rather
than raising.  Exceptions are reserved for failures to *submit*.

### Stop conditions

`ReplyPolicy` controls when capture ends; override per call:

```python
reply = chan.send("sensors", timeout=120, quiet=10)          # chatty command
reply = chan.send("run", until=re.compile(r"^DONE$"))        # known last line
```

Defaults are sized for an Iridium link (`timeout=45`, `quiet=5`).
There is no dependable prompt sentinel in Slocum dialog over a
fragmented link, so the quiet window is the usual end-of-reply signal;
`until=` is more precise when you know the command's final line.

### What a live surfacing actually looked like

Measured against the `osusim` simulator mid-mission, because it shapes
how much you should trust an uncorrelated capture:

* The glider's own banner states the rule — `Hit ! <GliderDos cmd>  to
  execute <GliderDos cmd>` — so a command sent to a glider **running a
  mission** needs the `!` prefix: `!get m_de_oil_vol`.
* The surfacing was dominated by a file transfer.  The first command's
  capture collected **615 lines of mission dialog** (`behavior
  goto_wpt_601: …`, `Total Bytes sent/received: …`) and not one line of
  answer.  It was correctly reported `correlated=False` — which is
  precisely what that flag is for.  Treat an uncorrelated capture as a
  transcript of a shared terminal, not as a reply.
* Three later commands landed in a stretch with **no dialog at all**.
  They are reported `silent`, not `complete`.
* Whether the glider executed them is unknown: nothing came back either
  way.  SFMC accepting a command remains the only thing a 200 proves.

The practical lesson: send commands when the link is quiet, prefer
`until=` over a quiet window when you know the answer's shape, and
check `correlated` before reading `lines` as an answer.

### Echo anchoring — verify before trusting

If the dockserver echoes submitted commands back onto the dialog
topic, capture can be anchored to that echo, which is the strongest
correlation available:

```python
reply = chan.send("sensor m_battery", echo_anchor=True)
```

Whether your server echoes is a property of the server, not of this
library, so `echo_anchor` defaults to `False`.  Find out first:

```bash
sfmc-api probe-command osu685 'sensor m_battery'
```

That dumps raw dialog frames with arrival offsets and sequence
numbers, uninterpreted, during a surfacing.  If the command text comes
back, `echo_anchor=True` is trustworthy; if it does not, every capture
would end as `no_echo` and the time-window default is the honest
option.

### From the command line

```bash
sfmc-api send-command osu685 'sensor m_battery'            # accept and return
sfmc-api send-command osu685 'sensor m_battery' --wait     # capture the reply
sfmc-api send-command osu685 'sensors' --wait --timeout 120 --quiet-for 10
sfmc-api send-command osu685 'run x.mi' --wait --until '^DONE$'
sfmc-api probe-command osu685 'sensor m_battery'           # diagnostic
```

`--wait` exits `2` when no complete reply arrived, so a script can
distinguish "the glider answered" from "SFMC took the command and the
glider said nothing".

### See also

* [`examples/send_command.py`](../examples/send_command.py) — a
  runnable script covering blocking sends, futures, and how to read a
  reply honestly.
* [async_operations.md](async_operations.md) — the same `Future` idiom
  applied to every other endpoint.
* [streaming.md](streaming.md) — the session and fan-out model the
  channel is built on.
