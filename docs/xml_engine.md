# XML Script Engine

SFMC runs a state machine, described in XML, beside the dockserver.
When a glider surfaces, that machine watches the dialog and sends
commands — download these files, get device reports, resume the
mission.  `sfmc-xml-engine` parses those scripts and executes them from
Python.

Three uses, in increasing order of consequence:

1. **Understand a script.**  `--describe` renders the state machine as
   readable text, so you can review one without reading XML.
2. **Replay one offline.**  `--replay` runs a script against a recorded
   dialog log and prints what it would have sent.  Nothing is
   transmitted, and no server is contacted.
3. **Drive a glider.**  `--glider` runs it live.  Even then nothing is
   sent unless you also pass `--send`.

If terms like "dialog output," "surfacing," or "GliderDos" are
unfamiliar, see the [glossary](glossary.md).

> **This can steer a glider.**  Read [Safety](#safety) before using
> `--send`.  The engine's defaults are chosen so that running it out of
> curiosity cannot command anything.

## Quick Start

```bash
# Show the state machine, no server contact
sfmc-xml-engine riot.xml --describe

# Replay against a recorded log (sfmc-monitor-glider output or raw dialog)
sfmc-xml-engine riot.xml --replay osusim-20260816T130516Z.log

# Live, but dry: prints WOULD SEND and transmits nothing
sfmc-xml-engine riot.xml --glider osusim

# Live, for real
sfmc-xml-engine riot.xml --glider osusim --send --max-runtime 3600
```

## The script language

The whole language is seven elements and four attributes:

```xml
<gliderScript>
  <initialState name="waitForConnect">        <!-- also <state>, <finalState> -->
    <transitions>
      <transition matchExpression="Vehicle Name:" toState="sendData">
        <action type="glider" command="!dockzr -archive *"/>
      </transition>
      <transition timeout="10" toState="giveUp"/>   <!-- 10 MINUTES -->
    </transitions>
  </initialState>
  <finalState name="giveUp"/>
</gliderScript>
```

*In state S, wait for glider output matching a regex or for a timeout;
optionally send commands; move to state T.*  That is all of it.

| Element | Meaning |
|---------|---------|
| `<gliderScript>` | Root.  Exactly one per file. |
| `<initialState name="...">` | Where execution begins.  Exactly one. |
| `<state name="...">` | An ordinary state. |
| `<finalState name="...">` | Entering one ends the run. |
| `<transitions>` | Container for a state's outgoing edges. |
| `<transition>` | One edge.  Tried in document order. |
| `<action type="glider" command="...">` | Send a command when this edge is taken. |

| Attribute | On | Meaning |
|-----------|-----|---------|
| `name` | states | State name.  Must be unique. |
| `matchExpression` | `transition` | Python-compatible regex against glider output.  Empty means *fire on entry*. |
| `timeout` | `transition` | Wait this many **minutes**, then take this edge. |
| `toState` | `transition` | State to enter.  Must exist. |
| `type` | `action` | Always `glider`.  Anything else is refused. |
| `command` | `action` | Text to send, e.g. `s *.sbd *.tbd` or `Ctrl-R`. |

A transition carries a `matchExpression` **or** a `timeout`, never
both.  Across the 20-script reference corpus that holds without
exception: 1023 `matchExpression` transitions have no timeout, and all
204 timeout transitions have no regex.

### `timeout` is in minutes

Nothing in the XML says so, which is exactly why it is worth stating.
Every author in the reference corpus documents it as minutes in their
own comments:

- `riot.xml`, beside all 22 of its timers:
  `<!-- If nothing within 10 minutes, then go back and wait for a new connection -->`
- `vacuum_test_send_data_2hrs.xml`: `timeout="120"` paired with
  `<!-- a 120 minute (2 hours) timeout -->`, and a filename that says
  the same

Reading it as seconds is a 60x error in the dangerous direction: a
script meant to wait ten minutes for a glider to answer would instead
give up after ten seconds and act on the silence.  The engine converts
at parse time — `Transition.timeout_seconds` is the value it runs on,
`Transition.timeout_minutes` is the number the file wrote — and the
unit is pinned by a test rather than left to be rediscovered.

### Control characters

Control characters are written as literal text: `Ctrl-C`, `Ctrl-R`,
`Ctrl-W`, `Ctrl-F`.  The dockserver translates them.  This is confirmed
live — sending the string `Ctrl-C` to a surfaced glider produces:

```
^C3159732    behavior surface_5: User Hit a Control-C, terminating the mission
3159732    behavior surface_5: STATE Active -> Mission Complete
3159733    behavior ?_-1: run_mission(): Mission completed: MS_COMPLETED_NORMALLY(-1)
```

Note the echo is `^C` inline at the head of a glider output line, not a
separate line echoing the text you sent.  So a reply capture of a
control character is **not** correlated by echo anchoring — see
[script_control.md](script_control.md).

### Empty `matchExpression`

An empty `matchExpression` fires on entry to the state, without waiting
for any input.  This appears in the corpus as the first transition of a
start state, paired with an action.  It is read that way because it is
the only reading under which such a state does anything, and every such
firing is traced explicitly so the interpretation can be checked
against real SFMC behaviour.

An immediate-transition *cycle* raises `ScriptError` rather than
spinning and sending commands forever.

## Matching

Glider output is matched against a rolling buffer that includes text
not yet terminated by a newline.  That is the default (`--match-mode
buffer`) and it is the faithful one: nine of the twenty reference
scripts match a GliderDos prompt —

```
(Glider(Dos|LAB) [AIN] (0|(-?[1-9]+))) >
```

— and a terminal prompt carries no trailing newline, so a line-oriented
matcher would never see one.

`--match-mode line` matches only complete lines.  It is cheaper to
reason about and adequate for scripts that match ordinary dialog
output, but it cannot match a prompt.  That inability is pinned by a
test rather than left to be discovered at sea.

### Live matching is line-oriented regardless of mode

**A limitation worth knowing before trusting a prompt-matching script
live.**  `run_live()` consumes `GliderSession.dialog_listener()`, and
that pump publishes only *complete* lines, dropping any unterminated
tail at a session boundary.  So in the live path the state machine sees
newline-terminated lines whatever `--match-mode` says, and `buffer`
mode's advantage does not reach it.

In practice a prompt is usually followed by more output, which
terminates it — a capture from osusim on 2026-08-16 shows
`GliderDos N -1 >` arriving as an ordinary line twice.  But the same
session also discarded a 16-byte unterminated fragment at its boundary,
and `GliderDos N -1 >` is exactly 16 bytes: a trailing idle prompt, the
last thing a glider says before going quiet, can be dropped before any
consumer sees it.

Replay does not have this problem — `--replay` feeds raw text straight
to the matcher.

### Keeping the connection up

SFMC drops the network connection after roughly **five minutes of
inactivity**.  During a mission this never matters: the glider produces
dialog at least once a minute.  It matters when the glider is sitting
at a GliderDos prompt saying nothing — exactly where a script with a
ten-minute timer would wait.

`run_live()` therefore sends `Ctrl-M` — a carriage return, in the same
literal form the scripts use for every other control character — after
**four minutes** of dialog silence, leaving a minute of margin.
Anything the script itself sends also defers it, since what counts is
silence on the wire.

Validated against osusim on 2026-08-16: `Ctrl-M` at this cadence held
the link `connected` for 6m47s across six keepalives, on a glider that
had otherwise been idle-dropping.

Two things this got wrong on the first attempt, both found by running
it against a live glider rather than by reasoning about it:

- **An empty command is not a bare return.**  SFMC rejects an empty
  body with `HTTP 400`, so nothing was sent and the link dropped on
  schedule.
- **A keepalive failure must not be fatal.**  That 400 raised out of
  `run_live()` and killed the run.  Losing a connection is recoverable;
  killing a process that is part-way through steering a glider is not.
  Keepalive errors are now caught, reported, and the run continues.

```bash
sfmc-xml-engine riot.xml --glider osu685 --send --keepalive 120  # tighter
sfmc-xml-engine riot.xml --glider osu685 --send --keepalive 0    # disable
```

Keepalives are transmissions, so they only happen with `--send`.  A dry
run that waits at a quiet prompt will be dropped after ~5 minutes;
that is the cost of a default that guarantees a dry run sends nothing.

## Safety

This module can command a glider, so the defaults are set against that:

- **Nothing is sent without `--send`.**  The default reports what it
  would do.  The other default is a program that steers a glider the
  first time somebody runs it to see what it does.
- **Unknown action types are refused at parse time** rather than
  assumed to be commands.  Every action in every known script is
  `type="glider"`; anything else stops the run before it starts.
- **Dangling `toState`, invalid regexes, and malformed XML all fail at
  parse time**, not weeks into a deployment when an edge is first
  taken.
- **An immediate-transition cycle raises** instead of spinning and
  sending commands forever.
- **Replay feeds only `DIALOG` lines.**  Capture logs interleave real
  glider dialog with the capturing tool's own bookkeeping (`POLL`,
  `SEND`, `REPLY`); feeding that back would let the tool drive the
  state machine.
- **`--max-runtime`** bounds a live run regardless of what state it is
  in.

Two gliders must not be driven by two engines at once.  SFMC's own XML
engine and this one will both react to the same dialog and both send
commands.  Clear or pause the assigned script before running this one
live.

## Python API

`XmlStateMachine` is pure: no I/O, no clock of its own, no knowledge of
SFMC.  You drive it and decide what to do with the actions it returns.

```python
from sfmc_api import parse_script, XmlStateMachine

script = parse_script("riot.xml")
machine = XmlStateMachine(script, on_trace=print)

for action in machine.start():
    print("would send:", action.command)

for action in machine.feed("Hit Control-R to RESUME the mission\r\n"):
    print("would send:", action.command)

print(machine.state, machine.finished, machine.timeout_remaining)
```

| Call | Returns |
|------|---------|
| `machine.start()` | Actions from entering the initial state |
| `machine.feed(text)` | Actions triggered by that glider output |
| `machine.check_timeout()` | Actions if this state's timer is due |
| `machine.state` | Current state name |
| `machine.finished` | `True` once a final state is reached |
| `machine.timeout_remaining` | Seconds until the timer fires, or `None` |

Because it has no clock, tests inject one:

```python
clock = [0.0]
machine = XmlStateMachine(script, now=lambda: clock[0])
machine.start()
clock[0] = 600.0          # ten minutes: timeout="10"
actions = machine.check_timeout()
```

The two drivers:

```python
from sfmc_api import replay, run_live

# Offline, against a recorded log.  Sends nothing.
with open("dialog.log") as fh:
    actions = replay(script, fh)

# Live.  Sends nothing unless send=True.
with SFMCClient() as client:
    actions = run_live(client, "osusim", script, send=False, max_runtime=3600)
```

`replay()` does not simulate timeouts.  Recorded dialog carries no
timing, so a replay exercises the *match* transitions and nothing more
— stated here rather than silently approximated, because a script whose
behaviour depends on its timers is only partly exercised offline.

## CLI reference

```
sfmc-xml-engine SCRIPT.xml [--describe | --replay LOG | --glider NAME]
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--describe` | — | Print the state machine and exit |
| `--replay DIALOG.log` | — | Replay against a recorded log, sending nothing |
| `--glider NAME` | — | Run against a live glider |
| `--send` | off | **Actually transmit commands.**  Without it, nothing is sent |
| `--match-mode {buffer,line}` | `buffer` | `buffer` can match an unterminated prompt; `line` cannot |
| `--max-runtime SECONDS` | — | Stop after this long regardless of state |
| `--keepalive SECONDS` | 240 | With `--send`, `Ctrl-M` after this much dialog silence (`0` disables) |
| `--host HOSTNAME` | — | Select host from a multi-host credentials file |
| `--credentials PATH` | `~/.config/sfmc/credentials.json` | Credentials file |

Exit status is `2` when the script fails to parse, `0` otherwise.

## What is validated, and what is not

**Validated.**  All 20 scripts in the reference corpus parse, including
`riot.xml` (25 states, 22 timers).  Replaying `riot.xml` against dialog
captured from osusim during a 19:01Z surfacing produces a seven-action
sequence — `dockzr`, send data twice, `Ctrl-W` for device info,
`dockzr` again, `Ctrl-R` to resume — which matches the Basic Flow
documented in that script's own header, and matches what the live SFMC
engine did: the capture contains the dockserver echoing the same
command the emulator chose.  Control-character delivery is confirmed
live, as shown above.

**Not yet validated.**  Two things:

- **Timer behaviour end-to-end against a live glider.**  Replay cannot
  exercise it, and the minutes-vs-seconds unit is established from
  script comments and operator confirmation rather than from watching a
  timer fire on a server.
- **The keepalive.**  The 90-second inactivity drop is reported by the
  operator; the 60-second bare return is a response to it that has not
  yet been observed holding a connection open across a long quiet
  stretch.

Treat a timer-heavy script's first live run as an experiment, with
`--max-runtime` set.

## See also

- [Script Control](script_control.md) — command submission and reply capture
- [Monitor Glider](monitor_glider.md) — producing the dialog logs `--replay` reads
- [Streaming](streaming.md) — the dialog topic underneath `run_live()`
