# Design: pluggable control engines

**Status:** proposal, not implemented.
**Depends on:** the session / command / executor layers in PR #12.

## Context

`sfmc-follow` lets a user plug their own logic into the middle of a
pipeline: the framework connects, parses dialog into
`SurfacingEvent`s, calls `on_surfacing`, and uploads whatever files the
follower produces.  That shape works, and users like it.  It is also
narrow in three ways:

1. **One input.** Only dialog, and only the part of it the parser
   recognises as a surfacing.  A user who wants script transitions,
   Zmodem activity, or connection events has to build that themselves.
2. **One output.** Files to `to-glider` / `to-science`.  Sending a
   command, updating a waypoint plan, or assigning a script means
   reaching around the framework to the client.
3. **No feedback.** Whatever the follower does out-of-band, the
   framework knows nothing about, so it cannot log it, retry it, rate
   limit it, or refuse it in a dry run.

The proposal is to generalise: a **control engine** receives a single
stream of tagged events from any set of sources, and can act on
essentially the whole SFMC API.  `sfmc-follow` becomes one control
engine; `sfmc-monitor-glider` becomes another.

## Goals

- One event stream, several sources, each event tagged with its source
  and the time it was received.
- Dialog delivered **as assembled lines by default**, with raw chunks
  available for engines that want them.
- Access to the whole SFMC API from engine code, without the engine
  managing threads, futures, reconnects, or rate limits.
- Engines can add sources at runtime (subscribe to another topic).
- An engine that a scientist writes on a Tuesday should still be
  running correctly in six weeks.

## Non-goals

- Not a scheduler, not a workflow engine, not a DSL.  Engines are
  Python classes.
- Not a sandbox.  An engine runs with the operator's credentials and
  can steer a glider.  Safety here is about *accidents*, not malice.
- Not a replacement for `SFMCClient`.  Scripts that want a blocking
  call should keep using one.

## Design principle: make the wrong thing hard

The intended authors are glider scientists, not systems programmers.
Every design choice below is biased toward *removing opportunities for
concurrency bugs* rather than toward flexibility:

| Temptation | What this design does instead |
|---|---|
| Callbacks on many threads | **One** engine thread; `on_event` is never re-entered |
| Futures in engine code | Results come back as ordinary events |
| Silent failures | Errors are delivered as events; ignoring one is logged |
| Locking shared state | Engine state is touched by one thread only |
| Blocking the loop with a download | Outbound work runs on the framework's executor |
| Discovering a bug on a live glider | Replay and dry-run are first-class |

An engine that ignores all of this — writes no locks, checks no
futures, never thinks about threads — should still be correct.  That is
the bar.

## The event model

```python
@dataclass(frozen=True)
class Event:
    source: str          # "dialog", "dialog.raw", "connections", "result", ...
    body: Any            # str for dialog, dict for STOMP topics, ...
    received_at: float   # time.time() when this arrived locally
    seq: int             # monotonic per-engine counter, for ordering/logging
    request_id: int | None = None   # set on "result" / "error"
    tag: str | None = None          # caller's label, echoed back on results
```

`received_at` is the **local host clock**, not the glider's.  Named
explicitly because that confusion is guaranteed otherwise: glider time
appears *inside* dialog text (`Curr Time: ...`) and is what
`DialogParser` extracts.  The two can differ by an hour on a simulator.

### Source tags

| Source | Body | Notes |
|---|---|---|
| `dialog` | `str` — one assembled line | **default**; no trailing newline |
| `dialog.raw` | `str` — one chunk as received | may be a partial line, or several |
| `connections` | `dict` | glider connect/disconnect |
| `scripts` | `dict` | script assignment / state transitions |
| `zmodem` | `dict` | transfer activity |
| `deployment` | `dict` | low-frequency deployment updates |
| `result` | return value of the operation | carries `request_id`, `tag` |
| `error` | the exception | carries `request_id`, `tag` |
| `dropped` | `DroppedNotice(source, count)` | the engine fell behind |
| `stream` | `StreamNotice(state, epoch)` | connected / disconnected / reconnected |
| `tick` | `None` | optional periodic wake-up |

Engines switch on `event.source`.  A `match` statement reads well and
degrades safely — an unknown source falls through the default arm.

### Dialog: lines or chunks

SFMC delivers dialog as fragments that are **not aligned to line
boundaries**: one message may carry half a line, or three lines and a
bit, and they can arrive out of sequence order.  Both forms are
offered because they answer different questions:

```python
class MyEngine(BaseControlEngine):
    sources = ["dialog"]        # assembled lines (default, recommended)
    # sources = ["dialog.raw"]  # chunks, exactly as received
```

- **`dialog`** — the framework runs `ordered_dialog` + `LineAssembler`
  once and emits one event per complete line.  `received_at` is when
  the *first fragment of that line* arrived, so a line that took three
  Iridium frames is timestamped by when the glider began sending it,
  not when it finished.  This is what almost every engine wants.
- **`dialog.raw`** — one event per reordered chunk.  For engines doing
  their own framing (byte accounting, protocol sniffing, verbatim
  capture).  `received_at` is the chunk's arrival.

Both may be selected at once; ordering and assembly still run once.
The interleaving is then well defined and worth stating, because it is
not what a reader assumes: a line is emitted **after** the chunk that
completed it, so a line spanning three chunks appears as
`raw, raw, raw, line` — never before its final fragment.

**Three honest caveats about `dialog.raw`**, which belong in the user
docs and not just here:

1. Chunk boundaries are an artefact of transport, not of meaning.  Code
   that treats a chunk as a unit will break the first time a line is
   split.
2. An unterminated tail at a stream boundary is dropped from `dialog`
   (a half line delivered as whole would corrupt a parse) but *was*
   visible in `dialog.raw`.  The two streams therefore do not contain
   identical bytes across a reconnect.
3. **Replay cannot reproduce chunks.**  Recorded dialog logs store
   lines, so a `dialog.raw` engine cannot be faithfully replayed.  That
   is a strong reason to prefer `dialog` unless chunks are genuinely
   required.

## The engine API

The whole surface an engine author must learn:

```python
class BaseControlEngine:
    sources: list[str] = ["dialog"]      # what to subscribe at start

    # ── you implement ────────────────────────────────────────────
    def on_start(self) -> None: ...
    def on_event(self, event: Event) -> None: ...
    def on_stop(self) -> None: ...

    # ── you call ─────────────────────────────────────────────────
    def request(self, op: str, *args, tag: str | None = None, **kwargs) -> int
    def subscribe(self, source: str) -> None
    def unsubscribe(self, source: str) -> None
    def log(self, msg: str, *args) -> None
    def notify(self, key: str, summary: str, detail: str) -> None
    @property
    def client(self) -> SFMCClient      # escape hatch; see below
```

`request` names an operation by its client method name and returns
immediately with a request id:

```python
def on_event(self, event):
    match event.source:
        case "dialog":
            if "Waypoint" in event.body:
                self.request("get_mission_plan", self.glider, tag="plan")
        case "result" if event.tag == "plan":
            self.plan = event.body          # ordinary state, no locks
        case "error" if event.tag == "plan":
            self.log("plan fetch failed: %s", event.body)
```

**Why a string, not the bound method?**  Because it gives the framework
a place to stand: dry-run interception, write-gating, rate limiting,
and audit logging all need to know *what* is being asked before it
happens.  Passing `client.get_mission_plan` would hand the engine a
direct line and leave the framework blind.  The cost is losing static
type checking on the call — mitigated by validating the name against
`SFMCClient` at engine start, so a typo fails at startup rather than at
3 a.m. on a surfacing.

`client` remains available as a documented escape hatch for the
genuinely synchronous case, with a docstring that says plainly: this
blocks the event loop, nothing else is processed while it runs, and
none of the safety rails apply.

**Outbound is REST only.**  Worth stating because "initiate output to
STOMP" is a natural way to describe the goal, and it is not what the
protocol offers here: SFMC's STOMP endpoint is subscribe-only in this
client.  An engine *reads* STOMP topics and *acts* through REST — which
is exactly how sending a command works today (`PUT
/v1/submit-command`, reply observed on the dialog topic).

### Configuration

Engines take a `config` dict from a YAML file, exactly as
`BaseFollower` does today (`--config`), and the framework does not
inspect it.  Keeping that convention means a follower author moving to
an engine has one less thing to relearn.

### Testing an engine

A scientist should be able to test a control algorithm without a
glider, a server, or a network.  Two supported paths, in order of
preference:

1. **Replay** (`--replay dialog.log`) — real recorded dialog, real
   engine, no network.  Closest to production.
2. **A synthetic harness** — construct the engine, hand it `Event`
   objects, assert on the requests it made:

   ```python
   engine, requests = harness(MyEngine, config={...})
   engine.on_event(Event(source="dialog", body="Waypoint: ...", ...))
   assert requests == [("get_mission_plan", ("osu685",), {"tag": "plan"})]
   engine.on_event(Event(source="result", tag="plan", body={...}, ...))
   ```

   Because `on_event` is single-threaded and side effects go through
   `request`, an engine is a pure-ish function of its event sequence —
   which is the main practical payoff of the whole design.

## Threading model, and what is guaranteed

```
   STOMP topics ─┐
                 ├─► GliderSession fan-out ─► merge ─► inbound queue ─► engine thread
   result/error ─┘                                                        (on_event)
                                                                              │
                                                                    request() │
                                                                              ▼
                                                              OperationExecutor (N workers)
                                                                              │
                                                        result/error events ──┘
```

Guarantees an engine author can rely on:

1. `on_start`, every `on_event`, and `on_stop` run on **one** thread,
   never concurrently.  Engine state needs no locking.
2. Events from a single source arrive in order.
3. `received_at` is non-decreasing within a source.
4. A `result`/`error` event always follows the `request` that caused it.

Explicitly **not** guaranteed, because pretending otherwise would be a
lie that bites later:

- Ordering *between* sources.  A `result` may land between two dialog
  lines.  This is correct actor behaviour and the docs must say so.
- That a result arrives at all before shutdown; `on_stop` may run with
  requests outstanding.
- That every event was delivered — see backpressure.

## Backpressure

Queues are bounded.  A slow engine must not grow memory without bound,
and must not be lied to about what it missed.

- Inbound queue per source, bounded, **drop oldest**.
- Every drop increments a counter; a `dropped` event is delivered as
  soon as the engine catches up.
- A `on_event` call that exceeds a watchdog threshold (default 30 s)
  logs a warning naming the engine and the event — the single most
  common way a novice will break this system is a slow `on_event`, and
  it should be self-diagnosing.

Rejected alternative: unbounded queues.  They turn a slow engine into
an OOM kill hours later, which is far harder to diagnose than a logged
drop.

## Failure policy

| Failure | Response |
|---|---|
| `on_event` raises | Log with traceback, deliver an `error` event, continue.  N consecutive failures (default 5) stop the engine and notify. |
| `request` names an unknown op | Fails at engine start (validated), not at call time |
| An operation fails | `error` event; no automatic retry for state-changing ops |
| Stream drops | `stream` event; the session reconnects underneath |
| Engine thread dies | Framework stops, notifies, exits non-zero for systemd |

Continuing after one bad event is the right default for a long-running
mission, but continuing *forever* with a wedged engine is not — hence
the strike counter.  Both bounds are configurable, neither is infinite.

**No automatic retry of state-changing operations.**  `_request`
already refuses to replay an ambiguous PUT because the server may have
applied it; the same reasoning applies here, more so, because the
engine may have moved a glider.

## Safety rails

1. **Dry run** (`--dry-run`): state-changing operations are logged and
   answered with a synthetic `result`, never sent.  The engine runs its
   full logic.  This already exists in spirit in `sfmc-follow`.
2. **Write gating** (`--allow-writes`): without it, only read
   operations are permitted; a write attempt yields an `error` event
   explaining the flag.  This matches `sfmc-api-test`'s existing
   posture, so the project has one rule, not two.
3. **Replay** (`--replay dialog.log`): run the engine against recorded
   dialog with no network at all.  This is how a scientist should
   develop a control algorithm, and it must be the path of least
   resistance.
4. **Rate limiting**: a cap on outstanding requests per engine, so a
   loop that fires a request per dialog line cannot melt the server.
   Exceeding it produces an `error` event rather than silent queueing.
5. **Audit log**: every request and its outcome, one line each.  When a
   glider does something surprising, this is the artefact that explains
   why.

## What is reused, and what is new

Reused as-is (all from PR #12):

- `GliderSession` — multi-topic subscription, fan-out, reconnect.
- `dialog_stream` — `ordered_dialog`, `LineAssembler`; both delivery
  modes come from this, run once.
- `OperationExecutor` — off-thread calls, `serialized()` for
  per-glider mutual exclusion, observers for the audit log.
- `StreamSupervisor` — reconnect policy.
- `DisconnectNotifier` — engine alerts via the existing `notify`.
- `load_follower_class` — plugin loading, already generic.

New, and roughly the whole cost of this project:

- `Event`, source-tag constants, and the **merge**: N broadcasters into
  one ordered queue with tags.  Fan-out exists; merge does not.
- `BaseControlEngine` and its runner loop.
- The request/result feedback path, including dry-run and write gating.
- `sfmc-control` CLI entry point.
- Docs and a worked example.

Then: **`BaseFollower` becomes a control engine** whose `on_event`
handles `dialog`, feeds `DialogParser`, and calls `on_surfacing`.
`sfmc-monitor-glider` becomes one that logs `dialog` and `scripts`.
That retires the last duplicated pipeline in the package, which is the
same anti-drift argument that motivated PR #12.

## Adversarial review of this design

Written against the proposal, not for it.

**1. Is the actor model overkill for "run my algorithm each
surfacing"?**  For that case, yes — and `BaseFollower` should stay as
the simple façade so nobody pays for machinery they do not need.  The
generalisation earns its keep only for engines with several inputs
(the monitor) or that act on the API (send a command, update a plan).
*Mitigation: keep both APIs; document which to choose first.*

**2. `request("get_mission_plan", ...)` loses type checking.**  Real
cost.  A typo in an operation name is caught at engine start by
validating against `SFMCClient`, but argument mistakes surface only as
an `error` event at runtime.  *Accepted*, because dry-run,
write-gating and audit need interposition.  Revisit if a typed
`Operation` enum can be generated from the client.

**3. Results-as-events forces engines to become state machines.**  A
scientist wanting `plan = get_mission_plan()` on one line now writes a
request and a handler.  This is the single biggest usability cost.
*Mitigations:* `tag=` so they switch on a label rather than track ids;
the synchronous `self.client` escape hatch for genuinely simple cases;
worked examples of both.

**4. Interleaving will surprise people.**  An engine that assumes its
result arrives before the next dialog line will be wrong occasionally
and mysteriously.  *Mitigation:* say so in the docs, and make replay
deliberately interleave results so the surprise happens on a laptop.

**5. One engine thread is a bottleneck.**  A slow `on_event` stalls
everything, and drops follow.  *Mitigation:* the watchdog names the
offender; heavy work belongs behind `request`.  Multiple engine threads
were rejected: they would hand every author a locking problem, which is
exactly what this design exists to avoid.

**6. The framework can now do real damage.**  An engine can assign a
script or deploy a file on a glider in the water.  *Mitigation:* writes
off by default, dry-run, audit log, per-glider serialisation.  None of
this stops a determined mistake, and the docs should not pretend it
does.

**7. Reconnect gaps are invisible unless surfaced.**  An engine that
misses ten minutes of dialog may make a bad decision on partial
information.  *Mitigation:* `stream` events carry the epoch, so an
engine can see a gap occurred.  It cannot see *what* it missed —
nothing can — and the docs must say that plainly.

**8. `dialog.raw` looks like the "more powerful" option** and will be
chosen for that reason by people who do not need it.  *Mitigation:*
default to lines, and document the replay limitation prominently — a
mode you cannot test offline is not the powerful choice.

**9. Is this just re-implementing an event framework?**  Partly.  The
justification is that the domain constraints — reconnects, sequence
reordering, chunked lines, rate limits, one-command-at-a-time per
glider — are already solved in this package, and a generic framework
would have to be taught all of them.

**10. Two ways to do everything.**  `self.client` versus `request`,
lines versus chunks, follower versus engine.  Every pair is a chance to
choose wrong.  *Mitigation:* one recommended path stated in the docs
each time, with the alternative marked as the exception.

**11. The `Event` shape is a compatibility commitment.**  Engines live
in user files outside this repo, so adding a required field later
breaks them.  *Mitigation:* fields are added only with defaults, and
the speculative ones we already know we may want — `glider` for
multi-glider engines — go in from the start rather than being bolted on.

**12. Nothing here has been built.**  The estimates above are
judgement, not measurement, and the merge step in particular hides
questions that only appear in code: whether per-source queues need
independent bounds, and how a source that is added at runtime slots
into an already-running merge.  Phase 1 exists to answer those before
anything depends on the answers.

## Phasing

Each phase is independently useful and independently reviewable.

1. **Merge + `Event`.**  N sources into one tagged queue, with drops
   counted.  Testable with no engine at all.
2. **`BaseControlEngine` + runner, read-only.**  `sources`, `on_event`,
   `request` limited to read operations, results as events, replay
   support.  A monitor-equivalent engine is the acceptance test.
3. **Writes.**  Dry run, `--allow-writes`, per-glider serialisation,
   audit log, rate limit.
4. **`sfmc-control` CLI** and worked examples, including a
   command-sending engine that waits for a quiet link (see
   `docs/script_control.md` on Zmodem timing).
5. **Fold `BaseFollower` in** as a specialisation, keeping its public
   API unchanged.

## Open questions

1. Should `tick` events be on by default?  A periodic wake-up makes
   time-based logic easy, and also makes it easy to write a busy loop.
2. Per-source queue bounds, or one shared bound?  Per-source protects a
   quiet control topic from a dialog flood, at the cost of another
   number to explain.
3. Should an engine be able to drive **several gliders**?  The event
   model allows it (add a `glider` field), but per-glider serialisation
   and the follower migration both assume one.  Deferring, but the
   `Event` field should exist from the start so adding it is not a
   breaking change.
4. Does `on_stop` get a chance to flush outstanding requests, or is
   shutdown always immediate?
