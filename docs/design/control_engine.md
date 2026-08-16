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

- One event stream, several sources, each event tagged with its glider,
  its source, and the time it was received.
- **One or many gliders.**  The common case is one; a formation is
  several, and coordinating them is the whole point of a formation
  controller.  Multi-glider is designed in from the start rather than
  retrofitted.
- Dialog delivered **as assembled lines by default**, with raw chunks
  available for engines that want them.
- Access to the whole SFMC API from engine code, without the engine
  managing threads, futures, reconnects, or rate limits.
- Engines can add sources — and gliders — at runtime.
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
    glider: str          # which glider this concerns — always present
    source: str          # "dialog", "dialog.raw", "connections", "result", ...
    body: Any            # str for dialog, dict for STOMP topics, ...
    received_at: float   # time.time() when this arrived locally
    seq: int             # monotonic per-engine counter, for ordering/logging
    request_id: int | None = None   # set on "result" / "error"
    tag: str | None = None          # caller's label, echoed back on results
```

`glider` is **required, not optional**.  A single-glider engine can
ignore it; a formation engine cannot function without it, and making it
optional would mean every multi-glider engine starts with a `None`
check that can only ever be dead code.  For the handful of operations
that are not per-glider (`upload_cache_files` takes a group,
`get_zmodem_transfers` takes a connection id), the `result` event
carries the glider the engine named when it made the request.

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

## Multiple gliders

A formation controller exists to make decisions *across* gliders —
"osu685 is 400 m behind, slow osu684" — so the cross-glider case drives
the design rather than being tolerated by it.  Four decisions follow.

### One engine thread for the whole fleet

Not one thread per glider.  Per-glider threads would isolate a slow
handler, but they would also make every piece of cross-glider state a
shared-mutable-state problem — which is precisely the class of bug this
design exists to keep away from its authors.  A formation engine
comparing two gliders' positions would need a lock, and would not have
one.

So: **one thread, one ordered queue, all gliders**.  The engine sees
`osu684`'s dialog and `osu685`'s dialog in one sequence, and can hold
fleet state in ordinary attributes with no locking.  The cost is that a
slow `on_event` stalls the whole fleet, not one glider; the watchdog
names the offending event, and heavy work belongs behind `request`.

An engine that wants per-glider independence gets it by ignoring the
other gliders — not by a second mechanism.

### One session per glider

Each glider gets its own `GliderSession`, and therefore its own STOMP
connection, supervisor, and reconnect timer.

- **Failures isolate.**  One glider's stream dropping does not blind the
  engine to the rest of the fleet; it produces a `stream` event for
  that glider and reconnects underneath.
- **Epochs are per glider**, which is what a per-glider reconnect gap
  means anyway.
- It reuses PR #12 unchanged, N times.

*Rejected for now:* multiplexing every glider's topics onto one shared
STOMP connection.  It is possible — STOMP subscriptions are independent
and `StompConnection.subscribe()` already takes an arbitrary topic — and
it would use one socket instead of N.  But it converts an isolated
failure into a correlated one: a single dropped WebSocket blinds the
operator to the entire formation at once, which is the worst possible
time to be blind.  Worth revisiting only if socket count becomes a real
constraint; a formation is on the order of ten gliders, not hundreds.

### Serialisation is per glider, rate limiting is global

These pull in opposite directions and must not be conflated:

- **Mutual exclusion is per glider.**  Two waypoint updates on `osu684`
  must not interleave; an update on `osu684` and one on `osu685` may
  run concurrently.  `OperationExecutor.serialized(glider, ...)`
  already does exactly this — the `KeyedLock` was built for it.
- **Rate limiting is fleet-wide.**  SFMC rate-limits the *account*, not
  the glider.  A per-glider cap would multiply by fleet size and
  produce exactly the 429 storm the cap exists to prevent.  One global
  cap on outstanding requests, and one executor pool shared by all
  gliders, sized for the server rather than the fleet.

### Gliders join and leave at runtime

A formation changes.  `add_glider(name)` starts a session and begins
delivering its events; `remove_glider(name)` closes it.  Both are
ordinary engine calls, so an engine can react to a glider going
permanently silent.

## The engine API

The whole surface an engine author must learn:

```python
class BaseControlEngine:
    sources: list[str] = ["dialog"]      # what to subscribe, per glider

    # ── you implement ────────────────────────────────────────────
    def on_start(self) -> None: ...
    def on_event(self, event: Event) -> None: ...
    def on_stop(self) -> None: ...

    # ── you call ─────────────────────────────────────────────────
    def request(self, op: str, *args, glider: str, tag: str | None = None, **kwargs) -> int
    def subscribe(self, source: str, glider: str | None = None) -> None
    def unsubscribe(self, source: str, glider: str | None = None) -> None
    def add_glider(self, name: str) -> None
    def remove_glider(self, name: str) -> None
    def log(self, msg: str, *args) -> None
    def notify(self, key: str, summary: str, detail: str) -> None
    @property
    def gliders(self) -> tuple[str, ...]
    @property
    def client(self) -> SFMCClient      # escape hatch; see below
```

`request` names an operation by its client method name and returns
immediately with a request id.  A formation engine reads naturally:

```python
def on_event(self, event):
    match event.source:
        case "dialog" if "Waypoint" in event.body:
            self.request("get_mission_plan", event.glider,
                         glider=event.glider, tag="plan")
        case "result" if event.tag == "plan":
            self.plans[event.glider] = event.body   # fleet state, no locks
            self.retask_formation()
        case "error" if event.tag == "plan":
            self.log("%s: plan fetch failed: %s", event.glider, event.body)
```

**`glider=` is a keyword, separate from the positional arguments**, and
that redundancy is deliberate.  It names the *serialisation key*, not
the argument.  Inferring it from `args[0]` would be right for most
endpoints and quietly wrong for the ones that take something else —
`get_zmodem_transfers(connection_id)`, `upload_cache_files(group_name)`
— and "quietly wrong about which glider we locked" is not a failure mode
worth accepting to save a keyword.  It also gives every `result` and
`error` event a glider to carry.

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
  osu684  GliderSession ─► fan-out ─┐
  osu685  GliderSession ─► fan-out ─┤
  osu686  GliderSession ─► fan-out ─┼─► merge ─► queue ─► engine thread
                      result/error ─┘   (tags glider              (on_event)
                                         + source)                     │
                                                             request() │
                                                                       ▼
                                              OperationExecutor — one pool, all gliders
                                              serialized per glider, capped fleet-wide
                                                                       │
                                                 result/error events ──┘
```

Guarantees an engine author can rely on:

1. `on_start`, every `on_event`, and `on_stop` run on **one** thread,
   never concurrently — across the whole fleet.  Engine state, including
   cross-glider state, needs no locking.
2. Events from a single (glider, source) pair arrive in order.
3. `received_at` is non-decreasing within a (glider, source) pair.
4. A `result`/`error` event always follows the `request` that caused it.

Explicitly **not** guaranteed, because pretending otherwise would be a
lie that bites later:

- Ordering *between* sources, or *between gliders*.  `osu685`'s dialog
  may land between two of `osu684`'s lines, and a `result` may land
  between two dialog lines.  This is correct actor behaviour and the
  docs must say so.
- That a result arrives at all before shutdown; `on_stop` may run with
  requests outstanding.
- That every event was delivered — see backpressure.
- That the fleet is in a consistent state at any instant.  Each glider
  is observed independently, and one may be minutes stale while
  another is current.  A formation engine must treat its fleet state as
  a set of last-known values with per-glider timestamps, not a
  snapshot.

## Backpressure

Queues are bounded.  A slow engine must not grow memory without bound,
and must not be lied to about what it missed.

- Inbound queue per **(glider, source)**, bounded, **drop oldest**.
  Per-glider bounds matter more than per-source ones: one glider
  surfacing and dumping a mission's worth of dialog must not evict the
  connection events of the five gliders still in the water.
- Every drop increments a counter; a `dropped` event is delivered as
  soon as the engine catches up, naming the glider and source.
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

**5. One engine thread is a bottleneck, and now for the whole fleet.**
A slow `on_event` stalls every glider, not one.  *Mitigation:* the
watchdog names the offender; heavy work belongs behind `request`.
Multiple engine threads were rejected: they would hand every author a
locking problem for exactly the cross-glider state a formation
controller exists to keep, which is the opposite of the goal.  If a
fleet ever outgrows one thread, the escape is more processes — one
engine per sub-formation — not more threads inside one.

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
questions that only appear in code: how a source or glider added at
runtime slots into an already-running merge, and whether per-(glider,
source) queues need independent bounds.  Phase 1 exists to answer those
before anything depends on the answers.

**13. Multi-glider makes every failure mode fan out.**  N sessions mean
N reconnect loops, N sets of drops, N epochs, and an engine that must
cope with the fleet being partially observed — one glider current,
another twenty minutes stale.  A single-glider engine could ignore
staleness; a formation engine that ignores it will steer on old
positions.  *Mitigation:* per-glider timestamps are in the event model,
and the "no consistent snapshot" caveat is stated as a guarantee we
explicitly do not make.  There is no way to make this go away — it is
the physics of gliders surfacing independently — so the design's job is
to keep it visible rather than to hide it.

**14. Was designing for the fleet the right call when most use is one
glider?**  It costs a `glider` field the common case ignores, and a
`glider=` keyword on every request.  That is a real tax on the majority
case.  It is worth paying because the alternative — a single-glider
API plus a later multi-glider one — is two frameworks, and because the
decisions that are genuinely hard to reverse (one thread or many; where
mutual exclusion and rate limiting sit) all had to be made now anyway.
Retrofitting the threading model later would be a rewrite, not an
addition.

## Phasing

Each phase is independently useful and independently reviewable.

1. **Merge + `Event`.**  N (glider, source) streams into one tagged
   queue, with drops counted per pair.  Multi-glider from the first
   commit — the merge is where that is either easy or impossible, and
   retrofitting it later would touch everything downstream.  Testable
   with no engine at all: two fake sessions, assert tagging, ordering
   within a pair, and drop accounting.
2. **`BaseControlEngine` + runner, read-only.**  `sources`, `on_event`,
   `request` limited to read operations, results as events, replay
   support, `add_glider`/`remove_glider`.  Acceptance tests: a
   monitor-equivalent engine on one glider, and a two-glider engine
   that proves cross-glider state needs no locking.
3. **Writes.**  Dry run, `--allow-writes`, per-glider serialisation,
   fleet-wide rate limit, audit log including the glider.
4. **`sfmc-control` CLI** (`--glider` repeatable, like
   `--notify-email`) and worked examples: a single-glider command
   engine that waits for a quiet link (see `docs/script_control.md` on
   Zmodem timing), and a two-glider formation engine.
5. **Fold `BaseFollower` in** as a specialisation, keeping its public
   API unchanged — see open question 5 on whether it gains multi-glider
   support or stays deliberately single.

## Open questions

1. Should `tick` events be on by default?  A periodic wake-up makes
   time-based logic easy, and also makes it easy to write a busy loop.
   For a formation engine a tick is close to necessary — "re-evaluate
   the formation every 60 s" is the natural shape — which argues for
   on-by-default at a slow interval.
2. Per-(glider, source) queue bounds: one number for all, or
   configurable per source?  A dialog flood and a connection-event
   trickle want different depths, but every extra knob is one more
   thing to explain to someone who should not have to care.
3. Does `on_stop` get a chance to flush outstanding requests, or is
   shutdown always immediate?  A formation engine mid-retask has more
   to lose here than a single-glider one.
4. What should happen when a glider named at startup does not exist?
   Failing fast is right for a typo; refusing to start a six-glider
   formation because one glider is not yet registered is not.  Probably:
   start the rest, deliver an `error` event for the missing one.
5. Should `sfmc-follow`'s migration expose multi-glider at all, or stay
   deliberately single-glider?  Its `on_surfacing(event)` signature has
   no glider in it, and adding one is a breaking change for existing
   followers.
