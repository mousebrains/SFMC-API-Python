# Plan: reconnect `sfmc-monitor-glider` and `sfmc-follow`

Status: implemented; final verification and adversarial review complete
(2026-07-14).

## Objective

Make both live commands recover automatically from a lost SFMC STOMP
session without losing their long-lived application state, spinning in a
reconnect storm, hiding non-network bugs, or turning dialog fragments from
opposite sides of an outage into one autonomous-following decision.

The implementation must preserve replay behavior and must make the limits
of recovery explicit: a new subscription restores future delivery, but the
SFMC topics expose no cursor or history API with which to recover messages
that were published while the client was offline.

## Current behavior and evidence

- `sfmc-monitor-glider` opens one STOMP connection. If either consumer
  thread stops, it logs `Stream disconnected`, leaves the connection
  context, and exits (`src/sfmc_api/monitor_glider.py:340-378`).
- `sfmc-follow` opens one STOMP connection. If the dialog reader stops, it
  logs `Dialog stream disconnected`, sets the caller-visible stop event,
  shuts down the follower and output worker, and returns
  (`src/sfmc_api/follow_glider.py:650-748`).
- The `sfmc-follow` CLI invokes `follow_glider()` once; there is no outer
  retry (`src/sfmc_api/follow_glider.py:885-948`).
- The STOMP receive loop closes subscriptions when the WebSocket ends. It
  deliberately does not recreate application subscriptions
  (`src/sfmc_api/stomp.py:506-569`). Reconnect therefore belongs in each
  long-running application, above `StompConnection`.
- `sfmc-pull-new-downloads` is the existing application-level precedent:
  nominal delays start at 15 seconds, double to 300 seconds, and reset after
  a session that remains healthy for 60 seconds
  (`src/sfmc_api/pull_new_downloads.py:672-707`).

## Required behavior contract

1. Live `sfmc-monitor-glider` and live `sfmc-follow` reconnect by default
   after a stream closes, a STOMP error is delivered, or session setup
   fails after initial validation.
2. Initial validation for live mode remains fail-fast. Invalid credentials,
   an unknown glider, an unusable log file, a follower construction error, or
   a bad initial REST response exits nonzero instead of retrying forever.
   Replay-plus-upload retains its current finite upload/error behavior.
3. Replay modes never reconnect. Natural replay EOF retains its current
   drain-and-return behavior.
4. A reconnect creates a new `StompConnection` and new subscriptions. It
   never calls `connect()` on, or subscribes through, a dead connection.
5. Retry delays use exponential backoff with an initial nominal delay of
   15 seconds, a 300-second cap, and bounded jitter. The nominal delay resets
   only after a successfully subscribed session has remained healthy for at
   least 60 seconds; time spent failing to connect does not count as healthy
   time.
6. Waiting for a retry is interruptible immediately by the stop event.
   Ctrl-C, SIGTERM, and a programmatic `stop.set()` do not wait for the
   backoff timer.
7. A transient disconnect does not set the caller's stop event. That event
   is reserved for an intentional, terminal shutdown.
8. `sfmc-follow` constructs its follower and output worker exactly once and
   keeps both alive across session changes. This preserves plugin state and
   lets already-generated output finish uploading during an outage.
9. Each live dialog session has fresh sequence-ordering, line-assembly, and
   `DialogParser` state. No unterminated character fragment is joined across
   an outage. Parser data that already satisfies the existing GPS-plus-sensor
   emission rule may be flushed once at the boundary; other partial parser
   state is explicitly discarded.
10. A strong surfacing identity is de-duplicated across reconnect overlap so
    a replayed final message cannot invoke the follower twice. Only events
    with both timestamp and mission time participate; ambiguous events are
    not silently dropped.
11. A stream boundary is written to monitor/follow logs in a stable,
    machine-recognizable form. `sfmc-follow --replay` recognizes it, flushes
    any sufficiently complete pre-boundary event once, and resets parser
    state before reading post-boundary dialog.
12. Expected session failures retry. Unexpected processing errors that reach
    the worker wrapper, a dead follower/output worker, or a session worker
    that cannot be joined are fatal and exit nonzero; reconnecting must not
    mask a code or plugin-infrastructure defect. Standard-library logging
    handlers can internally consume post-open write errors, so those require
    external log-file/journal monitoring unless logging is separately made
    strict.
13. Intentional CLI shutdown exits successfully after cleanup. With
    `--no-reconnect`, an unexpected stream loss exits nonzero so
    `Restart=on-failure` can act.
14. Logs identify the failed session, reason, retry number, actual jittered
    delay, and successful resubscription. Recovered disconnects are counted
    in `sfmc-follow`'s final `RunStats` but do not make `--strict` fail;
    `--strict` remains about upload errors.

## Design

### 1. Add a small shared retry policy

Create `src/sfmc_api/stream_reconnect.py` containing a pure, unit-testable
backoff object. It owns only policy state and delay calculation; it does not
open connections, catch arbitrary exceptions, or sleep.

- Inputs: initial delay, maximum delay, stable-session threshold, jitter
  fraction (default `0.2`, valid range `0.0..1.0`), and an injectable random
  source for deterministic tests.
- Output: the next actual delay and reconnect-attempt number.
- State transition: a session whose subscribed uptime reaches the stable
  threshold resets the nominal delay before the next failure; otherwise the
  next nominal delay doubles to the cap.
- Validation: reject negative delays, a maximum below the initial delay,
  negative stability thresholds, and jitter outside the documented range.
- Jitter: use a bounded symmetric range around the nominal delay and cap the
  actual wait at the configured maximum. Logs show the actual wait.

Keep sleeping in the application loops. They use `stop.wait(delay)` (or a
short health-check loop for `sfmc-follow`) so shutdown remains interruptible.
Do not move automatic reconnection into `StompConnection`: the transport
cannot recreate command-specific subscriptions or safely manage follower
state.

Use `sfmc-pull-new-downloads` as the nominal-policy reference but do not
change that command in this patch. Consolidating all three callers can be a
separate behavior-preserving refactor after the two new loops are stable.

### 2. Make reconnect authentication safe

A new STOMP connection currently reuses the client's cached bearer token.
If the old stream ended because that token expired, repeatedly calling
`open_stream()` can repeatedly present the same stale token.

Add a synchronized `SFMCClient.refresh_auth()` operation in
`src/sfmc_api/client.py` and refactor the existing authentication internals
so `_ensure_auth()`, HTTP 401 recovery, upload-thread requests, and an
explicit refresh cannot race or deadlock. Before the second and subsequent
STOMP session attempts, refresh the token, then call `open_stream()`.

Authentication/network failures during this post-validation refresh are
retryable session-setup failures. The logs must retain the exception type
and message without exposing tokens or credentials.

### 3. Refactor `sfmc-monitor-glider` into session and supervisor layers

In `src/sfmc_api/monitor_glider.py`:

1. Extract the argument parser into `build_parser()` so tests exercise the
   real CLI definition rather than reconstructing it.
2. Add a programmatically testable supervisor function accepting the client,
   glider name, loggers, stop event, reconnect toggle, and retry settings.
3. Keep the existing REST glider/deployment validation outside the retry
   loop. Record initial glider and active-script state once.
4. Put only STOMP session setup and operation inside the retry loop:
   open a new connection, subscribe to dialog and script topics, start one
   consumer per subscription, and record `subscribed_at` only after both
   subscriptions and workers are live.
5. Wrap each consumer so it reports normal EOF, `StompError`, or an
   unexpected exception through a per-session result queue. Do not infer all
   thread deaths are transport disconnects.
6. When either consumer finishes, close both subscriptions, join both
   workers, and leave the STOMP context before considering a retry. A join
   timeout is fatal; never start overlapping consumers for the same glider.
7. Retry normal stream closure and `StompError`. Propagate an unexpected
   dialog/script processing exception that reaches the worker wrapper so the
   CLI exits nonzero.
8. On successful reconnection, make a best-effort REST query of current
   active-script state and log it as a resynchronization snapshot. Failure of
   this optional snapshot is a warning and does not tear down a healthy
   stream; missed transition history remains unrecoverable.
9. Add `--no-reconnect`. Default behavior is infinite retry; disabling it
   turns unexpected session loss into a nonzero exit.
10. Have `main()` own a stop event and arrange graceful SIGINT/SIGTERM
    handling. Restore handlers if the reusable CLI path returns during a
    test. Signal-triggered cleanup closes subscriptions and joins consumers.

`monitor_dialog()` must distinguish intentional final shutdown from a
transient stream boundary. It may flush an unterminated fragment only on
intentional final shutdown. On a disconnect it reports and discards the
fragment outside the DIALOG logger, preventing a later replay from treating
the fragment as a complete line.

### 4. Refactor live `sfmc-follow` without restarting the pipeline

In `src/sfmc_api/follow_glider.py`:

1. Retain one `queue_in`, one `queue_out`, one follower instance, one output
   worker, one `RunStats`, and the recent-event de-duplication cache for the
   entire invocation.
2. Split live input into a per-session function and a reconnect supervisor.
   Each session creates a new connection, subscription, ordering generator,
   line assembler, and parser. Replay continues through a single finite
   session and bypasses the reconnect supervisor.
3. Reserve the supplied `stop` event for terminal shutdown. Use separate
   per-session completion/result objects for disconnect/error notification.
4. At a transient session boundary:
   - process all complete lines already yielded by `ordered_dialog()`;
   - on `StompError`, first flush `ordered_dialog()`'s pending out-of-order
     tail in modular stream order and then re-raise the error, matching its
     existing normal-EOF no-loss behavior without yielding during generator
     cancellation;
   - do not feed an unterminated character fragment to `DialogParser`;
   - call `parser.flush()` once, delivering an event only if it satisfies the
     parser's existing GPS-plus-sensor rule;
   - reset/discard all remaining session parser state;
   - close and join the old reader before opening another session;
   - leave the follower and output worker running.
5. Before enqueueing a surfacing, compare its strong identity
   `(vehicle_name, timestamp, mission_time)` against a bounded recent cache.
   Drop and warn on an exact duplicate. If timestamp or mission time is
   missing, deliver the event because safe de-duplication is impossible.
6. During both active sessions and backoff waits, check that the follower and
   output worker remain alive. Capture output-worker exceptions; do not let a
   dead uploader leave an apparently healthy follower filling an unconsumed
   queue.
7. Preserve producer-order shutdown exactly once at terminal exit: stop and
   join the dialog reader, enqueue the follower sentinel behind all accepted
   surfacings, wait for the follower, then enqueue the output sentinel behind
   all generated files and wait for the output worker. A transient
   disconnect must enqueue none of these sentinels.
8. If an old reader cannot be joined within the documented bound, fail
   rather than start a second reader. If the follower is still executing at
   terminal shutdown, do not enqueue the output sentinel ahead of it; wait
   with periodic warnings so late follower output cannot be silently placed
   behind the sentinel.
9. Add `reconnects` to `RunStats` and its formatted summary. Increment it only
   after a second-or-later session has successfully subscribed, not for a
   failed attempt. Keep `had_errors()` and `--strict` based only on upload
   failures. Fatal stream or worker failures raise and therefore already
   produce exit status 1.
10. Add a `reconnect: bool = True` keyword to the programmatic API and a
    matching `--no-reconnect` CLI flag. Define the keyword-only settings
    `reconnect_initial_delay=15.0`, `reconnect_max_delay=300.0`,
    `reconnect_stable_after=60.0`, and `reconnect_jitter=0.2`; the monitor
    supervisor accepts the same names. Tests pass zero/short delays and
    deterministic jitter without sleeping. The CLI exposes only the safety
    switch, not tuning knobs, until operational evidence shows they are
    needed.
11. Have the CLI pass a signal-driven stop event into `follow_glider()` so
    SIGTERM follows the same drain path as Ctrl-C and a programmatic stop.

### 5. Preserve log/replay boundaries

Define one stable marker, for example
`STREAM_BOUNDARY session=<n> reason=<category>`, and emit it through the
INFO/FOLLOW logger after the last complete DIALOG line from the failed
session and before any line from the new session.

Extend the replay reader narrowly:

- ordinary INFO/FOLLOW/SCRIPT lines remain filtered;
- the exact boundary marker becomes a private replay-control line;
- `_read_dialog` recognizes that control line before feeding the parser,
  flushes a sufficiently complete pre-boundary event once, resets parser and
  character-buffer state, and does not include the marker in `raw_lines`;
- raw replay files with no markers retain current behavior.

This keeps monitor logs replayable without allowing a pre-outage `Carrier
Detect`/GPS block to consume post-outage sensor lines. The marker is an
internal log contract, so document it and cover it with tests before release.

### 6. Failure classification and exit semantics

Retry after initial validation:

- WebSocket/SockJS close;
- subscription normal EOF caused by connection teardown;
- `StompError` from connection, handshake, subscription, or server ERROR;
- `SFMCError` while refreshing authentication or resolving a subscription
  during a later session attempt.

Exit nonzero:

- initial credential/glider/deployment/log/follower validation failure;
- unexpected event shape or processing exception in a worker;
- dead follower or output worker;
- a worker that does not stop after its subscription is closed;
- unexpected invariant failure in the supervisor;
- stream loss while `--no-reconnect` is selected.

Exit successfully after cleanup:

- Ctrl-C, SIGTERM, or programmatic stop;
- natural replay EOF (subject to existing `--strict` upload-error handling).

Do not catch `BaseException` in session workers or retry supervisors.
`KeyboardInterrupt`/`SystemExit` must retain terminal semantics.

### 7. Observability

Use one log vocabulary in both commands:

- `stream session N subscribed`;
- `stream session N ended: <category>: <message>`;
- `reconnect attempt N in X.Ys`;
- `stream session N reconnected after X.Ys offline`;
- `stream boundary discarded N-byte unterminated fragment`;
- `duplicate surfacing suppressed: <identity>`;
- `stopping; draining follower/output queues`.

Never log access tokens, credential paths' contents, or full URLs containing
the token query parameter. Repeated failures should produce one warning per
attempt, not a warning every supervisor poll.

## Tests

All timing tests use injected clocks/randomness or zero delays; no test waits
for production backoff intervals.

### Retry policy

- nominal sequence grows 15, 30, 60, 120, 240, 300 and stays capped;
- stable subscribed uptime resets the next nominal delay to 15;
- failed connection time does not reset backoff;
- jitter stays within bounds and never exceeds the cap;
- invalid settings fail immediately;
- stop interrupts a retry wait.

### `sfmc-monitor-glider`

- real `build_parser()` defaults and `--no-reconnect` parsing;
- first mock session delivers dialog/script events and closes; second session
  delivers more; both are logged and `open_stream()` is called twice;
- one consumer ending closes and joins its still-blocked sibling;
- STOMP handshake, first-subscription, and second-subscription failures clean
  up partial resources and retry;
- authentication is refreshed before a reconnect session;
- an unexpected dialog/script processing exception, or a test logger that
  explicitly raises to its caller, propagates instead of becoming an endless
  reconnect loop;
- stop during an active session closes both subscriptions with no retry;
- stop during backoff returns promptly;
- a worker join timeout prevents a new session and exits nonzero;
- a long healthy session resets backoff; repeated short sessions reach the
  cap;
- reconnection logs a current-script resynchronization snapshot, while a
  snapshot REST failure leaves the stream running;
- disconnect discards an unterminated dialog fragment and writes exactly one
  boundary marker.

### `sfmc-follow`

- two live sessions feed the same follower instance and same output worker;
- disconnect does not set the caller stop event or call follower shutdown;
- already queued follower output is uploaded while reconnecting;
- a `StompError` after an ordering gap flushes the buffered modular-order
  tail before the session boundary is finalized and the error is retried;
- ordering/parser state is fresh per session and pre/post-outage fragments
  are never concatenated;
- a boundary flush emits a sufficiently complete event once and discards an
  insufficient event;
- reconnect overlap with the same strong identity invokes the follower once;
  an event lacking a strong identity is not silently de-duplicated;
- the old reader is closed and joined before `open_stream()` is called again;
- STOMP setup/error/normal-close paths retry with the expected backoff;
- unexpected parser/worker exceptions and follower/output death are fatal;
- stop during the active session and stop during backoff both run one ordered
  drain and return;
- follower output produced late during terminal shutdown is still ahead of
  the output sentinel;
- live dry-run reconnects but never uploads;
- all replay combinations remain finite and never call the retry policy;
- a monitor/follow log containing a boundary marker resets replay parser
  state and never exposes the marker as dialog or `raw_lines`;
- raw replay files and older logs without markers retain existing behavior;
- `RunStats.reconnects` accumulates without changing `had_errors()`;
- `--no-reconnect` converts disconnect to exit 1; recovered disconnects do
  not affect `--strict`; SIGINT/SIGTERM stop cleanly.

### Authentication/client regression

- synchronized refresh replaces a cached token;
- concurrent HTTP use and reconnect refresh do not deadlock or expose a
  partially updated token;
- failed refresh leaves a subsequent attempt able to authenticate;
- existing HTTP 401 refresh tests and STOMP lifecycle tests continue to pass.

### Full verification

Run the repository's required checks:

```text
ruff check src/ examples/
ruff format --check src/ examples/
mypy src/sfmc_api/
pytest --cov=sfmc_api --cov-report=term-missing --cov-fail-under=70 tests/
```

Also perform a manual deterministic smoke test with a fake/local STOMP
endpoint that closes two sessions, then remains connected, verifying log
order, bounded retry timing, absence of overlapping readers, and immediate
Ctrl-C/SIGTERM shutdown. Do not require a production glider to validate the
failure paths.

## Documentation and release notes

Update:

- `docs/monitor_glider.md`: default reconnect behavior, backoff, boundary
  markers, `--no-reconnect`, shutdown, and the unrecoverable-gap limitation;
- `docs/follow_glider.md`: persistent follower state, replay behavior,
  de-duplication rule, new stats/CLI/API settings, and gap limitations;
- `docs/streaming.md`: transport-versus-application reconnect ownership;
- `README.md` quick descriptions if they currently imply a single session;
- `CHANGELOG.md` under Unreleased.

State plainly that reconnect is best-effort future recovery, not exactly-once
delivery. There is no dialog catch-up endpoint, so operators must treat a
boundary warning as evidence that some live dialog or script transitions may
have been missed.

## Deliberate non-goals

- No transparent reconnect inside `StompConnection`; it lacks the knowledge
  to reconstruct application subscriptions and state.
- No recovery of messages published during the offline interval; the current
  API exposes no cursor/history mechanism.
- No application-level idle timeout in this change. Quiet dialog/script
  topics are legitimate, and the repository does not establish a server
  heartbeat SLA from which to select a safe timeout. Explicit SockJS/STOMP/
  WebSocket closure and errors trigger reconnect. Add idle detection only
  after measuring and documenting the server heartbeat contract.
- No change to upload retry/idempotency. `_upload_files()` has its own current
  failure semantics and should be addressed separately.
- No strict post-open log-write failure detection. Python logging handlers may
  route an `emit()` failure through `handleError()` without raising it to the
  session worker. Journal/log-file health monitoring, or a separate strict
  logging design, is required for that failure class.
- No change to `sfmc-pull-new-downloads` behavior in this patch.

## Acceptance criteria

The work is complete only when:

1. Both live commands survive at least two consecutive forced stream losses,
   resubscribe, and process later events without a process restart.
2. No old session worker remains alive when a new session begins.
3. `sfmc-follow` uses one follower/output pipeline across all sessions and
   drains it exactly once at terminal shutdown.
4. A recorded boundary cannot combine opposite sides of an outage into one
   parser event during live operation or replay.
5. Duplicate strong surfacing identities across reconnect overlap are not
   delivered twice.
6. Retry waits back off, cap, jitter, reset only after healthy subscribed
   uptime, and are immediately interruptible.
7. Token refresh allows reconnection after a stale-token session failure.
8. Retryable transport failures remain in-process; fatal internal failures
   and `--no-reconnect` disconnects exit nonzero.
9. Replay modes and existing queue-drain guarantees do not regress.
10. Documentation describes both automatic recovery and its delivery gap.
11. All lint, formatting, typing, unit/integration, coverage, and smoke checks
    pass.

## Adversarial self-review record

### Pass 1: lifecycle and compatibility

Findings incorporated:

- Recreating the follower on every reconnect would erase plugin state.
  Resolution: keep follower, queues, uploader, stats, and de-dup cache for the
  invocation; replace only session-scoped input objects.
- Reusing the current stop event would make reconnect impossible and violate
  the programmatic API. Resolution: separate terminal stop from session done.
- Retrying replay EOF would turn finite tests into infinite jobs. Resolution:
  replay explicitly bypasses the supervisor.
- Starting a new session before closing/joining the old one could duplicate
  delivery. Resolution: strict close/join/context-exit ordering and fatal join
  timeout.

### Pass 2: autonomous-following data integrity

Findings incorporated:

- Carrying parser/line state across a message gap could synthesize telemetry
  from two sessions. Resolution: fresh session state and explicit boundary
  flush/reset semantics.
- Logging an unterminated fragment as a normal DIALOG line could make replay
  treat it as complete. Resolution: discard it on transient disconnect and
  report it outside the DIALOG logger.
- Filtering reconnect log lines during replay would erase the boundary and
  permit cross-gap parsing. Resolution: one stable marker with narrow replay
  control handling.
- Resubscription overlap could duplicate an autonomous action. Resolution:
  bounded de-duplication only for a strong timestamp-plus-mission-time
  identity; ambiguous events remain delivered.
- Reconnect cannot recover events sent while offline. Resolution: explicit
  non-goal, boundary warning, and documentation; do not claim exactly-once or
  lossless behavior.

### Pass 3: concurrency and failure containment

Findings incorporated:

- A monitor thread can fail from malformed data (or from a logging call that
  actually raises to its caller), not only from a disconnect. Resolution:
  typed worker outcomes and fatal unexpected errors that reach the wrapper.
- When one of two monitor subscriptions ends, the sibling can block forever.
  Resolution: close and join both on the first completion.
- A dead output worker was not monitored and could allow unbounded queue
  growth. Resolution: monitor both persistent workers during sessions and
  retry waits, and propagate captured failures.
- Sending the output sentinel after a timed-out follower join can place late
  files behind the sentinel. Resolution: never enqueue it until the follower
  has actually ended; emit periodic shutdown warnings.
- Polling throughout a five-minute delay would delay shutdown or worker-death
  detection. Resolution: interruptible waits with health checks.

### Pass 4: retry and authentication behavior

Findings incorporated:

- Resetting backoff based on total attempt duration rewards slow failed
  handshakes. Resolution: measure only successfully subscribed uptime.
- Fixed delays across a fleet can synchronize reconnect storms. Resolution:
  bounded jitter plus deterministic injection for tests.
- A reconnect may reuse the expired bearer token that caused the old session
  to fail. Resolution: synchronized authentication refresh before subsequent
  sessions and client concurrency regression tests.
- Catching every `Exception` forever would hide code defects and prevent
  systemd from seeing fatal failure. Resolution: retry expected STOMP/SFMC
  session failures; propagate unexpected processing/invariant failures.
- Disabling reconnect but returning zero would defeat
  `Restart=on-failure`. Resolution: `--no-reconnect` stream loss exits 1.

### Pass 5: operations, observability, and scope

Findings incorporated:

- A long backoff needs immediate Ctrl-C, SIGTERM, and programmatic stop.
  Resolution: signal-driven stop events and interruptible waiting.
- Recovered disconnects need visibility without making `--strict` fail.
  Resolution: consistent session logs and a separate reconnect counter.
- Best-effort script recovery can report current state but cannot reconstruct
  transitions. Resolution: post-reconnect snapshot plus an explicit history
  limitation.
- An idle watchdog chosen without a proven heartbeat interval could cycle a
  legitimately quiet stream. Resolution: keep it out of scope and document
  the transport-detection boundary.
- Refactoring the already-working pull command at the same time increases
  regression surface. Resolution: use it as policy precedent; defer shared
  adoption until this behavior is established.

### Pass 6: buffered-error and observability audit

Findings incorporated:

- `ordered_dialog()` flushes its pending reorder buffer on normal EOF but a
  queued `StompError` currently escapes before that footer runs. Resolution:
  explicitly flush pending messages in modular order before re-raising
  `StompError`, with a regression test; do not yield from a broad `finally`
  because generator cancellation must remain safe.
- The draft claimed all logging I/O faults would propagate from a monitor
  worker. Standard logging handlers may consume `emit()` failures through
  `handleError()`. Resolution: narrow the fatal-worker promise to errors that
  reach the wrapper and document post-open log durability monitoring as a
  non-goal rather than making an unsupported guarantee.

### Pass 7: compatibility and API precision audit

Findings incorporated:

- “Initial validation remains fail-fast” could be read as changing finite
  replay-plus-upload authentication/error behavior even though reconnect is a
  live-mode feature. Resolution: scope fail-fast startup language to live
  mode and explicitly preserve replay behavior.
- Retry timing was described but its programmatic surface and reconnect
  counter semantics were ambiguous. Resolution: name and default every
  keyword, define the jitter range, keep CLI tuning minimal, and count only
  successful second-or-later subscriptions as reconnects.

### Pass 8: final clean review

Reviewed the revised plan against normal close, STOMP ERROR, handshake and
partial-subscription failure, stale authentication, malformed events,
disconnect during a fragmented line and during a complete surfacing, duplicate
overlap, worker death, stop during active/backoff states, replay EOF, signal
shutdown, join timeout, logging safety, and systemd exit semantics.

No additional findings were identified. Implementation evidence is still
required; this clean review applies to the plan, not to code that has not yet
been written or tested.

## Implementation adversarial self-review record

### Pass 1: retry state and terminal semantics

Findings corrected:

- A successful subscription following an initial handshake/setup failure was
  not counted as a reconnect and had no offline-duration log. The supervisors
  now begin the offline interval after any retryable setup or subscribed-session
  failure, and `RunStats` counts a successful second-or-later attempt.
- Some `sfmc-follow` startup validation paths logged and returned success.
  Missing clients, malformed live glider responses, and missing replay files
  now raise so the CLI exits nonzero.
- The output-health check consumed the output worker's only result. Terminal
  cleanup could then replace the useful worker failure with “without reporting
  a result.” Health inspection now preserves the result for ordered cleanup.
- Live terminal shutdown always discarded an unterminated dialog fragment.
  It now retains the original terminal-flush behavior while transient
  boundaries still discard the fragment.

### Pass 2: concurrency and partial-start cleanup

Findings corrected:

- If the monitor dialog worker started but the script worker failed to start,
  the first worker was not explicitly joined. Both subscriptions are now
  closed and every successfully started worker is joined in a `finally` path,
  including failures during the post-reconnect script snapshot.
- A malformed optional script-resynchronization response could raise `KeyError`
  and tear down an otherwise healthy replacement stream. Response-shape errors
  are now normalized to `SFMCError`, warned, and ignored only for that optional
  resynchronization snapshot.
- The replay recognizer accepted any line beginning with `STREAM_BOUNDARY`,
  which could misclassify a lookalike dialog line as control data. It now
  accepts only the documented `STREAM_BOUNDARY session=N reason=category`
  grammar and only from INFO/FOLLOW loggers or an exact raw marker.
- Repeated monitor CLI invocations could accumulate INFO handlers. The CLI now
  clears that logger before attaching the current handlers.

### Pass 3: security, policy inputs, and failure observability

Findings corrected:

- Stream exception text redacted token query parameters but not bearer-header
  text. The shared formatter now redacts both forms, with regression tests.
- Non-finite retry settings such as NaN could pass comparison-based validation
  and later break waiting. All numeric policy inputs must now be finite.
- Session-end warnings lacked the failed session/setup attempt and reason.
  Both supervisors now log the attempt/session identifier, category, and
  sanitized detail before retrying.
- Authentication refresh serialization lacked a concurrency regression test.
  An eight-thread test verifies one authentication call at a time, no deadlock,
  and atomic final-token visibility.

### Pass 4: final acceptance audit

Audited initial and replacement handshake failure, two consecutive subscribed
session losses, partial thread startup, sibling shutdown, join timeout,
out-of-order tail delivery on `StompError`, character fragments, parser
boundaries, strong and ambiguous identities, worker death, authentication
refresh failure/retry, stop during a 300-second backoff, replay EOF, CLI exit
codes, signal-handler restoration, log redaction, and documentation claims.

No additional findings were identified.

Verification evidence:

- deterministic fake-stream smoke tests replace three consecutive sessions in
  each command (two forced losses), process later events in order, and assert
  one persistent follower pipeline and no replacement before worker cleanup;
- `ruff check src/ examples/` passed;
- `ruff format --check src/ examples/` passed;
- `mypy src/sfmc_api/` passed;
- the final full-suite rerun passed with 530 tests, one skipped, and 86.88%
  coverage.
