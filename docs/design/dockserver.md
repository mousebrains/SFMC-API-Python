# Design: a replacement dockserver

**Status:** proposal, not implemented.  Revision 2, rewritten after
four adversarial reviews; see [What changed in rev 2](#what-changed-in-rev-2).
**Scope:** the glider-facing half of SFMC, plus the decoded-data egress
that SFMC does not expose at all.
**Related:** [control_engine.md](control_engine.md),
[../xml_engine.md](../xml_engine.md).

## Why replace anything

Four things drive this.  Three are in the dockserver.  The fourth is
not, and is the one worth building first.

1. **Real connections cannot be replayed.**  When a surfacing goes
   wrong, what survives is a dialog log — text, already parsed, already
   lossy.  There is no way to re-run the same connection against a
   changed script or a fixed parser.  Every diagnosis is an argument
   from memory.
2. **Zmodem is fragile.**  Transfers fail over Iridium in ways that
   leave no good record of *what* failed, and recovery is "try again
   next surfacing, from the beginning."  Note what the current cost
   actually is: not usually lost data — the files stay on the glider
   and get re-requested — but Iridium airtime and surfacing windows.
3. **A script does not survive a dropped connection well.**  This one
   needs stating carefully, because the corpus shows the existing
   engine is not as memoryless as it first appears — see
   [What the XML actually does](#what-the-xml-actually-does).  What it
   cannot do is resume *mid-sequence*: a drop three steps into a
   seven-step procedure restarts the procedure, and every script in the
   corpus is written defensively around that.
4. **Decoded data is trapped.**  SFMC decodes Dinkum files and presents
   them on a page; that decoded layer is not exposed.  Only the raw
   binaries are reachable.  Users want NetCDF, and pushes to remote
   hosts are hand-rolled.

**Force 4 needs none of the rest of this document.**  It is a pipeline
downstream of file receipt, it can be built today against SFMC
untouched, and it is days of work.  It is listed first in
[Getting there](#getting-there) for that reason.

## What is actually fixed

Only the glider side.  The server side is a convention, not a
requirement.

| | |
|---|---|
| Link | Iridium/RUDICS, Freewave, or direct serial — a raw byte stream to a GliderDos terminal |
| Identity | A connection is anonymous until `whoru` is answered with `Vehicle Name: …` |
| Transfer | Zmodem, both directions, over that same stream |
| Command vocabulary | GliderDos's own: `zr`, `szr`, `s <patterns>`, `put`/`get`, `run`, `Ctrl-R`/`F`/`W`/`C` |
| Flight files | `.mi`, `.ma`, `sbdlist.dat` / `mbdlist.dat` |
| Science files | `proglets.dat` and friends |
| Returned files | `.[sdmn]bd`, `.[stmn]cd`, `.ebd`/`.ecd`, `.mrd`/`.mri`, `.nlg` |
| Echo | Commands echo back; control characters appear as `^C` inline |
| **Firmware generations** | Multiple, simultaneously.  TWR supports historical versions; so must we. |

That last row is a requirement, not a footnote.  `riot.xml`'s
`T0SelectSendType` branches three ways on `SCIENCE DATA LOGGING:
science IS running` / `science is NOT running` / `Enumerating and
selecting`, and the three paths **rejoin at different points** — two of
them skip `dockszr` entirely.  That is vehicle-generation
discrimination, rediscovered by regex on every surfacing.  It should be
detected once, persisted per vehicle as a capability record, and
handed to scripts as data.

**Not fixed, and ours:** where files live on the server, what they are
named, how they are grouped and versioned, what the API calls them, and
— crucially — everything the dockserver says about itself.

## The organizing fact: the dialog is two channels

`!dockzr` and `!dockszr` are **SFMC commands, not glider commands**.
SFMC intercepts them, sends `zr`/`szr` on the wire, and injects status
text into the dialog view.  `riot.xml` says so itself at line 103:
*"N.B. This is a dockserver side command, so not going to glider."*
Over RUDICS there is no carrier-detect signal at all; SFMC synthesizes
that event from a TCP close.

Splitting `riot.xml`'s 97 match transitions by who generates the text
they match:

| Source | Transitions |
|---|---|
| **Dockserver-synthesized** — `!zr`, `!szr`, `not found`, `Done!`, `FAILED: zr`, `Total Bytes sent/received:`, `Starting zModem transfer of`, `xxx command verify fail xxx`, `Connection Event: Carrier Detect lost.` | **56** |
| **Real glider output** — `Hit Control-R to RESUME`, `MAFILES will be re-read`, `devices:`, `SCIENCE DATA LOGGING: …`, `Enumerating and selecting`, `GLD:`/`SCI:` results, the GliderDos prompt | **41** |

**58% of that script's logic exists to scrape status out of a
transcript the dockserver wrote in the first place.**  That is not
protocol.  It is the script and the dockserver doing IPC through a
lossy ASCII channel that also happens to carry the vehicle's console.

Four consequences, and they shape everything below.

**1. The journal must be two-channel.**  A wire capture between the
glider and SFMC contains *none* of the synthesized strings.  Journaling
only the wire produces a corpus against which the existing scripts
cannot run.  Both the wire bytes and SFMC's
`/topic/glider-link-output/{gliderId}` must be captured,
timestamp-aligned.

**2. The diff between those two channels is a deliverable.**  For every
`matchExpression` in all 20 scripts, establish empirically which side
emits the matched text.  That census *is* the specification of what
SFMC synthesizes, and nothing downstream is trustworthy without it.
It is also the **clean-room specification** — see
[Provenance](#provenance).

**3. A replacement must re-emit the synthesized strings
byte-compatibly**, in the same interleave position, or the twenty
existing scripts silently stop working.  The earlier claim that "nobody
is forced to rewrite anything" was resting on an unchecked assumption.
The obligation is bounded and enumerable — the census produces the
list — but it is real, and it belongs in the effort estimate.

**4. Owning that channel is the strongest argument for owning the
dockserver.**  Give a script a *typed* transfer result and whole
categories of its complexity stop existing:

| Corpus pattern | Why it exists | With a typed channel |
|---|---|---|
| 101 `xxx command verify fail xxx` self-loops | the command may not have been received | the call dispatched or raised |
| `Total Bytes` / `Starting zModem` timer-reset loops | a static timeout would kill a long transfer | progress is a ledger field |
| `!zr` / `not found` / `Done!` / `FAILED: zr` branching | outcome inferred from prose | a return value |
| `Carrier Detect lost` edges | a TCP close rendered as a sentence | an event with a reason code |
| `T0WaitReloadMA`'s 3-minute forward timer | *"the Iridium Output Buffer fills up and we might miss relevant lines"* | we are not reading lines |

The last row is the tell: the author built a timer to compensate for
*losing status text*.  No language design fixes that.  Owning the
channel does.

**Cheaper hybrid to price first:** `get_zmodem_transfers`
(`src/sfmc_api/client.py:559`) and the zmodem STOMP topic already
provide a partial typed status channel today, against SFMC.  If that is
enough to delete the scraping states, the case for a replacement
dockserver weakens considerably.  If it is not, we will know precisely
why.  Test this before writing linkd.

## Architecture

> **The byte streams are the record.  Everything else is a projection.**

Dialog logs, parsed surfacings, script traces, transfer ledgers,
database rows — all derived, all rebuildable.  That is what makes
force #1 tractable: replay is not a feature to design, it is what you
get when every component's input was written down.

### Journal schema

Bytes alone are not enough.  The reviews found six omissions, all of
which are load-bearing:

| Field | Why |
|---|---|
| `channel` | `wire` or `dockserver` — the two-channel split above |
| `connection_id`, `direction`, `bytes` | the basics |
| `t_arrival`, `t_processed` | two clocks, so replay can distinguish "the glider was slow" from "our event loop blocked" |
| `chunk_boundary` | read sizes are a TCP artifact, but this repo already has the scar — a 16-byte `GliderDos N -1 >` fragment discarded at a stream boundary.  Replay that re-chunks differently exercises different code. |
| `segment_kind` | `dialog` / `zmodem` / `zmodem-aborted` / `binary` |
| socket lifecycle records | connect, per-direction half-close, FIN vs RST, `SO_ERROR`, keepalive expiry, **who closed**.  `Connection Event: Carrier Detect lost.` is derived from exactly this. |
| timer firings | otherwise a timeout that fired at T+601s replays as never firing |
| `(boot_id, CLOCK_MONOTONIC, CLOCK_REALTIME)` at open | monotonic offsets do not survive a restart, and resume needs to relate offsets across connections |
| initial state digest | staging contents, quarantine, `.cac` files, lease holder, engine version — resume behaviour is a function of disk, and disk is not in the byte stream |

Every component takes its clock by injection.  `xml_engine.py` already
does this; the Zmodem retry timers are the hard case.

### Two kinds of replay, and only one is free

This distinction was missing from rev 1 and it undercut the whole
force-#1 argument.

- **Conformance replay** — feed recorded input, assert byte-identical
  recorded output.  Valid, free, and an excellent regression harness.
- **Counterfactual replay** — "what would a *changed* script have
  done?"  **Not** free.  The recorded glider bytes were answers to the
  recorded implementation's output.  Change the script and it sends
  `Ctrl-F` at a different point; the recorded glider never saw that,
  so everything after is fiction that looks like evidence.  The same
  applies to a Zmodem implementation advertising a different `ZRINIT`.

Counterfactual replay requires a **glider simulator** — a model of
GliderDos plus a Zmodem peer, calibrated by the journal rather than
driven by it.  That is unbudgeted work comparable in size to Zmodem
itself, and rev 1 promised it for free.

### Components

`linkd` below is a name for something we would write.  It is not
existing software.

| Component | Job |
|---|---|
| **linkd** | Owns the sockets.  `whoru` binding, Zmodem, journal, demultiplexer.  The **only** process that can transmit to a glider. |
| **bus** | Fan-out.  ActiveMQ is what SFMC uses; NATS or Postgres `LISTEN/NOTIFY` would do. |
| **runner** | Executes scripts.  Durable per-run state. |
| **api** | REST + WebSocket.  Stateless. |
| **pipeline** | Binary → NetCDF → egress.  Independently restartable. |

### Fan-out

One incoming connection can feed many consumers: once we own the
socket, the bytes are ours to copy.  A slow subscriber cannot stall the
link because the journal write already happened.

The wrinkle is that the stream is not always dialog, and the
demultiplexer that decides is the piece most likely to be subtly wrong:

- It must be **one state machine over both directions**, not two
  independent taggers.  During a glider→server transfer the reverse
  direction carries `ZRINIT`/`ZRPOS`/`ZACK`, which a per-direction
  tagger reads as noise.
- Drive it off a real `ZDLE` frame parser with `ZFILE`-tracked byte
  counts, not a sentinel scanner.  You know how many bytes to expect.
- **Only the clean end has a marker** (`ZFIN`/`ZFIN`/`OO`).  A 5×`CAN`
  abort, a retry-budget exhaustion, a carrier drop mid-`ZDATA` and an
  idle stall have none.  So "transfer ended" is a decision the
  **ledger** makes with a timer, not something the byte scanner claims
  to observe.  Hence the `zmodem-aborted` tag.
- Test it against deliberately corrupted journals, not clean ones.

### Arbitration

One outbound queue per link, with an explicit lease.  Rev 1 had this
biased the wrong way; the corrected rules:

- **A human operator's write always wins**, immediately, logged and
  attributed.  The lease constrains *automation against automation* —
  which is the actual bug class — never the pilot.  A refused keystroke
  during an abort is the worst possible behaviour.
- Leases are **time-bounded and renewed**.  A wedged runner loses its
  lease in seconds, not never.
- Preemption **purges the preempted principal's queued bytes**.
  Otherwise an operator's `Ctrl-C` is followed a second later by a
  `Ctrl-R` that was already in the queue, silently undoing the abort.
- **Break-glass**: if arbitration state is unreadable, the operator
  wins by default.
- Preemption *during a transfer* is not byte injection — the glider's
  `rz` would consume it as protocol input.  It needs a Zmodem-aware
  abort primitive (`CAN`×5 plus drain) in linkd.
- Every transmitted byte is journaled with its originating principal.

**Below the lease, a transmit precondition:** no bytes leave unless a
prompt or surfacing banner was observed on *that connection* within N
seconds, with explicit operator override.  This is the lesson from the
keepalive incident — a gate whose own traffic held the link up saw a
connected glider and kept firing at a submerged one.  A lease alone
does not prevent that; it only says *who* may write, not whether now is
safe.

**`whoru` is a named exception**, since it is a transmission sent
before any lease exists to a connection of unknown identity.  Binding
rules, because the failure mode is commanding the wrong vehicle:

- Bind only from a `Vehicle Name:` seen within N seconds of *our own*
  `whoru` on that connection.  Matching it anywhere in the stream is
  forgeable — by an operator typing `whoru`, by buffer residue, or by
  transferring a file containing the string.
- Never re-bind a bound connection; a conflicting answer is a fatal
  connection error.
- On a new bind for an already-bound vehicle, forcibly `RST` the old
  connection and invalidate its lease **before any write**.  A dead
  RUDICS session produces no FIN and can stay half-open for minutes, so
  two live bindings are normal, and writes to the stale one are
  journaled as transmitted and then vanish.
- Enable aggressive `TCP_KEEPALIVE` on glider sockets.
- In observe mode, bind from the *observed* exchange and record that
  the binding is inferred, not asserted — so the safe build does not
  take a structurally different path through the most safety-critical
  decision in the system.

## Zmodem

### ZRPOS is a gate, not an assumption

Rev 1 treated resume as a switch to flip.  It is a negotiation with
**TWR firmware we cannot read or patch**, and the entire staging design
rested on it.  Nothing here proceeds until this is answered:

> **Experiment.**  On osusim, kill a transfer mid-file and attempt
> resume in each direction.  Does the Slocum sender honour a non-zero
> `ZRPOS`?  Does it answer a `ZCRC` challenge?  Does it set `ZCRESUM`?
> What is its post-send file disposition — marked-sent or deleted?
> Write the pass/fail down before any design depends on it.

The realistic bad outcome is not a clean refusal.  It is a sender that
ignores the offset and streams from byte 0 while carrying `ZDATA`
position 0, the receiver re-`ZRPOS`ing, and a retry loop that burns the
whole surfacing.

**Never resume *to* the glider.**  Full retransfer only.  A resumed
upload means a window in which the vehicle holds a truncated `.ma` and
a `Ctrl-F` reloads it.  That is a vehicle-safety failure, not a
transfer failure — and `riot.xml` follows every successful `dockzr`
with exactly that `Ctrl-F`.

**Resume *from* the glider only after a `ZCRC` challenge** validating
the CRC-32 of the prefix we hold.  Zmodem's CRCs cover subpackets, not
files, so without this a stale partial splices silently.  If the sender
does not answer `ZCRC`, resume is disabled.  Full stop.

The rev-1 staging key `(glider, filename, size, mtime)` is unsafe:
glider filenames are counter-derived and repeat across card reformats,
and `mtime` comes from a clock that drifts.  Key on `(glider,
deployment, prefix_hash)` and never resume across a cache-signature or
mission-number change.

### The rest

**Ledger.**  Every transfer gets an entry from recognition: direction,
filename, expected size, bytes confirmed, frames retried, terminal
state, and the journal offsets bracketing it.  A failure becomes a
queryable object with a replayable byte range.

**Quarantine, with a promotion path.**  Received files are published
only after a confirmed terminal state, so nothing downstream sees a
truncated file as data.  But a partial may be the *only* copy that will
ever exist — a glider lost after transferring 40 KB of 60 leaves those
40 KB as the last data anyone gets, containing whatever went wrong.
Quarantined partials must be enumerable, downloadable, and promotable
by explicit operator action with a permanent `partial=true` marker
carried into the NetCDF attributes.  (`xdbd` already offers
corrupt-file repair; walling it off from the case that needs it would
be perverse.)

**Archive, structurally.**  Every file sent is copied, timestamped, and
retained with connection id, script run, lease holder, and transfer
outcome attached.  This already earns its keep; the change is that it
becomes part of the journal rather than a side effect of `-archive`.

**Never fsync on the forwarding path.**  An fsync on SD storage is
routinely hundreds of milliseconds and occasionally seconds.  Zmodem is
windowed with second-scale retry timers; adding that jitter to the ack
path converts working transfers into retry storms you then debug.
Forward first, append to an in-memory ring, group-commit on a separate
thread.  Disk-full degrades to *stop journaling and alarm*, never to
stop forwarding.

## Scripts

### What the XML actually does

Rev 1's numbers were wrong, in the direction that flattered the
argument.  Corrected against the parsed file, not grep:

| | rev 1 claimed | actual |
|---|---|---|
| `riot.xml` transitions | 143 | **119** (grep counted the 24 `<transitions>` containers) |
| actions | 51 | **48** (three are inside comments) |
| carrier-detect edges | 21 | **20** (one is in a comment) |
| T0/T1 duplication | "a second parallel copy of the entire machine" | **3 states of 25** |

And two of rev 1's three characterisations were simply false:

- **Carrier-detect edges do not mean "start over."**  16 of 20 are
  *self-loops*: stay in this step, restart the 10-minute timer, and
  continue if the glider redials on the same surfacing.  Two restart,
  one crosses to the T1 lane (an interrupted upload deliberately
  *setting* the reload-MA flag), and one — `T0WaitForDisconnect` — is
  the **success** path: the script waits for the hangup so that
  matching it flushes the rolling buffer and stale dialog cannot
  retrigger the start state.  `resume.xml` and all three `glmpc-*.xml`
  use the same construct.
- **Timeouts do not all restart either.**  16 → `T0WaitForSurfacing`,
  5 → `T1WaitForSurfacing` (carrying the flag forward), and one is a
  3-minute **forward** timer with an action attached, compensating for
  Iridium buffer loss.

There is more structure than "T0/T1": **five thread prefixes** —
T0=16 states, T1=3, T2=1, T3=1, T4=3 — encoding a boolean × a
three-valued device class × a phase marker.  `T2`/`T3` are the
firmware-generation branch and they rejoin at different points.  `T4`
is the second `dockzr`/`Ctrl-F` pass.

`riot.xml` also has **no reachable final state**.  Both `final` and
`T0DoneDockzr` are unreachable.  It is not a run that completes; it is
a perpetual per-surfacing service that re-arms at `T0WaitForSurfacing`.

Finally, the corpus's real redundancy is copy-paste, not control flow:
`riot.xml`, `drifter.xml`, `g3mrnlg.xml` and `g3mrnlg2.xml` are the
*identical* 25-state/119-transition graph differing only in command
payloads and two timeout values.  The 9,907 lines are roughly **three
distinct machines copied twenty times with the file list edited** — and
the copying has already gone wrong: `g3mrnlg2.xml` updated its `s`
command at 8 sites but not 4 others, including an echo-verify pattern
that now prefix-matches the longer command by accident.

### Retracted: "a linear list of steps"

Rev 1 proposed replacing the state machine with a linear sequence and
showed a 30-line YAML "translation" of `riot.xml`.  That example was
not the script.  It dropped `Ctrl-W`, dropped the entire second
`dockzr`/`Ctrl-F` pass, moved `Ctrl-F` from second position to fourth,
and contained a real bug: it sent `!dockszr` and waited for `!zr`,
which is not a substring of `!szr`.  It would have hung forever.  It
compared a subset to the whole.

The corpus needs, demonstrably: backward jumps to named steps (210
across the corpus), self-loops that re-send their own command (101),
forward skips of a *variable* number of steps, per-edge actions,
mutable flags that persist across a dive, and a reactive/looping shape
for the four scripts that are not sequences at all.  Adding all of that
to a step list reconstructs a transition table with different syntax.

### The defensible claim

**A state machine with a typed transfer API and a sane runtime.**  Not
a simpler language — a simpler *job*, because the typed channel deletes
58% of what the states are for, and the runtime supplies what every
author currently hand-writes.

Runtime requirements, each traceable to a corpus behaviour:

| Requirement | Because |
|---|---|
| **No timeout by default** | 151 of 355 non-final states carry none *on purpose*: *"adding a timeout transition out of this state is dangerous since the time required for a zModem transfer is unknown and highly variable."*  Rev 1's "default timeout" would tear down 40-minute uploads. |
| `idle_timeout:` reset by matched progress | riot's watchdogs are fed by `Total Bytes` / `Starting zModem`, not deadlines |
| Per-edge `goto:` / `retry:` with a cap | the 210 backward jumps |
| Actions on timeout edges | `T0WaitReloadMA`, and `vacuum_test_send_data_2hrs.xml` where the timeout *is* the program |
| Ordered `expect:` as a **list**, order = priority | `xml_engine.py:551` takes the first transition in document order matching *anywhere* in the buffer — not the earliest match position.  Any replacement must reproduce that. |
| Mutable flags in the checkpoint (`set:` / `when:`) | the reload-MA latch is set by a timeout on surfacing *N* and read on *N+1* |
| An explicit `loop:` shape | riot, and 8 more scripts, never terminate by design |
| Durable position + connection-loss unwinding | genuinely unavailable today, and genuinely worth having |
| Per-step `on_drop: stay \| unwind \| restart`, default **`stay`** | `stay` is what 16 of 20 carrier edges do |
| Vehicle capability record | replaces the T2/T3 rediscovery |
| Parameterisation | one script with a file list, not twenty copies |

**Checkpointing must be two-phase.**  A checkpoint written at *transmit*
time records only that we sent something, never that the glider ran it.
It must advance on *observed effect*.  Otherwise resume guesses, and
the guesses are unsafe in both directions: `skip` on an unconfirmed
`!put c_iridium_current_num 0` leaves the vehicle dialling the
secondary number for a deployment; `redo` on `Ctrl-R` can resume a
mission the pilot deliberately stopped and send the glider diving away
from the recovery vessel.

So: **steps that change vehicle state are non-resumable by
construction** (`Ctrl-R`, `Ctrl-C`, `run`, `callback`, `put`).  Reaching
one with a stale checkpoint aborts the run, not the step.  And a
checkpoint carries `(connection_id, mission_name, mission_number,
ma_file_set_hash, deployment_id)`; resuming across a *new* connection
requires re-observing and matching them.  A mismatch forces restart.
That is a precondition on the checkpoint, not a per-step option — hours
pass during a dive, and the glider may have aborted, changed missions,
or be in `lastgasp.mi` when it next appears.

**Confirmation is by effect, not echo.**  Control characters do not
echo as text — `^C` appears inline at the head of a glider output
line — and `Ctrl-F`/`Ctrl-R`/`Ctrl-W` are the plurality of corpus
traffic.  Every step must name its observable (`MAFILES will be
re-read`, `devices:`).  For `Ctrl-R` there may be no positive
confirmation short of carrier loss, which is itself informative and
should be a first-class expectation.

**The XML engine stays** as a second front end onto the same runtime,
so the twenty existing scripts keep running and gain checkpointing.
That is what makes the synthesized-string compatibility obligation
worth paying.

## Data and egress

Solved outside SFMC already.  [dbd2netcdf-python][ddn] (published as
`xarray-dbd`, CLI `xdbd`) reads every returned format —
`.dbd`/`.dcd`, `.ebd`/`.ecd`, `.sbd`/`.scd`, `.tbd`/`.tcd`,
`.mbd`/`.mcd`, `.nbd`/`.ncd`, including LZ4-framed compressed
variants — into xarray or NetCDF, with a C++ parser and optional
corrupt-file repair.

Egress emits **both shapes**, because there are two real consumers:

- **A drop directory** containing, by construction, only
  confirmed-complete files.  The existing rsync+inotify transport keeps
  working unchanged and quietly becomes more trustworthy.  NetCDF lands
  in a sibling directory the same transport already covers.
- **A typed event** on the bus carrying the ledger entry, for anything
  written from here on.

inotify is a workaround for a missing event; the ledger emits the real
one.  But the drop directory stays the interface, because a mature
transport that already works beats a migration.

**Open risk in the current setup:** if SFMC writes into `from-glider`
as Zmodem receives, rather than writing to a temp name and renaming,
then a transfer that dies at 40% fires inotify and rsync ships a
truncated file.  It would be silent — `.sbd` records are fixed-width,
so a truncated file parses cleanly and just ends early.  Worth checking
which inotify event the current trigger uses and whether SFMC renames.

[ddn]: https://github.com/mousebrains/dbd2netcdf-python

## Provenance

**Decided: clean room.**  This repo is public and GPL-3, a replacement
dockserver reimplements a vendor protocol, and TWR source exists under
NDA.  Deriving the implementation from that source is not reversible
after the fact, so the project does not do it.

The clean-room path and the good-engineering path coincide: the
provenance census (below) *is* the specification, and it is built from
observation rather than from anyone's source.

Practical rules that follow:

- **Permitted sources:** wire and dialog captures, the 20-script
  corpus, the public SFMC User Manual (Appendix E), the published
  Zmodem specification, and behaviour observed by experiment against
  osusim and real vehicles.
- **Not a source:** the NDA'd TWR source, in any form — not quoted, not
  paraphrased, not consulted to settle a design question.
- Every non-obvious protocol claim in this document and in the code
  cites *how it was observed*, so the derivation is auditable later.
  The corrections in [rev 2](#what-changed-in-rev-2) are the model:
  each one names the file, capture, or operator statement it came from.
- If a question can only be answered from vendor source, it becomes an
  **experiment**, not a lookup.  The `!`-prefix question in
  [Open questions](#open-questions) is the working example.

The NDA'd copy remains useful for *operating* SFMC and for
understanding what the vendor's software does.  It is walled off from
the reimplementation, not from the operator.

## Getting there

**Phase A — egress.  Days.  Independent of everything else.**
Watch `from-glider`, run `xdbd`, write NetCDF to a sibling directory
the existing rsync already ships.  No SFMC changes, no API dependency,
no dockserver.  Delivers force #4 immediately and survives whatever
happens to the rest of this document.
*Alongside it, for free:* run `pull_new_downloads.py` next to the
rsync for a season and diff what each delivers.  The mature path
becomes the oracle for the less-tested API path.

**Phase B — passive tap.  Days.  Zero operational risk.**
Rev 1 proposed an inline proxy.  Don't.  A `tcpdump` capture on the
dockserver's listening port gets the identical byte stream from real
deployments with **no process in the live path**, no failure mode, and
no rollback story.  Capture SFMC's `/topic/glider-link-output`
simultaneously and timestamp-align the two.

Deliverables: the **provenance census** (which side emits every matched
string); a real-failure corpus; and an answer to a question nobody has
asked — whether RUDICS presents Zmodem with a genuinely *lossy* channel
or a reliable-but-stall-prone one.  If it is the latter, "Zmodem
correct on a lossy link" aims months at the wrong failure mode.

*Caveat on osusim:* Serial2RUDICS has fault-injection hooks for slow
and noisy links, but they are barely exercised — exercising them is a
work item.  And osusim's callback behaviour differs from real Iridium,
so anything depending on call-in timing, `c_iridium_current_num`, or
redial-during-surfacing cannot be validated there.  Say so rather than
papering over it.

**Phase C — replay.  Weeks.  No glider involved.**
Demultiplexer, dialog projection, transfer ledger, script runner, all
against Phase B journals.  Success is *conformance* replay: recorded
input reproduces recorded output byte-identically, and the Zmodem
implementation reconstructs the same files.  Counterfactual replay
needs the simulator and is explicitly out of scope here.

> ### This is a legitimate stopping point.
>
> Phase A + B + C fully and permanently closes force #1 and delivers
> force #4, at days-plus-weeks, with nothing inserted into the live
> path.  Stopping here is a **successful outcome**, not an abandoned
> project.  Rev 1 described five phases each licensing the next and
> never said this; that omission is what made the ramp a trap.

**Gates before Phase D.**  All three answered in writing, or we stop:

1. Does GliderDos honour non-zero `ZRPOS` and answer `ZCRC`?
2. Does step-level checkpointing in the *existing* Python engine —
   which outlives the link and can persist state, ~2 weeks of
   work — fix force #3 without owning the dockserver?
3. Does the existing `get_zmodem_transfers` typed channel delete the
   scraping states?  If yes, the 58% argument does not require linkd.

**Phase D — linkd owns osusim.**  Terminal only, no transfers.

**Phase E — transfers, validated by tee.**  Rev 1 said "compare every
received file against what SFMC receives."  That is not executable: the
glider sends each file *once*, to whichever dockserver owns the
session, and alternating surfacings compare different files.  Worse, if
linkd's Zmodem completes the protocol but its ledger declines to
publish, the vehicle has moved on and the file is **permanently lost**.
So run linkd as a *tee on the capture*: SFMC stays the protocol
endpoint and owns success; linkd reconstructs files from observed
frames and diffs against SFMC's output.  A true A/B on identical bytes,
zero risk.  Only after byte-identical reconstruction over many
surfacings does linkd become an endpoint.

**Phase F — cutover.**  No hot standby: one process owns the listening
port.  Requirements before this is even discussed:

- A supervisor holds the listening socket (fd passing or
  `SO_REUSEPORT`) and survives the worker, failing over to dumb
  passthrough after N crashes in M minutes.  Otherwise a malformed
  frame that panics the demultiplexer becomes a crash loop: the glider
  redials, the same bytes arrive, it panics again, and the vehicle
  burns call-ins until a human wakes up.
- `.cac` divergence settled — if linkd receives the cache-generating
  exchange and idle SFMC does not, "rollback is a config change" leaves
  SFMC unable to decode anything it later receives.
- Someone **other than the author** has restored service from cold,
  unaided, in a drill.
- Never during a funded deployment; never on a vehicle under warranty.

## Effort

| | |
|---|---|
| Phase A (egress + differential test) | Days |
| Phase B (tap + census) | Days |
| Phase C (demux, projections, ledger, runner) | Weeks |
| Checkpointing in the existing XML engine | ~2 weeks — and it may close force #3 alone |
| Zmodem, correct against one unreadable peer | **The long pole.** Measured in *deployments*, not developer-months: bugs appear a few surfacings per week per vehicle, with a fix cycle gated on the next surfacing. Two to three seasons. |
| Glider simulator (only if counterfactual replay is wanted) | Comparable to Zmodem |
| Synthesized-string compatibility layer | Weeks, bounded by the census |
| API compatibility shim | Weeks — and note it is an **obligation**, not reuse: every endpoint `client.py` calls must now be *served* |
| Web UI | **Unpriced.** SFMC's UI is coupled to its internal DB and ActiveMQ, not to the REST API. Replace the dockserver and either the UI goes dark or you integrate with those internals. Decide which before Phase D. |

### What actually carries over

Rev 1's reuse list was padded.  Honestly:

| Carries over | Must be rewritten | Becomes an obligation |
|---|---|---|
| `XmlStateMachine` (the pure/`run_live()` split is the right shape) | linkd: sockets, Zmodem, journal, demux, arbitration | `client.py`'s 2,029 lines — the compat shim means *serving* all of it |
| `dialog_parser.py`, `dialog_stream.py` (projections, and the easy part) | `commands.py` echo anchoring — a heuristic that exists *because* we don't own the link; owning it deletes this | the 20-script corpus must keep running |
| the event model in `control_engine.md` | | |

`ma_writer.py` and `coordinates.py` are equally useful with SFMC in
place and are not evidence for this project.  Real carry-over is on the
order of 1,500–2,000 lines against 13,274.

## Risks that are not engineering

- **Vendor and institutional.**  Non-vendor software commanding a
  $150k+ asset on a funded deployment.  If a vehicle is lost while
  linkd owned the link, the first question from the vendor, the
  insurer, the PI, and the funding agency is *what was talking to it*.
  This needs an answer from the institution, not from the author,
  before Phase D.
- **Bus factor of one.**  After cutover, a single-instance service
  written and understood by one person sits between every vehicle in
  the water and its pilot.  Name a second maintainer, or do not cut
  over.
- **Provenance.**  See above.

## Open questions

1. **What does the `!` prefix mean?**  `!dockzr` is dockserver-side,
   but `!put`/`!get` are GliderDos commands, and both `dockzr` and
   `!dockzr` appear in the corpus.  Hypotheses: send-without-waiting-
   for-prompt; suppress command verification; or no effect at all.
   Discriminate by submitting each form at a quiet prompt, mid-output,
   and against a forced verify failure, with timestamped dialog
   capture.  **Nothing in the design may depend on this until it is
   answered.**
2. **ZRPOS / ZCRC / `ZCRESUM` / post-send file disposition.**  Gate 1
   above.
3. **How does each link actually terminate?**  gliderpi0 can be pointed
   at a different host/port, which makes osusim easy.  Freewave and
   direct-serial gliders need a different capture story.
4. **Which Zmodem?**  Port a known-good implementation, or write one
   against the spec with the journal as harness?  Given one unreadable
   peer whose *bugs* must be reproduced bug-compatibly, porting should
   be the default, not the alternative.
5. **Do we keep SFMC's REST shape?**  Keeping it preserves this library
   and the 48 Node programs; it also inherits decisions we would not
   repeat.  Suggest compatible surface first, better API alongside.
   Note that lease refusal has no representation in the existing
   API — map it to `503` + `Retry-After`, the thing an existing client
   is most likely to survive.
6. **Which firmware generations must be supported, and how are they
   detected?**  The T2/T3 branch is the current answer and it is
   rediscovered by regex every surfacing.

## What changed in rev 2

Four adversarial reviews — operational safety, protocol feasibility,
script-language expressiveness, and scope — plus corrections from the
operator.  The substantive changes:

- **Corrected numbers.**  119 transitions not 143; 48 actions not 51;
  20 carrier edges not 21; T1 is 3 states not a parallel machine; five
  thread prefixes not one boolean.  Rev 1's errors all ran in the
  direction that flattered its argument.
- **`dockzr`/`dockszr` are SFMC-side** (operator-confirmed).  This
  produced the two-channel journal, the provenance census, the
  byte-compatibility obligation, and the 56/41 split that is now the
  organizing idea.
- **Retracted "a linear list of steps"**, along with a worked example
  that was not the script it claimed to translate and contained a
  hang-forever bug.
- **Replay split into conformance and counterfactual.**  Only the first
  is free.
- **ZRPOS demoted from assumption to gate.**
- **Inline proxy replaced by a passive tap.**
- **Lease inverted**: the operator always wins.
- **An explicit stopping point**, with written gates before the
  expensive half.
- **Force #4 added** — decoded data and egress — and identified as the
  one requirement that needs none of this.
- **Historical firmware support added** as a fixed constraint.
- **Provenance, vendor risk, bus factor, and the unpriced web UI**
  added, having been absent entirely.
