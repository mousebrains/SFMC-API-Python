# Control Engines (`sfmc-control`)

A **control engine** reacts to a fleet's events on one thread and acts
through the SFMC API.  It is the general form of a follower: instead of
"what happened at a surfacing?", it asks "what happened?" — dialog,
connection events, script transitions, the results of its own requests.

> **This is the entry point that can command a glider.**  Read
> [Safety](#safety) before using `--allow-writes`.  Nothing changes
> state without it.

For the reasoning behind the design — the event model, the threading
guarantees, why `glider=` is a keyword — see
[design/control_engine.md](design/control_engine.md).

## The three postures

Two of these look identical from outside and only one can move a
vehicle:

```bash
# Read-only.  Writes are refused; the engine still sees everything.
sfmc-control --glider osu685 --engine my_engine.py

# Full logic, writes simulated.  Reads really happen, so the engine
# makes real decisions; only the consequences are withheld.
sfmc-control --glider osu685 --engine my_engine.py --allow-writes --dry-run

# For real.
sfmc-control --glider osu685 --engine my_engine.py --allow-writes
```

The posture is written to the audit log before anything else happens,
so *"was this run allowed to touch the glider?"* is answerable from the
log alone:

```
engine=MyEngine writes=blocked dry_run=False max_outstanding=12
```

## Writing an engine

```python
from sfmc_api import Event
from sfmc_api.engine import BaseControlEngine

class WatchBattery(BaseControlEngine):
    sources = ("dialog",)

    def __init__(self, config=None):
        super().__init__(config)
        self.last = {}

    def on_event(self, event: Event) -> None:
        match event.source:
            case "dialog" if "Vehicle Name:" in event.body:
                self.request("get_glider_details", event.glider,
                             glider=event.glider, tag="details")
            case "result" if event.tag == "details":
                self.last[event.glider] = event.body   # no locks needed
            case "error" if event.tag == "details":
                self.log("%s: %s", event.glider, event.body)
```

`on_start`, every `on_event`, and `on_stop` run on **one thread for the
whole fleet**, so cross-glider state is ordinary attributes with no
locking.  That is the point of the design.

`request()` names a client method by string, runs it off-thread, and
delivers the outcome back as a `result` or `error` event carrying the
request id and your tag.  **It does not raise for a blocked write** — a
refusal arrives as an `error` event, so you handle it like any other
failed operation.

`glider=` is a keyword, separate from the positional arguments, because
it names the *serialisation key* rather than an argument.  Two
operations on one glider never interleave; the same two on different
gliders may run concurrently.

## Running a follower on the engine

An existing `BaseFollower` runs unchanged, and gains formations:

```bash
sfmc-control --glider osu684 --glider osu685 \
             --follower my_follower.py --allow-writes
```

`SurfacingEvent.vehicle_name` has always carried the glider identity, so
`on_surfacing` needs no signature change.  One follower instance sees
every glider's surfacings, on one thread.

To steer a *different* glider than the one that surfaced, name it:

```python
self.send_files(to_glider={"goto_l10.ma": plan}, glider="osu686")
```

Outside `on_surfacing` — from a worker thread, say — there is no
"current" glider to default to, so in a formation you **must** name the
target or the batch is refused rather than sent to whichever glider
drains it next.

## Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `--glider NAME` | — | Repeatable.  The gliders to stream. |
| `--engine FILE` | — | Python file with a `BaseControlEngine` subclass |
| `--follower FILE` | — | Python file with a `BaseFollower` subclass |
| `--class NAME` | auto | Which class, if the file has several |
| `--config FILE` | — | YAML passed to the engine/follower |
| `--allow-writes` | off | Permit state-changing operations |
| `--dry-run` | off | Simulate writes; reads still happen |
| `--replay LOG` | — | Offline, no client constructed at all |
| `--tick SECONDS` | off | Periodic wake-up per glider |
| `--max-outstanding N` | 12 | Requests in flight, fleet-wide |
| `--max-runtime SECONDS` | — | Stop after this long |
| `--audit-log PATH` | — | Also write the audit trail to a file |
| `--hostname`, `--credentials` | — | As other commands |

## Safety

1. **Writes are off by default.**  A blocked write produces an `error`
   event naming the flag.
2. **Dry run** simulates every write with a distinct `DryRun` result —
   a distinct type, so an engine mistaking it for server data can
   notice.
3. **Replay** constructs no client at all.  Writes are simulated; reads
   are impossible, and say so.
4. **A fleet-wide cap** on outstanding requests, because SFMC rate-limits
   the account rather than the glider.  Exceeding it produces an `error`
   event rather than queueing silently.
5. **An audit log** (`sfmc_api.engine.audit`) records every request and
   outcome with its glider, the run's posture at startup, and every
   change to the fleet.

**`--glider` is what is streamed, not a capability boundary.**  A write
whose serialisation key falls outside the fleet is *warned about* in the
audit log, not refused — refusing on that key blocked legitimate
operations (`register_glider` names a glider that cannot be in the fleet
yet) while stopping nothing, since the real target is an argument.  If
you need a hard boundary, run one engine per glider.

**The raw client** (`self.client`) bypasses every rail: it requires
`--allow-writes`, is refused under `--dry-run`, and is logged at
`WARNING` as the point where the audit trail stops being complete.

## Testing an engine without a glider

```bash
sfmc-control --glider osusim --engine my_engine.py --replay dialog.log
```

No client is constructed and nothing is contacted.  Writes are
simulated, so an engine that acts can be rehearsed offline.  Note that
`--tick` and `--max-runtime` are live-only, so an engine that reacts to
the *absence* of dialog cannot be fully exercised this way.

Dialog logs come from
[`sfmc-monitor-glider`](monitor_glider.md).

## See also

- [design/control_engine.md](design/control_engine.md) — the design and its reasoning
- [follow_glider.md](follow_glider.md) — the single-glider follower pipeline
- [script_control.md](script_control.md) — command submission and reply capture
- `examples/control_engine_formation.py`, `control_engine_command.py`,
  `control_engine_joint_decision.py`
