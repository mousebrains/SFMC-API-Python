# Asynchronous Operations

> **What is this for?**  Every `SFMCClient` method is synchronous: it
> sends a request and returns the parsed response.  That is the right
> default.  This page is for when a driving script wants to issue work
> *without blocking*, be *told* when it finishes, or run several
> operations at once — for any endpoint, not just commands.

## The one idiom

Everything asynchronous in this package returns a
[`concurrent.futures.Future`](https://docs.python.org/3/library/concurrent.futures.html#future-objects).
There is no second client, no async variant of each method, and no
separate protocol stack to keep in sync:

```python
with client.operations() as ops:
    future = ops.submit(client.get_glider_details, "osu685")

    details = future.result(timeout=30)     # block for it
    future.add_done_callback(handle)        # or be told
    details = await asyncio.wrap_future(future)   # or await it
```

`CommandChannel.send_async()` returns the same type, so one idiom
covers commands and plain REST calls alike.

## Why not a per-endpoint async API?

Because it would be a second description of the API, and second
descriptions drift.  `OperationExecutor` runs *whatever bound method
you hand it*, so an endpoint added to `SFMCClient` tomorrow is
asynchronously callable the moment it exists — there is no wrapper
here to forget to update.

Types survive the trip: `ops.submit(client.get_glider_details,
"osu685")` is a `Future[dict[str, Any]]` to a type checker, because
`submit` is generic over the callable's signature.

### Why not a full asyncio port?

An `asyncio` client would mean two protocol stacks — `httpx.Client`
and `httpx.AsyncClient`, sync and async WebSockets — maintained
forever, and the synchronous one could never be removed: the CLI,
the systemd services, and the follower framework all depend on it.
Threads plus `asyncio.wrap_future` give async callers what they need
with one implementation.

## Ordering

Futures complete in whatever order the server answers.  When
operations must not interleave, say so explicitly:

```python
# Serialize on the glider name: two plan updates racing on one glider
# is a real hazard; the same operations on different gliders are not.
ops.serialized("osu685", client.update_waypoint_plan, "osu685", goto_file)

# Steps that depend on each other, in order, under one lock:
ops.sequence(
    "osu685",
    (client.upload_glider_files, "osu685", "to-glider", paths),
    (client.deploy_goto_file, "osu685"),
)
```

`sequence()` stops at the first exception, which surfaces from
`Future.result()`.  Steps already completed are **not** undone — there
is no rollback, because most SFMC operations have none.

## Concurrency and rate limits

SFMC rate-limits (HTTP 429).  The pool defaults to four workers so a
fan-out does not become a 429 storm; each individual request still
backs off on 429 inside `SFMCClient._request`.

```python
futures = ops.map(client.get_glider_details, ["osu684", "osu685", "osu686"])
details = [f.result(timeout=30) for f in futures]
```

Raise `max_workers` only if you know the server tolerates it.

## Observing every operation

```python
ops.on_result(lambda r: log.info("%s ok=%s in %.2fs", r.name, r.ok, r.elapsed))
```

Observers run on the worker thread, after the operation, successful or
not.  An observer that raises is logged and skipped — it can never
fail the operation it is watching.

## Cancellation is limited

`Future.cancel()` succeeds only while an operation is still queued.
An HTTP request already in flight cannot be recalled, and a
state-changing request that reached the server has already been
applied.  `shutdown(cancel_pending=True)` drops queued work only.

This is not a limitation of the executor — it is what "the request
already left" means.  Design around it rather than assuming a cancel
undoes anything.

## Event-driven streams

Futures cover request/response.  For *unsolicited* events — dialog
lines, connection events, script transitions — use a session's
callbacks and listeners instead; see
[streaming.md](streaming.md).

```python
with client.session("osu685", topics=["dialog", "connections"]) as session:
    session.on_line(lambda line: print(line.text))
    session.on_connect(lambda reconnected: print("stream up"))
```

## See also

* [`examples/send_command.py`](../examples/send_command.py) — all of
  the above in one runnable script.
* [script_control.md](script_control.md) — commands and reply capture.
* [streaming.md](streaming.md) — sessions, fan-out, reconnect.
