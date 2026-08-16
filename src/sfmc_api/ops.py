"""Asynchronous execution for any SFMC operation.

Every method on :class:`~sfmc_api.client.SFMCClient` is synchronous: it
sends a request and returns the parsed response.  That is the right
default, but a driving script often wants to issue work without
blocking, or to be told when it finishes rather than waiting for it.

:class:`OperationExecutor` provides that **without describing the API a
second time**.  It runs whatever bound method you hand it on a worker
thread and returns a :class:`concurrent.futures.Future`, so an endpoint
added to ``SFMCClient`` tomorrow is asynchronously callable the moment
it exists — there is no per-endpoint wrapper here to fall out of date::

    with client.operations() as ops:
        future = ops.submit(client.get_glider_details, "osu685")
        future.add_done_callback(lambda f: print(f.result()))
        details = future.result(timeout=30)

The same :class:`~concurrent.futures.Future` is what
:meth:`~sfmc_api.commands.CommandChannel.send_async` returns, so one
idiom covers the whole package.  ``asyncio`` callers need no separate
client::

    details = await asyncio.wrap_future(ops.submit(client.get_glider_details, "osu685"))

Two hazards this layer exists to manage, beyond what a bare
:class:`~concurrent.futures.ThreadPoolExecutor` would give you:

* **Concurrency.** SFMC rate-limits (HTTP 429).  The pool is small by
  default so a fan-out of requests does not turn into a 429 storm;
  :meth:`SFMCClient._request` still backs off per request.
* **Ordering.** Futures complete in whatever order the server answers.
  Operations that must not interleave — two plan updates on one
  glider, an upload followed by the deploy that consumes it — go
  through :meth:`serialized` or :meth:`sequence`, which hold a
  per-key lock for the duration.

**Cancellation is limited.**  ``Future.cancel()`` succeeds only while
an operation is still queued.  An HTTP request already in flight
cannot be recalled, and a state-changing request that reached the
server has already been applied.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, ParamSpec, TypeVar

__all__ = [
    "DEFAULT_MAX_WORKERS",
    "KeyedLock",
    "OperationExecutor",
    "OperationResult",
]

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")

#: Default worker count.  Small on purpose: SFMC rate-limits, and
#: glider operations are latency-bound rather than throughput-bound.
DEFAULT_MAX_WORKERS = 4


class KeyedLock:
    """A registry of locks, one per key, created on demand.

    Used to serialize operations that must not interleave for a given
    glider while leaving operations on *other* gliders concurrent.
    """

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    def get(self, key: str) -> threading.Lock:
        """Return the lock for *key*, creating it if needed."""
        with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            return lock

    @contextmanager
    def hold(self, key: str, timeout: float = -1.0) -> Iterator[None]:
        """Hold *key*'s lock for the duration of the block.

        Args:
            key: What to serialize on (a glider name, typically).
            timeout: Seconds to wait for the lock; ``-1`` waits
                forever.

        Raises:
            TimeoutError: If the lock was not acquired in *timeout*.
        """
        lock = self.get(key)
        if not lock.acquire(timeout=timeout):
            raise TimeoutError(f"Timed out after {timeout}s waiting for the {key!r} lock")
        try:
            yield
        finally:
            lock.release()


@dataclass(frozen=True)
class OperationResult:
    """The outcome of one submitted operation, for observers.

    Attributes:
        name: ``__qualname__`` of the callable that ran.
        ok: ``True`` if it returned, ``False`` if it raised.
        value: The return value when *ok*, else ``None``.
        error: The exception when not *ok*, else ``None``.
        elapsed: Wall-clock seconds the operation took.
    """

    name: str
    ok: bool
    value: Any = None
    error: BaseException | None = None
    elapsed: float = 0.0


@dataclass
class OperationExecutor:
    """Run SFMC operations on a bounded thread pool.

    Args:
        max_workers: Concurrent operations.  Keep it small; SFMC
            rate-limits.
        thread_name_prefix: Prefix for worker thread names.

    Use as a context manager so the pool is shut down on exit; a
    lingering pool keeps a program alive at exit.
    """

    max_workers: int = DEFAULT_MAX_WORKERS
    thread_name_prefix: str = "sfmc-op"
    _pool: ThreadPoolExecutor = field(init=False, repr=False)
    _locks: KeyedLock = field(init=False, repr=False, default_factory=KeyedLock)
    _observers: list[Callable[[OperationResult], None]] = field(
        init=False, repr=False, default_factory=list
    )

    def __post_init__(self) -> None:
        if self.max_workers < 1:
            raise ValueError("max_workers must be >= 1")
        self._pool = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix=self.thread_name_prefix,
        )

    # ── Submission ───────────────────────────────────────────────────

    def submit(
        self,
        fn: Callable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[R]:
        """Run *fn* on a worker thread and return its future.

        *fn* is normally a bound :class:`SFMCClient` method, but any
        callable works.  Argument and return types are preserved, so
        ``ops.submit(client.get_glider_details, "osu685")`` is a
        ``Future[dict[str, Any]]`` to a type checker.

        Exceptions raised by *fn* — :class:`~sfmc_api.exceptions.APIError`,
        :class:`~sfmc_api.exceptions.RateLimitError` — surface from
        :meth:`Future.result`, not here.
        """
        return self._pool.submit(self._observed, fn, args, kwargs)

    def serialized(
        self,
        key: str,
        fn: Callable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[R]:
        """Like :meth:`submit`, but serialized against *key*.

        Operations sharing a key never run concurrently.  Use the
        glider name as the key for state-changing operations: two plan
        updates or two commands racing on one glider is a real hazard,
        while the same operations on different gliders are safely
        concurrent.
        """

        def run() -> R:
            with self._locks.hold(key):
                return fn(*args, **kwargs)

        return self._pool.submit(self._observed, run, (), {})

    def sequence(
        self,
        key: str,
        *calls: tuple[Callable[..., Any], ...],
    ) -> Future[list[Any]]:
        """Run *calls* in order under *key*'s lock, returning all results.

        Each call is a tuple of ``(callable, *args)``.  Use it when a
        later step depends on an earlier one::

            ops.sequence(
                "osu685",
                (client.upload_glider_files, "osu685", "to-glider", paths),
                (client.deploy_goto_file, "osu685"),
            )

        The sequence stops at the first exception, which surfaces from
        :meth:`Future.result`; steps already completed are not undone.
        Return values are untyped (``list[Any]``) because the steps
        need not agree on a type — use :meth:`serialized` when you
        want the return type preserved.
        """
        if not calls:
            raise ValueError("sequence() requires at least one call")

        def run() -> list[Any]:
            results: list[Any] = []
            with self._locks.hold(key):
                for call in calls:
                    fn, *args = call
                    results.append(fn(*args))
            return results

        return self._pool.submit(self._observed, run, (), {})

    def map(
        self,
        fn: Callable[..., R],
        items: list[Any],
    ) -> list[Future[R]]:
        """Submit *fn* once per item, returning the futures in order.

        Convenience for fanning one operation across many gliders::

            futures = ops.map(client.get_glider_details, ["osu684", "osu685"])
            details = [f.result() for f in futures]
        """
        return [self._pool.submit(self._observed, fn, (item,), {}) for item in items]

    # ── Observation ──────────────────────────────────────────────────

    def on_result(self, callback: Callable[[OperationResult], None]) -> None:
        """Call *callback* after every operation, successful or not.

        Runs on the worker thread that ran the operation.  Exceptions
        from the callback are logged and swallowed — an observer must
        not be able to fail the operation it is observing.
        """
        self._observers.append(callback)

    def _observed(
        self,
        fn: Callable[..., R],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> R:
        """Run *fn*, reporting the outcome to observers either way."""
        name = getattr(fn, "__qualname__", repr(fn))
        started = time.monotonic()
        try:
            value = fn(*args, **kwargs)
        except BaseException as exc:
            self._notify(
                OperationResult(
                    name=name,
                    ok=False,
                    error=exc,
                    elapsed=time.monotonic() - started,
                )
            )
            raise
        self._notify(
            OperationResult(
                name=name,
                ok=True,
                value=value,
                elapsed=time.monotonic() - started,
            )
        )
        return value

    def _notify(self, result: OperationResult) -> None:
        for observer in list(self._observers):
            try:
                observer(result)
            except Exception:
                logger.exception("operation observer failed; continuing")

    # ── Lifecycle ────────────────────────────────────────────────────

    def shutdown(self, wait: bool = True, *, cancel_pending: bool = False) -> None:
        """Stop accepting work and tear the pool down.

        Args:
            wait: Block until running operations finish.
            cancel_pending: Drop operations that have not started.
                In-flight requests cannot be cancelled.
        """
        self._pool.shutdown(wait=wait, cancel_futures=cancel_pending)

    def __enter__(self) -> OperationExecutor:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.shutdown()
