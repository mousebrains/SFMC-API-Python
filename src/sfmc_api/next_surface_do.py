"""Inject ad-hoc "next surfacing do" commands into a glider surfacing.

A Slocum glider surfaces mid-mission, interrupts for comms, and prints
``Hit Control-R to RESUME``.  While it is loitering at that prompt you
can inject commands with the ``!`` prefix (e.g. ``!put u_alt_min_depth
50``).  Normally an SFMC *script* owns that prompt and drives the
routine surfacing work (file up/downloads, then a ``Ctrl-R`` to resume
the mission).

This module lets a user queue a few commands to be sent on the *next*
surfacing without editing the dockserver script.  The sequence is:

1. Watch the connection-event stream and wait for the glider to connect.
2. Pause the assigned script so it stops driving the prompt (only the
   daemon touches it during the injection window).
3. Wait for a *quiescent* ``Hit Control-R to RESUME`` prompt — i.e. the
   loiter prompt with no zModem transfer in progress — so we never
   write text into a live binary transfer.
4. Send each command (``!``-prefixed), pacing on the command's echo.
5. Rewind + resume the script so it runs its normal surfacing work
   (including its own ``Ctrl-R``) this same surfacing.  The resume is in
   a ``finally`` — a crash mid-injection must never leave the script
   paused across future surfacings.

Because the assigned script's ``Ctrl-R`` is the last thing it does, the
injected commands land *before* the routine work, and the daemon never
has to send a control character itself.

Why pause on connection instead of ahead of time?  Pre-pausing leaves
the script visibly paused for hours while the glider is down, which
invites an operator to "fix" it.  Pausing on connect confines the
paused state to the active surfacing.  The cost is a small race: the
script may fire its first command before the pause lands, so we wait
for a quiescent prompt (step 3) rather than injecting immediately.

The :func:`run_next_surface_do` function is the reusable entry point —
a blocking CLI uses it here, and a web backend can import and call it
the same way.

.. warning::

   If a queued command sets a parameter that also lives in a mafile,
   the assigned script's ``Ctrl-F`` (reread mafiles) may clobber the
   injected value, because injection happens before the script runs.
   Use this for runtime puts that are not overridden by mafiles.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from .client import SFMCClient
    from .stomp import StompConnection, StompSubscription

log = logging.getLogger("sfmc.next_surface_do")

# ── Dialog vocabulary (from the SFMC gliderScripts) ──────────────────

#: The glider prints this while loitering at the surface, ready for a
#: command.  It is re-emitted periodically, which is what lets us (and
#: the resumed script) catch it.
DEFAULT_PROMPT = "Hit Control-R to RESUME"

#: The dockserver prints this when a sent command fails to echo back
#: within ~20 s, i.e. the glider missed it — resend.
VERIFY_FAIL = "command verify fail"

_LINE_SEP = re.compile(r"\r\n|\r|\n")
_WS = re.compile(r"\s+")


# ── Configuration & result ───────────────────────────────────────────


@dataclass
class InjectConfig:
    """Tunables for the injection sequence."""

    prompt: str = DEFAULT_PROMPT
    #: Add a leading ``!`` to commands that lack one (required to inject
    #: while the mission is interrupted at the surface).
    bang_prefix: bool = True
    #: Seconds of dialog silence *after* a prompt before we treat the
    #: glider as idle-at-prompt (not mid-transfer) and start injecting.
    quiet_seconds: float = 8.0
    #: Max seconds to wait for a quiescent prompt (a long zModem
    #: transfer may already be running when we pause).
    quiescent_timeout: float = 1200.0
    #: Seconds to wait for a sent command to echo before resending.
    echo_timeout: float = 30.0
    #: Number of resends after the first attempt.
    echo_retries: int = 2
    #: Max seconds to wait for the glider to connect (``None`` = wait
    #: forever, stop with Ctrl-C).
    connect_timeout: float | None = None


@dataclass
class InjectResult:
    """Outcome of an injection attempt."""

    connected: bool = False
    quiescent: bool = False
    sent: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    paused: bool = False
    resumed: bool = False

    @property
    def ok(self) -> bool:
        """True if we connected, sent every command, and resumed."""
        return (
            self.connected
            and self.quiescent
            and self.resumed
            and not self.failed
        )

    def format(self) -> str:
        return (
            f"connected={self.connected} quiescent={self.quiescent} "
            f"sent={len(self.sent)} failed={len(self.failed)} "
            f"resumed={self.resumed}"
        )


# ── Actions abstraction (real vs. dry-run) ───────────────────────────


class ProgressFn(Protocol):
    """Called at each injection transition (e.g. ``"injecting"``)."""

    def __call__(self, event: str, result: InjectResult) -> None: ...


def _emit(progress: ProgressFn | None, event: str, result: InjectResult) -> None:
    if progress is None:
        return
    try:
        progress(event, result)
    except Exception:  # noqa: BLE001 - status reporting must never break injection
        log.exception("progress callback for %r failed", event)


class Actions(Protocol):
    """The mutating operations the injector performs on the glider.

    Abstracted so live runs hit the SFMC client while dry-run/replay
    runs just log what *would* happen.
    """

    def pause(self, glider: str) -> None: ...
    def send(self, glider: str, command: str) -> None: ...
    def rewind(self, glider: str) -> None: ...
    def resume(self, glider: str) -> None: ...


class LiveActions:
    """Real actions backed by an :class:`SFMCClient`."""

    def __init__(self, client: SFMCClient) -> None:
        self._client = client

    def pause(self, glider: str) -> None:
        self._client.pause_assigned_script(glider)

    def send(self, glider: str, command: str) -> None:
        self._client.send_command(glider, command)

    def rewind(self, glider: str) -> None:
        self._client.rewind_assigned_script(glider)

    def resume(self, glider: str) -> None:
        self._client.resume_assigned_script(glider)


class DryRunActions:
    """No-op actions that only log — for dry-run and replay."""

    def pause(self, glider: str) -> None:
        log.info("[dry-run] pause script on %s", glider)

    def send(self, glider: str, command: str) -> None:
        log.info("[dry-run] send to %s: %s", glider, command)

    def rewind(self, glider: str) -> None:
        log.info("[dry-run] rewind script on %s", glider)

    def resume(self, glider: str) -> None:
        log.info("[dry-run] resume script on %s", glider)


# ── Dialog line source ───────────────────────────────────────────────


class LineSource(Protocol):
    """A source of dialog lines that the injector polls with timeouts."""

    def next_line(self, timeout: float | None) -> str | None:
        """Next line, ``None`` if ended; raises ``queue.Empty`` on timeout."""
        ...

    def stop(self) -> None: ...


class DialogLines:
    """Background reader that turns dialog data into whole lines.

    Wraps either a live :class:`StompSubscription` (via
    :func:`ordered_dialog`) or an iterable of raw text (replay).  Each
    complete line is pushed onto a queue that the orchestrator polls
    with timeouts, which is how we measure dialog silence.
    """

    def __init__(self, chunks: Iterable[str]) -> None:
        self._chunks = chunks
        self._queue: Queue[str | None] = Queue()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._stop = threading.Event()

    def start(self) -> DialogLines:
        self._thread.start()
        return self

    def _run(self) -> None:
        buf = ""
        try:
            for chunk in self._chunks:
                if self._stop.is_set():
                    break
                buf += chunk
                parts = _LINE_SEP.split(buf)
                buf = parts.pop()  # trailing partial line
                for line in parts:
                    self._queue.put(line)
        finally:
            if buf.strip():
                self._queue.put(buf)
            self._queue.put(None)  # end sentinel

    def next_line(self, timeout: float | None) -> str | None:
        """Return the next line, or ``None`` if the stream ended.

        Raises :class:`queue.Empty` on timeout with no line — that is
        how callers detect a silent gap in the dialog.
        """
        item = self._queue.get(timeout=timeout)
        return item

    def stop(self) -> None:
        self._stop.set()


class ManualDialog:
    """A dialog source you feed by hand — used by the replay simulator.

    Unlike :class:`DialogLines`, lines are pushed in via :meth:`feed`
    rather than read from a stream.  This lets the replay inject each
    command's echo at the moment the injector "sends" it, closing the
    request/response loop a static log file cannot.
    """

    def __init__(self) -> None:
        self._queue: Queue[str | None] = Queue()

    def feed(self, line: str) -> None:
        self._queue.put(line)

    def end(self) -> None:
        self._queue.put(None)

    def next_line(self, timeout: float | None) -> str | None:
        return self._queue.get(timeout=timeout)

    def stop(self) -> None:  # pragma: no cover - nothing to tear down
        pass


# ── Matching helpers ─────────────────────────────────────────────────


def _norm(text: str) -> str:
    return _WS.sub(" ", text).strip().lower()


def _echo_matches(wire: str, line: str) -> bool:
    """True if *line* looks like the glider echoing back *wire*."""
    return _norm(wire) in _norm(line)


def _to_wire(command: str, cfg: InjectConfig) -> str:
    cmd = command.strip()
    if cfg.bang_prefix and not cmd.startswith("!"):
        return "!" + cmd
    return cmd


# ── Core injection sequence ──────────────────────────────────────────


def _wait_quiescent_prompt(lines: LineSource, cfg: InjectConfig) -> bool:
    """Block until the glider is idle at the surface prompt.

    We consider the glider quiescent once we have seen the prompt and
    then observed ``quiet_seconds`` of dialog silence.  If we paused the
    script mid-transfer, output keeps flowing until the transfer ends,
    the glider re-emits the prompt, and then goes quiet — so this also
    covers the pause-lands-mid-transfer race.
    """
    deadline = time.monotonic() + cfg.quiescent_timeout
    seen_prompt = False
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            log.warning("Timed out waiting for a quiescent prompt")
            return False
        try:
            line = lines.next_line(timeout=min(cfg.quiet_seconds, remaining))
        except Empty:
            # A silent gap.  If we have seen the prompt, the glider is
            # idle and ready; otherwise keep waiting.
            if seen_prompt:
                return True
            continue
        if line is None:
            log.warning("Dialog stream ended before a quiescent prompt")
            return False
        log.debug("dialog: %s", line)
        if cfg.prompt in line:
            seen_prompt = True


def _send_and_verify(
    actions: Actions,
    glider: str,
    wire: str,
    lines: LineSource,
    cfg: InjectConfig,
) -> bool:
    """Send one command and wait for its echo, resending on miss."""
    for attempt in range(cfg.echo_retries + 1):
        if attempt:
            log.info("Resending (attempt %d): %s", attempt + 1, wire)
        actions.send(glider, wire)
        deadline = time.monotonic() + cfg.echo_timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                log.warning("No echo for %r within %.0fs", wire, cfg.echo_timeout)
                break
            try:
                line = lines.next_line(timeout=remaining)
            except Empty:
                break
            if line is None:
                log.warning("Dialog stream ended while verifying %r", wire)
                return False
            log.debug("dialog: %s", line)
            if _echo_matches(wire, line):
                log.info("Confirmed: %s", wire)
                return True
            if VERIFY_FAIL in line:
                log.info("Dockserver reported verify fail for %r", wire)
                break  # resend
    return False


def inject_commands(
    actions: Actions,
    glider: str,
    commands: list[str],
    lines: LineSource,
    cfg: InjectConfig,
    result: InjectResult,
    progress: ProgressFn | None = None,
) -> None:
    """Pause, wait for a clean prompt, inject, then rewind + resume.

    Assumes the glider is already connected and *lines* is a started
    dialog source.  Always resumes the script if it was paused.

    *progress*, if given, is called with a short event name at each
    transition (``"injecting"``, ``"resumed"``) so a caller — e.g. the
    worker daemon — can update external status.
    """
    try:
        if not result.paused:
            actions.pause(glider)
            result.paused = True
            log.info("Paused assigned script; waiting for a quiescent prompt")
        else:
            log.info("Script already paused; waiting for a quiescent prompt")

        result.quiescent = _wait_quiescent_prompt(lines, cfg)
        if not result.quiescent:
            log.error("Never reached a quiescent prompt; aborting injection")
            return

        _emit(progress, "injecting", result)
        for command in commands:
            wire = _to_wire(command, cfg)
            log.info("Injecting: %s", wire)
            if _send_and_verify(actions, glider, wire, lines, cfg):
                result.sent.append(command)
            else:
                result.failed.append(command)
                log.error("Giving up on %r; aborting remaining commands", command)
                break
    finally:
        if result.paused and not result.resumed:
            log.info("Rewinding and resuming assigned script")
            try:
                actions.rewind(glider)
            except Exception:  # noqa: BLE001 - resume must still run
                log.exception("Rewind failed; resuming anyway")
            actions.resume(glider)
            result.resumed = True
            log.info("Script resumed")
            _emit(progress, "resumed", result)


# ── Connection wait (live) ───────────────────────────────────────────


def _iter_connection_events(msg: Any) -> Iterator[dict[str, Any]]:
    """Connection messages arrive as a dict or a list of dicts."""
    if isinstance(msg, list):
        yield from (e for e in msg if isinstance(e, dict))
    elif isinstance(msg, dict):
        yield msg


def _wait_for_connect(
    client: SFMCClient,
    glider: str,
    stomp: StompConnection,
    cfg: InjectConfig,
) -> bool:
    """Block until the glider connects (an event with ``active`` true)."""
    sub = client.subscribe_connection_events(glider, stomp)
    deadline = (
        None if cfg.connect_timeout is None else time.monotonic() + cfg.connect_timeout
    )
    try:
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                log.warning("Timed out waiting for %s to connect", glider)
                return False
            timeout = None if deadline is None else max(0.1, deadline - time.monotonic())
            try:
                msg = sub.get(timeout=timeout)
            except Empty:
                continue
            if msg is None:
                log.warning("Connection-event stream closed")
                return False
            for evt in _iter_connection_events(msg):
                if evt.get("active"):
                    log.info("%s connected (id=%s)", glider, evt.get("id"))
                    return True
                log.debug("Connection event (not active): %s", evt)
    finally:
        sub.close()


# ── Public entry points ──────────────────────────────────────────────


def run_next_surface_do(
    client: SFMCClient,
    glider: str,
    commands: list[str],
    cfg: InjectConfig | None = None,
    *,
    dry_run: bool = False,
    pre_pause: bool = False,
    progress: ProgressFn | None = None,
) -> InjectResult:
    """Wait for the next surfacing and inject *commands* into it.

    This is the reusable core: the CLI calls it, and a web backend can
    call it the same way (e.g. in a worker thread/task).  It blocks
    until the glider connects, so callers that must stay responsive
    should run it off the main thread.

    Args:
        client: An authenticated :class:`SFMCClient`.
        glider: Registered glider name.
        commands: Command strings (without the ``!`` prefix; it is added
            unless :attr:`InjectConfig.bang_prefix` is false).
        cfg: Tunables; defaults are used when ``None``.
        dry_run: If true, connect and watch the real dialog but do not
            pause/send/resume — just log intended actions.
        pre_pause: If true, pause the script *before* waiting for the
            glider to connect, removing the race where the script fires
            its first command before the pause lands.  The trade-off is
            the script sits visibly paused until the glider surfaces.
            Whether pre- or post-connect pausing, the script is always
            resumed — even if the glider never connects.

    Returns:
        An :class:`InjectResult` describing the outcome.
    """
    cfg = cfg or InjectConfig()
    result = InjectResult()
    actions: Actions = DryRunActions() if dry_run else LiveActions(client)

    with client.open_stream() as stomp:
        try:
            if pre_pause:
                actions.pause(glider)
                result.paused = True
                log.info("Pre-paused assigned script; waiting for %s to connect", glider)

            result.connected = _wait_for_connect(client, glider, stomp, cfg)
            if not result.connected:
                return result
            _emit(progress, "connected", result)

            output = client.subscribe_glider_output(glider, stomp)
            from .monitor_glider import ordered_dialog

            lines = DialogLines(ordered_dialog(output)).start()
            try:
                inject_commands(
                    actions, glider, commands, lines, cfg, result, progress
                )
            finally:
                lines.stop()
                output.close()
        finally:
            # Safety net: if we paused (typically pre-pause) but never
            # got as far as resuming — e.g. the glider never connected —
            # resume now so the script is never stranded paused.
            if result.paused and not result.resumed:
                log.info("Resuming assigned script (was paused, never resumed)")
                actions.resume(glider)
                result.resumed = True

    return result


class _ReplayActions:
    """Logging actions that also feed each command's echo back.

    On :meth:`send`, it pushes the echo line into the :class:`ManualDialog`
    so the injector's echo-verify step sees it — simulating the glider
    echoing a received command.
    """

    def __init__(self, dialog: ManualDialog, echo_delay: float = 0.2) -> None:
        self._dialog = dialog
        self._echo_delay = echo_delay

    def pause(self, glider: str) -> None:
        log.info("[replay] pause script on %s", glider)

    def send(self, glider: str, command: str) -> None:
        log.info("[replay] send to %s: %s", glider, command)

        def _echo() -> None:
            time.sleep(self._echo_delay)
            self._dialog.feed(command)  # glider echoes the command back

        threading.Thread(target=_echo, daemon=True).start()

    def rewind(self, glider: str) -> None:
        log.info("[replay] rewind script on %s", glider)

    def resume(self, glider: str) -> None:
        log.info("[replay] resume script on %s", glider)


def run_replay(
    glider: str,
    commands: list[str],
    dialog_path: Path | str,
    cfg: InjectConfig | None = None,
    *,
    interval: float = 0.3,
) -> InjectResult:
    """Exercise the injection logic offline, no SFMC connection.

    Lines from *dialog_path* are fed as the pre-surfacing dialog (the
    transfer chatter and prompts a real script would produce).  After
    that scripted dialog goes quiet, the injector pauses, waits for a
    quiescent prompt, and sends its commands; the simulator feeds each
    command's echo back so the send→verify loop actually closes.  All
    mutating actions are logged, never performed.

    Use this to watch the whole pause/wait/inject/resume sequence
    without a glider.
    """
    cfg = cfg or InjectConfig()
    result = InjectResult(connected=True)
    dialog = ManualDialog()
    actions: Actions = _ReplayActions(dialog)

    scripted = [
        line.rstrip("\n")
        for line in open(dialog_path, encoding="utf-8", errors="replace")
    ]

    def _feed_scripted() -> None:
        for line in scripted:
            time.sleep(interval)
            dialog.feed(line)
        # Then go silent so the quiescence timer can fire; the stream is
        # left open so injected-command echoes can still be delivered.

    feeder = threading.Thread(target=_feed_scripted, daemon=True)
    feeder.start()
    try:
        inject_commands(actions, glider, commands, dialog, cfg, result)
    finally:
        dialog.end()
    return result


# ── Command file parsing ─────────────────────────────────────────────


def read_commands(path: Path | str) -> list[str]:
    """Read commands from a file, skipping blanks and ``#`` comments."""
    commands: list[str] = []
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            commands.append(line)
    return commands


# ── CLI ──────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="sfmc-next-surface-do",
        description=(
            "Inject 'next surfacing do' commands into a glider's next "
            "surfacing by pausing its SFMC script, sending the commands, "
            "then rewinding and resuming the script."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("glider", help="Registered glider name")
    p.add_argument(
        "commands_file",
        help="Text file of commands, one per line (# comments allowed)",
    )
    p.add_argument("--host", help="Host to select from a multi-host credentials file")
    p.add_argument(
        "--credentials",
        help="Path to credentials JSON (default ~/.config/sfmc/credentials.json)",
    )

    mode = p.add_argument_group("modes")
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Connect and watch the real dialog but do not pause/send/resume",
    )
    mode.add_argument(
        "--pre-pause",
        action="store_true",
        help=(
            "Pause the script before the glider connects (no first-command "
            "race), at the cost of it sitting paused until it surfaces"
        ),
    )
    mode.add_argument(
        "--replay",
        metavar="DIALOG_LOG",
        help="Offline: feed dialog from a log file, make no SFMC connection",
    )
    mode.add_argument(
        "--replay-interval",
        type=float,
        default=0.0,
        help="Seconds between replayed dialog lines (default 0)",
    )

    tune = p.add_argument_group("tuning")
    tune.add_argument(
        "--raw",
        action="store_true",
        help="Send commands verbatim (do not add a leading '!')",
    )
    tune.add_argument("--connect-timeout", type=float, default=None)
    tune.add_argument("--quiet-seconds", type=float, default=8.0)
    tune.add_argument("--quiescent-timeout", type=float, default=1200.0)
    tune.add_argument("--echo-timeout", type=float, default=30.0)
    tune.add_argument("--echo-retries", type=int, default=2)
    tune.add_argument(
        "--keep",
        action="store_true",
        help="Do not move the commands file to .done after success",
    )

    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return p


def _consume_file(path: Path) -> None:
    """Move a successfully processed commands file aside (one-shot)."""
    done = path.with_suffix(path.suffix + ".done")
    try:
        path.rename(done)
        log.info("Consumed %s -> %s", path.name, done.name)
    except OSError as exc:
        log.warning("Could not move %s aside: %s", path, exc)


def main() -> None:
    """CLI entry point for ``sfmc-next-surface-do``."""
    args = build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    cmd_path = Path(args.commands_file)
    try:
        commands = read_commands(cmd_path)
    except OSError as exc:
        sys.stderr.write(f"Error reading {cmd_path}: {exc}\n")
        sys.exit(1)
    if not commands:
        sys.stderr.write(f"No commands found in {cmd_path}\n")
        sys.exit(1)

    cfg = InjectConfig(
        bang_prefix=not args.raw,
        quiet_seconds=args.quiet_seconds,
        quiescent_timeout=args.quiescent_timeout,
        echo_timeout=args.echo_timeout,
        echo_retries=args.echo_retries,
        connect_timeout=args.connect_timeout,
    )

    log.info("Queued %d command(s) for %s", len(commands), args.glider)

    try:
        if args.replay:
            result = run_replay(
                args.glider,
                commands,
                args.replay,
                cfg,
                interval=args.replay_interval,
            )
        else:
            from .client import SFMCClient

            with SFMCClient(host=args.host, config_path=args.credentials) as client:
                result = run_next_surface_do(
                    client,
                    args.glider,
                    commands,
                    cfg,
                    dry_run=args.dry_run,
                    pre_pause=args.pre_pause,
                )
    except KeyboardInterrupt:
        sys.stderr.write("\nStopped.\n")
        sys.exit(130)
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"Error: {exc}\n")
        sys.exit(1)

    log.info("Result: %s", result.format())

    # One-shot: only consume the file on a real, fully successful run.
    live = not (args.dry_run or args.replay)
    if result.ok and live and not args.keep:
        _consume_file(cmd_path)

    sys.exit(0 if result.ok else 2)


if __name__ == "__main__":
    main()
