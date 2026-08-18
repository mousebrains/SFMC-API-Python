"""Interpreter for SFMC glider-script XML.

SFMC runs a state machine, described in XML, beside the dockserver.
This module parses those scripts and executes them, so the same
behaviour can run from Python — as a way to understand a script, to
replay one offline against recorded dialog, or to drive a glider
directly.

The whole SFMC script language is seven elements and six attributes::

    <gliderScript>
      <initialState name="...">              <!-- also <state>, <finalState> -->
        <transitions>
          <transition matchExpression="REGEX" toState="NEXT">
            <action type="glider" command="s *.sbd *.tbd"/>
          </transition>
          <transition timeout="10" toState="NEXT"/>   <!-- 10 MINUTES -->
        </transitions>
      </initialState>
    </gliderScript>

*In state S, wait for glider output matching a regex or for a timeout;
optionally send commands; move to state T.*  That is all of it.  Across
the 20-script reference corpus every one of the 397 live actions is
``type="glider"`` — the language has no other verb — so an interpreter
needs exactly one capability beyond bookkeeping: send a command.  (The
corpus holds 404 ``<action>`` tags; seven are inside XML comments.)

Split deliberately in two:

* :class:`XmlStateMachine` is pure.  It performs no I/O, knows nothing
  about SFMC, and is driven by :meth:`~XmlStateMachine.feed` and
  :meth:`~XmlStateMachine.check_timeout`.  All the interesting
  behaviour, and all the tests, live here.
* :func:`run_live` wires it to a glider, and is the only part of this
  module that can transmit anything.  :func:`replay` runs a script
  against a recorded log instead, sending nothing.

Usage::

    script = parse_script("riot.xml")
    machine = XmlStateMachine(script)
    for action in machine.start():
        print("would send:", action.command)
    for action in machine.feed("Hit Control-R to RESUME\\r\\n"):
        print("would send:", action.command)
"""

from __future__ import annotations

import logging
import re
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from .exceptions import APIError, AuthenticationError, RateLimitError

if TYPE_CHECKING:  # pragma: no cover
    from .client import SFMCClient

__all__ = [
    "KEEPALIVE_COMMAND",
    "KEEPALIVE_SECONDS",
    "SECONDS_PER_TIMEOUT_UNIT",
    "Action",
    "MatchMode",
    "Script",
    "ScriptChain",
    "ScriptError",
    "State",
    "Transition",
    "XmlStateMachine",
    "describe",
    "parse_script",
    "replay",
    "run_live",
]

logger = logging.getLogger(__name__)

#: How incoming glider output is matched against ``matchExpression``.
#:
#: ``buffer`` matches against a rolling buffer that includes text not
#: yet terminated by a newline.  This is the faithful mode: several
#: scripts match a GliderDos prompt (``GliderDos A 0 >``), and terminal
#: prompts carry no trailing newline, so a line-oriented matcher would
#: never see one.
#:
#: ``line`` matches only complete lines.  Cheaper to reason about, and
#: adequate for scripts that only match ordinary dialog output, but it
#: cannot match a prompt.
MatchMode = str

#: Cap on the rolling match buffer.  Long enough to hold any plausible
#: multi-line match, short enough that a glider emitting megabytes
#: without a match cannot grow it without bound.
MAX_BUFFER_CHARS = 64 * 1024

#: Seconds per unit of the XML ``timeout`` attribute.
#:
#: SFMC's ``timeout`` is expressed in **minutes**, not seconds.  Nothing
#: in the XML says so, which is exactly why it is worth stating here:
#: every author in the reference corpus documents it as minutes in their
#: own comments — all 22 of ``riot.xml``'s timers carry "If nothing
#: within 10 minutes", and ``vacuum_test_send_data_2hrs.xml`` pairs
#: ``timeout="120"`` with "a 120 minute (2 hours) timeout" and a
#: filename that says the same.  Confirmed by the SFMC operator whose
#: scripts these are.
#:
#: Reading it as seconds is a 60x error in the dangerous direction: a
#: script meant to wait 10 minutes for a glider to answer would give up
#: after 10 seconds and act on the silence.
SECONDS_PER_TIMEOUT_UNIT = 60.0

#: What :func:`run_live` sends to keep a quiet link from being dropped.
#:
#: ``Ctrl-M`` is a carriage return, written in the same literal form the
#: scripts use for every other control character (``Ctrl-C``,
#: ``Ctrl-R``, ``Ctrl-W``) — a form confirmed live, where sending the
#: text ``Ctrl-C`` made the dockserver emit ``^C`` and end the mission.
#:
#: The obvious alternative, an empty command, does not work: SFMC
#: rejects an empty body with HTTP 400.  Found by watching a keepalive
#: run fail against a live glider.
KEEPALIVE_COMMAND = "Ctrl-M"

#: Suggested seconds of dialog silence before sending
#: :data:`KEEPALIVE_COMMAND`, for callers that opt in.
#:
#: SFMC drops an idle link after about five minutes -- the operator's
#: figure, and consistent with an observed idle drop -- so four leaves a
#: minute of margin without sending more than necessary.
#:
#: **What was actually measured, and what was not.**  A live run against
#: osusim on 2026-08-16 held the link ``connected`` for 6m47s across six
#: keepalives spaced about **68 seconds** apart, on a glider at a
#: GliderDos prompt that had otherwise been idle-dropping.  That
#: demonstrates ``Ctrl-M`` works and that a keepalive holds the link.  It
#: does **not** demonstrate that 240s is short enough: no run has tested
#: this interval.  Lower it if you see an idle drop.
#:
#: **Not a default.**  Keepalives are off unless asked for — see
#: :func:`run_live`.
KEEPALIVE_SECONDS = 240.0


def _cut_at(offset: int) -> Callable[[int], int]:
    """Return a cut function that ignores the match end (line mode)."""
    return lambda _end: offset


class ScriptError(Exception):
    """A glider script could not be parsed or is not well formed."""


@dataclass(frozen=True)
class Action:
    """One thing to do on taking a transition.

    Attributes:
        type: Always ``"glider"`` in every known script; retained so an
            unknown type can be reported rather than silently executed
            as if it were a command.
        command: The command text to send, e.g. ``"s *.sbd *.tbd"``.
    """

    type: str
    command: str


@dataclass(frozen=True)
class Transition:
    """One edge out of a state.

    Exactly one of *match* or *timeout_seconds* is set: across the
    reference corpus, all 1023 ``matchExpression`` transitions have no
    timeout and all 204 timeout transitions have no regex.

    Attributes:
        to_state: Name of the state to enter.
        match: Compiled ``matchExpression``, or ``None`` for a timer.
        pattern: The original expression text, for logs.
        timeout_seconds: How long to wait, or ``None`` for a match
            transition.  Named for its unit on purpose: the XML
            attribute is in **minutes** (see
            :data:`SECONDS_PER_TIMEOUT_UNIT`) and a bare ``timeout``
            invites reading the file's number as seconds.
        timeout_minutes: The value exactly as the XML wrote it, for
            display and for comparing against the script.
        actions: Commands to send when this transition is taken, in
            document order.
        immediate: ``True`` when ``matchExpression`` was empty.  Such a
            transition fires on entry to the state without waiting for
            input — see :meth:`XmlStateMachine.start`.
    """

    to_state: str
    match: re.Pattern[str] | None
    pattern: str | None
    timeout_seconds: float | None
    timeout_minutes: float | None = None
    actions: tuple[Action, ...] = ()
    immediate: bool = False


@dataclass(frozen=True)
class State:
    """One state and its outgoing transitions, in document order."""

    name: str
    transitions: tuple[Transition, ...] = ()
    is_final: bool = False


@dataclass(frozen=True)
class Script:
    """A parsed glider script."""

    name: str
    initial: str
    states: dict[str, State] = field(default_factory=dict)

    def state(self, name: str) -> State:
        try:
            return self.states[name]
        except KeyError:  # pragma: no cover - guarded at parse time
            raise ScriptError(f"{self.name}: no such state {name!r}") from None


# ── Parsing ──────────────────────────────────────────────────────────


def parse_script(source: str | Path) -> Script:
    """Parse a glider script from a file path or an XML string.

    Args:
        source: Path to a ``.xml`` file, or the XML text itself.

    Returns:
        The parsed :class:`Script`.

    Raises:
        ScriptError: If the XML is malformed, a required attribute is
            missing, a ``matchExpression`` is not a valid regex, or a
            transition names a state that does not exist.  Every one of
            these is caught here rather than mid-mission.
    """
    text, name = _read_source(source)
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        raise ScriptError(f"{name}: malformed XML: {exc}") from exc

    if root.tag != "gliderScript":
        raise ScriptError(f"{name}: root element is {root.tag!r}, expected 'gliderScript'")

    states: dict[str, State] = {}
    initial: str | None = None

    for element in root:
        if element.tag not in {"initialState", "state", "finalState"}:
            raise ScriptError(f"{name}: unexpected element {element.tag!r}")
        state_name = element.get("name")
        if not state_name:
            raise ScriptError(f"{name}: a {element.tag} has no name")
        if state_name in states:
            raise ScriptError(f"{name}: duplicate state {state_name!r}")

        state = State(
            name=state_name,
            transitions=tuple(_parse_transitions(element, name, state_name)),
            is_final=element.tag == "finalState",
        )
        states[state_name] = state
        if element.tag == "initialState":
            if initial is not None:
                raise ScriptError(f"{name}: more than one initialState")
            initial = state_name

    if initial is None:
        raise ScriptError(f"{name}: no initialState")

    # A dangling toState is a script bug that would otherwise surface
    # only when that edge is taken, potentially weeks in.
    for state in states.values():
        for transition in state.transitions:
            if transition.to_state not in states:
                raise ScriptError(
                    f"{name}: state {state.name!r} transitions to unknown "
                    f"state {transition.to_state!r}"
                )

    return Script(name=name, initial=initial, states=states)


def _read_source(source: str | Path) -> tuple[str, str]:
    """Return ``(xml_text, display_name)`` from a path or raw XML."""
    if isinstance(source, Path):
        return source.read_text(encoding="utf-8"), source.name
    stripped = source.lstrip()
    if stripped.startswith("<"):
        return source, "<string>"
    path = Path(source)
    return path.read_text(encoding="utf-8"), path.name


def _parse_transitions(element: ET.Element, script: str, state: str) -> Iterator[Transition]:
    """Parse the ``<transitions>`` block of one state."""
    for container in element:
        if container.tag != "transitions":
            raise ScriptError(f"{script}: unexpected element {container.tag!r} in state {state!r}")
        for node in container:
            if node.tag != "transition":
                raise ScriptError(
                    f"{script}: unexpected element {node.tag!r} in state {state!r} transitions"
                )
            yield _parse_transition(node, script, state)


def _parse_transition(node: ET.Element, script: str, state: str) -> Transition:
    to_state = node.get("toState")
    if not to_state:
        raise ScriptError(f"{script}: a transition in state {state!r} has no toState")

    raw_match = node.get("matchExpression")
    raw_timeout = node.get("timeout")
    if raw_match is None and raw_timeout is None:
        raise ScriptError(
            f"{script}: transition to {to_state!r} in state {state!r} "
            "has neither matchExpression nor timeout"
        )

    minutes: float | None = None
    seconds: float | None = None
    if raw_timeout is not None:
        try:
            minutes = float(raw_timeout)
        except ValueError:
            raise ScriptError(
                f"{script}: transition to {to_state!r} has non-numeric timeout {raw_timeout!r}"
            ) from None
        seconds = minutes * SECONDS_PER_TIMEOUT_UNIT

    pattern: re.Pattern[str] | None = None
    immediate = False
    if raw_match is not None:
        if raw_match == "":
            # An empty expression appears as the first transition of a
            # start state, paired with an action.  Read as "fire on
            # entry": it is the only reading under which such a state
            # does anything.  Traced explicitly so the interpretation
            # can be checked against real SFMC behaviour.
            immediate = True
        else:
            try:
                pattern = re.compile(raw_match)
            except re.error as exc:
                raise ScriptError(
                    f"{script}: transition to {to_state!r} has invalid "
                    f"matchExpression {raw_match!r}: {exc}"
                ) from exc

    return Transition(
        to_state=to_state,
        match=pattern,
        pattern=raw_match,
        timeout_seconds=seconds,
        timeout_minutes=minutes,
        actions=tuple(_parse_actions(node, script, state)),
        immediate=immediate,
    )


def _parse_actions(node: ET.Element, script: str, state: str) -> Iterator[Action]:
    for child in node:
        if child.tag != "action":
            raise ScriptError(f"{script}: unexpected element {child.tag!r} in state {state!r}")
        action_type = child.get("type") or ""
        command = child.get("command")
        if command is None:
            raise ScriptError(f"{script}: an action in state {state!r} has no command")
        if action_type != "glider":
            # Unknown verbs are refused rather than assumed to be
            # commands: sending the wrong thing to a glider is worse
            # than refusing to start.
            raise ScriptError(
                f"{script}: action type {action_type!r} in state {state!r} is not supported "
                "(only 'glider' exists in any known script)"
            )
        yield Action(type=action_type, command=command)


# ── Execution ────────────────────────────────────────────────────────


@dataclass
class XmlStateMachine:
    """Executes a parsed script.  Pure: no I/O, no clock of its own.

    Drive it with :meth:`start`, then :meth:`feed` for glider output
    and :meth:`check_timeout` on a timer.  Each returns the actions to
    perform, in order; the caller decides whether to send them.

    Args:
        script: The parsed script.
        match_mode: ``"buffer"`` (default) matches against a rolling
            buffer including text not yet newline-terminated, which is
            required to match a GliderDos prompt.  ``"line"`` matches
            only complete lines.
        on_trace: Called with a human-readable string at every state
            change, match, and timeout.  This is how a script's
            behaviour is made observable.
        now: Clock, injectable for tests.
    """

    script: Script
    match_mode: MatchMode = "buffer"
    on_trace: Callable[[str], None] | None = None
    now: Callable[[], float] = time.monotonic

    _state: State = field(init=False)
    _buffer: str = field(init=False, default="")
    _entered_at: float = field(init=False, default=0.0)
    _started: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        if self.match_mode not in {"buffer", "line"}:
            raise ValueError(f"match_mode must be 'buffer' or 'line', not {self.match_mode!r}")
        self._state = self.script.state(self.script.initial)

    # ── Introspection ────────────────────────────────────────────────

    @property
    def state(self) -> str:
        """Name of the current state."""
        return self._state.name

    @property
    def finished(self) -> bool:
        """True once a final state has been reached."""
        return self._state.is_final

    @property
    def timeout_remaining(self) -> float | None:
        """Seconds until this state's timeout fires, or ``None``."""
        timeout = self._current_timeout()
        if timeout is None:
            return None
        if timeout.timeout_seconds is None:  # pragma: no cover - guarded by _current_timeout
            return None
        return max(0.0, timeout.timeout_seconds - (self.now() - self._entered_at))

    # ── Driving ──────────────────────────────────────────────────────

    def start(self) -> list[Action]:
        """Enter the initial state and return any immediate actions.

        A state whose first transition has an empty ``matchExpression``
        fires it at once, without waiting for glider output.
        """
        if self._started:
            raise RuntimeError("XmlStateMachine already started")
        self._started = True
        self._trace(f"start -> {self._state.name}")
        self._entered_at = self.now()
        return self._run_immediate()

    def feed(self, text: str) -> list[Action]:
        """Supply glider output; return the actions it triggered.

        Args:
            text: A chunk of glider output.  Need not be a whole line —
                in ``buffer`` mode partial text is retained and matched
                against as more arrives.
        """
        if self.finished:
            return []
        self._buffer += text
        actions: list[Action] = []
        # Loop: one input can drive several transitions, each of which
        # may immediately enable the next.
        while True:
            fired = self._match_once()
            if fired is None:
                break
            actions.extend(fired)
            if self.finished:
                break
            actions.extend(self._run_immediate())
            if self.finished:
                break
        if len(self._buffer) > MAX_BUFFER_CHARS:
            # Keep the tail: a match can only be completed by what
            # follows, so the newest text is the part worth keeping.
            self._buffer = self._buffer[-MAX_BUFFER_CHARS:]
        return actions

    def check_timeout(self) -> list[Action]:
        """Fire this state's timeout transition if it is due."""
        if self.finished:
            return []
        transition = self._current_timeout()
        if transition is None or transition.timeout_seconds is None:
            return []
        if self.now() - self._entered_at < transition.timeout_seconds:
            return []
        self._trace(
            f"{self._state.name}: timeout after "
            f"{transition.timeout_minutes}min ({transition.timeout_seconds}s)"
        )
        actions = self._take(transition)
        if not self.finished:
            actions.extend(self._run_immediate())
        return actions

    # ── Internals ────────────────────────────────────────────────────

    def _trace(self, message: str) -> None:
        if self.on_trace is not None:
            self.on_trace(message)
        logger.debug("%s: %s", self.script.name, message)

    def _current_timeout(self) -> Transition | None:
        for transition in self._state.transitions:
            if transition.timeout_seconds is not None:
                return transition
        return None

    def _run_immediate(self) -> list[Action]:
        """Fire empty-matchExpression transitions until none applies."""
        actions: list[Action] = []
        seen: set[str] = set()
        while not self.finished:
            transition = next(
                (t for t in self._state.transitions if t.immediate),
                None,
            )
            if transition is None:
                break
            if self._state.name in seen:
                # An immediate cycle would spin forever sending
                # commands to a glider.  Stop and say so.
                raise ScriptError(
                    f"{self.script.name}: immediate-transition loop at state {self._state.name!r}"
                )
            seen.add(self._state.name)
            self._trace(f"{self._state.name}: immediate (empty matchExpression)")
            actions.extend(self._take(transition))
        return actions

    def _match_once(self) -> list[Action] | None:
        """Take the first matching transition, or return ``None``."""
        haystacks = self._haystacks()
        for transition in self._state.transitions:
            if transition.match is None:
                continue
            for haystack, consume_to in haystacks:
                found = transition.match.search(haystack)
                if found is None:
                    continue
                self._trace(
                    f"{self._state.name}: matched {transition.pattern!r} "
                    f"in {haystack[max(0, found.start() - 20) : found.end() + 20]!r}"
                )
                self._consume(consume_to(found.end()))
                return self._take(transition)
        return None

    def _haystacks(self) -> list[tuple[str, Callable[[int], int]]]:
        """Text to match against, with how to convert a match end to a cut."""
        if self.match_mode == "buffer":
            return [(self._buffer, lambda end: end)]
        # Line mode: only complete lines are eligible, and the cut is
        # always the end of the matched line.
        parts = self._buffer.split("\n")
        offset = 0
        out: list[tuple[str, Callable[[int], int]]] = []
        for line in parts[:-1]:
            offset += len(line) + 1
            out.append((line, _cut_at(offset)))
        return out

    def _consume(self, upto: int) -> None:
        self._buffer = self._buffer[upto:]

    def _take(self, transition: Transition) -> list[Action]:
        """Perform a transition: collect its actions, enter its target."""
        target = self.script.state(transition.to_state)
        if transition.actions:
            for action in transition.actions:
                self._trace(f"{self._state.name}: send {action.command!r}")
        self._trace(f"{self._state.name} -> {target.name}")
        self._state = target
        self._entered_at = self.now()
        if target.is_final:
            self._trace(f"reached final state {target.name!r}")
        return list(transition.actions)


@dataclass
class ScriptChain:
    """Runs scripts back to back: a final state starts the next one.

    SFMC's language has no chaining.  A ``<finalState>`` simply ends the
    run, and across the whole corpus there is no attribute that names a
    successor — 1227 ``toState`` and nothing else.  So this composes at
    the runner level rather than inventing an attribute: **every script
    in a chain stays a script SFMC itself could run**, which is the
    point, since the reason to run them here is to emulate SFMC.

    Useful for the step SFMC does out of band.  ``riot.xml`` begins by
    waiting for a surfacing; it cannot start the mission it then
    shepherds.  Chain a small script that issues ``run`` in front of it
    and the pair covers the whole procedure.

    Each script starts with a **fresh match buffer**.  Text that arrived
    before a script began is not its to act on, and carrying a buffer
    across the boundary would let a permissive first pattern in the next
    script fire on history.  The cost is that unconsumed text at the
    moment of hand-off is dropped; the hand-off itself does no I/O, so
    nothing arriving *during* it is lost.

    Presents the same interface as :class:`XmlStateMachine`, and a
    one-script chain behaves exactly like the machine alone.

    Args:
        scripts: The scripts to run, in order.  At least one.
        match_mode: See :class:`XmlStateMachine`.
        on_trace: Called at every state change, match, timeout, and
            hand-off.
        now: Clock, injectable for tests.
    """

    scripts: tuple[Script, ...]
    match_mode: MatchMode = "buffer"
    on_trace: Callable[[str], None] | None = None
    now: Callable[[], float] = time.monotonic

    _index: int = field(init=False, default=0)
    _machine: XmlStateMachine = field(init=False)

    def __post_init__(self) -> None:
        if not self.scripts:
            raise ValueError("a chain needs at least one script")
        self._machine = self._build(0)

    def _build(self, index: int) -> XmlStateMachine:
        return XmlStateMachine(
            self.scripts[index],
            match_mode=self.match_mode,
            on_trace=self.on_trace,
            now=self.now,
        )

    # ── Introspection ────────────────────────────────────────────────

    @property
    def script(self) -> Script:
        """The script currently running."""
        return self.scripts[self._index]

    @property
    def state(self) -> str:
        """Current state name, qualified by script when chained."""
        if len(self.scripts) == 1:
            return self._machine.state
        return f"{self.script.name}:{self._machine.state}"

    @property
    def finished(self) -> bool:
        """True only once the *last* script has reached a final state."""
        return self._machine.finished and self._index == len(self.scripts) - 1

    @property
    def timeout_remaining(self) -> float | None:
        return self._machine.timeout_remaining

    # ── Driving ──────────────────────────────────────────────────────

    def start(self) -> list[Action]:
        actions = self._machine.start()
        actions.extend(self._advance())
        return actions

    def feed(self, text: str) -> list[Action]:
        if self.finished:
            return []
        actions = self._machine.feed(text)
        actions.extend(self._advance())
        return actions

    def check_timeout(self) -> list[Action]:
        if self.finished:
            return []
        actions = self._machine.check_timeout()
        actions.extend(self._advance())
        return actions

    def _advance(self) -> list[Action]:
        """Start successors while the current script has finished."""
        actions: list[Action] = []
        while self._machine.finished and self._index < len(self.scripts) - 1:
            self._index += 1
            if self.on_trace is not None:
                self.on_trace(
                    f"chaining to {self.script.name} ({self._index + 1}/{len(self.scripts)})"
                )
            self._machine = self._build(self._index)
            actions.extend(self._machine.start())
        return actions


def _as_chain(
    script: Script | Sequence[Script],
    *,
    match_mode: MatchMode,
    on_trace: Callable[[str], None] | None,
) -> ScriptChain:
    scripts = (script,) if isinstance(script, Script) else tuple(script)
    return ScriptChain(scripts, match_mode=match_mode, on_trace=on_trace)


def describe(script: Script) -> str:
    """Render a script as readable text, for review without XML."""
    lines = [f"script {script.name}  (initial: {script.initial})"]
    for name, state in script.states.items():
        marker = "  [final]" if state.is_final else ""
        lines.append(f"\nstate {name}{marker}")
        for transition in state.transitions:
            if transition.timeout_seconds is not None:
                trigger = f"after {transition.timeout_minutes}min ({transition.timeout_seconds}s)"
            elif transition.immediate:
                trigger = "immediately"
            else:
                trigger = f"on /{transition.pattern}/"
            lines.append(f"    {trigger} -> {transition.to_state}")
            for action in transition.actions:
                lines.append(f"        send: {action.command}")
    return "\n".join(lines)


# ── Running against a glider ─────────────────────────────────────────


def _emit(trace: str) -> None:
    print(f"    {trace}", flush=True)


def _utcnow() -> str:
    """UTC stamp matching ``sfmc-monitor-glider``'s log format.

    The same shape on purpose: diagnosing a long run means lining this
    output up against a dialog capture line by line.
    """
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")


def replay(
    script: Script | Sequence[Script],
    dialog: Iterator[str],
    *,
    match_mode: MatchMode = "buffer",
    verbose: bool = True,
) -> list[Action]:
    """Run *script* against recorded dialog, sending nothing.

    Timeouts are not simulated: recorded dialog has no timing, so a
    replay shows what the *match* transitions would have done and
    nothing more.  Stated rather than silently approximated, because a
    script whose behaviour depends on its 22 timers is only partly
    exercised here.

    Args:
        script: The parsed script.
        dialog: Lines of recorded glider output.
        match_mode: See :class:`XmlStateMachine`.
        verbose: Print a trace as it runs.

    Returns:
        Every action the script would have taken, in order.
    """
    machine = _as_chain(
        script,
        match_mode=match_mode,
        on_trace=_emit if verbose else None,
    )
    performed = list(machine.start())
    for line in dialog:
        performed.extend(machine.feed(line))
        if machine.finished:
            break
    return performed


def run_live(
    client: SFMCClient,
    glider: str,
    script: Script | Sequence[Script],
    *,
    send: bool = False,
    match_mode: MatchMode = "buffer",
    poll: float = 1.0,
    max_runtime: float | None = None,
    keepalive: float | None = None,
    status_every: float | None = 300.0,
) -> list[Action]:
    """Drive a glider with *script* until it reaches a final state.

    Reads the *raw* dialog stream
    (:meth:`~sfmc_api.session.GliderSession.raw_dialog_listener`) rather
    than reassembled lines, and feeds each chunk to the machine
    untouched.  That is what makes ``match_mode="buffer"`` mean the same
    thing live as it does in :func:`replay`: a GliderDos prompt carries
    no trailing newline, so it never becomes a complete line, and a
    line consumer would never see an idle one.

    Args:
        client: An :class:`~sfmc_api.client.SFMCClient`.
        glider: Registered glider name.
        script: The parsed script.
        send: **Commands are only transmitted when this is True.**
            The default reports what it would send and sends nothing,
            because the alternative default is a program that steers a
            glider the first time someone runs it to see what it does.
        match_mode: See :class:`XmlStateMachine`.
        poll: Seconds between timeout checks.
        max_runtime: Stop after this long regardless of state.
        keepalive: Seconds of dialog silence after which to send
            :data:`KEEPALIVE_COMMAND`, stopping SFMC dropping an idle
            link.  **Off by default, and deliberately so.**  It is for
            a glider parked at a GliderDos prompt; during a mission it
            injects traffic no script asked for, into a vehicle that is
            supposed to be left alone, and the engine cannot reliably
            tell the two apart — ``connected`` reports the dockserver
            link, which stays up while the glider is submerged.  So the
            operator decides.  :data:`KEEPALIVE_SECONDS` is a sensible
            value when you do want it.  Only ever sends when *send* is
            True.
        status_every: Seconds between status lines (state, epoch, chunk
            and byte counts, quiet time, dropped chunks).  ``None``
            silences them.  A run can legitimately be quiet for hours,
            which is also what a broken one looks like; this is what
            tells them apart.

    Returns:
        Every action taken (or that would have been taken), in order.
        Keepalive returns are not actions and are not included.
    """

    # Everything this function prints is timestamped.  A run lasting
    # hours is diagnosed after the fact by lining its output up against
    # a dialog capture, and untimestamped output cannot answer the only
    # question that matters -- was the engine listening when the glider
    # spoke?  Learned by failing to answer it.
    def say(message: str) -> None:
        print(f"{_utcnow()} {message}", flush=True)

    machine = _as_chain(script, match_mode=match_mode, on_trace=say)
    performed: list[Action] = []
    mode = "SENDING" if send else "dry run — nothing will be sent"
    names = " -> ".join(one.name for one in machine.scripts)
    say(f"running {names} against {glider} [{mode}]")

    last_activity = time.monotonic()
    chunks = 0
    bytes_in = 0
    last_report = time.monotonic()

    def dispatch(actions: list[Action]) -> None:
        nonlocal last_activity
        for action in actions:
            performed.append(action)
            if send:
                client.send_command(glider, action.command)
                last_activity = time.monotonic()
                say(f"SENT: {action.command}")
            else:
                say(f"WOULD SEND: {action.command}")

    deadline = None if max_runtime is None else time.monotonic() + max_runtime
    # start=False plus start(timeout=None) hands every retry, including
    # the first connection, to the session's own supervisor.  Starting
    # with a 30s deadline instead raises TimeoutError on a transient
    # auth hiccup and kills the run -- survivable for a five minute
    # test, fatal for a run meant to last hours across many dives, where
    # the stream legitimately drops every time the glider submerges.
    session = client.session(glider, topics=("dialog",), start=False)

    def on_connect(reconnected: bool) -> None:
        if not reconnected:
            say("stream connected")
            return
        # A reconnect restores future messages only -- SFMC's live
        # topics offer no cursor or replay.  And SFMC delivers a
        # surfacing as one burst (437 lines inside 10ms, observed), so
        # a gap does not degrade the dialog, it loses all of it.  A
        # script waiting for a trigger that was published into the gap
        # then waits forever, looking exactly like a glider that never
        # surfaced.  Say so rather than let the two be confused.
        say("stream RECONNECTED -- dialog published during the gap is lost, unrecoverably")

    session.on_connect(on_connect)
    session.on_disconnect(lambda: say("stream dropped; supervisor retrying"))
    session.start(timeout=None)
    with session:
        listener = session.raw_dialog_listener()
        dispatch(machine.start())
        while not machine.finished:
            if deadline is not None and time.monotonic() > deadline:
                say("stopping: max runtime reached")
                break
            try:
                chunk = listener.get(timeout=poll)
            except Exception:  # queue.Empty and friends
                chunk = None
            if chunk is not None:
                last_activity = time.monotonic()
                chunks += 1
                bytes_in += len(chunk)
                # Fed exactly as it arrived.  Reassembling into lines
                # and re-adding a terminator would lose the one thing
                # buffer mode exists to match: an unterminated prompt.
                dispatch(machine.feed(chunk))
            dispatch(machine.check_timeout())

            # A quiet engine is normal for hours at a time, and is also
            # what a broken one looks like.  Report periodically so the
            # two are distinguishable without attaching a debugger to a
            # glider run already in progress.
            if status_every and time.monotonic() - last_report >= status_every:
                last_report = time.monotonic()
                quiet = time.monotonic() - last_activity
                dropped = listener.dropped
                say(
                    f"status: state={machine.state} epoch={session.epoch} "
                    f"chunks={chunks} bytes={bytes_in} quiet={quiet:.0f}s "
                    f"dropped={dropped} connected={session.connected}"
                )
                if dropped:
                    # Bounded queues drop the OLDEST, so a lagging
                    # consumer loses the start of a burst -- which is
                    # where a surfacing's trigger lives.
                    say(f"WARNING: {dropped} chunk(s) dropped; a trigger may have been missed")
            if keepalive is not None and send and time.monotonic() - last_activity >= keepalive:
                # A submerged glider is *supposed* to be silent, and its
                # link is legitimately down.  Sending into that keeps
                # nothing alive; worse, a command accepted for a
                # disconnected glider may be queued and delivered on the
                # next surfacing, injecting a stray return into the very
                # dialog the script is matching against.  So the
                # keepalive is for a connected-but-quiet glider only,
                # which is the case it was written for: an idle
                # GliderDos prompt.
                if not session.glider_is_connected():
                    last_activity = time.monotonic()
                    continue
                # Never fatal.  A keepalive is a convenience; losing the
                # connection it was meant to hold open is recoverable,
                # but killing a run that may be mid-way through steering
                # a glider is not.  Live testing found this the hard
                # way: an empty body is rejected HTTP 400, and the
                # exception took the whole engine down.
                try:
                    client.send_command(glider, KEEPALIVE_COMMAND)
                    last_activity = time.monotonic()
                    print(
                        f"    keepalive: {KEEPALIVE_COMMAND} after {keepalive}s quiet", flush=True
                    )
                except (APIError, RateLimitError, AuthenticationError) as exc:
                    # Back off a full interval rather than retrying every
                    # poll and turning one failure into a flood.
                    last_activity = time.monotonic()
                    print(
                        f"    keepalive failed ({exc.__class__.__name__}), continuing", flush=True
                    )
        listener.close()
    return performed


def main(argv: list[str] | None = None) -> int:
    """Entry point for ``sfmc-xml-engine``."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Parse, replay, or run an SFMC glider-script XML file.",
    )
    parser.add_argument(
        "script",
        metavar="SCRIPT.xml",
        nargs="+",
        help=(
            "One or more scripts.  Several run as a chain: reaching a final "
            "state starts the next, each with a fresh match buffer"
        ),
    )
    parser.add_argument("--describe", action="store_true", help="Print the state machine and exit")
    parser.add_argument("--replay", metavar="DIALOG.log", help="Replay against a recorded log")
    parser.add_argument("--glider", help="Run against this glider (dry run unless --send)")
    parser.add_argument(
        "--send",
        action="store_true",
        help="Actually transmit commands.  Without this nothing is sent",
    )
    parser.add_argument(
        "--match-mode",
        choices=("buffer", "line"),
        default="buffer",
        help="buffer (default) can match an unterminated prompt; line cannot",
    )
    parser.add_argument(
        "--host",
        help="SFMC server hostname (selects entry from multi-host credentials file)",
    )
    parser.add_argument(
        "--credentials",
        metavar="PATH",
        help="Path to credentials JSON file (default: ~/.config/sfmc/credentials.json)",
    )
    parser.add_argument("--max-runtime", type=float, help="Stop after this many seconds")
    parser.add_argument(
        "--status-every",
        type=float,
        default=300.0,
        metavar="SECONDS",
        help="Seconds between status lines (0 silences them)",
    )
    parser.add_argument(
        "--keepalive",
        type=float,
        default=0.0,
        metavar="SECONDS",
        help=(
            f"With --send, send {KEEPALIVE_COMMAND} (a carriage return) after this "
            "much dialog silence, so SFMC does not drop the connection while "
            "sitting at a GliderDos prompt.  OFF by default: during a mission "
            f"this injects traffic no script asked for.  {KEEPALIVE_SECONDS:.0f} follows "
            "a ~5 min idle drop; only ~68s has actually been tested"
        ),
    )
    args = parser.parse_args(argv)

    try:
        parsed = [parse_script(path) for path in args.script]
    except ScriptError as exc:
        print(f"error: {exc}", flush=True)
        return 2

    if args.describe:
        print("\n\n".join(describe(one) for one in parsed))
        return 0

    if args.replay:
        with open(args.replay, encoding="utf-8", errors="replace") as handle:
            # Accept both a raw capture and an sfmc-monitor-glider log,
            # skipping any non-dialog bookkeeping lines.
            lines = (
                stripped
                for stripped in (_strip_log_prefix(raw) for raw in handle)
                if stripped is not None
            )
            actions = replay(parsed, lines, match_mode=args.match_mode)
        print(f"\n{len(actions)} action(s) would have been sent:")
        for action in actions:
            print(f"  {action.command}")
        return 0

    if not args.glider:
        parser.error("one of --describe, --replay, or --glider is required")

    from .client import SFMCClient

    with SFMCClient(host=args.host, config_path=args.credentials) as client:
        actions = run_live(
            client,
            args.glider,
            parsed,
            send=args.send,
            match_mode=args.match_mode,
            max_runtime=args.max_runtime,
            keepalive=args.keepalive or None,
            status_every=args.status_every or None,
        )
    print(f"\n{len(actions)} action(s) {'sent' if args.send else 'would have been sent'}")
    return 0


#: A capture line: ISO timestamp, a kind, two spaces, then the payload.
#:
#: The kind may be bare (``DIALOG``) or the tail of a dotted logger name
#: (``sfmc.osusim.DIALOG``, which is what ``sfmc-monitor-glider``
#: writes — ``%(asctime)s %(name)s  %(message)s`` with a name of
#: ``sfmc.{glider}.{DIALOG,SCRIPT,INFO}``).  Both are accepted because
#: both exist in real captures, and a stripper that silently fails to
#: match feeds the capturing tool's own bookkeeping to the matcher
#: instead of skipping it.
_LOG_LINE_RE = re.compile(r"^\d{4}-\d\d-\d\dT[\d:.]+\s+(?:[\w.]*\.)?([A-Z?]+)\s{2}(.*)$")


def _strip_log_prefix(raw: str) -> str | None:
    """Turn a capture line back into glider output, or ``None`` to skip.

    Capture logs interleave real dialog with the capturing tool's own
    bookkeeping (``POLL``, ``SEND``, ``REPLY``, …).  Feeding those to
    the matcher would let the tool's own output drive the state
    machine, so only ``DIALOG`` lines are replayed.  A file with no
    such prefixes is treated as raw glider output and passed through.
    """
    text = raw.rstrip("\n")
    found = _LOG_LINE_RE.match(text)
    if found is None:
        return text + "\r\n"
    kind, payload = found.groups()
    if kind != "DIALOG":
        return None
    return payload + "\r\n"
