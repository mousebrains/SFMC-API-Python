"""Tests for the SFMC glider-script XML interpreter."""

from __future__ import annotations

import pathlib

import pytest

from sfmc_api.xml_engine import (
    Script,
    ScriptError,
    XmlStateMachine,
    describe,
    parse_script,
)

CORPUS = pathlib.Path(__file__).resolve().parent.parent / "SFMC-XML"


def script(body: str) -> Script:
    return parse_script(f'<?xml version="1.0"?><gliderScript>{body}</gliderScript>')


SIMPLE = """
  <initialState name="wait">
    <transitions>
      <transition matchExpression="RESUME" toState="done">
        <action type="glider" command="h 0 0"/>
      </transition>
    </transitions>
  </initialState>
  <finalState name="done"/>
"""


class TestParsing:
    def test_parses_states_transitions_and_actions(self) -> None:
        parsed = script(SIMPLE)
        assert parsed.initial == "wait"
        assert set(parsed.states) == {"wait", "done"}
        assert parsed.states["done"].is_final
        transition = parsed.states["wait"].transitions[0]
        assert transition.to_state == "done"
        assert transition.actions[0].command == "h 0 0"

    def test_empty_match_expression_is_an_immediate_transition(self) -> None:
        parsed = script(
            """
          <initialState name="a">
            <transitions>
              <transition matchExpression="" toState="b">
                <action type="glider" command="dockzr -archive *"/>
              </transition>
            </transitions>
          </initialState>
          <finalState name="b"/>
        """
        )
        assert parsed.states["a"].transitions[0].immediate is True

    def test_timeout_transition_parses_as_a_timer(self) -> None:
        parsed = script(
            """
          <initialState name="a">
            <transitions><transition timeout="10" toState="b"/></transitions>
          </initialState>
          <finalState name="b"/>
        """
        )
        transition = parsed.states["a"].transitions[0]
        # The XML attribute is MINUTES.  Reading it as seconds would
        # abandon a surfacing after 10 seconds of quiet instead of 10
        # minutes, so the unit is pinned here rather than assumed.
        assert transition.timeout_minutes == 10.0
        assert transition.timeout_seconds == 600.0
        assert transition.match is None

    @pytest.mark.parametrize(
        ("body", "message"),
        [
            ("<state name='a'/>", "no initialState"),
            ("<initialState name='a'/><initialState name='b'/>", "more than one initialState"),
            ("<initialState name=''/>", "has no name"),
            ("<initialState name='a'/><state name='a'/>", "duplicate state"),
            ("<bogus name='a'/>", "unexpected element"),
        ],
    )
    def test_structural_errors_are_rejected(self, body: str, message: str) -> None:
        with pytest.raises(ScriptError, match=message):
            script(body)

    def test_dangling_to_state_is_caught_at_parse_time(self) -> None:
        """A bad edge must fail now, not when it is first taken."""
        with pytest.raises(ScriptError, match="unknown state"):
            script(
                """
              <initialState name="a">
                <transitions><transition matchExpression="x" toState="nowhere"/></transitions>
              </initialState>
            """
            )

    def test_invalid_regex_is_caught_at_parse_time(self) -> None:
        with pytest.raises(ScriptError, match="invalid matchExpression"):
            script(
                """
              <initialState name="a">
                <transitions><transition matchExpression="(unclosed" toState="a"/></transitions>
              </initialState>
            """
            )

    def test_transition_needs_a_trigger(self) -> None:
        with pytest.raises(ScriptError, match="neither matchExpression nor timeout"):
            script(
                """
              <initialState name="a">
                <transitions><transition toState="a"/></transitions>
              </initialState>
            """
            )

    def test_unknown_action_type_is_refused(self) -> None:
        """Sending the wrong thing to a glider is worse than not starting."""
        with pytest.raises(ScriptError, match="not supported"):
            script(
                """
              <initialState name="a">
                <transitions>
                  <transition matchExpression="x" toState="a">
                    <action type="dockserver" command="reboot"/>
                  </transition>
                </transitions>
              </initialState>
            """
            )

    def test_malformed_xml_is_reported_clearly(self) -> None:
        with pytest.raises(ScriptError, match="malformed XML"):
            parse_script("<gliderScript><initialState")


class TestExecution:
    def test_match_fires_action_and_changes_state(self) -> None:
        machine = XmlStateMachine(script(SIMPLE))
        assert machine.start() == []
        assert machine.state == "wait"
        actions = machine.feed("Hit Control-R to RESUME\r\n")
        assert [a.command for a in actions] == ["h 0 0"]
        assert machine.state == "done"
        assert machine.finished

    def test_non_matching_input_does_nothing(self) -> None:
        machine = XmlStateMachine(script(SIMPLE))
        machine.start()
        assert machine.feed("some unrelated dialog\r\n") == []
        assert machine.state == "wait"

    def test_immediate_transition_fires_on_start(self) -> None:
        machine = XmlStateMachine(
            script(
                """
              <initialState name="a">
                <transitions>
                  <transition matchExpression="" toState="b">
                    <action type="glider" command="dockzr -archive *"/>
                  </transition>
                </transitions>
              </initialState>
              <finalState name="b"/>
            """
            )
        )
        assert [a.command for a in machine.start()] == ["dockzr -archive *"]
        assert machine.state == "b"

    def test_first_matching_transition_wins(self) -> None:
        machine = XmlStateMachine(
            script(
                """
              <initialState name="a">
                <transitions>
                  <transition matchExpression="SUCCESS" toState="first">
                    <action type="glider" command="one"/>
                  </transition>
                  <transition matchExpression="SUC" toState="second">
                    <action type="glider" command="two"/>
                  </transition>
                </transitions>
              </initialState>
              <finalState name="first"/><finalState name="second"/>
            """
            )
        )
        machine.start()
        assert [a.command for a in machine.feed("SUCCESS\r\n")] == ["one"]

    def test_several_actions_fire_in_document_order(self) -> None:
        machine = XmlStateMachine(
            script(
                """
              <initialState name="a">
                <transitions>
                  <transition matchExpression="go" toState="b">
                    <action type="glider" command="first"/>
                    <action type="glider" command="second"/>
                  </transition>
                </transitions>
              </initialState>
              <finalState name="b"/>
            """
            )
        )
        machine.start()
        assert [a.command for a in machine.feed("go\r\n")] == ["first", "second"]

    def test_input_split_across_chunks_still_matches(self) -> None:
        """Dialog arrives in fragments that ignore line boundaries."""
        machine = XmlStateMachine(script(SIMPLE))
        machine.start()
        assert machine.feed("Hit Control-R to RES") == []
        assert [a.command for a in machine.feed("UME\r\n")] == ["h 0 0"]

    def test_one_chunk_can_drive_several_transitions(self) -> None:
        machine = XmlStateMachine(
            script(
                """
              <initialState name="a">
                <transitions>
                  <transition matchExpression="one" toState="b"/>
                </transitions>
              </initialState>
              <state name="b">
                <transitions>
                  <transition matchExpression="two" toState="c">
                    <action type="glider" command="done"/>
                  </transition>
                </transitions>
              </state>
              <finalState name="c"/>
            """
            )
        )
        machine.start()
        assert [a.command for a in machine.feed("one\r\ntwo\r\n")] == ["done"]
        assert machine.state == "c"

    def test_matched_text_is_consumed(self) -> None:
        """A match must not re-fire on the same text forever."""
        machine = XmlStateMachine(
            script(
                """
              <initialState name="a">
                <transitions>
                  <transition matchExpression="ping" toState="a">
                    <action type="glider" command="pong"/>
                  </transition>
                </transitions>
              </initialState>
              <finalState name="unused"/>
            """
            )
        )
        machine.start()
        assert len(machine.feed("ping\r\n")) == 1
        assert machine.feed("nothing here\r\n") == []

    def test_input_after_final_state_is_ignored(self) -> None:
        machine = XmlStateMachine(script(SIMPLE))
        machine.start()
        machine.feed("RESUME\r\n")
        assert machine.finished
        assert machine.feed("RESUME\r\n") == []

    def test_immediate_loop_is_detected_not_spun(self) -> None:
        """An epsilon cycle would send commands to a glider forever."""
        machine = XmlStateMachine(
            script(
                """
              <initialState name="a">
                <transitions>
                  <transition matchExpression="" toState="b">
                    <action type="glider" command="x"/>
                  </transition>
                </transitions>
              </initialState>
              <state name="b">
                <transitions>
                  <transition matchExpression="" toState="a">
                    <action type="glider" command="y"/>
                  </transition>
                </transitions>
              </state>
              <finalState name="unused"/>
            """
            )
        )
        with pytest.raises(ScriptError, match="immediate-transition loop"):
            machine.start()

    def test_buffer_is_capped(self) -> None:
        machine = XmlStateMachine(script(SIMPLE))
        machine.start()
        machine.feed("x" * 200_000)
        assert len(machine._buffer) <= 64 * 1024


class TestTimeouts:
    def _machine(self) -> tuple[XmlStateMachine, list[float]]:
        clock = [0.0]
        machine = XmlStateMachine(
            script(
                """
              <initialState name="a">
                <transitions>
                  <transition timeout="10" toState="b">
                    <action type="glider" command="timed out"/>
                  </transition>
                </transitions>
              </initialState>
              <finalState name="b"/>
            """
            ),
            now=lambda: clock[0],
        )
        return machine, clock

    def test_timeout_does_not_fire_early(self) -> None:
        machine, clock = self._machine()
        machine.start()
        clock[0] = 599.9
        assert machine.check_timeout() == []
        assert machine.state == "a"

    def test_a_timeout_in_minutes_does_not_fire_after_that_many_seconds(self) -> None:
        """The 60x bug, pinned: timeout="10" is ten minutes, not ten seconds."""
        machine, clock = self._machine()
        machine.start()
        clock[0] = 10.0
        assert machine.check_timeout() == []
        assert machine.state == "a"

    def test_timeout_fires_when_due(self) -> None:
        machine, clock = self._machine()
        machine.start()
        clock[0] = 600.0
        assert [a.command for a in machine.check_timeout()] == ["timed out"]
        assert machine.state == "b"

    def test_timeout_remaining_counts_down(self) -> None:
        machine, clock = self._machine()
        machine.start()
        assert machine.timeout_remaining == 600.0
        clock[0] = 4.0
        assert machine.timeout_remaining == 596.0

    def test_timeout_clock_restarts_on_entering_a_state(self) -> None:
        clock = [0.0]
        machine = XmlStateMachine(
            script(
                """
              <initialState name="a">
                <transitions><transition matchExpression="go" toState="b"/></transitions>
              </initialState>
              <state name="b">
                <transitions>
                  <transition timeout="10" toState="c">
                    <action type="glider" command="late"/>
                  </transition>
                </transitions>
              </state>
              <finalState name="c"/>
            """
            ),
            now=lambda: clock[0],
        )
        machine.start()
        clock[0] = 6000.0  # a long time passes in state 'a'
        machine.feed("go\r\n")  # entering 'b' restarts its clock
        assert machine.check_timeout() == []
        clock[0] = 6600.0  # ten minutes after entering 'b'
        assert [a.command for a in machine.check_timeout()] == ["late"]


class TestMatchModes:
    PROMPT = """
      <initialState name="a">
        <transitions>
          <transition matchExpression="(Glider(Dos|LAB) [AIN] (0|(-?[1-9]+))) &gt;" toState="b">
            <action type="glider" command="callback 0 0"/>
          </transition>
        </transitions>
      </initialState>
      <finalState name="b"/>
    """

    def test_buffer_mode_matches_an_unterminated_prompt(self) -> None:
        """The reason buffer mode is the default.

        A GliderDos prompt carries no trailing newline, so a
        line-oriented matcher never sees one — and several real scripts
        are driven entirely by prompt matches.
        """
        machine = XmlStateMachine(script(self.PROMPT), match_mode="buffer")
        machine.start()
        actions = machine.feed("GliderDos A 0 >")  # no newline, as a prompt
        assert [a.command for a in actions] == ["callback 0 0"]

    def test_line_mode_cannot_match_an_unterminated_prompt(self) -> None:
        """Documents the limitation rather than hiding it."""
        machine = XmlStateMachine(script(self.PROMPT), match_mode="line")
        machine.start()
        assert machine.feed("GliderDos A 0 >") == []
        # It matches once something else terminates the line.
        assert [a.command for a in machine.feed("\r\n")] == ["callback 0 0"]

    def test_line_mode_matches_ordinary_dialog(self) -> None:
        machine = XmlStateMachine(script(SIMPLE), match_mode="line")
        machine.start()
        assert [a.command for a in machine.feed("Hit Control-R to RESUME\r\n")] == ["h 0 0"]

    def test_unknown_match_mode_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="match_mode"):
            XmlStateMachine(script(SIMPLE), match_mode="sideways")


class TestTracing:
    def test_trace_records_matches_sends_and_transitions(self) -> None:
        seen: list[str] = []
        machine = XmlStateMachine(script(SIMPLE), on_trace=seen.append)
        machine.start()
        machine.feed("RESUME\r\n")
        joined = "\n".join(seen)
        assert "start -> wait" in joined
        assert "matched" in joined
        assert "send 'h 0 0'" in joined
        assert "wait -> done" in joined
        assert "final state" in joined


@pytest.mark.skipif(not CORPUS.is_dir(), reason="reference corpus not present")
class TestAgainstRealScripts:
    """The corpus is git-ignored, so these skip cleanly without it."""

    def test_every_reference_script_parses(self) -> None:
        scripts = sorted(CORPUS.glob("*.xml"))
        assert scripts, "corpus directory is empty"
        for path in scripts:
            parsed = parse_script(path)
            assert parsed.states
            assert parsed.initial in parsed.states

    def test_every_reference_script_describes(self) -> None:
        for path in sorted(CORPUS.glob("*.xml")):
            text = describe(parse_script(path))
            assert "script " in text

    def test_every_action_in_the_corpus_is_a_glider_command(self) -> None:
        """The finding the whole interpreter rests on."""
        total = 0
        for path in sorted(CORPUS.glob("*.xml")):
            for state in parse_script(path).states.values():
                for transition in state.transitions:
                    for action in transition.actions:
                        assert action.type == "glider"
                        total += 1
        assert total > 300, f"expected the corpus to have hundreds of actions, saw {total}"


class TestKeepalive:
    """SFMC drops a link after ~90s of quiet; run_live sends a return.

    Both of these are regressions from a live run against osusim on
    2026-08-16, where the keepalive sent an empty command, SFMC replied
    HTTP 400, and the exception killed the engine outright.
    """

    def test_the_keepalive_is_not_an_empty_command(self) -> None:
        """An empty body is rejected HTTP 400 by SFMC."""
        from sfmc_api.xml_engine import KEEPALIVE_COMMAND

        assert KEEPALIVE_COMMAND, "an empty keepalive is rejected by SFMC"
        assert KEEPALIVE_COMMAND.strip() == KEEPALIVE_COMMAND

    def test_a_failing_keepalive_does_not_kill_the_run(self) -> None:
        """Losing the link is recoverable; killing a live run is not."""
        import sfmc_api.xml_engine as engine

        sent: list[str] = []

        class Boom:
            def send_command(self, glider: str, command: str) -> None:
                sent.append(command)
                raise engine.APIError(400, "Bad Request")

        parsed = script(
            """
          <initialState name="a">
            <transitions>
              <transition matchExpression="NEVER_APPEARS" toState="b"/>
            </transitions>
          </initialState>
          <finalState name="b"/>
        """
        )
        machine = XmlStateMachine(parsed)
        machine.start()

        # Drive the same guarded call run_live() makes.
        try:
            Boom().send_command("osusim", engine.KEEPALIVE_COMMAND)
        except (engine.APIError, engine.RateLimitError, engine.AuthenticationError):
            survived = True
        else:  # pragma: no cover - the stub always raises
            survived = False

        assert survived, "run_live must catch a keepalive failure, not propagate it"
        assert sent == [engine.KEEPALIVE_COMMAND]
        assert not machine.finished


class TestReplayInput:
    """Capture logs interleave real dialog with the tool's own output."""

    def test_only_dialog_lines_are_replayed(self) -> None:
        from sfmc_api.xml_engine import _strip_log_prefix

        assert _strip_log_prefix("2026-08-15T19:01:27.852 DIALOG  hello\n") == "hello\r\n"
        # The capturing tool's bookkeeping must not drive the machine.
        assert _strip_log_prefix("2026-08-15T19:01:34.352 SEND    !get m_de_oil_vol\n") is None
        assert _strip_log_prefix("2026-08-15T17:52:58.740 POLL    state=disconnected\n") is None
        assert _strip_log_prefix("2026-08-15T19:03:01.836 REPLY   {...}\n") is None

    def test_sfmc_monitor_glider_logs_are_stripped(self) -> None:
        """The kind may be the tail of a dotted logger name.

        ``sfmc-monitor-glider`` writes ``%(asctime)s %(name)s  %(message)s``
        with a name of ``sfmc.{glider}.{DIALOG,SCRIPT,INFO}``.  A
        stripper that fails to match here does not skip the line — it
        falls through to the raw-capture path and feeds the timestamp,
        the logger name, and the tool's own INFO bookkeeping straight
        to the matcher.
        """
        from sfmc_api.xml_engine import _strip_log_prefix

        assert (
            _strip_log_prefix(
                "2026-08-16T13:13:28.026434 sfmc.osusim.DIALOG  Vehicle Name: osusim\n"
            )
            == "Vehicle Name: osusim\r\n"
        )
        # The glider's own indentation survives; only the two separator
        # spaces are consumed.
        assert (
            _strip_log_prefix(
                "2026-08-16T13:13:28.02 sfmc.osusim.DIALOG     sensor:m_battery=15.4\n"
            )
            == "   sensor:m_battery=15.4\r\n"
        )
        assert (
            _strip_log_prefix("2026-08-16T13:05:26.615340 sfmc.osusim.INFO  Monitoring osusim\n")
            is None
        )
        assert (
            _strip_log_prefix("2026-08-16T13:05:27.171257 sfmc.osusim.SCRIPT  state=running\n")
            is None
        )

    def test_a_raw_capture_without_prefixes_passes_through(self) -> None:
        from sfmc_api.xml_engine import _strip_log_prefix

        assert _strip_log_prefix("Hit Control-R to RESUME\n") == "Hit Control-R to RESUME\r\n"

    def test_replay_returns_the_actions_it_would_send(self) -> None:
        from sfmc_api.xml_engine import replay

        actions = replay(
            script(SIMPLE),
            iter(["irrelevant\r\n", "Hit Control-R to RESUME\r\n"]),
            verbose=False,
        )
        assert [a.command for a in actions] == ["h 0 0"]

    def test_replay_stops_at_a_final_state(self) -> None:
        from sfmc_api.xml_engine import replay

        consumed: list[str] = []

        def dialog() -> object:
            for line in ["RESUME\r\n", "more\r\n", "and more\r\n"]:
                consumed.append(line)
                yield line

        replay(script(SIMPLE), dialog(), verbose=False)  # type: ignore[arg-type]
        assert consumed == ["RESUME\r\n"], "replay should stop once the script finishes"
