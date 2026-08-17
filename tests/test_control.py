"""Tests for sfmc-control: engine loading, flags, and safety posture."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from sfmc_api.control import build_parser, load_engine_class, main

ENGINE_SOURCE = textwrap.dedent(
    '''
    from sfmc_api.engine import BaseControlEngine

    SEEN = []

    class Recorder(BaseControlEngine):
        """One engine, so --class is not needed."""

        def on_event(self, event):
            SEEN.append((event.glider, event.source, event.body))
    '''
)


def _write(tmp_path: Path, name: str, source: str) -> Path:
    path = tmp_path / name
    path.write_text(source)
    return path


class TestLoadEngineClass:
    def test_a_single_engine_is_auto_detected(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "one.py", ENGINE_SOURCE)
        assert load_engine_class(path).__name__ == "Recorder"

    def test_a_named_class_is_used(self, tmp_path: Path) -> None:
        source = ENGINE_SOURCE + textwrap.dedent(
            """
            class Second(BaseControlEngine):
                pass
            """
        )
        path = _write(tmp_path, "two.py", source)
        assert load_engine_class(path, "Second").__name__ == "Second"

    def test_several_engines_without_a_name_is_refused(self, tmp_path: Path) -> None:
        source = ENGINE_SOURCE + "\nclass Second(BaseControlEngine):\n    pass\n"
        path = _write(tmp_path, "two.py", source)
        with pytest.raises(ValueError, match="defines several engines"):
            load_engine_class(path)

    def test_no_engine_is_refused(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "empty.py", "x = 1\n")
        with pytest.raises(ValueError, match="no BaseControlEngine subclass"):
            load_engine_class(path)

    def test_a_missing_file_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="engine file not found"):
            load_engine_class(tmp_path / "nope.py")

    def test_a_wrong_class_name_is_refused(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "one.py", ENGINE_SOURCE)
        with pytest.raises(ValueError, match="not a BaseControlEngine subclass"):
            load_engine_class(path, "SEEN")

    def test_the_base_class_itself_is_not_a_candidate(self, tmp_path: Path) -> None:
        """Importing the base must not make it look like the engine."""
        path = _write(
            tmp_path,
            "imports.py",
            "from sfmc_api.engine import BaseControlEngine\n"
            "class Mine(BaseControlEngine):\n    pass\n",
        )
        assert load_engine_class(path).__name__ == "Mine"


class TestFlags:
    def test_glider_is_repeatable_for_a_formation(self) -> None:
        args = build_parser().parse_args(
            ["--engine", "e.py", "--glider", "osu684", "--glider", "osu685"]
        )
        assert args.gliders == ["osu684", "osu685"]

    def test_writes_are_off_unless_asked(self) -> None:
        args = build_parser().parse_args(["--engine", "e.py", "--glider", "osu685"])
        assert args.allow_writes is False
        assert args.dry_run is False

    def test_no_glider_is_refused(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        path = _write(tmp_path, "one.py", ENGINE_SOURCE)
        with pytest.raises(SystemExit):
            main(["--engine", str(path)])
        assert "at least one --glider" in capsys.readouterr().err

    def test_replay_refuses_a_formation(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Replay drives one glider; two would silently mean one."""
        path = _write(tmp_path, "one.py", ENGINE_SOURCE)
        log = _write(tmp_path, "d.log", "line\n")
        with pytest.raises(SystemExit):
            main(
                [
                    "--engine",
                    str(path),
                    "--glider",
                    "a",
                    "--glider",
                    "b",
                    "--replay",
                    str(log),
                ]
            )
        assert "exactly one --glider" in capsys.readouterr().err

    def test_a_bad_engine_file_exits_two_not_a_traceback(self, tmp_path: Path) -> None:
        assert main(["--engine", str(tmp_path / "nope.py"), "--glider", "osu685"]) == 2


class TestReplayRun:
    def test_replay_contacts_nothing(self, tmp_path: Path) -> None:
        """The whole point: an algorithm testable with no server."""
        path = _write(tmp_path, "one.py", ENGINE_SOURCE)
        log = _write(tmp_path, "d.log", "Vehicle Name: osusim\nGPS Location: 1 N\n")

        assert main(["--engine", str(path), "--glider", "osusim", "--replay", str(log)]) == 0

        module = __import__("one")
        assert [body for _, source, body in module.SEEN if source == "dialog"] == [
            "Vehicle Name: osusim",
            "GPS Location: 1 N",
        ]

    def test_replay_passes_config_through(self, tmp_path: Path) -> None:
        pytest.importorskip("yaml")
        source = textwrap.dedent(
            """
            from sfmc_api.engine import BaseControlEngine

            CONFIG = {}

            class Configured(BaseControlEngine):
                def on_start(self):
                    CONFIG.update(self.config)
            """
        )
        path = _write(tmp_path, "conf_engine.py", source)
        config = _write(tmp_path, "c.yaml", "command: sensor m_battery\nquiet_seconds: 5\n")
        log = _write(tmp_path, "d.log", "x\n")

        assert (
            main(
                [
                    "--engine",
                    str(path),
                    "--glider",
                    "osusim",
                    "--replay",
                    str(log),
                    "--config",
                    str(config),
                ]
            )
            == 0
        )
        module = __import__("conf_engine")
        assert module.CONFIG == {"command": "sensor m_battery", "quiet_seconds": 5}

    def test_a_non_mapping_config_is_refused(self, tmp_path: Path) -> None:
        pytest.importorskip("yaml")
        path = _write(tmp_path, "one.py", ENGINE_SOURCE)
        config = _write(tmp_path, "bad.yaml", "- just\n- a list\n")
        log = _write(tmp_path, "d.log", "x\n")
        with pytest.raises(SystemExit, match="expected a YAML mapping"):
            main(
                [
                    "--engine",
                    str(path),
                    "--glider",
                    "osusim",
                    "--replay",
                    str(log),
                    "--config",
                    str(config),
                ]
            )


class TestExamplesLoad:
    """The shipped examples must actually be loadable by the CLI."""

    @pytest.mark.parametrize(
        ("filename", "class_name"),
        [
            ("control_engine_command.py", "QuietLinkCommander"),
            ("control_engine_formation.py", "Formation"),
        ],
    )
    def test_example_engine_loads(self, filename: str, class_name: str) -> None:
        path = Path(__file__).resolve().parent.parent / "examples" / filename
        if not path.is_file():  # pragma: no cover - examples ship with the sdist
            pytest.skip(f"{filename} not present")
        loaded = load_engine_class(path, class_name)
        assert loaded.__name__ == class_name
