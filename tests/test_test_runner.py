"""Tests for the sfmc-api-test runner's read-only default.

The runner talks to a live server, so these tests stub out the test
groups and only verify the gating logic: which groups run, and how
runner-level consent reaches the child CLI's destructive commands.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from sfmc_api import test_runner

#: Maps each stubbed group function to the --skip group it belongs to.
_GROUP_FUNCS = {
    "_test_auth": "auth",
    "_test_glider_queries": "queries",
    "_test_deployment": "deployment",
    "_test_folder_file_listing": "listing",
    "_test_surface_sensor_samples": "sensors",
    "_test_file_upload_download_delete": "files",
    "_test_upload_to_science": "files",
    "_test_plan_updates": "plans",
    "_test_deploy_files": "deploy",
    "_test_delete_plan_rules": "delete-rules",
    "_test_script_control": "scripts",
    "_test_send_command": "command",
}


@pytest.fixture(autouse=True)
def _reset_results() -> None:
    test_runner._results.clear()


def _run_main(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    extra_args: list[str],
) -> tuple[set[str], int | None]:
    """Run main() with all groups stubbed; return (groups run, exit code)."""
    ran: set[str] = set()

    def make_stub(group: str, returns: bool) -> object:
        def stub(*args: object, **kwargs: object) -> bool:
            ran.add(group)
            return returns

        return stub

    for func_name, group in _GROUP_FUNCS.items():
        # Only _test_auth's return value is inspected by main().
        monkeypatch.setattr(test_runner, func_name, make_stub(group, True))
    monkeypatch.setattr(test_runner.subprocess, "run", MagicMock())

    creds = tmp_path / "creds.json"
    creds.write_text(json.dumps({"h": {"apiCredentials": {"clientId": "a", "secret": "b"}}}))

    argv = [
        "sfmc-api-test",
        "--host",
        "h",
        "--glider",
        "g",
        "--credentials",
        str(creds),
        *extra_args,
    ]
    monkeypatch.setattr(sys, "argv", argv)

    with pytest.raises(SystemExit) as exc_info:
        test_runner.main()
    return ran, exc_info.value.code


class TestWriteGating:
    def test_default_runs_only_read_only_groups(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setenv("SFMC_ASSUME_YES", "0")

        ran, code = _run_main(tmp_path, monkeypatch, [])

        assert code == 0
        assert ran == set(test_runner._READ_ONLY_GROUPS)
        # No runner-level consent was given, so none is forwarded.
        assert os.environ["SFMC_ASSUME_YES"] == "0"
        assert "read-only" in capsys.readouterr().out

    def test_allow_writes_runs_mutation_groups_and_forwards_consent(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setenv("SFMC_ASSUME_YES", "0")

        ran, code = _run_main(tmp_path, monkeypatch, ["--allow-writes"])

        assert code == 0
        assert ran == set(test_runner._READ_ONLY_GROUPS) | set(test_runner._WRITE_GROUPS)
        # Consent reaches the child CLI's destructive commands, whose
        # confirmation prompts would otherwise hang or fail cleanup.
        assert os.environ["SFMC_ASSUME_YES"] == "1"
        assert "READ-WRITE" in capsys.readouterr().out

    def test_skip_still_honoured_with_allow_writes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("SFMC_ASSUME_YES", "0")

        ran, code = _run_main(tmp_path, monkeypatch, ["--allow-writes", "--skip", "command"])

        assert code == 0
        assert "command" not in ran

    def test_groups_partition_is_complete(self) -> None:
        """Every group named in the --skip help text is classified."""
        assert set(test_runner._READ_ONLY_GROUPS) | set(test_runner._WRITE_GROUPS) == {
            "auth",
            "queries",
            "deployment",
            "listing",
            "files",
            "plans",
            "deploy",
            "scripts",
            "command",
            "sensors",
            "delete-rules",
        }
        assert not set(test_runner._READ_ONLY_GROUPS) & set(test_runner._WRITE_GROUPS)
