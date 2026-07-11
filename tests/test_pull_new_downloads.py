"""Tests for the sfmc-pull-new-downloads command."""

from __future__ import annotations

import json
import queue as queue_mod
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from sfmc_api.exceptions import SFMCError
from sfmc_api.pull_new_downloads import (
    PullState,
    _drain,
    _nonnegative_int,
    baseline,
    cutoff_before,
    download_new_files,
    list_new_files,
    reconcile,
)
from sfmc_api.stomp import StompSubscription


def entry(name: str, mtime: str, size: int) -> dict[str, Any]:
    return {"fileName": name, "dateTimeModified": mtime, "fileSize": size}


class TestCutoffBefore:
    def test_floors_to_minute_and_subtracts_margin(self) -> None:
        assert cutoff_before("2026-07-11 01:23:44", 5) == "202607110118"

    def test_zero_margin_keeps_minute(self) -> None:
        assert cutoff_before("2026-07-11 01:23:44", 0) == "202607110123"

    def test_margin_crosses_midnight(self) -> None:
        assert cutoff_before("2026-07-11 00:02:10", 5) == "202607102357"


class TestPullState:
    def test_load_missing_file_returns_empty(self, tmp_path: Path) -> None:
        state = PullState.load(tmp_path / "nope.json")
        assert state.hwm is None
        assert state.files == {}

    def test_round_trip(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        state = PullState()
        state.observe("g-2026-191-0-1.sbd", "2026-07-11 01:23:44", 6841)
        state.observe("g-2026-191-0-2.sbd", "2026-07-11 00:55:00", 694)
        state.save(path)

        loaded = PullState.load(path)
        assert loaded.hwm == "2026-07-11 01:23:44"
        assert loaded.files["g-2026-191-0-2.sbd"] == {"size": 694, "mtime": "2026-07-11 00:55:00"}

    def test_hwm_only_advances(self) -> None:
        state = PullState()
        state.observe("g-2026-191-0-1.sbd", "2026-07-11 01:00:00", 1)
        state.observe("g-2026-190-9-9.sbd", "2026-07-10 09:00:00", 2)
        assert state.hwm == "2026-07-11 01:00:00"

    def test_non_dinkum_names_never_advance_hwm(self) -> None:
        state = PullState()
        state.observe("48280006.sbd", "2026-07-11 09:00:00", 5120)
        assert state.hwm is None
        state.observe("g-2026-191-0-1.sbd", "2026-07-11 01:00:00", 1)
        state.observe("12345678.mrd", "2026-07-11 23:00:00", 9)
        assert state.hwm == "2026-07-11 01:00:00"

    def test_is_new_for_unseen_and_changed_mtime(self) -> None:
        state = PullState()
        state.observe("a.sbd", "2026-07-11 01:00:00", 1)
        assert not state.is_new("a.sbd", "2026-07-11 01:00:00")
        assert state.is_new("a.sbd", "2026-07-11 02:00:00")
        assert state.is_new("new.sbd", "2026-07-11 01:00:00")

    def test_load_rejects_unknown_version(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        path.write_text(json.dumps({"version": 99}), encoding="utf-8")
        with pytest.raises(Exception, match="version"):
            PullState.load(path)

    def test_load_rejects_corrupt_json(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        path.write_text("{truncated", encoding="utf-8")
        with pytest.raises(SFMCError, match="Corrupt state file"):
            PullState.load(path)

    def test_load_rejects_non_object(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        path.write_text("null", encoding="utf-8")
        with pytest.raises(SFMCError, match="Corrupt state file"):
            PullState.load(path)


class TestListNewFiles:
    def test_filters_known_files_and_uses_cutoff(self) -> None:
        client = MagicMock()
        client.get_folder_file_listing.return_value = {
            "limit": 20,
            "results": [
                entry("g-2026-192-0-1.sbd", "2026-07-11 01:23:44", 6841),
                entry("g-2026-191-0-9.sbd", "2026-07-11 01:00:00", 694),
            ],
        }
        state = PullState()
        state.observe("g-2026-191-0-9.sbd", "2026-07-11 01:00:00", 694)

        new = list_new_files(client, "osusim", state, margin_minutes=5)

        assert [e["fileName"] for e in new] == ["g-2026-192-0-1.sbd"]
        client.get_folder_file_listing.assert_called_once_with(
            "osusim",
            "from-glider",
            page=0,
            last_modified_after="202607110055",
        )

    def test_no_cutoff_before_baseline(self) -> None:
        client = MagicMock()
        client.get_folder_file_listing.return_value = {"limit": 20, "results": []}

        list_new_files(client, "osusim", PullState(), margin_minutes=5)

        kwargs = client.get_folder_file_listing.call_args.kwargs
        assert kwargs["last_modified_after"] is None

    def test_defers_non_dinkum_names_while_connected(self) -> None:
        client = MagicMock()
        client.get_folder_file_listing.return_value = {
            "limit": 20,
            "results": [
                # Possibly mid-transfer (8.3 DOS name, partial size) —
                # deferred while a connection is open.
                entry("48280006.sbd", "2026-07-11 01:42:35", 5120),
                entry("osusim-2026-191-0-5.sbd", "2026-07-11 02:01:46", 694),
            ],
        }

        new = list_new_files(client, "osusim", PullState(), margin_minutes=5, connected=True)

        assert [e["fileName"] for e in new] == ["osusim-2026-191-0-5.sbd"]

    def test_includes_non_dinkum_names_when_disconnected(self) -> None:
        client = MagicMock()
        client.get_folder_file_listing.return_value = {
            "limit": 20,
            "results": [
                # Never-renamed file classes must be downloadable once
                # the glider disconnects.
                entry("48280006.tcd", "2026-07-11 01:42:35", 5120),
                entry("12345678.mrd", "2026-07-11 01:40:00", 200),
                entry("osusim-2026-191-0-5.sbd", "2026-07-11 02:01:46", 694),
            ],
        }

        new = list_new_files(client, "osusim", PullState(), margin_minutes=5, connected=False)

        assert [e["fileName"] for e in new] == [
            "48280006.tcd",
            "12345678.mrd",
            "osusim-2026-191-0-5.sbd",
        ]

    def test_paginates_full_pages(self) -> None:
        client = MagicMock()
        page0 = {
            "limit": 2,
            "results": [
                entry("a.sbd", "2026-07-11 01:00:02", 1),
                entry("b.sbd", "2026-07-11 01:00:01", 2),
            ],
        }
        page1 = {"limit": 2, "results": [entry("c.sbd", "2026-07-11 01:00:00", 3)]}
        client.get_folder_file_listing.side_effect = [page0, page1]
        state = PullState()
        state.observe("g-2026-190-0-0.sbd", "2026-07-11 00:00:00", 0)

        new = list_new_files(client, "osusim", state, margin_minutes=5)

        assert len(new) == 3
        assert client.get_folder_file_listing.call_count == 2


def make_zip_client(tmp_path: Path, members: dict[str, bytes]) -> MagicMock:
    """A mock client whose download_glider_files writes a real zip."""

    def fake_download(
        glider_name: str,
        folder: str,
        download_path: Path,
        **kwargs: Any,
    ) -> Path:
        with zipfile.ZipFile(download_path, "w") as zf:
            for name, content in members.items():
                zf.writestr(name, content)
        return download_path

    client = MagicMock()
    client.download_glider_files.side_effect = fake_download
    return client


class TestDownloadNewFiles:
    def test_extracts_only_wanted_members(self, tmp_path: Path) -> None:
        outdir = tmp_path / "out"
        outdir.mkdir()
        state_path = tmp_path / "state.json"
        state = PullState()
        client = make_zip_client(
            tmp_path,
            {
                "g-2026-192-0-1.sbd": b"x" * 10,
                "overlap.sbd": b"y" * 4,  # inside margin window, already known
            },
        )
        new_entries = [entry("g-2026-192-0-1.sbd", "2026-07-11 01:23:44", 10)]

        n = download_new_files(client, "osusim", new_entries, outdir, state, state_path)

        assert n == 1
        assert (outdir / "g-2026-192-0-1.sbd").read_bytes() == b"x" * 10
        assert not (outdir / "overlap.sbd").exists()
        assert state.files["g-2026-192-0-1.sbd"]["mtime"] == "2026-07-11 01:23:44"
        assert PullState.load(state_path).hwm == "2026-07-11 01:23:44"
        kwargs = client.download_glider_files.call_args.kwargs
        assert kwargs["last_modified_after"] == "202607110122"
        # Listing mtimes are glider-clock UTC; the local copy's mtime
        # must not shift with the host timezone.
        expected = datetime(2026, 7, 11, 1, 23, 44, tzinfo=UTC).timestamp()
        assert (outdir / "g-2026-192-0-1.sbd").stat().st_mtime == expected
        assert not (outdir / "g-2026-192-0-1.sbd.part").exists()

    def test_missing_member_not_recorded(self, tmp_path: Path) -> None:
        outdir = tmp_path / "out"
        outdir.mkdir()
        state = PullState()
        client = make_zip_client(tmp_path, {})
        new_entries = [entry("pending.sbd", "2026-07-11 01:23:44", 10)]

        n = download_new_files(
            client,
            "osusim",
            new_entries,
            outdir,
            state,
            state_path=tmp_path / "s.json",
        )

        assert n == 0
        assert "pending.sbd" not in state.files


class TestReconcile:
    def test_no_new_files_no_download(self, tmp_path: Path) -> None:
        client = MagicMock()
        client.get_folder_file_listing.return_value = {"limit": 20, "results": []}
        state = PullState()
        state.observe("a.sbd", "2026-07-11 01:00:00", 1)

        n = reconcile(client, "osusim", tmp_path, state, tmp_path / "s.json", margin_minutes=5)

        assert n == 0
        client.download_glider_files.assert_not_called()


class TestBaseline:
    def test_records_without_downloading(self, tmp_path: Path) -> None:
        client = MagicMock()
        client.get_folder_file_listing.return_value = {
            "limit": 20,
            "results": [entry("g-2026-191-0-1.sbd", "2026-07-11 01:00:00", 1)],
        }
        state = PullState()
        state_path = tmp_path / "state.json"

        baseline(client, "osusim", state, state_path, margin_minutes=5)

        assert state.hwm == "2026-07-11 01:00:00"
        assert state_path.exists()
        client.download_glider_files.assert_not_called()

    def test_non_dinkum_name_kept_out_of_hwm(self, tmp_path: Path) -> None:
        client = MagicMock()
        client.get_folder_file_listing.return_value = {
            "limit": 20,
            "results": [
                # 8.3-named entry carries a dockserver-clock mtime that
                # must not contaminate the glider-clock high-water
                # mark, but its name is recorded for dedup.
                entry("48280006.sbd", "2026-07-11 09:59:59", 5120),
                entry("osusim-2026-191-0-5.sbd", "2026-07-11 01:00:00", 694),
            ],
        }
        state = PullState()

        baseline(client, "osusim", state, tmp_path / "state.json", margin_minutes=5)

        assert state.hwm == "2026-07-11 01:00:00"
        assert "48280006.sbd" in state.files


class TestDrain:
    def _sub(self) -> StompSubscription:
        return StompSubscription("sub-0", "/topic/test", queue_mod.Queue())

    def test_returns_queued_messages_not_closed(self) -> None:
        sub = self._sub()
        sub._queue.put([{"active": True}])
        sub._queue.put([40633])

        messages, closed = _drain(sub)

        assert messages == [[{"active": True}], [40633]]
        assert not closed

    def test_reports_close_sentinel_behind_events(self) -> None:
        # The sentinel is enqueued exactly once; swallowing it here
        # would leave the caller looping forever on a dead stream.
        sub = self._sub()
        sub._queue.put([{"active": False}])
        sub._queue.put(None)

        messages, closed = _drain(sub)

        assert messages == [[{"active": False}]]
        assert closed


class TestBaselineAnchor:
    def test_walks_past_undinkum_page_to_find_hwm(self, tmp_path: Path) -> None:
        # Page 0 holds only un-renamed names (renames pending); the
        # walk must continue until a Dinkum name anchors the mark.
        client = MagicMock()
        page0 = {
            "limit": 2,
            "results": [
                entry("48280007.sbd", "2026-07-11 02:00:00", 100),
                entry("48280006.sbd", "2026-07-11 01:59:00", 100),
            ],
        }
        page1 = {
            "limit": 2,
            "results": [
                entry("g-2026-191-0-5.sbd", "2026-07-11 01:30:00", 694),
                entry("g-2026-191-0-4.sbd", "2026-07-11 01:00:00", 694),
            ],
        }
        window = {"limit": 2, "results": [entry("g-2026-191-0-5.sbd", "2026-07-11 01:30:00", 694)]}
        client.get_folder_file_listing.side_effect = [page0, page1, window]
        state = PullState()

        baseline(client, "osusim", state, tmp_path / "s.json", margin_minutes=5)

        assert state.hwm == "2026-07-11 01:30:00"
        # Everything walked past was recorded for dedup.
        assert "48280007.sbd" in state.files
        assert "48280006.sbd" in state.files

    def test_no_dinkum_anywhere_records_all_without_hwm(self, tmp_path: Path) -> None:
        client = MagicMock()
        client.get_folder_file_listing.return_value = {
            "limit": 20,
            "results": [entry("12345678.mrd", "2026-07-11 01:00:00", 9)],
        }
        state = PullState()

        baseline(client, "osusim", state, tmp_path / "s.json", margin_minutes=5)

        assert state.hwm is None
        assert "12345678.mrd" in state.files


class TestMarginValidation:
    def test_rejects_negative(self) -> None:
        with pytest.raises(Exception, match=">= 0"):
            _nonnegative_int("-5")

    def test_accepts_zero(self) -> None:
        assert _nonnegative_int("0") == 0
