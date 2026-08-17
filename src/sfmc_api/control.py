"""``sfmc-control`` — run a control engine against one or more gliders.

Phase 4 of ``docs/design/control_engine.md``: the terminal front end for
the engine in :mod:`sfmc_api.engine`.

The flags mirror ``sfmc-follow`` wherever they mean the same thing, so
somebody moving from a follower to an engine has one less thing to
relearn — with one deliberate exception: ``--glider`` is **repeatable**,
because an engine exists to make decisions across a formation.

::

    # Read-only against one glider, the safe default
    sfmc-control --glider osu685 --engine my_engine.py

    # A formation
    sfmc-control --glider osu684 --glider osu685 --engine formation.py

    # Full logic, nothing sent
    sfmc-control --glider osu685 --engine retask.py --allow-writes --dry-run

    # For real
    sfmc-control --glider osu685 --engine retask.py --allow-writes

    # Offline, against recorded dialog, with no network at all
    sfmc-control --glider osusim --engine my_engine.py --replay dialog.log

Three states are worth keeping straight, because two of them look alike
from the outside and only one can move a glider:

===================================  ==========================
flags                                what reaches the glider
===================================  ==========================
*(none)*                             nothing; writes are refused
``--allow-writes --dry-run``         nothing; writes are simulated
``--allow-writes``                   **commands**
===================================  ==========================
"""

from __future__ import annotations

import argparse
import importlib.util
import inspect
import logging
import sys
import threading
from pathlib import Path
from typing import Any

from .client import SFMCClient
from .engine import BaseControlEngine, EngineRunner, audit_log

__all__ = ["load_engine_class", "main"]

logger = logging.getLogger(__name__)


def load_engine_class(
    file_path: str | Path,
    class_name: str | None = None,
) -> type[BaseControlEngine]:
    """Load a :class:`~sfmc_api.engine.BaseControlEngine` from a file.

    Mirrors :func:`~sfmc_api.follower.load_follower_class`, including
    auto-detection when the file holds exactly one engine.

    Args:
        file_path: Python file containing the engine class.
        class_name: Which class to use.  Auto-detected when ``None``
            and the file defines exactly one engine.

    Raises:
        FileNotFoundError: If *file_path* does not exist.
        ValueError: If *class_name* is absent, or auto-detection finds
            zero or several candidates.
        ImportError: If the file cannot be imported.
    """
    path = Path(file_path).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"engine file not found: {path}")

    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {path} as a Python module")
    module = importlib.util.module_from_spec(spec)
    # Registered before exec so dataclasses and pickling inside the
    # engine file can find their own module.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    if class_name is not None:
        found = getattr(module, class_name, None)
        if found is None or not _is_engine(found):
            raise ValueError(f"{class_name!r} is not a BaseControlEngine subclass in {path}")
        return found  # type: ignore[no-any-return]

    candidates: list[type[BaseControlEngine]] = [
        value
        for value in vars(module).values()
        if _is_engine(value) and value.__module__ == spec.name
    ]
    if not candidates:
        raise ValueError(f"no BaseControlEngine subclass found in {path}")
    if len(candidates) > 1:
        names = sorted(c.__name__ for c in candidates)
        raise ValueError(f"{path} defines several engines ({names}); name one with --class")
    return candidates[0]


def _is_engine(value: Any) -> bool:
    return (
        inspect.isclass(value)
        and issubclass(value, BaseControlEngine)
        and value is not BaseControlEngine
    )


def _load_config(path: str | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        import yaml
    except ImportError:  # pragma: no cover - depends on the extra
        raise SystemExit(
            "--config needs PyYAML; install it with: pip install 'sfmc-api[follow]'"
        ) from None
    with open(path, encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle)
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise SystemExit(f"{path}: expected a YAML mapping, got {type(loaded).__name__}")
    return loaded


def _configure_audit(path: str | None) -> None:
    """Send the audit log to its own file, in addition to the console."""
    if path is None:
        return
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    audit_log.addHandler(handler)
    audit_log.setLevel(logging.INFO)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sfmc-control",
        description="Run a control engine against one or more gliders.",
        epilog=(
            "safety:\n"
            "  (no flags)               read-only; writes are refused\n"
            "  --allow-writes --dry-run full logic, writes simulated\n"
            "  --allow-writes           writes are SENT to the glider\n"
            "  --replay LOG             offline, no network at all\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--glider",
        action="append",
        dest="gliders",
        metavar="NAME",
        help="Registered glider name.  Repeat for a formation.",
    )
    parser.add_argument(
        "--engine",
        required=True,
        metavar="FILE",
        help="Path to a Python file containing the engine class",
    )
    parser.add_argument(
        "--class",
        dest="class_name",
        default=None,
        metavar="NAME",
        help="Engine class name (auto-detected if the file has exactly one)",
    )
    parser.add_argument(
        "--config",
        default=None,
        metavar="FILE",
        help="YAML configuration file passed to the engine",
    )
    parser.add_argument(
        "--hostname",
        default=None,
        help="SFMC server hostname (selects entry from multi-host credentials file)",
    )
    parser.add_argument(
        "--credentials",
        default=None,
        metavar="PATH",
        help="Path to credentials JSON file (default: ~/.config/sfmc/credentials.json)",
    )

    safety = parser.add_argument_group("safety")
    safety.add_argument(
        "--allow-writes",
        action="store_true",
        help="Permit state-changing operations.  Without this they are refused.",
    )
    safety.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the engine's full logic but simulate every write",
    )
    safety.add_argument(
        "--replay",
        default=None,
        metavar="LOGFILE",
        help="Replay dialog from a log instead of connecting.  No network is used.",
    )
    safety.add_argument(
        "--max-outstanding",
        type=int,
        default=None,
        metavar="N",
        help="Cap on requests in flight, fleet-wide",
    )
    safety.add_argument(
        "--tick",
        type=float,
        default=None,
        metavar="SECONDS",
        help=(
            "Emit a periodic tick event per glider.  Needed by an engine that "
            "reacts to the absence of dialog, since silence delivers nothing."
        ),
    )
    safety.add_argument(
        "--max-runtime",
        type=float,
        default=None,
        metavar="SECONDS",
        help="Stop after this long",
    )

    logs = parser.add_argument_group("logging")
    logs.add_argument(
        "--audit-log",
        default=None,
        metavar="PATH",
        help="Also write the request/outcome audit trail to this file",
    )
    logs.add_argument(
        "--verbose",
        action="store_true",
        help="Debug-level logging",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point for ``sfmc-control``."""
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    _configure_audit(args.audit_log)

    if not args.gliders:
        parser.error("at least one --glider is required")
    if args.dry_run and not args.allow_writes:
        # Not an error, but silence here would let someone believe a dry
        # run was the thing protecting them when the gate already was.
        logger.info("--dry-run is redundant without --allow-writes; writes are refused anyway")
    if args.replay and len(args.gliders) > 1:
        parser.error("--replay drives a single glider; give exactly one --glider")

    try:
        engine_class = load_engine_class(args.engine, args.class_name)
    except (FileNotFoundError, ValueError, ImportError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    engine = engine_class(_load_config(args.config))
    extra: dict[str, Any] = {}
    if args.max_outstanding is not None:
        extra["max_outstanding"] = args.max_outstanding
    if args.tick is not None:
        extra["tick"] = args.tick

    if args.replay:
        # No client at all: a replay run contacts nothing.
        runner = EngineRunner(
            engine,
            client=None,
            allow_writes=args.allow_writes,
            dry_run=args.dry_run,
            **extra,
        )
        try:
            with open(args.replay, encoding="utf-8", errors="replace") as handle:
                runner.replay(handle, glider=args.gliders[0])
        finally:
            runner.close()
        return 0

    if args.allow_writes and not args.dry_run:
        logger.warning("writes are ENABLED: this run can command %s", ", ".join(args.gliders))

    with SFMCClient(host=args.hostname, config_path=args.credentials) as client:
        runner = EngineRunner(
            engine,
            client,
            gliders=args.gliders,
            allow_writes=args.allow_writes,
            dry_run=args.dry_run,
            **extra,
        )
        if args.max_runtime:
            threading.Timer(args.max_runtime, runner.stop).start()
        try:
            runner.run()
        except KeyboardInterrupt:
            runner.stop()
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
