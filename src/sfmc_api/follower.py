"""Base class and loader for sfmc-follow follower plugins.

A *follower* is a user-supplied class that receives parsed telemetry
from each glider surfacing and can generate files to send back to the
glider.  Subclass :class:`BaseFollower` and implement
:meth:`~BaseFollower.on_surfacing` to create your own.

Example — a minimal follower that logs each surfacing::

    from sfmc_api.follower import BaseFollower
    from sfmc_api.dialog_parser import SurfacingEvent

    class LogFollower(BaseFollower):
        def on_surfacing(self, event: SurfacingEvent) -> None:
            print(f"{event.vehicle_name} at "
                  f"{event.gps_lat:.4f}, {event.gps_lon:.4f}")

Loading a follower from a file::

    cls = load_follower_class("my_follower.py", "LogFollower")
    follower = cls(config={}, queue_in=q_in, queue_out=q_out)
    follower.start()
"""

from __future__ import annotations

import importlib.util
import inspect
import logging
import threading
from abc import abstractmethod
from pathlib import Path
from queue import Empty, Queue
from typing import Any

from sfmc_api.dialog_parser import SurfacingEvent

logger = logging.getLogger(__name__)


class BaseFollower(threading.Thread):
    """Abstract base class for sfmc-follow follower plugins.

    The follower runs in its own thread.  The default :meth:`run`
    implementation reads :class:`SurfacingEvent` objects from
    *queue_in* and calls :meth:`on_surfacing` for each one.  Override
    :meth:`on_surfacing` to process telemetry and generate files.

    Args:
        config: Configuration dictionary (typically loaded from YAML).
        queue_in: Queue supplying :class:`SurfacingEvent` objects.
            A ``None`` sentinel signals the follower to shut down.
        queue_out: Queue for output file dictionaries.  Each item
            should be ``{"to-glider": {name: content}, "to-science":
            {name: content}}``.
    """

    def __init__(
        self,
        config: dict[str, Any],
        queue_in: Queue[SurfacingEvent | None],
        queue_out: Queue[dict[str, dict[str, str | bytes]] | None],
    ) -> None:
        super().__init__(daemon=True, name=type(self).__name__)
        self.config = config
        self.queue_in = queue_in
        self.queue_out = queue_out

    def run(self) -> None:
        """Main loop: read surfacing events and call on_surfacing.

        Catches exceptions in :meth:`on_surfacing` so that a single
        bad surfacing does not kill the follower thread.
        """
        while True:
            try:
                event = self.queue_in.get(timeout=1.0)
            except Empty:
                continue
            if event is None:
                logger.debug("%s: received shutdown sentinel", self.name)
                break
            try:
                self.on_surfacing(event)
            except Exception:
                logger.exception(
                    "%s: error processing surfacing for %s",
                    self.name,
                    event.vehicle_name,
                )

    @abstractmethod
    def on_surfacing(self, event: SurfacingEvent) -> None:
        """Process a single surfacing event.

        Override this method in your follower.  Use :meth:`send_files`
        to queue files for upload to SFMC.

        Args:
            event: Parsed telemetry from the glider's surfacing.
        """

    def send_files(
        self,
        to_glider: dict[str, str | bytes] | None = None,
        to_science: dict[str, str | bytes] | None = None,
    ) -> None:
        """Queue files for upload to SFMC.

        Args:
            to_glider: Dict of ``{filename: content}`` for the
                ``to-glider`` folder.
            to_science: Dict of ``{filename: content}`` for the
                ``to-science`` folder.
        """
        output: dict[str, dict[str, str | bytes]] = {}
        if to_glider:
            output["to-glider"] = to_glider
        if to_science:
            output["to-science"] = to_science
        if output:
            self.queue_out.put(output)

    def shutdown(self) -> None:
        """Signal the follower to stop.

        Puts a ``None`` sentinel on *queue_in* to unblock the
        :meth:`run` loop.
        """
        self.queue_in.put(None)


# ── Dynamic class loader ───────────────────────────────────────────


def load_follower_class(
    file_path: str | Path,
    class_name: str | None = None,
) -> type[BaseFollower]:
    """Load a :class:`BaseFollower` subclass from a Python file.

    Uses :mod:`importlib` to load the module from *file_path*.  If
    *class_name* is given, that class is returned.  Otherwise, the
    module is inspected for a single :class:`BaseFollower` subclass.

    Args:
        file_path: Path to the Python file containing the follower.
        class_name: Name of the class to load.  If ``None``, the
            single :class:`BaseFollower` subclass in the file is used.

    Returns:
        The follower class (not an instance).

    Raises:
        FileNotFoundError: If *file_path* does not exist.
        ValueError: If *class_name* is not found, or if auto-detection
            finds zero or more than one subclass.
        ImportError: If the file cannot be loaded as a Python module.
    """
    path = Path(file_path).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Follower file not found: {path}")

    module_name = path.stem
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot create module spec from {path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if class_name is not None:
        cls = getattr(module, class_name, None)
        if cls is None:
            raise ValueError(f"Class {class_name!r} not found in {path}")
        if not (inspect.isclass(cls) and issubclass(cls, BaseFollower)):
            raise ValueError(f"{class_name!r} in {path} is not a BaseFollower subclass")
        return cls

    # Auto-detect: find all BaseFollower subclasses.
    candidates = [
        obj
        for _, obj in inspect.getmembers(module, inspect.isclass)
        if issubclass(obj, BaseFollower) and obj is not BaseFollower
    ]

    if len(candidates) == 0:
        raise ValueError(f"No BaseFollower subclass found in {path}")
    if len(candidates) > 1:
        names = [c.__name__ for c in candidates]
        raise ValueError(
            f"Multiple BaseFollower subclasses in {path}: {names}. "
            f"Use --class to specify which one."
        )
    return candidates[0]
