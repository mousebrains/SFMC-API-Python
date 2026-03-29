"""Base class and loader for sfmc-follow follower plugins.

What is a follower?
-------------------

A **follower** is a Python class that you write.  Its job is to watch
what a Slocum glider reports each time it surfaces, decide what the
glider should do next, and generate new mission files to send back.

Think of it as a three-step loop that runs automatically every time
the glider comes up for air:

1. **Receive telemetry** -- the glider surfaces and transmits its GPS
   position, sensor readings, and timestamps.  This data arrives as a
   :class:`~sfmc_api.dialog_parser.SurfacingEvent` object.
2. **Compute** -- your code examines the telemetry, runs whatever
   logic you need (read a forecast, solve an optimisation, look up a
   drifter position, etc.), and decides what the glider should do on
   its next dive.
3. **Send files** -- your code calls :meth:`BaseFollower.send_files`
   to queue mission-argument files (``.ma`` files) for upload.  The
   framework takes care of the actual SFMC upload.

You only need to write one method: :meth:`BaseFollower.on_surfacing`.
The framework handles connecting to SFMC, parsing the raw dialog
stream, calling your method each time there is a new surfacing, and
uploading any files you produce.

How queues work
---------------

Your follower communicates with the framework through two queues
(think of them as conveyor belts that pass objects between threads):

``queue_in``
    The framework places a :class:`SurfacingEvent` on this queue
    every time the glider surfaces with a valid GPS fix and sensor
    data.  Your follower reads from this queue automatically -- you
    never need to call ``queue_in.get()`` yourself.  The base class
    :meth:`~BaseFollower.run` loop does that for you.

``queue_out``
    When your follower calls :meth:`~BaseFollower.send_files`, the
    files are placed on this queue.  A separate upload thread reads
    from it and pushes the files to SFMC (or prints them in dry-run
    mode).  Again, you never touch this queue directly -- just call
    :meth:`~BaseFollower.send_files`.

What goes in the ``config`` dict?
---------------------------------

The ``config`` dictionary is loaded from the YAML file you pass via
``--config`` on the command line.  It can contain anything your
follower needs: file paths, algorithm parameters, glider performance
numbers, etc.  The framework does not inspect it -- it simply hands
the dict to your follower's ``__init__``.  See
``examples/drifter_config.yaml`` for a real example.

How ``send_files`` works
------------------------

Call :meth:`BaseFollower.send_files` with one or both keyword
arguments:

``to_glider``
    A dict of ``{filename: content}`` for files that go into the
    glider's ``to-glider`` folder.  These are typically ``.ma``
    mission-argument files (e.g. ``goto_l30.ma``).

``to_science``
    A dict of ``{filename: content}`` for files destined for the
    ``to-science`` folder (e.g. science configuration overrides).

The framework uploads each file to the corresponding SFMC folder the
next time the glider calls in.  Example::

    self.send_files(
        to_glider={"goto_l30.ma": ma_file_content},
    )

Running with the CLI
--------------------

Once you have a follower file (say ``my_follower.py``), run it with::

    sfmc-follow --glider osu685 \\
                --follower my_follower.py \\
                --config my_config.yaml

Add ``--dry-run`` to see what files would be generated without
uploading anything.  Add ``--replay dialog.log`` to feed recorded
dialog output instead of connecting to a live glider.  See
:mod:`sfmc_api.follow_glider` for all options.

Complete minimal example
------------------------

Here is a small but complete follower that logs every surfacing and
sends back a waypoint file telling the glider to go to a fixed
position::

    # file: my_follower.py
    from sfmc_api.follower import BaseFollower
    from sfmc_api.dialog_parser import SurfacingEvent
    from sfmc_api.ma_writer import generate_goto_ma

    class FixedWaypointFollower(BaseFollower):
        \"\"\"Send the glider to a fixed lat/lon every surfacing.\"\"\"

        def on_surfacing(self, event: SurfacingEvent) -> None:
            print(f"{event.vehicle_name} surfaced at "
                  f"{event.gps_lat:.4f}, {event.gps_lon:.4f}")

            # Target position (decimal degrees: lon, lat).
            target_lon = self.config.get("target_lon", -124.5)
            target_lat = self.config.get("target_lat", 44.6)

            filename, content = generate_goto_ma(
                waypoints=[(target_lon, target_lat)],
                sequence_number=30,
            )
            # Queue the file for upload to the glider.
            self.send_files(to_glider={filename: content})

Save that file, create a YAML config with ``target_lon`` and
``target_lat``, and run::

    sfmc-follow --glider osu685 \\
                --follower my_follower.py \\
                --config my_config.yaml --dry-run

Loading a follower programmatically
------------------------------------

::

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
    """Abstract base class for glider-following plugins.

    Subclass this and implement :meth:`on_surfacing` to create your
    own follower.  You do **not** need to worry about threads, queues,
    or network connections -- the framework handles all of that.  Your
    only job is to look at the surfacing data and optionally call
    :meth:`send_files` to queue files for upload.

    Behind the scenes, the follower runs in its own background thread.
    The :meth:`run` loop reads :class:`SurfacingEvent` objects from
    ``queue_in`` one at a time and calls your :meth:`on_surfacing`
    for each one.  If your code raises an exception, the error is
    logged and the follower keeps running -- a single bad surfacing
    will not crash the whole pipeline.

    Args:
        config: A dictionary of configuration values loaded from the
            YAML file you pass via ``--config``.  For example, if
            your YAML contains ``speed_horizontal: 0.5``, then
            ``self.config["speed_horizontal"]`` will be ``0.5``.
        queue_in: The framework puts :class:`SurfacingEvent` objects
            on this queue.  You never need to read from it yourself;
            the base-class :meth:`run` loop does that.  A ``None``
            value is a shutdown signal.
        queue_out: When you call :meth:`send_files`, the files are
            placed on this queue for the upload thread to pick up.
            You never need to write to it yourself.
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
        """Process a single surfacing event -- **you must override this**.

        This is the heart of your follower.  The framework calls it
        once per glider surfacing with a :class:`SurfacingEvent` that
        contains the glider's GPS position, sensor readings, and
        timestamps.

        Inside this method you can do anything: read external data
        files, run calculations, log information, etc.  When you want
        to send a file to the glider, call :meth:`send_files`.

        If this method raises an exception, the error is logged and
        the follower continues to the next surfacing.  You do not need
        to add your own try/except unless you want custom error
        handling.

        Args:
            event: Parsed telemetry from one glider surfacing.  Key
                fields include:

                - ``event.vehicle_name`` -- the glider's name
                  (e.g. ``"osu685"``).
                - ``event.gps_lat``, ``event.gps_lon`` -- position in
                  decimal degrees.
                - ``event.timestamp`` -- UTC datetime of the surfacing.
                - ``event.sensors`` -- a dict mapping sensor names
                  (e.g. ``"m_water_vx"``) to
                  :class:`~sfmc_api.dialog_parser.SensorReading`
                  objects with ``.value`` and ``.age_secs`` attributes.
                - ``event.raw_lines`` -- the original dialog text.

        Example::

            def on_surfacing(self, event):
                lat = event.gps_lat
                lon = event.gps_lon
                print(f"Glider is at {lat:.4f}, {lon:.4f}")
                # Check depth-average current:
                vx = event.sensors.get("m_water_vx")
                if vx:
                    print(f"  water vx = {vx.value:.3f} m/s "
                          f"({vx.age_secs:.0f}s ago)")
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
