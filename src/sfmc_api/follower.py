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
from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any

from sfmc_api.dialog_parser import SurfacingEvent, SurfacingStream
from sfmc_api.engine import BaseControlEngine
from sfmc_api.events import Event

if TYPE_CHECKING:
    from sfmc_api.disconnect_notify import DisconnectNotifier

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UploadBatch:
    """Files queued for upload, and the glider they are for.

    Before multi-glider followers, a batch needed no glider: there was
    only one, and the upload thread knew it.  A formation follower
    steering two vehicles cannot rely on that, and a batch that does
    not say where it goes is the kind of ambiguity that puts a waypoint
    file on the wrong glider.

    Attributes:
        glider: Target name, or ``None`` to mean "whichever glider the
            pipeline is for" — what a single-glider follower written
            before any of this still produces.
        folders: ``{folder: {filename: contents}}``.
    """

    folders: dict[str, dict[str, str | bytes]]
    glider: str | None = None


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

    #: Glider these files are for, set by the framework to the identity
    #: the *operator* supplied -- never to a name parsed out of dialog.
    #: A class-level default so a subclass that does not chain
    #: ``super().__init__`` cannot raise inside ``send_files``: that
    #: exception is caught by the run loop, so the pipeline would look
    #: healthy while never uploading another file.
    current_glider: str | None = None

    def __init__(
        self,
        config: dict[str, Any],
        queue_in: Queue[SurfacingEvent | None],
        queue_out: Queue[UploadBatch | dict[str, dict[str, str | bytes]] | None],
    ) -> None:
        super().__init__(daemon=True, name=type(self).__name__)
        self.config = config
        self.queue_in = queue_in
        self.queue_out = queue_out
        self._notifier: DisconnectNotifier | None = None
        self.current_glider = None

    def set_notifier(self, notifier: DisconnectNotifier | None) -> None:
        """Attach the operator-email notifier.

        Called by the ``sfmc-follow`` framework before the pipeline
        starts; follower code should not call this itself.  When no
        ``--notify-email`` was given (or in replay mode) the notifier
        stays ``None`` and :meth:`notify` is a silent no-op.
        """
        self._notifier = notifier

    def notify(
        self,
        key: str,
        summary: str,
        detail: str = "",
        *,
        min_gap_seconds: float = 900.0,
    ) -> bool:
        """Email the operator about a condition only this follower sees.

        For application-level trouble the framework cannot detect on
        its own: an external float-position feed gone quiet, an ``.ma``
        file that cannot be generated, a target outside the operating
        box.  Delivery is non-blocking (background thread, retried),
        and repeats of the same *key* within *min_gap_seconds* are
        dropped — so calling this on every surfacing while a condition
        persists costs one email per window, not one per surfacing.

        Example::

            def on_surfacing(self, event):
                fix = self.fetch_float_position()
                if fix is None:
                    self.notify(
                        "float-feed-down",
                        "float position feed unavailable",
                        "No position from the drifter feed; holding "
                        "the previous waypoint.",
                    )
                    return

        Args:
            key: Stable identifier of the condition (e.g.
                ``"float-feed-down"``).  Rate limiting is per key.
            summary: One line for the email subject.
            detail: Optional body text (subject line reused if empty).
            min_gap_seconds: Minimum spacing between emails for this
                key (default 900 = 15 min).

        Returns:
            ``True`` if an email was queued; ``False`` if notifications
            are disabled or the key is still inside its rate window.
        """
        if self._notifier is None:
            return False
        return self._notifier.notify_event(
            key,
            summary,
            detail,
            min_gap_seconds=min_gap_seconds,
        )

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
            # NOT event.vehicle_name.  That is scraped from glider
            # output by an unanchored regex, and using it as an upload
            # target means untrusted firmware text decides which vehicle
            # receives steering files.  current_glider is set once, by
            # the framework, to the identity the operator gave.
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
        glider: str | None = None,
    ) -> None:
        """Queue files for upload to SFMC.

        Args:
            to_glider: Dict of ``{filename: content}`` for the
                ``to-glider`` folder.
            to_science: Dict of ``{filename: content}`` for the
                ``to-science`` folder.
            glider: Which glider to send to.  Defaults to the glider
                whose surfacing is being handled right now, so a
                single-glider follower written before formations
                existed keeps working untouched::

                    def on_surfacing(self, event):
                        self.send_files(to_glider={"goto_l10.ma": ma})

                A formation follower names the target explicitly::

                        self.send_files(to_glider={...}, glider="osu686")

                ``event.vehicle_name`` has always carried the glider
                identity, which is why :meth:`on_surfacing` needs no
                signature change for any of this.
        """
        output: dict[str, dict[str, str | bytes]] = {}
        if to_glider:
            output["to-glider"] = to_glider
        if to_science:
            output["to-science"] = to_science
        if output:
            self.queue_out.put(UploadBatch(folders=output, glider=glider or self.current_glider))
            depth = self.queue_out.qsize()
            if depth > 8:
                logger.warning(
                    "upload backlog at %d batch(es) — uploads appear to be failing or stalled",
                    depth,
                )

    def shutdown(self) -> None:
        """Signal the follower to stop.

        Puts a ``None`` sentinel on *queue_in* to unblock the
        :meth:`run` loop.
        """
        self.queue_in.put(None)


class FollowerEngine(BaseControlEngine):
    """Runs a :class:`BaseFollower` on the control engine.

    A follower is a control engine with a narrower question: instead of
    "what happened?", it asks "what happened *at a surfacing*?".  So it
    folds in as a specialisation rather than staying a parallel
    mechanism, and gets multi-glider support for free::

        sfmc-control --glider osu684 --glider osu685 \
                     --engine my_follower.py --allow-writes

    **Existing followers work unchanged.**  ``SurfacingEvent`` has
    always carried ``vehicle_name``, so :meth:`BaseFollower.on_surfacing`
    needs no signature change to see a formation — one instance handles
    every glider's surfacings, on one thread, with the same no-locks
    guarantee the engine gives everything else.

    The follower's thread is deliberately **not** started.  Its
    :meth:`~BaseFollower.on_surfacing` is called directly on the engine
    thread, so a follower keeps its one-at-a-time contract and gains the
    engine's ordering guarantees rather than having two schedulers
    disagree about which is in charge.

    Args:
        follower_class: A :class:`BaseFollower` subclass.
        config: Passed to the follower, as ``--config`` does today.
    """

    sources = ("dialog",)

    def __init__(
        self,
        follower_class: type[BaseFollower],
        config: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(config)
        self._queue_in: Queue[SurfacingEvent | None] = Queue()
        self._queue_out: Queue[UploadBatch | dict[str, dict[str, str | bytes]] | None] = Queue()
        self.follower = follower_class(self.config, self._queue_in, self._queue_out)
        # One stream per glider: a parser accumulates the lines of one
        # surfacing, and two gliders surfacing at once would otherwise
        # braid their GPS fixes into a single event.  SurfacingStream
        # also carries the de-duplication sfmc-follow has always had --
        # an earlier version of this class re-implemented the parser
        # feeding and simply omitted it, so a surfacing replayed after a
        # reconnect reached the follower twice.
        self._streams: dict[str, SurfacingStream] = {}

    def set_notifier(self, notifier: DisconnectNotifier | None) -> None:
        """Pass the operator-email notifier through to the follower.

        sfmc-follow has always wired this; without it every
        :meth:`BaseFollower.notify` is a silent no-op, and a follower
        that alerts an operator when its external feed goes quiet simply
        stops alerting anyone.
        """
        self.follower.set_notifier(notifier)

    def on_start(self) -> None:
        self.log("following %s", ", ".join(self.gliders) or "no gliders yet")

    def on_stop(self) -> None:
        # Whatever a partial surfacing has accumulated is worth one last
        # look before shutdown.
        for glider in list(self._streams):
            self._flush(glider)

    def on_event(self, event: Event) -> None:
        match event.source:
            case "dialog":
                surfacing = self._stream(event.glider).feed(event.body)
                if surfacing is not None:
                    self._deliver(surfacing, event.glider)
            case "stream" if event.body.state == "disconnected":
                # She dove: the surfacing is over, so anything the
                # parser is still holding is complete.
                self._flush(event.glider)
            case "error" if event.tag == "upload":
                self.log("%s: upload failed: %s", event.glider, event.body)

    def _stream(self, glider: str) -> SurfacingStream:
        stream = self._streams.get(glider)
        if stream is None:
            stream = SurfacingStream(
                on_duplicate=lambda identity, g=glider: self.log(  # type: ignore[misc]
                    "%s: duplicate surfacing suppressed: %s", g, identity
                )
            )
            self._streams[glider] = stream
        return stream

    def _flush(self, glider: str) -> None:
        stream = self._streams.get(glider)
        if stream is None:
            return
        surfacing = stream.flush()
        if surfacing is not None:
            self._deliver(surfacing, glider)
        stream.reset()

    def _deliver(self, surfacing: SurfacingEvent, glider: str) -> None:
        """Hand one surfacing to the follower, then send what it queued."""
        self.follower.current_glider = glider
        try:
            self.follower.on_surfacing(surfacing)
        except Exception:
            # Same policy the follower's own loop has always had: one
            # bad surfacing must not end a deployment.
            logger.exception("%s: error processing surfacing for %s", type(self).__name__, glider)
        finally:
            self.follower.current_glider = None
        self._drain_uploads(glider)

    def _drain_uploads(self, default_glider: str) -> None:
        """Turn queued files into upload requests.

        Uploads go through :meth:`~BaseControlEngine.request`, so they
        inherit every rail the engine already has: refused without
        ``allow_writes``, simulated under ``dry_run``, serialised per
        glider, capped fleet-wide, and audited.
        """
        while True:
            try:
                batch = self._queue_out.get_nowait()
            except Empty:
                return
            if batch is None:
                return
            if isinstance(batch, UploadBatch):
                folders, target = batch.folders, batch.glider or default_glider
            else:
                folders, target = batch, default_glider
            for folder, files in folders.items():
                if not files:
                    continue
                self.request(
                    "upload_glider_file_contents",
                    target,
                    folder,
                    files,
                    glider=target,
                    tag="upload",
                )


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
