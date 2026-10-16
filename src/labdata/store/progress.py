"""Progress lines for a long phase: items done, rate, errors by kind and a projected finish."""

from __future__ import annotations

import datetime as dt
import logging
import threading
import time
from collections import Counter
from types import TracebackType

#: Seconds between progress lines.
EVERY = 60.0


class Progress:
    """Logs a phase's progress every ``every`` seconds from a background thread, and at the end.

    Parameters
    ----------
    logger : logging.Logger
        Where the lines go, at INFO.
    what : str
        The plural noun counted, e.g. ``"experiments"``.
    total : int
        Items the phase will handle.
    every : float
        Seconds between lines.

    Examples
    --------
    >>> with Progress(logging.getLogger("x"), "experiments", 10) as progress:
    ...     progress.add(10)
    """

    def __init__(
        self, logger: logging.Logger, what: str, total: int, *, every: float = EVERY
    ) -> None:
        self.logger = logger
        self.what = what
        self.total = total
        self.done = 0
        self.errors: Counter[str] = Counter()
        self._every = every
        self._start = time.monotonic()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._tick, daemon=True)

    def __enter__(self) -> Progress:
        """Start the clock and the background logger."""
        self._start = time.monotonic()
        self._thread.start()
        return self

    def __exit__(
        self,
        kind: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Stop the background logger and log the last line."""
        self._stop.set()
        self._thread.join()
        self.log()

    def add(self, done: int, **errors: int) -> None:
        """Count items done and errors by kind."""
        with self._lock:
            self.done += done
            self.errors.update({k: v for k, v in errors.items() if v})

    def line(self) -> str:
        """Return the current progress line."""
        with self._lock:
            done, errors = self.done, dict(self.errors)
        elapsed = max(time.monotonic() - self._start, 1e-9)
        rate = done / elapsed
        share = f" ({done / self.total:.1%})" if self.total else ""
        text = f"{done:,}/{self.total:,} {self.what}{share}, {rate:,.1f}/s"
        text += ", errors: " + (
            ", ".join(f"{k} {v:,}" for k, v in sorted(errors.items())) or "none"
        )
        left = self.total - done
        if left <= 0:
            return text + ", finished"
        if rate <= 0:
            return text + ", finish unknown"
        eta = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=left / rate)
        return text + f", finish ~{eta:%Y-%m-%d %H:%M} UTC"

    def log(self) -> None:
        """Log the current progress line."""
        self.logger.info(self.line())

    def _tick(self) -> None:
        while not self._stop.wait(self._every):
            self.log()
