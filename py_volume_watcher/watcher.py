from __future__ import annotations

from pathlib import Path
from logging import getLogger
from queue import Empty, Queue
from threading import Event

from ._observer import Observer


logger = getLogger("py_volume_watcher")

class FileWatcher:
    """Watches a directory for file changes matching a specific pattern.

    This class uses polling to detect new or modified files matching a wildcard
    pattern inside a base directory. It is designed to work well with Docker
    volumes where event-based mechanisms (like inotify) may not function reliably.

    Attributes:
        queue (Queue[Path]): A queue containing paths to changed or new files.
        stop_event (Event): Threading event to signal stopping the watcher thread.
        observer (Observer): Background thread responsible for scanning files.
        timeout (float): Timeout in seconds for joining the observer thread on exit.
    """

    def __init__(
        self,
        base: str | Path,
        pattern: str = "*",
        *,
        polling_interval_sec: float | int = 0.5,
        on_exit_timeout: float | int = 1.0
    ) -> None:
        """Initializes the file watcher.

        Args:
            base (str | Path): The base directory to monitor.
            pattern (str): Glob pattern to match files (e.g., "*.txt").
            polling_interval_sec (float | int): How often to scan for changes (in seconds).
            on_exit_timeout (float | int): Max time to wait for clean shutdown (in seconds).
        """
        self.queue: Queue[Path] = Queue()
        self.stop_event = Event()
        self.observer = Observer(base, pattern, self.queue, polling_interval_sec, self.stop_event)
        self.timeout = on_exit_timeout

    def __enter__(self):
        """Starts the observer thread.

        Returns:
            FileWatcher: The watcher instance itself.
        """
        self.observer.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Signals the observer thread to stop and waits for shutdown.

        Args:
            exc_type: Exception type, if any.
            exc_val: Exception value, if any.
            exc_tb: Exception traceback, if any.
        """
        self.stop_event.set()
        self.observer.join(timeout=self.timeout)

    def __iter__(self):
        """Returns the iterator itself.

        Returns:
            FileWatcher: The watcher instance as an iterator.
        """
        return self

    def __next__(self) -> Path:
        """Retrieves the next file path from the queue when a change is detected.

        Returns:
            Path: The path to the new or changed file.

        Raises:
            StopIteration: If the watcher has been stopped.
        """
        try:
            return self.queue.get(timeout=0.5)
        except Empty:
            if self.stop_event.is_set():
                raise StopIteration
            return self.__next__()
