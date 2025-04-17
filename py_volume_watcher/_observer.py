from __future__ import annotations

import hashlib
from pathlib import Path
from logging import getLogger
from queue import Queue
from threading import Thread
import time
from threading import Event


logger = getLogger("py_volume_watcher")


def sha256sum(file: Path) -> str:
    """Computes the SHA-256 hash of a file.

    Reads the file in chunks using a memoryview to reduce memory overhead,
    and returns the SHA-256 hexadecimal digest of the file contents.

    Args:
        file: Path or file-like object pointing to the file to hash.

    Returns:
        str: The hexadecimal SHA-256 digest of the file.
    """
    h = hashlib.sha256()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(file, "rb", buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()


class Observer(Thread):
    """Background thread that watches for file additions and changes.

    Scans a directory on a fixed interval and detects new or modified files
    matching a specified glob pattern. Uses SHA-256 hashes to determine if
    a file has changed.

    Attributes:
        base (Path): The directory to monitor.
        pattern (str): Glob pattern to match files.
        queue (Queue): Queue to which changed or new file paths are added.
        polling_interval_sec (float | int): Scan interval in seconds.
        stack (dict[Path, str]): Cache of seen files and their last known hash.
        seen_last_check (set[Path]): Files seen in the current scan iteration.
        stop_event (Event): Event used to signal the thread to stop.
    """

    def __init__(
        self,
        base: str | Path,
        pattern: str,
        queue: Queue,
        polling_interval_sec: float | int,
        stop_event: Event,
    ) -> None:
        """Initializes the observer thread.

        Args:
            base (str | Path): Directory to monitor.
            pattern (str): File glob pattern (e.g., "*.json").
            queue (Queue): Queue where detected file events are pushed.
            polling_interval_sec (float | int): Time in seconds between scans.
            stop_event (Event): Threading event to signal stopping.
        """
        super().__init__(daemon=True)
        self.base = Path(base)
        self.pattern = pattern
        self.queue = queue
        self.polling_interval_sec = polling_interval_sec
        self.stack: dict[Path, str] = {}
        self.seen_last_check: set[Path] = set()
        self.stop_event = stop_event

    def run(self) -> None:
        """Main loop of the observer thread.

        Periodically scans the directory and pushes new or changed files
        matching the pattern to the queue. Exits when the stop_event is set.
        """
        while not self.stop_event.is_set():
            to_delete = []

            for file in self.stack.keys():
                if file not in self.seen_last_check:
                    logger.debug(f"File removed: {file}")
                    to_delete.append(file)

            for event in to_delete:
                del self.stack[event]

            self.seen_last_check.clear()

            for file in self.base.glob(self.pattern):
                if file.is_file():
                    current_sha256 = sha256sum(file)
                    self.seen_last_check.add(file)
                    if file in self.stack:
                        if current_sha256 != self.stack[file]:
                            logger.debug(f"File changed: {file}")
                            self.stack[file] = current_sha256
                            self.queue.put(file)
                    else:
                        logger.debug(f"New file detected: {file}")
                        self.stack[file] = current_sha256
                        self.queue.put(file)

            time.sleep(self.polling_interval_sec)
