from __future__ import annotations

import hashlib
from pathlib import Path
from logging import getLogger
from queue import Empty, Queue
from threading import Thread
import time


logger = getLogger(__name__)
logger.setLevel("DEBUG")

class FileEvent:
    def __init__(self, filepath: Path) -> None:
        self.filepath = filepath
        self.seen = True

    def __repr__(self) -> str:
        return f"FileEvent(filepath={self.filepath})"
    
    def __hash__(self) -> int:
        return hash(self.filepath.suffix)
    
    def __eq__(self, other: FileEvent) -> bool:
        if not isinstance(other, FileEvent):
            raise TypeError(f"Cannot compare FileEvent with {type(other)}")
        return self.filepath == other.filepath
    
    def sha256sum(self) -> str:
        h  = hashlib.sha256()
        b  = bytearray(128*1024)
        mv = memoryview(b)
        with open(self.filepath, 'rb', buffering=0) as f:
            for n in iter(lambda : f.readinto(mv), 0):
                h.update(mv[:n])
        return h.hexdigest()


class Observer(Thread):
    def __init__(self, path: Path, queue: Queue, polling_interval_sec: float | int = 1.0) -> None:
        super().__init__(daemon=False)
        self.path = path
        self.queue = queue
        self.polling_interval_sec = polling_interval_sec
        self.stack: dict[FileEvent, str] = {}  # FileEvent with last seen sha256sum

    def run(self) -> None:
        while True:
            to_delete = []

            for event in self.stack.keys():
                if not event.seen:
                    logger.debug(f"File removed: {event}")
                    to_delete.append(event)
                else:
                    event.seen = False

            for event in to_delete:
                del self.stack[event]

            
            for file in self.path.glob('*'):
                if file.is_file():
                    event = FileEvent(file)
                    current_sha256 = event.sha256sum()
                    if event in self.stack:
                        if current_sha256 != self.stack[event]:
                            logger.debug(f"File changed: {event}")
                            self.stack[event] = current_sha256
                            self.queue.put(event)
                    else:
                        logger.debug(f"New file detected: {event}")
                        self.stack[event] = current_sha256
                        self.queue.put(event)
            time.sleep(self.polling_interval_sec)


class FileWatcher():
    def __init__(self, path: str | Path) -> None:
        self.path = FileWatcher._validate_path(path)
        self.queue: Queue[FileEvent] = Queue()
        self.observer = Observer(self.path, self.queue)

    @staticmethod
    def _validate_path(path) -> None:
        if not isinstance(path, (str, Path)):
            raise TypeError("Path must be a string or Path object.")
        
        if isinstance(path, str):
            path = Path(path)

        if not path.exists() or not path.is_dir():
            raise FileNotFoundError(f"Path does not exist or is not a folder: {path}")
        
        return path

    def __enter__(self):
        self.observer.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.observer.join(timeout=1)

    def __iter__(self):
        return self

    def __next__(self) -> FileEvent:
        try:
            return self.queue.get(block=True)
        except Empty:
            raise StopIteration