import time
import pytest
from pathlib import Path
from py_volume_watcher import FileWatcher


def test_new_file_triggers_event(tmp_path: Path):
    watched_dir = tmp_path / "watched"
    watched_dir.mkdir()

    test_file = watched_dir / "test.txt"

    with FileWatcher(watched_dir, pattern="*.txt", polling_interval_sec=0.1) as watcher:
        # Write a file to trigger the watcher
        test_file.write_text("hello")
        
        # Wait and get event
        for file in watcher:
            assert file == test_file
            break


def test_file_change_triggers_event(tmp_path: Path):
    watched_dir = tmp_path / "watched"
    watched_dir.mkdir()

    test_file = watched_dir / "test.txt"
    test_file.write_text("initial")

    with FileWatcher(watched_dir, pattern="*.txt", polling_interval_sec=0.1) as watcher:
        # Drain first "new file" event
        _ = next(watcher)

        # Modify file to trigger change detection
        test_file.write_text("updated")

        for file in watcher:
            assert file == test_file
            break


def test_ignore_unrelated_files(tmp_path: Path):
    watched_dir = tmp_path / "watched"
    watched_dir.mkdir()

    unrelated_file = watched_dir / "not_watched.log"

    with FileWatcher(watched_dir, pattern="*.txt", polling_interval_sec=0.1) as watcher:
        unrelated_file.write_text("something")
        
        try:
            file = watcher.queue.get(timeout=0.3)
            pytest.fail(f"Unexpected file detected: {file}")
        except Exception:
            pass  # expected, nothing in the queue