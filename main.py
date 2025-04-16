from py_volume_watcher import FileWatcher, FileEvent

with FileWatcher("demo/") as watcher:
    for file_event in watcher:
        print("Detected file event:", file_event)