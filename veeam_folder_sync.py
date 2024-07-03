import schedule
import os
import hashlib
import argparse
import logging
from shutil import rmtree, copy2, copystat, move
from time import sleep
from watchdog.events import FileSystemEventHandler, FileSystemEvent
from watchdog.observers import Observer
from threading import Timer
from pathlib import Path

# Init parser for CLI args
parser = argparse.ArgumentParser(
    prog='top',
    description='Show top lines from each file')
parser.add_argument('-s', '--source_path', type=str, default='')
parser.add_argument('-d', '--destination_path', type=str, default='')
parser.add_argument('-si', '--sync_interval', type=int, default=10)
parser.add_argument('-l', '--log_file', type=str, default='')

# Init logger with agnostic settings. Settings related to log_file parameter are instantiated after the args are parsed
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

class SyncHandler(FileSystemEventHandler):
    def __init__(self, folder_to_monitor, folder_to_sync, observer):
        self.folder_to_monitor = folder_to_monitor
        self.folder_to_sync = folder_to_sync
        self.observer = observer

    def join_paths(self, event: FileSystemEvent) -> os.path:
        if event.event_type == "moved":  
            src_rel_path = os.path.relpath(event.src_path, self.folder_to_monitor)
            dest_rel_path = os.path.relpath(event.dest_path, self.folder_to_monitor)
            return (os.path.join(self.folder_to_sync, src_rel_path),
                    os.path.join(self.folder_to_sync, dest_rel_path))
        else: 
            rel_path = os.path.relpath(event.src_path, self.folder_to_monitor)
            return os.path.join(self.folder_to_sync, rel_path)

    def on_created(self, event: FileSystemEvent) -> None:
        try:
            dst_path = self.join_paths(event)
            if not os.path.exists(dst_path): 
                if event.is_directory:
                    sync_folder(event.src_path, dst_path)
                    return
                copy_wrapper(event.src_path, dst_path)
            else:
                logger.info(f"SKIP: file {event.src_path} already present in {dst_path}")
                return
        except FileNotFoundError:
            logging.error(f"on_created: File {event.src_path} not found")

    def on_deleted(self, event: FileSystemEvent) -> None:
        try:
            dst_path = self.join_paths(event)
            # event.is_directory works strangely here. Even tho the event is triggered when a folder is deleted, a file event handler is passed
            if os.path.isdir(dst_path):
                logger.info(f"DELETE: Folder in {event.src_path} was deleted, updating synced folder {dst_path}")
                rmtree(dst_path)
            else:
                logger.info(f"DELETE: File in {event.src_path} was deleted, updating synced folder {dst_path}")
                os.remove(dst_path)
        except FileNotFoundError:
            logging.error(f"on_deleted: File {event.src_path} not found")
        
    def on_moved(self, event: FileSystemEvent) -> None:
        try:
            old_dst_path, new_dst_path = self.join_paths(event)
            if not os.path.exists(new_dst_path): 
                if event.is_directory:
                    os.rename(old_dst_path, new_dst_path)
                    logger.info(f"MODIFY: Folder renamed from {old_dst_path} to {new_dst_path}")
                    return

                move(old_dst_path, new_dst_path)
                logger.info(f"MODIFY: File moved from {old_dst_path} to {new_dst_path}")

        except FileNotFoundError:
            logging.error(f"on_created: File {event.src_path} not found")

def checksum(filename: str) -> bytes:
    hash_obj = hashlib.md5()
    file_size = os.path.getsize(filename)
    # processed_size = 0
    # last_percent = 0
    chunk_size = 8192

    with open(filename, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hash_obj.update(chunk)
    return hash_obj.hexdigest()

def is_same_file(src: str, dst: str):
    if not os.path.exists(dst):
        return False

    src_stat = os.stat(src)
    dst_stat = os.stat(dst)
    if src_stat.st_size != dst_stat.st_size or src_stat.st_mtime != dst_stat.st_mtime:
        logging.info("STAT")
        return False

    return checksum(src) == checksum(dst)

def copy_wrapper(src: str, dst: str) -> None:
    if os.path.exists(dst):
        logger.info(f"SKIP: file {src} already present in {dst}")
        return

    if is_same_file(src, dst):
        logger.info(f"SKIP: file {src} already present in {dst}")
        return

    logger.info(f"COPY: file: {src} to {dst}")
    copy2(src, dst)
    copystat(src, dst)

def sync_folder(src: str, dst: str) -> None:
    if not os.path.exists(dst):
        logger.info(f"CREATE: new directory: {dst}")
        os.makedirs(dst)
    else:
        logger.info(f"SKIP: skipping creation because {dst} already exists")

    try:
        items = os.listdir(src)
        for item in items:
            src_path =  os.path.join(src, item)
            dst_path = os.path.join(dst, item)
            if os.path.isdir(src_path):
                sync_folder(src_path, dst_path)
            else:
                copy_wrapper(src_path, dst_path)

    except FileNotFoundError:
        pass

if __name__ == "__main__":

    args = parser.parse_args()
    original_src = args.source_path
    original_dst = args.destination_path
    log_file = args.log_file
    sync_interval = args.sync_interval
    src_path = Path(original_src)
    # Get the last segment of the src path to append to dst to recreate an exact copy of the src folder, inside dst.
    # So, if src = D:\original_folder_name and dst = D:\copy, the synced folder will be at path D:\copy\original_folder_name
    dst_path = Path(original_dst).joinpath(src_path.parts[-1])

    # More logger config based on log_file
    file_handler= logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if original_src == original_dst:
        logger.error(f"Same path passed in arguments: {original_src} == {original_dst}")
    elif src_path == dst_path:
        logger.error(f"Destination folder will be inside source folder: {src_path} == {dst_path}")
    else:
        observer = Observer()
        event_handler = SyncHandler(src_path, dst_path, observer)
        observer.schedule(event_handler, src_path, recursive=True)
        observer.start()
        sync_folder(src_path, dst_path)
        schedule.every(sync_interval).seconds.do(sync_folder, src=src_path, dst=dst_path)
        try:
            while True:
                schedule.run_pending()
                pass
        except KeyboardInterrupt:
            observer.stop()
        observer.join()