import schedule
import os
import hashlib
import argparse
import logging
from shutil import copy2, copystat, move
from watchdog.events import FileSystemEventHandler, FileSystemEvent
from watchdog.observers import Observer
from threading import Thread
from time import sleep
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Init parser for CLI args
parser = argparse.ArgumentParser(
    prog='top',
    description='Show top lines from each file')
parser.add_argument('-s', '--source_path', type=str, default='')
parser.add_argument('-d', '--destination_path', type=str, default='')
parser.add_argument('-si', '--sync_interval', type=int, default=10)
parser.add_argument('-l', '--log_file', type=str, default='')

# Init observer
observer = Observer()

# Init logger with agnostic settings. Settings related to log_file parameter are instantiated after the args are parsed
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

class SyncHandler(FileSystemEventHandler):
    def __init__(self, folder_to_monitor, folder_to_sync):
        self.folder_to_monitor = folder_to_monitor
        self.folder_to_sync = folder_to_sync

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
                else:
                    copy_wrapper(event.src_path, dst_path)
        except FileNotFoundError:
            logger.error(f"on_created: File {event.src_path} not found")
        except PermissionError:
            self.on_created(event)


    def on_deleted(self, event: FileSystemEvent) -> None:
        try:
            dst_path = self.join_paths(event)
            # event.is_directory works strangely here. Even tho the event is triggered when a folder is deleted, a file event handler is passed
            if os.path.isdir(dst_path):
                delete_folder(dst_path)
            else:
                delete_file(dst_path)
        except FileNotFoundError:
            logger.error(f"on_deleted: File {event.src_path} not found")
        except PermissionError:
            self.on_deleted(event)

        
    def on_moved(self, event: FileSystemEvent) -> None:
        try:
            old_dst_path, new_dst_path = self.join_paths(event)
            if not os.path.exists(new_dst_path): 
                if event.is_directory:
                    os.rename(old_dst_path, new_dst_path)
                    logger.info(f"MODIFY: Folder renamed from {old_dst_path} to {new_dst_path}")
                    return
                else:
                    move(old_dst_path, new_dst_path)
                    logger.info(f"MODIFY: File moved from {old_dst_path} to {new_dst_path}")

        except FileNotFoundError:
            logger.error(f"on_moved: File {event.src_path} not found")
        except PermissionError:
            self.on_moved(event)


def delete_file(file_path):
    try:
        os.unlink(file_path)
        logger.info(f"DELETE: deleting file {file_path}")
    except Exception as e:
        logger.error(f"Error deleting file {file_path}: {e}")


def delete_folder(path_to_delete: str) -> None:
    try:
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for root, dirs, files in os.walk(path_to_delete, topdown=False):
                for name in files:
                    file_path = os.path.join(root, name)
                    if os.path.exists(file_path):
                        futures.append(executor.submit(delete_file, file_path))
            
                for future in futures:
                    future.result()
                
                for name in dirs:
                    dst_path = os.path.join(root, name)
                    try:
                        os.rmdir(dst_path)
                        logger.info(f"DELETE: deleting directory {dst_path}")
                    except Exception as e:
                        logger.error(f"Error deleting directory {dst_path}: {e}")
                
            os.rmdir(path_to_delete)

    except Exception as e:
        logger.error(f"Error deleting folder {path_to_delete}: {e}")


def file_checksum(filename: str) -> bytes:
    hash_obj = hashlib.md5()
    chunk_size = 8192

    with open(filename, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hash_obj.update(chunk)
    return hash_obj.hexdigest()


def dir_checksum(directory):
    hasher = hashlib.sha256()  # Or another algorithm of choice
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    hasher.update(chunk)
    return hasher.hexdigest()


def is_same_file(src: str, dst: str):
    if not os.path.exists(dst):
        return False

    src_stat = os.stat(src)
    dst_stat = os.stat(dst)
    if src_stat.st_size != dst_stat.st_size or src_stat.st_mtime != dst_stat.st_mtime:
        return False

    return file_checksum(src) == file_checksum(dst)

def copy_wrapper(src: str, dst: str) -> bool:
    if is_same_file(src, dst):
        return False

    logger.info(f"COPY: file: {src} to {dst}")
    copy2(src, dst)
    copystat(src, dst)
    return True

def sync_folder(src: str, dst: str) -> None:
    if dir_checksum(src) == dir_checksum(dst):
        logger.info(f"CREATE: Skipping creation because {src} and {dst} are already synced")
        return

    try:
        if not os.path.exists(dst):
            os.makedirs(dst)

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for root, dirs, files in os.walk(src):
                rel_path = os.path.relpath(root, src)
                dst_root = os.path.join(dst, rel_path)

                for name in dirs:
                    dst_path = os.path.join(dst_root, name)
                    if not os.path.exists(dst_path):
                        os.makedirs(dst_path)
                        logger.info(f"CREATE: directory {dst_path}")

                for name in files:
                    src_path = os.path.join(root, name)
                    dst_path = os.path.join(dst_root, name)
                    futures.append(executor.submit(copy_wrapper, src_path, dst_path))

            for future in futures:
                future.result()
    
    except Exception as e:
        logger.error(f"Error in sync_folder: {e}")

def run_periodic_task(sync_interval):
    while True:
        schedule.run_pending()
        sleep(sync_interval)

if __name__ == "__main__":

    try:
        args = parser.parse_args()
        src_path = args.source_path
        dst_path = args.destination_path
        dst_path = str(Path(args.destination_path).joinpath(Path(src_path).parts[-1]))
        log_file = args.log_file
        sync_interval = args.sync_interval

        file_handler= logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        if src_path == dst_path:
            logger.error(f"Same path passed in arguments: {src_path} == {dst_path}")
        else:
            event_handler = SyncHandler(src_path, dst_path)
            observer.schedule(event_handler, src_path, recursive=True)
            observer.start()
            sync_folder(src_path, dst_path)
            schedule.every(sync_interval).seconds.do(sync_folder, src=src_path, dst=dst_path)

            # Start a separate thread for running scheduled tasks
            periodic_task_thread = Thread(target=run_periodic_task, args=(sync_interval,))
            periodic_task_thread.daemon = True
            periodic_task_thread.start()
            while True:
                sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    finally:
        observer.join()