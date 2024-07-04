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


MAX_WORKERS = 5


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
            breakpoint()
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

# Sync functions

def delete_file(file_path):
    try:
        os.unlink(file_path)
        logger.info(f"DELETE: deleting file {file_path}")
    except Exception as e:
        logger.error(f"Error deleting file {file_path}: {e}")


def delete_folder(path_to_delete: str) -> None:
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
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


def copy_wrapper(src: str, dst: str) -> bool:
    if is_same_file(src, dst):
        return False

    logger.info(f"COPY: file: {src} to {dst}")
    copy2(src, dst)
    copystat(src, dst)
    return True


def sync_folder(src: str, dst: str) -> None:
    try:
        if dir_checksum(src) == dir_checksum(dst):
            logger.info(f"CREATE: Skipping creation because {src} and {dst} are already synced")
            return
        if not os.path.exists(dst):
            os.makedirs(dst)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
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
        
        if dir_checksum(src) != dir_checksum(dst):
            logger.error("Checksum of directories is different")
            raise ValueError("Checksum of directories is different")

        logger.info(f"Synchronization between {src}:{dst} completed")
    
    except Exception as e:
        logger.error(f"Error in sync_folder: {e}")


# Helper functions

def file_checksum(filename: str) -> bytes:
    # After some tests, it appears md5 is faster for files, while sha256 is faster for directions. dir_checksum uses sha256 for that reason
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
    hash_obj = hashlib.sha256()
    chunk_size = 8192
    total_size = 0
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            total_size += os.path.getsize(file_path)
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    hash_obj.update(chunk)
    logger.info(f"CHECKSUM: {directory} : {total_size}")
    return hash_obj.hexdigest()


def is_same_file(src: str, dst: str):
    if not os.path.exists(dst):
        return False

    src_stat = os.stat(src)
    dst_stat = os.stat(dst)
    if src_stat.st_size != dst_stat.st_size or src_stat.st_mtime != dst_stat.st_mtime:
        return False

    return file_checksum(src) == file_checksum(dst)



def run_periodic_task(sync_interval):
    while True:
        schedule.run_pending()
        sleep(sync_interval)


def validate_paths(src_path, dst_path):
    if not os.path.exists(src_path):
        logger.error(f"Src path does not exist: {src_path}")
        return False

    if src_path == dst_path:
        logger.error(f"Same path passed in arguments: {src_path} == {dst_path}")
        return False

    return True


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog='folder_sync',
        description='Synchronize folders and monitor for changes')
    parser.add_argument('-s', '--source_path', type=str, required=True)
    parser.add_argument('-d', '--destination_path', type=str, required=True)
    parser.add_argument('-si', '--sync_interval', type=int, default=10)
    parser.add_argument('-l', '--log_file', type=str, default='')
    return parser.parse_args()


def init_observer(src_path: str, dst_path: str):
    global observer
    observer = Observer()
    event_handler = SyncHandler(src_path, dst_path)
    observer.schedule(event_handler, src_path, recursive=True)
    observer.start()


def init_logger(log_file):
    global logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler= logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

# Main function

def init_sync():
    args = parse_arguments()
    src_path = args.source_path
    #Get last segment of src_path so the script can create a new folder names exactly like src folder, inside dst folder
    dst_path = str(Path(args.destination_path).joinpath(Path(src_path).parts[-1]))

    init_logger(args.log_file)

    if not validate_paths(src_path, dst_path):
        return

    init_observer(src_path, dst_path)

    sync_folder(src_path, dst_path)
    schedule.every(args.sync_interval).seconds.do(sync_folder, src=src_path, dst=dst_path)

    periodic_task_thread = Thread(target=run_periodic_task, args=(args.sync_interval,))
    periodic_task_thread.daemon = True
    periodic_task_thread.start()

    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    finally:
        try:
            observer.join()
        except RuntimeError:
            pass  # In case the src_path doesn't exist


if __name__ == "__main__":
    init_sync()