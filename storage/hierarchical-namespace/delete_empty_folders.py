# Copyright 2025 Google LLC.
# Licensed under the Apache License, Version 2.0.

import concurrent.futures
import logging
import threading
import time

from google.api_core import exceptions as google_exceptions
from google.api_core import retry
from google.cloud import storage  # For object operations
from google.cloud import storage_control_v2 # For folder operations
import grpc

ThreadPoolExecutor = concurrent.futures.ThreadPoolExecutor

# --- Configuration ---
BUCKET_NAME = "your-gcs-bucket-name"
FOLDER_PREFIX = "chain_103/" # Must end with '/'
MAX_WORKERS = 100
STATS_REPORT_INTERVAL = 5
DELETE_OBJECTS_FIRST = True  # Set to True to clear all files before folders

# --- Data Structures & Globals ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(threadName)s - %(message)s"
)

folders_by_depth = {}
stats = {
    "found_total": 0,
    "successful_deletes": 0,
    "failed_deletes_precondition": 0,
    "failed_deletes_internal": 0,
}
stats_lock = threading.Lock()

storage_control_client = storage_control_v2.StorageControlClient()
storage_client = storage.Client()

# --- Object Deletion Logic (Batching) ---

def delete_batch_of_blobs(blob_names_chunk: list):
    """Deletes a chunk of up to 100 blobs using a single batch request."""
    try:
        with storage_client.batch():
            bucket = storage_client.bucket(BUCKET_NAME)
            for name in blob_names_chunk:
                bucket.delete_blob(name)
        return len(blob_names_chunk)
    except Exception as e:
        logging.error(f"Batch deletion failed for a chunk: {e}")
        return 0

def clear_all_objects_batched():
    """Discovers and deletes all objects under FOLDER_PREFIX in parallel batches."""
    logging.info(f"Scanning for objects to delete in bucket '{BUCKET_NAME}'...")
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=FOLDER_PREFIX)
    blob_names = [blob.name for blob in blobs if not blob.name.endswith('/')]

    if not blob_names:
        logging.info("No objects found to delete.")
        return

    chunk_size = 100
    chunks = [blob_names[i:i + chunk_size] for i in range(0, len(blob_names), chunk_size)]
    
    logging.info(f"Found {len(blob_names)} objects. Executing {len(chunks)} batch requests...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(delete_batch_of_blobs, chunks))
    
    logging.info(f"Object deletion complete. Total deleted: {sum(results)}")

# --- Folder Deletion Logic ---

def _get_simple_path_and_depth(full_resource_name: str) -> tuple[str, int]:
    base_folders_prefix = f"projects/_/buckets/{BUCKET_NAME}/folders/"
    expected_validation_prefix = base_folders_prefix + FOLDER_PREFIX

    if not full_resource_name.startswith(expected_validation_prefix) or not full_resource_name.endswith("/"):
        raise ValueError(f"Invalid folder resource name: {full_resource_name}")

    simple_path = full_resource_name[len(base_folders_prefix) :]
    depth = simple_path.count("/")
    return simple_path, depth

def discover_and_partition_folders():
    parent_resource = f"projects/_/buckets/{BUCKET_NAME}"
    list_folders_request = storage_control_v2.ListFoldersRequest(
        parent=parent_resource, prefix=FOLDER_PREFIX
    )

    num_folders_found = 0
    try:
        for folder in storage_control_client.list_folders(request=list_folders_request):
            full_resource_name = folder.name
            _, depth = _get_simple_path_and_depth(full_resource_name)

            if depth not in folders_by_depth:
                folders_by_depth[depth] = set()
            folders_by_depth[depth].add(full_resource_name)

            num_folders_found += 1
            with stats_lock:
                stats["found_total"] = num_folders_found
    except Exception as e:
        logging.error("Failed to list folders: %s", e, exc_info=True)

def should_retry(exception):
    if not isinstance(exception, (google_exceptions.GoogleAPICallError, grpc.RpcError)):
        return False
    retryable_grpc_codes = [
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.UNKNOWN,
    ]
    status_code = exception.code if isinstance(exception, google_exceptions.GoogleAPICallError) else exception.code()
    return status_code in retryable_grpc_codes

def delete_folder(folder_full_resource_name: str):
    simple_path, _ = _get_simple_path_and_depth(folder_full_resource_name)
    retry_policy = retry.Retry(predicate=should_retry, initial=1.0, maximum=60.0, multiplier=2.0, deadline=120.0)

    try:
        request = storage_control_v2.DeleteFolderRequest(name=folder_full_resource_name)
        storage_control_client.delete_folder(request=request, retry=retry_policy)
        with stats_lock:
            stats["successful_deletes"] += 1
    except google_exceptions.NotFound:
        return
    except google_exceptions.FailedPrecondition as e:
        logging.warning("Folder '%s' not empty: %s", simple_path, e.message)
        with stats_lock:
            stats["failed_deletes_precondition"] += 1
    except Exception as e:
        logging.error("Failed to delete '%s': %s", simple_path, e)
        with stats_lock:
            stats["failed_deletes_internal"] += 1

# --- Stats Reporting ---

def stats_reporter_thread_logic(stop_event: threading.Event, start_time: float):
    while not stop_event.wait(STATS_REPORT_INTERVAL):
        with stats_lock:
            elapsed = time.time() - start_time
            rate = stats["successful_deletes"] / elapsed if elapsed > 0 else 0
            logging.info(f"[STATS] Found: {stats['found_total']} | Success: {stats['successful_deletes']} | Rate: {rate:.2f} f/s")

# --- Main Execution ---

if __name__ == "__main__":
    if BUCKET_NAME == "your-gcs-bucket-name":
        print("ERROR: Update BUCKET_NAME first."); exit(1)
    if FOLDER_PREFIX and not FOLDER_PREFIX.endswith("/"):
        print("ERROR: FOLDER_PREFIX must end with '/'."); exit(1)

    start_time = time.time()
    stop_event = threading.Event()
    
    stats_thread = threading.Thread(target=stats_reporter_thread_logic, args=(stop_event, start_time), daemon=True)
    stats_thread.start()

    # Step 0: Object Deletion
    if DELETE_OBJECTS_FIRST:
        clear_all_objects_batched()

    # Step 1: Folder Discovery
    discover_and_partition_folders()

    if not folders_by_depth:
        logging.info("No folders found. Exiting."); exit(0)

    # Step 2: Recursive Folder Deletion
    deletion_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="DeleteWorker")
    try:
        for current_depth in sorted(folders_by_depth.keys(), reverse=True):
            folders = folders_by_depth[current_depth]
            logging.info(f"Depth {current_depth}: Deleting {len(folders)} folders...")
            futures = [deletion_executor.submit(delete_folder, f) for f in folders]
            concurrent.futures.wait(futures)
    except KeyboardInterrupt:
        logging.info("Interrupt received.")
    finally:
        stop_event.set()
        deletion_executor.shutdown(wait=True)
        logging.info("Execution finished.")
