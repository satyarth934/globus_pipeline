import os
import sys
import globus_sdk
from tqdm import tqdm

import util
import util_constants as c
import transfer
import computing as cmp

import logging
import logger_config

exp_logger = logger_config.setup_logging(
    module_name=__name__,
    level=logging.DEBUG,
    logfile_dir=c.LOGFILE_DIR,
)


def export_data(
        src_endpoint_uuid,
        dst_endpoint_uuid,
        src_path,
        dst_path,
        authorizer,
        check_prev_task=True,
        label=None,
):
    exp_logger.info("Initiating import transfer...")

    # Initializing TransferClient
    # -------------------------------------------------
    transfer_client = globus_sdk.TransferClient(authorizer=authorizer)

    # Check previous task status
    # -------------------------------------------------
    if check_prev_task:
        if os.path.exists(c.COMPUTE_FLAG_FILE):
            exp_logger.debug("Terminating the process to wait for compute process to finish.")
            sys.exit("Waiting for compute process to complete!")

        # Get the number of new files processed in the last COMPUTE run
        processed_filepaths = cmp.get_processed_filepaths()
        exp_logger.info(f"{len(processed_filepaths)} files processed in the last COMPUTE run.")
        
    # Transferring data
    # -------------------------------------------------
    # Check if there are any files to export
    files_to_export = os.listdir(src_path)
    if len(files_to_export) <= 0:
        exp_logger.debug("No new files to export. Terminating process!")
        exp_result = None
    else:
        exp_result = transfer.transfer_data(
            src_endpoint_uuid=src_endpoint_uuid,
            dst_endpoint_uuid=dst_endpoint_uuid,
            src_path=src_path,
            dst_path=dst_path,
            transfer_client=transfer_client,
            label=label,
        )

    return exp_result


def export_cleanup(
        dir_path,
        authorizer, 
        prev_task_id=True,
):
    exp_logger.info("Initiating export cleanup...")

    # Check previous task status
    # -------------------------------------------------
    if prev_task_id is not None:
        # Initializing TransferClient
        transfer_client = globus_sdk.TransferClient(authorizer=authorizer)

        # prev_task_details = transfer_client.get_task(prev_task_id)
        prev_task_details = transfer.get_task_stats(
            task_id=prev_task_id, 
            transfer_client=transfer_client,
        )
        exp_logger.info(f"Prev Task ID: {prev_task_id} has status: {prev_task_details['status']}")
        exp_logger.info(f"Prev Task details: {prev_task_details}")

        if prev_task_details['status'] in ['ACTIVE', 'FAILED']:
            # close the program and wait for the next cronjob call hoping that the function would be completed by then.
            msg = f"Prev Task is still ACTIVE. Terminating the current run to wait for previous process to complete."
            exp_logger.warning(msg)
            sys.exit(msg)
            
        elif prev_task_details['status'] == 'SUCCEEDED':
            # continue with the program.
            pass
    
    # Delete the processed files in compute destination
    # -------------------------------------------------
    exp_logger.debug("Deleting the processed files in compute dst")
    try:
        util.touch(path=c.EXPORT_CLEANUP_FLAG_FILE)

        with os.scandir(dir_path) as path_files:
            for path_file in path_files:
                if path_file.is_file():
                    util.delete_file(path_file.path, sleep_time=0)
        
        exp_logger.debug("Export cleanup complete.")
        
    except Exception as e:
        raise e
    
    finally:
        util.delete_file(c.EXPORT_CLEANUP_FLAG_FILE, sleep_time=2)
        exp_logger.debug("Deleted the export cleanup flag file.")
    