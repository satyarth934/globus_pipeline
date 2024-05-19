import os
import sys
import globus_sdk
from tqdm import tqdm

import util
import util_constants as c
import transfer

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
        delete_source_on_successful_transfer=False,
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

    # Transferring data
    # -------------------------------------------------
    exp_result = transfer.transfer_data(
        src_endpoint_uuid=src_endpoint_uuid,
        dst_endpoint_uuid=dst_endpoint_uuid,
        src_path=src_path,
        dst_path=dst_path,
        transfer_client=transfer_client,
        label=label,
    )

    # Updating the .last_state file
    util.update_last_state(c.STATES.EXPORT)

    # Updating the .last_task_id file
    util.update_last_taskid(exp_result['task_id'])

    # Delete all files in the src_path!!!
    # FIXIT: NOTE: ERROR: PROBLEM: SOLVE: TODO:
    # This does not work because the files are deleted as soon as the task is submitted. 
    # So the files are deleted even before the transfer task can take place!!! 
    # So I need to submit this as another globus task. 
    # This "state" can be "EXPORT_CLEANUP" and the "taskid" can be updated as well to make sure all the files are cleared before the next of tasks!!!
    if delete_source_on_successful_transfer:
        exp_logger.debug("Deleting the input files in compute src")
        exp_logger.warning("Assuming that the src_path for export exists on the same machine as the compute machine!")

        with os.scandir(src_path) as path_files:
            for path_file in path_files:
                # path_file_path = f"{src_path}/{path_file}"
                if path_file.is_file():
                    util.delete_file(path_file.path, sleep_time=0)

    return exp_result