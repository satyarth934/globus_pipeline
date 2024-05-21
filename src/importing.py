import os
import sys
import globus_sdk

import util
import util_constants as c
import transfer

import logging
import logger_config

imp_logger = logger_config.setup_logging(
    module_name=__name__,
    level=logging.DEBUG,
    logfile_dir=c.LOGFILE_DIR,
)


def import_data(
        src_endpoint_uuid,
        dst_endpoint_uuid,
        src_path,
        dst_path,
        authorizer,
        prev_task_id=None,
        exclude_previously_imported=True,
        label=None,
):
    imp_logger.info("Initiating import transfer...")

    # Initializing TransferClient
    # -------------------------------------------------
    transfer_client = globus_sdk.TransferClient(authorizer=authorizer)

    # Check previous task status
    # -------------------------------------------------
    if prev_task_id is not None:
        # prev_task_details = transfer_client.get_task(prev_task_id)
        prev_task_details = transfer.get_task_stats(
            task_id=prev_task_id, 
            transfer_client=transfer_client,
        )
        imp_logger.info(f"Prev Task ID: {prev_task_id} has status: {prev_task_details['status']}")
        imp_logger.info(f"Prev Task details: {prev_task_details}")

        if prev_task_details['status'] in ['ACTIVE', 'FAILED']:
            # close the program and wait for the next cronjob call hoping that the function would be completed by then.
            msg = f"Prev Task is still ACTIVE. Terminating the current run to wait for previous process to complete."
            imp_logger.warning(msg)
            sys.exit(msg)
            
        elif prev_task_details['status'] == 'SUCCEEDED':
            # continue with the program.
            pass
    
    # Exclude previously imported files
    # -------------------------------------------------
    if exclude_previously_imported and \
        os.path.exists(c.ALL_PROCESSED_FILEPATHS_FILE):
        
        with open(c.ALL_PROCESSED_FILEPATHS_FILE, 'r') as fh:
            exclude_files = fh.readlines()
        exclude_files = [os.path.basename(f.strip()) for f in exclude_files]
    else:
        exclude_files = None

    imp_logger.debug(f"len(exclude_files) = {len(exclude_files) if exclude_files else 0}")
    # imp_logger.debug(f"{exclude_files = }")

    # Transferring data
    # -------------------------------------------------
    imp_result = transfer.transfer_data(
        src_endpoint_uuid=src_endpoint_uuid,
        dst_endpoint_uuid=dst_endpoint_uuid,
        src_path=src_path,
        dst_path=dst_path,
        transfer_client=transfer_client,
        exclude_files=exclude_files,
        label=label,
    )

    return imp_result