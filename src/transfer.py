import sys
import globus_sdk

import util
import util_constants as c

import logging
import logger_config

transfer_logger = logger_config.setup_logging(
    module_name=__name__,
    level=logging.DEBUG,
    logfile_dir=c.LOGFILE_DIR,
)


def get_task_stats(
        task_id,
        authorizer=None,
        transfer_client=None,
):
    # Error handling
    if authorizer is None and transfer_client is None:
        raise ValueError(
            "At least one of `transfer_client` or `authorizer` must be defined.\
            `transfer_client` takes precedence if both are defined."
        )
    
    # Create Transfer Client of not passed in arguments
    if transfer_client is None:
        transfer_client = globus_sdk.TransferClient(authorizer=authorizer)
    
    # Get task details
    task_details = transfer_client.get_task(task_id)

    return task_details


def transfer_data(
    src_endpoint_uuid,
    dst_endpoint_uuid,
    src_path,
    dst_path,
    authorizer=None,
    transfer_client=None,
    exclude_pattern=None,
    exclude_files=None,
    label=None,
):
    # Exception Handling
    # -------------------------------------------------
    if authorizer is None and transfer_client is None:
        raise ValueError(
            "At least one of `authorizer` or `transfer_client` must be defined.\
            `transfer_client` takes precedence if both are defined."
        )

    transfer_logger.info("Initiating transfer...")

    if transfer_client is None:
        transfer_client = globus_sdk.TransferClient(authorizer=authorizer)

    try:
        # Execute current task
        transfer_data = globus_sdk.TransferData(
            transfer_client, 
            src_endpoint_uuid, 
            dst_endpoint_uuid,
            label=label,
        )

        if exclude_pattern:
            # Transfer all files in the src_path directory except the ones that match the exclude_pattern

            transfer_logger.info("Transferring pattern-filtered files...")
            transfer_data.add_filter_rule(
                name=exclude_pattern,
                method="exclude",
                type="file",
            )
            transfer_data.add_item(src_path, dst_path)

        elif exclude_files:
            # Transfer all files in the src_path directory except the ones in exclude_files
            transfer_logger.info("Transferring files not in exclude list...")
            ls_data = transfer_client.operation_ls(
                endpoint_id=src_endpoint_uuid,
                path=src_path,
            )
            transfer_files = [ls_entry['name'] for ls_entry in ls_data if ls_entry['name'] not in exclude_files]    # TODO - optimize this using set difference
            transfer_logger.info(f"Number of files to transfer: {len(transfer_files)}")
            transfer_logger.debug(f"{transfer_files = }")

            for transfer_file in transfer_files:
                src_file_path = f"{src_path}/{transfer_file}"
                transfer_logger.debug(f"{src_file_path = }")
                
                dst_file_path = f"{dst_path}/{transfer_file}"
                transfer_logger.debug(f"{dst_file_path = }")

                transfer_data.add_item(src_file_path, dst_file_path)

        else:
            # Transfer all files in the src_path directory
            transfer_logger.info("Transferring all files at src...")
            transfer_data.add_item(src_path, dst_path)

        # Submitting the transfer task (if needed)
        if len(transfer_data['DATA']) > 0:
            transfer_result = transfer_client.submit_transfer(transfer_data)
            transfer_logger.debug(f"Transfer Task submitted! Task ID: {transfer_result['task_id']}")
        else:
            transfer_result = None
            transfer_logger.debug(f"No new files to transfer! Terminating the process.")

    except Exception as e:
        transfer_logger.debug(f"Ran into exception - {e}")
        raise e
    
    return transfer_result


def cancel_transfer(
        task_id,
        authorizer=None,
        transfer_client=None,
):
    # Exception Handling
    # -------------------------------------------------
    if authorizer is None and transfer_client is None:
        raise ValueError(
            "At least one of `authorizer` or `transfer_client` must be defined.\
            `transfer_client` takes precedence if both are defined."
        )

    transfer_logger.info(f"Initiating task cancellation for Task ID = {task_id}...")

    if transfer_client is None:
        transfer_client = globus_sdk.TransferClient(authorizer=authorizer)
    
    try:
        ctask_result = transfer_client.cancel_task(task_id)
    
    except Exception as e:
        transfer_logger.debug(f"Ran into exception while requesting Task Cancellation - {e}")
        ctask_result = None    # FUTURE TODO (for gracefully handling exception)
        raise e

    return ctask_result
