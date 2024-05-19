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


def transfer_data_old(
    src_endpoint_uuid,
    dst_endpoint_uuid,
    src_path,
    dst_path,
    authorizer,
    prev_task_id=None,
    delete_source_on_successful_transfer=False,
    label=None,
):
    transfer_logger.info("Initiating import transfer...")

    transfer_client = globus_sdk.TransferClient(authorizer=authorizer)

    # Check previous task status
    if prev_task_id is not None:
        # prev_task_details = transfer_client.get_task(prev_task_id)
        prev_task_details = get_task_stats(
            task_id=prev_task_id, 
            transfer_client=transfer_client,
        )
        transfer_logger.info(f"Prev Task ID: {prev_task_id} has status: {prev_task_details['status']}")
        transfer_logger.info(f"Prev Task details: {prev_task_details}")

        if prev_task_details['status'] in ['ACTIVE', 'FAILED']:
            # close the program and wait for the next cronjob call hoping that the function would be completed by then.
            msg = f"Prev Task is still ACTIVE. Terminating the current run to wait for previous process to complete."
            transfer_logger.warning(msg)
            sys.exit(msg)
            
        elif prev_task_details['status'] == 'SUCCEEDED':
            # continue with the program.
            pass

    try:
        # Execute current task
        transfer_data = globus_sdk.TransferData(
            transfer_client, 
            src_endpoint_uuid, 
            dst_endpoint_uuid,
            label=label,
        )

        transfer_data.add_item(src_path, dst_path)

        # Enable deletion of source files after a successful transfer
        if delete_source_on_successful_transfer:
            transfer_logger.warning("CAUTION: Files are the source will be deleted after the transfer is complete!")
            transfer_data['preserve_timestamps'] = False    # TODO - facing issues. KeyError => Try 'preserve_timestamp'
            transfer_data['delete_source_on_successful_transfer'] = True

        transfer_result = transfer_client.submit_transfer(transfer_data)

        transfer_logger.info(f"Transfer Task submitted! Task ID: {transfer_result['task_id']}")

        # Updating the .last_state file
        transfer_logger.info(f"Updating the last state!")
        with open(c.LAST_STATE_RECORD_FILE, "w") as lsrf:
            lsrf.write(c.STATES.IMPORT)

        # Updating the .last_task_id file
        transfer_logger.info(f"Updating the last task ID!")
        with open(c.LAST_TASKID_RECORD_FILE, "w") as ltrf:
            ltrf.write(transfer_result['task_id'])

    except Exception as e:
        transfer_logger.debug(f"Ran into exception!!!")
        raise e
    
    finally:
        pass
    
    return transfer_result










def transfer_data(
    src_endpoint_uuid,
    dst_endpoint_uuid,
    src_path,
    dst_path,
    authorizer=None,
    transfer_client=None,
    delete_source_on_successful_transfer=False,
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

        transfer_data.add_item(src_path, dst_path)

        # # Enable deletion of source files after a successful transfer
        # if delete_source_on_successful_transfer:
        #     transfer_logger.warning("CAUTION: Files are the source will be deleted after the transfer is complete!")
        #     transfer_data['preserve_timestamp'] = False
        #     transfer_data['delete_source_on_successful_transfer'] = True    # TODO - facing issues. KeyError

        transfer_result = transfer_client.submit_transfer(transfer_data)

        transfer_logger.info(f"Transfer Task submitted! Task ID: {transfer_result['task_id']}")

    except Exception as e:
        transfer_logger.debug(f"Ran into exception - {e}")
        raise e
    
    return transfer_result


