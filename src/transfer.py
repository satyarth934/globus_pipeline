import sys
import globus_sdk

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
        transfer_client=None,
        authorizer=None,
):
    # Error handling
    if transfer_client is None and authorizer is None:
        raise ValueError("At least one of `transfer_client` or `authorizer` must be passed. `transfer_client` takes precedence if both are provided.")
    
    # Create Transfer Client of not passed in arguments
    if transfer_client is None and authorizer is not None:
        transfer_client = globus_sdk.TransferClient(authorizer=authorizer)
    
    # Get task details
    task_details = transfer_client.get_task(task_id)

    return task_details


def transfer_data(
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

    try:
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

            if prev_task_details['status'] == 'ACTIVE':
                # close the program and wait for the next cronjob call hoping that the function would be completed by then.
                msg = f"Prev Task is still ACTIVE. Terminating the current run to wait for previous process to complete."
                transfer_logger.warning(msg)
                sys.exit(msg)
                
            elif prev_task_details['status'] == 'SUCCEEDED':
                # continue with the program.
                pass
            elif prev_task_details['status'] == 'FAILED':
                # TODO - Run the previous task again. (modify the flags to initiate the previous run again.)
                # TODO - Check what flags to modify.
                # TODO - Maybe just terminating the program is good enough since no flags files are updated yet.
                pass

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
            transfer_data['preserve_timestamps'] = False    # TODO - facing issues. KeyError
            transfer_data['delete_source_on_successful_transfer'] = True

        transfer_result = transfer_client.submit_transfer(transfer_data)

        transfer_logger.info(f"Transfer Task submitted! Task ID: {transfer_result['task_id']}")

    except Exception as e:
        transfer_logger.debug(f"Ran into exception!!!")
        raise e
    
    finally:
        # Updating the .last_state file
        transfer_logger.info(f"Updating the last state!")
        with open(c.LAST_STATE_RECORD_FILE, "w") as lsrf:
            lsrf.write(c.STATES.IMPORT)

        # Updating the .last_task_id file
        transfer_logger.info(f"Updating the last task ID!")
        with open(c.LAST_TASKID_RECORD_FILE, "w") as ltrf:
            ltrf.write(transfer_result['task_id'])
    
    return transfer_result


