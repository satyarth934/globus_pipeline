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


# def import_data(
#     src_endpoint_uuid,
#     dst_endpoint_uuid,
#     src_path,
#     dst_path,
#     authorizer,
#     prev_task_id=None,
# ):
#     imp_logger.info("Initiating import transfer...")

#     try:
#         transfer_client = globus_sdk.TransferClient(authorizer=authorizer)

#         # Check previous task status
#         if prev_task_id is not None:
#             prev_task_details = transfer_client.get_task(prev_task_id)
#             imp_logger.info(f"Prev Task ID: {prev_task_id} has status: {prev_task_details['status']}")
#             imp_logger.info(f"Prev Task details: {prev_task_details}")

#             if prev_task_details['status'] == 'ACTIVE':
#                 # close the program and wait for the next cronjob call hoping that the function would be completed by then.
#                 msg = f"Prev Task is still ACTIVE. Terminating the current run to wait for previous process to complete."
#                 imp_logger.warning(msg)
#                 sys.exit(msg)
                
#             elif prev_task_details['status'] == 'SUCCEEDED':
#                 # continue with the program.
#                 pass
#             elif prev_task_details['status'] == 'FAILED':
#                 # TODO - Run the previous task again. (modify the flags to initiate the previous run again.)
#                 # TODO - Check what flags to modify.
#                 # TODO - Maybe just terminating the program is good enough since no flags files are updated yet.
#                 pass

#         # Execute current task
#         transfer_data = globus_sdk.TransferData(
#             transfer_client, 
#             src_endpoint_uuid, 
#             dst_endpoint_uuid,
#         )

#         transfer_data.add_item(src_path, dst_path)

#         transfer_result = transfer_client.submit_transfer(transfer_data)

#         imp_logger.info(f"Transfer Task submitted! Task ID: {transfer_result['task_id']}")

#     except Exception as e:
#         imp_logger.debug(f"Ran into exception!!!")
#         raise e
    
#     finally:
#         # Updating the .last_state file
#         imp_logger.info(f"Updating the last state!")
#         with open(c.LAST_STATE_RECORD_FILE, "w") as lsrf:
#             lsrf.write(c.STATES.IMPORT)

#         # Updating the .last_task_id file
#         imp_logger.info(f"Updating the last task ID!")
#         with open(c.LAST_TASKID_RECORD_FILE, "w") as ltrf:
#             ltrf.write(transfer_result['task_id'])
    
#     return transfer_result









def import_data(
        src_endpoint_uuid,
        dst_endpoint_uuid,
        src_path,
        dst_path,
        authorizer,
        prev_task_id=None,
        delete_source_on_successful_transfer=False,
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

    # Transferring data
    # -------------------------------------------------
    imp_result = transfer.transfer_data(
        src_endpoint_uuid=src_endpoint_uuid,
        dst_endpoint_uuid=dst_endpoint_uuid,
        src_path=src_path,
        dst_path=dst_path,
        transfer_client=transfer_client,
        delete_source_on_successful_transfer=delete_source_on_successful_transfer,
        label=label,
    )

    # Updating the .last_state file
    # with open(c.LAST_STATE_RECORD_FILE, "w") as lsrf:
    #     lsrf.write(c.STATES.IMPORT)
    util.update_last_state(c.STATES.IMPORT)
    # transfer_logger.info(f"Last state updated!")

    # Updating the .last_task_id file
    # with open(c.LAST_TASKID_RECORD_FILE, "w") as ltrf:
    #     ltrf.write(transfer_result['task_id'])
    util.update_last_taskid(imp_result['task_id'])
    # transfer_logger.info(f"Last task ID updated!")

    return imp_result