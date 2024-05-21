import os
import sys
import time
from tqdm import tqdm

import util
import transfer
import arg_parser
import importing as imp
import computing as cmp
import exporting as exp
import util_constants as c
import authentication as auth
# import read_n_process_fits as rnp_fits

import logging
import logger_config

main_logger = logger_config.setup_logging(
    module_name=__name__,
    # level=logging.INFO,
    level=logging.DEBUG,
    logfile_dir=c.LOGFILE_DIR,
    main_module=True,
)


def main():
    args = arg_parser.parse_args()

    # Get the last state of the program
    # -------------------------------------------------
    last_state = util.get_last_state()
    main_logger.debug(f"{last_state = }")


    # Get current state
    # -------------------------------------------------
    curr_state = util.get_current_state(last_state=last_state)
    main_logger.debug(f"{curr_state = }")


    # Authenticate
    # -------------------------------------------------
    authorizer = auth.authenticate(
        # args,
        client_id=args.client_id,
        uuids=[
            args.src_collection_uuid,
            args.compute_collection_uuid,
            args.dst_collection_uuid,
        ],
        # force_new_token=True,
    )

    
    # Run stages based on the last state
    # -------------------------------------------------
    if curr_state == c.STATES.IMPORT:
        # Get previous task ID (previous state can be None or Export_cleanup)
        prev_task_id = util.get_prev_taskid()

        # Run import
        try:
            imp_result = imp.import_data(
                src_endpoint_uuid=args.src_collection_uuid,
                dst_endpoint_uuid=args.compute_collection_uuid,
                src_path=args.src_collection_path,
                dst_path=args.compute_collection_src_path,
                authorizer=authorizer,
                prev_task_id=prev_task_id,
                exclude_previously_imported=True,
                label="Importing data in compute collection to process",
            )

            if imp_result is None:    # No file IMPORTed. Terminate process.
                log_msg = f"No new files to transfer! Terminating the process."
                main_logger.info(log_msg)
                sys.exit(log_msg)
            else:
                log_msg = f"Transfer Task submitted! Task ID: {imp_result['task_id']}"
                main_logger.info(log_msg)

            # Updating the .last_state file
            util.update_last_state(c.STATES.IMPORT)

            # Updating the .last_task_id file
            if imp_result:
                util.update_last_taskid(imp_result['task_id'])

        except Exception as e:
            raise e

    # -----------------
    elif curr_state == c.STATES.COMPUTE:
        # Get previous task ID (previous state would be Import)
        prev_task_id = util.get_prev_taskid()
        
        # Run compute
        try:
            cmp.compute_data(
                input_dir=args.compute_collection_src_path,
                output_dir=args.compute_collection_dst_path,
                prev_task_id=prev_task_id,
                authorizer=authorizer,
            )

            # Delete the compute input files
            # # cat `c.TMP_INPUT_FILEPATHS_FILE` | xargs rm
            # # TODO - Optimize this
            # main_logger.debug("Deleting the input files in compute src")
            # with open(c.TMP_INPUT_FILEPATHS_FILE, 'r') as fh:
            #     input_files = fh.readlines()
            #     for input_file in tqdm(input_files, desc="Temp storage inp files"):
            #         util.delete_file(input_file.strip(), sleep_time=0)

            # # Dump processed files to the record file
            # if os.path.exists(c.TMP_PROCESSED_FILEPATHS_FILE):
            #     util.dump_tmpfile_to_record(
            #         src_file=c.TMP_PROCESSED_FILEPATHS_FILE,
            #         dst_file=c.ALL_PROCESSED_FILEPATHS_FILE,
            #     )
            cmp.delete_tmp_input_files(record_processesd_files=True)

            # Update .last_state
            util.update_last_state(c.STATES.COMPUTE)

            # Delete .last_taskid
            util.delete_file(c.LAST_TASKID_RECORD_FILE)
        
        except Exception as e:
            raise e

    # -----------------
    elif (curr_state == c.STATES.EXPORT):
        # Run export
        try:
            exp_result = exp.export_data(
                src_endpoint_uuid=args.compute_collection_uuid,
                dst_endpoint_uuid=args.dst_collection_uuid,
                src_path=args.compute_collection_dst_path,
                dst_path=args.dst_collection_path,
                authorizer=authorizer,
                check_prev_task=True,
                label="Exporting processed data to destination collection",
            )

            if exp_result is None:
                log_msg = "No new files to export. Terminating process!"
                main_logger.info(log_msg)
                sys.exit(log_msg)
            else:
                log_msg = f"Transfer Task submitted! Task ID: {exp_result['task_id']}"
                main_logger.info(log_msg)

            # Updating the .last_state file
            util.update_last_state(c.STATES.EXPORT)

            # Updating the .last_task_id file
            if exp_result:
                util.update_last_taskid(exp_result['task_id'])
        
        except Exception as e:
            raise e

    # -----------------
    elif (curr_state == c.STATES.EXPORT_CLEANUP):
        # Get previous task ID (previous state can be Export)
        prev_task_id = util.get_prev_taskid()

        # Remove the tmp processed files after the export process has completed
        try:
            main_logger.warning("Assuming that the src_path for export exists on the same machine as the compute machine!")
            exp.export_cleanup(
                dir_path=args.compute_collection_dst_path,
                authorizer=authorizer,
                prev_task_id=prev_task_id,
            )

            # Updating the .last_state file
            util.update_last_state(c.STATES.EXPORT_CLEANUP)

            # Updating the .last_task_id file
            util.delete_file(c.LAST_TASKID_RECORD_FILE)
        
        except Exception as e:
            raise e

    # -----------------
    else:
        main_logger.debug("Unknown last state!!!")
        raise ValueError("Unknown last state!")


if __name__ == "__main__":
    main()