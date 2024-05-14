import os
import sys
import time

import arg_parser
import util
import util_constants as c
import authentication as auth
import importing as imp
import computing as cmp
import exporting as exp
import transfer as tfr
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
    if os.path.exists(c.LAST_STATE_RECORD_FILE):
        with open(c.LAST_STATE_RECORD_FILE, "r") as lsrf:
            last_state = lsrf.read().strip()
    else:
        last_state = None
    main_logger.debug(f"{last_state = }")


    # Get current state
    # -------------------------------------------------
    if (last_state is None) or (last_state == c.STATES.EXPORT):
        curr_state = c.STATES.IMPORT
    elif last_state == c.STATES.IMPORT:
        curr_state = c.STATES.COMPUTE
    elif last_state == c.STATES.COMPUTE:
        curr_state = c.STATES.EXPORT
    else:
        raise ValueError("Unknown 'last_state'!!!")
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
        # Get previous task ID (previous state can be Export)
        if os.path.exists(c.LAST_TASKID_RECORD_FILE):
            with open(c.LAST_TASKID_RECORD_FILE, 'r') as fh:
                prev_task_id = fh.read().strip()
        else:
            prev_task_id = None

        # Run import
        # imp_result = imp.import_data(
        #     src_endpoint_uuid=args.src_collection_uuid,
        #     dst_endpoint_uuid=args.compute_collection_uuid,
        #     src_path=args.src_collection_path,
        #     dst_path=args.compute_collection_src_path,
        #     authorizer=authorizer,
        #     prev_task_id=prev_task_id,
        # )

        imp_result = tfr.transfer_data(
            src_endpoint_uuid=args.src_collection_uuid,
            dst_endpoint_uuid=args.compute_collection_uuid,
            src_path=args.src_collection_path,
            dst_path=args.compute_collection_src_path,
            authorizer=authorizer,
            prev_task_id=prev_task_id,
            label="Importing data to process in compute collection.",
        )
    
    elif curr_state == c.STATES.COMPUTE:
        # TODO - Check if transfer is complete (use last state process task ID)
        # Get previous task ID (previous state would be Import)
        if os.path.exists(c.LAST_TASKID_RECORD_FILE):
            with open(c.LAST_TASKID_RECORD_FILE, 'r') as fh:
                prev_task_id = fh.read().strip()
        else:
            prev_task_id = None
        
        # Run compute
        cmp_results = cmp.compute_data(
            input_dir=args.compute_collection_src_path,
            output_dir=args.compute_collection_dst_path,
            prev_task_id=prev_task_id,
            authorizer=authorizer,
        )

        # Delete the compute input files
        # cat `c.TMP_INPUT_FILEPATHS_FILE` | xargs rm
        # TODO - Will need to optimize this
        main_logger.debug("Deleting the input files in compute src")
        with open(c.TMP_INPUT_FILEPATHS_FILE, 'r') as fh:
            input_files = fh.readlines()
            for input_file in input_files:
                util.delete_file(input_file)

        # Update .last_state
        main_logger.info(f"Updating the last state!")
        with open(c.LAST_STATE_RECORD_FILE, "w") as lsrf:
            lsrf.write(c.STATES.COMPUTE)

        # Delete .last_taskid
        util.delete_file(".last_taskid")

    
    elif (curr_state == c.STATES.EXPORT):
        # Check if compute is still running
        if os.path.exists(c.COMPUTE_FLAG_FILE):
            main_logger.debug("Terminating the process to wait for compute process to finish.")
            sys.exit("Waiting for compute process to complete!")

        # Run export
        exp_result = tfr.transfer_data(
            src_endpoint_uuid=args.src_collection_uuid,
            dst_endpoint_uuid=args.compute_collection_uuid,
            src_path=args.src_collection_path,
            dst_path=args.compute_collection_src_path,
            authorizer=authorizer,
            delete_source_on_successful_transfer=True,
            label="Export and Delete compute output from compute storage"
        )
    
    else:
        main_logger.debug("Unknown last state!!!")
        raise ValueError("Unknown last state!")


if __name__ == "__main__":
    main()