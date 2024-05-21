import os
import sys
from tqdm import tqdm

import transfer
import util
import util_constants as c
import read_n_process_fits as rnp_fits

import logging
import logger_config
cmp_logger = logger_config.setup_logging(
    module_name=__name__,
    # level=logging.INFO,
    level=logging.DEBUG,
    logfile_dir=c.LOGFILE_DIR,
)


# Preparing data
# ls .../processing_src/*.fits > c.TMP_INPUT_FILEPATHS
# TODO - might need to update this based on the actual number of files
def prepare_input(input_dir):
    input_files = []
    with os.scandir(input_dir) as entries:
        for entry in entries:
            if entry.is_file() and entry.name.endswith('.fits'):
                input_files.append(entry.path)

    with open(c.TMP_INPUT_FILEPATHS_FILE, 'w') as file:
        file.write('\n'.join(input_files))
    
    cmp_logger.debug(f"Prepared input file: {c.TMP_INPUT_FILEPATHS_FILE} => {len(input_files)} files.")


# Compute function
def compute_data(
        input_dir,
        output_dir,
        prev_task_id=None,
        authorizer=None,
):
    # Check if the previous process has completed
    # -------------------------------------------------
    if prev_task_id is not None:
        prev_task_details = transfer.get_task_stats(
            prev_task_id,
            authorizer=authorizer,
        )
    
        cmp_logger.info(f"Prev Task ID: {prev_task_id} has status: {prev_task_details['status']}")
        cmp_logger.debug(f"Prev Task details: {prev_task_details}")

        if prev_task_details['status'] in ['ACTIVE', 'FAILED']:
            # close the program and wait for the next cronjob call hoping that the function would be completed by then.
            msg = f"Prev Task is still ACTIVE. Terminating the current run to wait for previous process to complete."
            cmp_logger.warning(msg)
            sys.exit(msg)
            
        elif prev_task_details['status'] in ['SUCCEEDED', 'NULL']:
            # continue with the program.
            pass

    # Prepare input
    # -------------------------------------------------
    cmp_logger.info("Preparing data for compute...")
    prepare_input(input_dir=input_dir)    # Lists inputs in TMP_INPUT_FILEPATHS_FILE

    # Run compute
    # -------------------------------------------------
    cmp_logger.info("Initiating compute...")
    rnp_fits.process_fits_file(
        # input_dir=input_dir, 
        input_files_file=c.TMP_INPUT_FILEPATHS_FILE, 
        output_dir=output_dir, 
        plot_dir=None,
    )


def record_processed_files():
    if os.path.exists(c.TMP_PROCESSED_FILEPATHS_FILE):
        util.dump_tmpfile_to_record(
            src_file=c.TMP_PROCESSED_FILEPATHS_FILE,
            dst_file=c.ALL_PROCESSED_FILEPATHS_FILE,
        )


def delete_tmp_input_files(record_processesd_files=True):
    # cat `c.TMP_INPUT_FILEPATHS_FILE` | xargs rm
    # TODO - Optimize this
    cmp_logger.debug("Deleting the input files in compute src")
    with open(c.TMP_INPUT_FILEPATHS_FILE, 'r') as fh:
        input_files = fh.readlines()
        for input_file in tqdm(input_files, desc="Deleting tmp storage input files"):
            util.delete_file(input_file.strip(), sleep_time=0)

    # Dump processed files to the record file
    if record_processesd_files:
        record_processed_files()


def get_processed_filepaths():
    if os.path.exists(c.TMP_PROCESSED_FILEPATHS_FILE):
        with open(c.TMP_PROCESSED_FILEPATHS_FILE, 'r') as pfp_fh:
            processed_files = pfp_fh.readlines()

        processed_files = [f.strip() for f in processed_files]
    else:
        processed_files = list()

    return processed_files