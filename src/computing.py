import os
import sys

import transfer as tfr
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
    prev_task_details = tfr.get_task_stats(
        prev_task_id,
        authorizer=authorizer,
    )
    
    cmp_logger.info(f"Prev Task ID: {prev_task_id} has status: {prev_task_details['status']}")
    cmp_logger.info(f"Prev Task details: {prev_task_details}")

    if prev_task_details['status'] == 'ACTIVE':
        # close the program and wait for the next cronjob call hoping that the function would be completed by then.
        msg = f"Prev Task is still ACTIVE. Terminating the current run to wait for previous process to complete."
        cmp_logger.warning(msg)
        sys.exit(msg)
        
    elif prev_task_details['status'] == 'SUCCEEDED':
        # continue with the program.
        pass
    elif prev_task_details['status'] == 'FAILED':
        # TODO - Run the previous task again. (modify the flags to initiate the previous run again.)
        # TODO - Check what flags to modify.
        # TODO - Maybe just terminating the program is good enough since no flags files are updated yet.
        pass

    # Prepare input
    # -------------------------------------------------
    cmp_logger.info("Preparing data for compute...")
    prepare_input(input_dir=input_dir)
    
    # Define the directory path where the FITS files are located
    # fits_dir = "/global/cfs/projectdirs/m2845/pipe"
    # input_dir = "/global/cfs/cdirs/m2845/satyarth/globus_proj/data/processing_src"
    # output_dir = "/global/cfs/cdirs/m2845/satyarth/globus_proj/data/processing_dest"
    # plot_dir = "/global/cfs/cdirs/m2845/satyarth/globus_proj/data/plots"

    # Run compute
    # -------------------------------------------------
    cmp_logger.info("Initiating compute...")
    rnp_fits.process_fits_file(
        # input_dir=input_dir, 
        input_files_file=c.TMP_INPUT_FILEPATHS_FILE, 
        output_dir=output_dir, 
        plot_dir=None,
    )