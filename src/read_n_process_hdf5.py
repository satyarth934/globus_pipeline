import os
import sys
import glob
import h5py
import threading
import numpy as np
from tqdm import tqdm
import concurrent.futures
from functools import partial
import matplotlib.pyplot as plt

import util
import util_constants as c


# # Defining logger
import logging
import logger_config
hdf5_logger = logger_config.setup_logging(
    module_name=__name__,
    # level=logging.INFO,
    level=logging.DEBUG,
    logfile_dir=c.LOGFILE_DIR,
)


# Process each HDF5 file
def _process_hdf5_file(
        hdf5_file,
        output_dir,
        plot_dir=None,
        log_processed_filehandler=None,
        log_processed_concurrency_lock=None,
    ):

    try:
        # Open the hdf5 file
        with h5py.File(hdf5_file, 'r') as hdul:
            # Get the image data
            image_data = hdul['full'][:]

        # Compute the median and average of the image data
        median_value = np.median(image_data)
        average_value = np.mean(image_data)

        # Create the output file name
        output_file = f"{output_dir}/{os.path.splitext(os.path.basename(hdf5_file))[0]}.txt"

        # Write the results to the output file
        with open(output_file, 'w') as f:
            f.write(f'median={median_value}\n')
            f.write(f'average={average_value}\n')

        # Plot and visualize the image
        if plot_dir:
            plot_file = f"{plot_dir}/{os.path.splitext(os.path.basename(hdf5_file))[0]}.png"
            plt.imshow(image_data, cmap='gray')
            plt.title(os.path.basename(hdf5_file))
            plt.savefig(plot_file)
            plt.close()
        
        # Logging all the processed filepaths in a file
        if log_processed_filehandler:
            if log_processed_concurrency_lock:
                with log_processed_concurrency_lock:
                    log_processed_filehandler.write(f"{hdf5_file}\n")
            else:
                log_processed_filehandler.write(f"{hdf5_file}\n")
    
    except Exception as e:
        hdf5_logger.warning(e)


def process_hdf5_file(
        output_dir, 
        input_dir=None, 
        input_files_file=None,
        plot_dir=None,
    ):
    """Process hdf5 files.

    This function processes hdf5 files by performing the following steps:
    1. Prepare inputs and output directory.
    2. Get a list of all hdf5 files in the directory.
    3. Create the output directory if it doesn't exist.
    4. Optionally, create the plot directory if it is provided.
    5. Process each hdf5 file in parallel.

    Args:
        output_dir (str): The directory where the processed files will be saved.
        input_dir (str, optional): The directory containing the input hdf5 files. Defaults to None.
        input_files_file (str, optional): The file containing the list of input hdf5 files. Defaults to None.
        plot_dir (str, optional): The directory where the plot files will be saved. Defaults to None.

    Raises:
        ValueError: If both `input_dir` and `input_files_file` are not defined. One of them must be defined as input.
    """

    hdf5_logger.info("Entering process_hdf5_file()")

    # Prepare inputs and output directory
    # -------------------------------------------------
    # Get a list of all hdf5 files in the directory
    if input_dir:
        hdf5_files = glob.glob(f"{input_dir}/*.hdf5")
    elif input_files_file:
        with open(input_files_file, 'r') as inp_fh:
            hdf5_files = [
                i_file.strip()
                for i_file in inp_fh.readlines()
                if i_file.strip().endswith(".hdf5")
            ]
    else:
        raise ValueError("Input source not provided! Either `input_dir` or `input_files_file` must be defined.")

    hdf5_logger.info(f"Number of input files: {len(hdf5_files)}")

    if len(hdf5_files) > 0:
        # Create the output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        if plot_dir:
            os.makedirs(plot_dir, exist_ok=True)
        
        # Process each hdf5 file
        # -------------------------------------------------
        try:
            # Create a flag file to mark that compute is in progress
            util.touch(path=c.COMPUTE_FLAG_FILE)

            # Create a lock object
            lock = threading.Lock()    # Needed for concurrent write to TMP_PROCESSED_FILEPATHS_FILE
            util.delete_file(
                c.TMP_PROCESSED_FILEPATHS_FILE, 
                success_msg="Preparing for the run...",
                sleep_time=2,
            )
            with open(c.TMP_PROCESSED_FILEPATHS_FILE, 'a') as tpf_fh:
                # # Process each hdf5 file in a loop
                # hdf5_logger.debug("Starting compute")
                # for hdf5_file in tqdm(hdf5_files, desc="Processing hdf5 File"):
                #     _process_hdf5_file(
                #         hdf5_file=hdf5_file,
                #         output_dir=output_dir,
                #     )

                # Process each hdf5 file in parallel
                hdf5_logger.debug("Starting parallel compute")
                _process_hdf5_file_partialfunc = partial(
                    _process_hdf5_file, 
                    output_dir=output_dir, 
                    plot_dir=plot_dir,
                    log_processed_filehandler=tpf_fh,
                    log_processed_concurrency_lock=lock,
                )

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.map(
                        _process_hdf5_file_partialfunc, 
                        hdf5_files,
                    )

        except Exception as e:
            raise e
        finally:
            util.delete_file(c.COMPUTE_FLAG_FILE, sleep_time=2)
            hdf5_logger.debug("Deleted the compute flag file.")
    
    else:
        hdf5_logger.info(f"No new file to process! Terminating the task.")
    
    hdf5_logger.info("Exiting process_hdf5_file()")


def main():
    process_hdf5_file(
        output_dir="/global/cfs/cdirs/m2845/satyarth/globus_proj/scripts/globus_pipeline/testing_data",
        input_dir="/global/cfs/cdirs/m2845/satyarth/globus_proj/data/cosmoflow_test_data",
    )


if __name__ == "__main__":
    main()