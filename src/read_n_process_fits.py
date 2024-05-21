import os
import sys
import glob
import threading
import numpy as np
from tqdm import tqdm
import concurrent.futures
from astropy.io import fits
from functools import partial
import matplotlib.pyplot as plt

import argparse

import util
import util_constants as c

# logging.basicConfig(
#     level=logging.INFO,
#     handlers=[logging.StreamHandler(sys.stdout)],
#     format="%(asctime)s [%(levelname)s] File %(module)s: line %(lineno)d: %(message)s",
#     force=True,
# )

# # Defining logger
# rnp_logger = logging.getLogger(__name__)
# rnp_logger.setLevel(logging.INFO)
import logging
import logger_config
rnp_logger = logger_config.setup_logging(
    module_name=__name__,
    # level=logging.INFO,
    level=logging.DEBUG,
    logfile_dir=c.LOGFILE_DIR,
)




# def parse_args():
#     rnp_logger.info("Entering parse_args()")

#     parser = argparse.ArgumentParser(
#         "Process FITS Files",
#         fromfile_prefix_chars="@",  # helps read the arguments from a file.
#     )

#     requiredNamed = parser.add_argument_group("required named arguments")

#     requiredNamed.add_argument(
#         "-i",
#         "--input_files",
#         type=str,
#         help="Path to the file containing list of FITS input filepaths.",
#         required=True,
#     )

#     requiredNamed.add_argument(
#         "-o",
#         "--output_dir",
#         type=str,
#         help="Path to the directory where the processed output will be stored.",
#         required=True,
#     )

#     args, unknown = parser.parse_known_args()

#     return args


# Process each FITS file
def _process_fits_file(
        fits_file,
        output_dir,
        plot_dir=None,
        log_processed_filehandler=None,
        log_processed_concurrency_lock=None,
    ):

    try:
        # Open the FITS file
        with fits.open(fits_file) as hdul:
            # Get the image data
            image_data = hdul[0].data

        # Compute the median and average of the image data
        median_value = np.median(image_data)
        average_value = np.mean(image_data)

        # Create the output file name
        output_file = f"{output_dir}/{os.path.splitext(os.path.basename(fits_file))[0]}.txt"

        # Write the results to the output file
        with open(output_file, 'w') as f:
            f.write(f'median={median_value}\n')
            f.write(f'average={average_value}\n')

        # Plot and visualize the image
        if plot_dir:
            plot_file = f"{plot_dir}/{os.path.splitext(os.path.basename(fits_file))[0]}.png"
            plt.imshow(image_data, cmap='gray')
            plt.title(os.path.basename(fits_file))
            plt.savefig(plot_file)
            plt.close()
        
        # Logging all the processed filepaths in a file
        if log_processed_filehandler:
            if log_processed_concurrency_lock:
                with log_processed_concurrency_lock:
                    log_processed_filehandler.write(f"{fits_file}\n")
            else:
                log_processed_filehandler.write(f"{fits_file}\n")
    
    except Exception as e:
        rnp_logger.warning(e)


def process_fits_file(
        output_dir, 
        input_dir=None, 
        input_files_file=None,
        plot_dir=None,
    ):
    """Process FITS files.

    This function processes FITS files by performing the following steps:
    1. Prepare inputs and output directory.
    2. Get a list of all FITS files in the directory.
    3. Create the output directory if it doesn't exist.
    4. Optionally, create the plot directory if it is provided.
    5. Process each FITS file in parallel.

    Args:
        output_dir (str): The directory where the processed files will be saved.
        input_dir (str, optional): The directory containing the input FITS files. Defaults to None.
        input_files_file (str, optional): The file containing the list of input FITS files. Defaults to None.
        plot_dir (str, optional): The directory where the plot files will be saved. Defaults to None.

    Raises:
        ValueError: If both `input_dir` and `input_files_file` are not defined. One of them must be defined as input.
    """

    rnp_logger.info("Entering process_fits_file()")

    # Prepare inputs and output directory
    # -------------------------------------------------
    # Get a list of all FITS files in the directory
    if input_dir:
        fits_files = glob.glob(f"{input_dir}/*.fits")
    elif input_files_file:
        with open(input_files_file, 'r') as inp_fh:
            fits_files = [
                i_file.strip()
                for i_file in inp_fh.readlines()
                if i_file.strip().endswith(".fits")
            ]
    else:
        raise ValueError("Input source not provided! Either `input_dir` or `input_files_file` must be defined.")

    rnp_logger.info(f"Number of input files: {len(fits_files)}")

    if len(fits_files) > 0:
        # Create the output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        if plot_dir:
            os.makedirs(plot_dir, exist_ok=True)
        
        # Process each FITS file
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
            rnp_logger.debug("Starting parallel compute")
            with open(c.TMP_PROCESSED_FILEPATHS_FILE, 'a') as tpf_fh:
                # # Process each FITS file in a loop
                # for fits_file in tqdm(fits_files, desc="Processing FITS File"):
                #     _process_fits_file(
                #         fits_file=fits_file,
                #         output_dir=output_dir,
                #     )

                # Process each FITS file in parallel
                _process_fits_file_partialfunc = partial(
                    _process_fits_file, 
                    output_dir=output_dir, 
                    plot_dir=plot_dir,
                    log_processed_filehandler=tpf_fh,
                    log_processed_concurrency_lock=lock,
                )

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.map(
                        _process_fits_file_partialfunc, 
                        fits_files,
                    )

        except Exception as e:
            raise e
        finally:
            util.delete_file(c.COMPUTE_FLAG_FILE, sleep_time=2)
            rnp_logger.debug("Deleted the compute flag file.")
    
    else:
        rnp_logger.info(f"No new file to process! Terminating the task.")
    
    rnp_logger.info("Exiting process_fits_file()")


# def main():
#     args = parse_args()

#     # Define the directory path where the FITS files are located
#     # fits_dir = "/global/cfs/projectdirs/m2845/pipe"
#     # input_dir = "/global/cfs/cdirs/m2845/satyarth/globus_proj/data/processing_src"
#     # output_dir = "/global/cfs/cdirs/m2845/satyarth/globus_proj/data/processing_dest"
#     # plot_dir = "/global/cfs/cdirs/m2845/satyarth/globus_proj/data/plots"

#     process_fits_file(
#         # input_dir=input_dir, 
#         input_files_file=args.input_files, 
#         output_dir=args.output_dir, 
#         plot_dir=None,
#     )


# if __name__ == "__main__":
#     main()