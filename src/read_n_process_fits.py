import os
import glob
import numpy as np
from tqdm import tqdm
import concurrent.futures
from astropy.io import fits
from functools import partial
import matplotlib.pyplot as plt


# Process each FITS file
def _process_fits_file(
        fits_file,
        output_dir,
        plot_dir=None,
    ):
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


def process_fits_file(input_dir, output_dir, plot_dir=None):
    # Read FITS files
    # -------------------------------------------------
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    if plot_dir:
        os.makedirs(plot_dir, exist_ok=True)

    # Get a list of all FITS files in the directory
    fits_files = glob.glob(f"{input_dir}/*.fits")

    # Process each FITS file
    # -------------------------------------------------
    # # Process each FITS file in a loop
    # for fits_file in tqdm(fits_files, desc="Processing FITS File"):
    #     process_fits_file(fits_file)

    # Process each FITS file in parallel
    _process_fits_file_partialfunc = partial(
        _process_fits_file, 
        output_dir=output_dir, 
        plot_dir=plot_dir,
    )

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(
            _process_fits_file_partialfunc, 
            fits_files,
        )


def main():
    # # Read FITS files
    # # -------------------------------------------------
    # # Create the output directory if it doesn't exist
    # os.makedirs(output_dir, exist_ok=True)
    # os.makedirs(plot_dir, exist_ok=True)

    # # Get a list of all FITS files in the directory
    # fits_files = glob.glob(f"{fits_dir}/*.fits")

    # # Process each FITS file
    # # -------------------------------------------------
    # # # Process each FITS file in a loop
    # # for fits_file in tqdm(fits_files, desc="Processing FITS File"):
    # #     process_fits_file(fits_file)

    # # Process each FITS file in parallel
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     executor.map(process_fits_file, fits_files)

    # Define the directory path where the FITS files are located
    # fits_dir = "/global/cfs/projectdirs/m2845/pipe"
    input_dir = "/global/cfs/cdirs/m2845/satyarth/globus_proj/data/processing_src"
    output_dir = "/global/cfs/cdirs/m2845/satyarth/globus_proj/data/processing_dest"
    plot_dir = "/global/cfs/cdirs/m2845/satyarth/globus_proj/data/plots"

    process_fits_file(
        input_dir=input_dir, 
        output_dir=output_dir, 
        plot_dir=None,
    )


if __name__ == "__main__":
    main()