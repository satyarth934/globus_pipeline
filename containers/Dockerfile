# Use an official base image, such as Ubuntu
FROM ubuntu:latest

# Set noninteractive to avoid prompts during package installation
ARG DEBIAN_FRONTEND=noninteractive

# Install Python and pip, along with required utilities for logging
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    git \
    ca-certificates \
    rsync && \  # Added for potential file management needs
    # Clean up to reduce image size
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Clone the GitHub repository containing the processing code
RUN git clone https://github.com/satyarth934/globus_pipeline.git

# Define environment variables for source and destination directories
ENV PROCESSING_SRC_DIR="/mnt/processing_src"
ENV PROCESSING_DST_DIR="/mnt/processing_dst"

# # Change WORKDIR
# WORKDIR $PROCESSING_SRC_DIR
WORKDIR globus_pipeline

# Install any required Python dependencies
# COPY processing_test_1/requirements.txt $PROCESSING_SRC_DIR
RUN if [ -f requirements.txt ]; then pip3 install --no-cache-dir -r requirements.txt; fi

# # Copy the main processing Python script into the container
# COPY processing_test_1/process.py $PROCESSING_SRC_DIR

# Script to process files and handle outputs using Bash commands
CMD bash -c "\
echo 'Starting processing...'; \

ls $PROCESSING_SRC_DIR/*.fits > .tmp_input_filepaths; \

python3 read_n_process_fits.py --input_files .tmp_input_filepaths --output_dir $PROCESSING_DST_DIR; \
echo 'Processing completed.' \

echo 'Removing processed files...'; \
cat .tmp_processed_filepaths | xargs -I{} rm -v {}; \

echo 'Cleanup completed.';"
