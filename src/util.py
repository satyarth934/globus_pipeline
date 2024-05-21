import os
import time

import util_constants as c

import logging
import logger_config
util_logger = logger_config.setup_logging(
    module_name=__name__,
    # level=logging.INFO,
    level=logging.DEBUG,
    logfile_dir=c.LOGFILE_DIR,
)


# File functions
# -------------------------------------------------

# To create a blank file and update it's time if already exists
def touch(path):
    with open(path, 'a'):
        os.utime(path, None)


def delete_file(
        filepath, 
        success_msg="", 
        failure_msg="",
        sleep_time=2,
):
    try:
        os.remove(filepath)
        time.sleep(sleep_time)
        util_logger.info(f"The file {filepath} has been deleted successfully. {success_msg}")

    except FileNotFoundError as fnfe:
        util_logger.warning(f"The file {filepath} does not exist. {failure_msg}")

    except PermissionError as pe:
        util_logger.warning(f"Permission denied: unable to delete {filepath}. {failure_msg}")

    except Exception as e:
        util_logger.warning(f"Error occurred: {e}. {failure_msg}")


def dump_file_to_file(src_file, dst_file, chunk_size=None, write_mode='a'):
    # Check if file exists
    if not os.path.exists(src_file):
        raise ValueError("`src_file` does not exist. Recheck the input parameters.")

    # Open the source file to read and destination file to append
    with open(src_file, 'r') as src, open(dst_file, write_mode) as dst:
        while True:
            if chunk_size is None:
                data = src.read()
            else:
                # Read data in chunks (bytes). Recommended for very large files.
                data = src.read(chunk_size)
            if not data:
                break    # exit condition

            # Write the read data to the destination file
            dst.write(data)


def dump_tmpfile_to_record(src_file, dst_file):
    # Check if file exists
    if not os.path.exists(src_file):
        raise ValueError("`src_file` does not exist. Recheck the input parameters.")
    
    # Decide if to write file in chunks or all at once
    max_file_size = 16*1024*1024
    if os.path.getsize(src_file) > max_file_size:
        chunk_size = max_file_size
    else:
        chunk_size = None

    # Dump data
    dump_file_to_file(
        src_file=src_file, 
        dst_file=dst_file, 
        chunk_size=chunk_size, 
        write_mode='a',
    )


# Update functions
# -------------------------------------------------

def update_last_state(state):
    with open(c.LAST_STATE_RECORD_FILE, "w") as lsrf:
        lsrf.write(state)
    util_logger.info(f"Last state updated successfully!")


def update_last_taskid(taskid):
    with open(c.LAST_TASKID_RECORD_FILE, "w") as ltrf:
        ltrf.write(taskid)
    util_logger.info(f"Last taskid updated successfully!")


# Get functions
# -------------------------------------------------

def get_last_state():
    if os.path.exists(c.LAST_STATE_RECORD_FILE):
        with open(c.LAST_STATE_RECORD_FILE, "r") as lsrf:
            last_state = lsrf.read().strip()
    else:
        last_state = None

    return last_state


def get_prev_taskid():
    if os.path.exists(c.LAST_TASKID_RECORD_FILE):
        with open(c.LAST_TASKID_RECORD_FILE, 'r') as fh:
            prev_task_id = fh.read().strip()
        
        if prev_task_id == "":
            prev_task_id = None
            
    else:
        prev_task_id = None
    
    return prev_task_id


def get_current_state(last_state):
    # if (last_state is None) or (last_state == c.STATES.EXPORT_CLEANUP):
    if last_state in [None, c.STATES.EXPORT_CLEANUP, c.STATES.IMPORT_CANCELLED]:
        curr_state = c.STATES.IMPORT
    elif last_state == c.STATES.IMPORT:
        curr_state = c.STATES.COMPUTE
    # elif last_state == c.STATES.COMPUTE:
    elif last_state in [c.STATES.COMPUTE, c.STATES.EXPORT_CANCELLED]:
        curr_state = c.STATES.EXPORT
    elif last_state == c.STATES.EXPORT:
        curr_state = c.STATES.EXPORT_CLEANUP
    else:
        raise ValueError("Unknown 'last_state'!!!")
    
    return curr_state