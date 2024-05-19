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
        util_logger.error(f"The file {filepath} does not exist. {failure_msg}")

    except PermissionError as pe:
        util_logger.error(f"Permission denied: unable to delete {filepath}. {failure_msg}")

    except Exception as e:
        util_logger.error(f"Error occurred: {e}. {failure_msg}")


def update_last_state(state):
    with open(c.LAST_STATE_RECORD_FILE, "w") as lsrf:
        lsrf.write(state)
    util_logger.info(f"Last state updated successfully!")


def update_last_taskid(taskid):
    with open(c.LAST_TASKID_RECORD_FILE, "w") as ltrf:
        ltrf.write(taskid)
    util_logger.info(f"Last taskid updated successfully!")