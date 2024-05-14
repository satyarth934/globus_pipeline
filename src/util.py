import os

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


def delete_file(filepath, success_msg=None, failure_msg=None):
    try:
        os.remove(filepath)
        util_logger.info(f"The file {filepath} has been deleted successfully. {success_msg}")

    except FileNotFoundError as fnfe:
        util_logger.error(f"The file {filepath} does not exist. {failure_msg}")

    except PermissionError as pe:
        util_logger.error(f"Permission denied: unable to delete {filepath}. {failure_msg}")

    except Exception as e:
        util_logger.error(f"Error occurred: {e}. {failure_msg}")