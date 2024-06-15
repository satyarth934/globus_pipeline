import collections


# Flag/Record files
# -------------------------------------------------
IMPORT_FLAG_FILE = ".importing.flag"
COMPUTE_FLAG_FILE = ".computing.flag"
EXPORT_FLAG_FILE = ".exporting.flag"
EXPORT_CLEANUP_FLAG_FILE = ".export_cleanup.flag"

LAST_STATE_RECORD_FILE = ".last_state"
LAST_TASKID_RECORD_FILE = ".last_taskid"

TOKEN_CACHE_PATH = ".curr_token.json"
OFFAUTH_TOKEN_FILE = "offauth_token_info.txt"
SPIN_SECRETS_TOKEN_FILE = "/etc/secrets/token-info"


# States
# -------------------------------------------------
StatesNamedTuple = collections.namedtuple(
    "StatesNamedTuple", 
    "IMPORT COMPUTE EXPORT EXPORT_CLEANUP IMPORT_CANCELLED EXPORT_CANCELLED",
)
STATES = StatesNamedTuple(
    IMPORT="IMPORT",
    COMPUTE="COMPUTE",
    EXPORT="EXPORT",
    EXPORT_CLEANUP="EXPORT_CLEANUP",
    IMPORT_CANCELLED="IMPORT_CANCELLED",
    EXPORT_CANCELLED="EXPORT_CANCELLED",
)


# Logger Details
# -------------------------------------------------
LOGFILE_DIR = "./logs"

# LOG_FORMAT = "%(asctime)s [%(levelname)s] File %(module)s: line %(lineno)d: %(message)s"
# LOG_FORMAT = "%(asctime)s [%(levelname)s] File %(pathname)s: line %(lineno)d:\n%(message)s"
# LOG_FORMAT = "%(asctime)s [%(levelname)s] File %(pathname)s: line %(lineno)d: %(message)s"
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(filename)s: line %(lineno)d: %(message)s"

LOG_SEPARATOR = f"\n\n\n{'-'*50}\n\n\n"


# Compute Related
# -------------------------------------------------
TMP_INPUT_FILEPATHS_FILE = ".tmp_input_filepaths"
TMP_PROCESSED_FILEPATHS_FILE = ".tmp_processed_filepaths"
ALL_PROCESSED_FILEPATHS_FILE = ".all_processed_filepaths"


# Globus Task Related
# -------------------------------------------------
TASK_MAX_FAULTS = 3

TaskCancelStatusTuple = collections.namedtuple(
    "TaskCancelStatusTuple", 
    "CANCELED TASKCOMPLETE",
)
TASK_CANCEL_STATUS = TaskCancelStatusTuple(
    CANCELED="Canceled",
    TASKCOMPLETE="TaskComplete",
)


# Pipeline Related
# -------------------------------------------------
ENV_ARG_KEYS = [
    "CLIENT_ID",
    "SRC_COLLECTION_UUID",
    "SRC_COLLECTION_PATH",
    "COMPUTE_COLLECTION_UUID",
    "COMPUTE_COLLECTION_SRC_PATH",
    "COMPUTE_COLLECTION_DST_PATH",
    "DST_COLLECTION_UUID",
    "DST_COLLECTION_PATH",
    "FILE_TYPE",
]