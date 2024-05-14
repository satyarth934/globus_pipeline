import collections

IMPORT_FLAG_FILE = ".importing.flag"
COMPUTE_FLAG_FILE = ".computing.flag"
EXPORT_FLAG_FILE = ".exporting.flag"

LAST_STATE_RECORD_FILE = ".last_state"
LAST_TASKID_RECORD_FILE = ".last_taskid"

StatesNamedTuple = collections.namedtuple(
    "StatesNamedTuple", 
    "IMPORT COMPUTE EXPORT",
)
STATES = StatesNamedTuple(
    IMPORT="IMPORT",
    COMPUTE="COMPUTE",
    EXPORT="EXPORT",
)


TOKEN_CACHE_PATH = ".curr_token.json"
# TOKEN_CACHE_PATH = ".curr_token.pkl"


# Logger Details
# -------------------------------------------------
LOGFILE_DIR = "./logs"

# LOG_FORMAT = "%(asctime)s [%(levelname)s] File %(module)s: line %(lineno)d: %(message)s"
# LOG_FORMAT = "%(asctime)s [%(levelname)s] File %(pathname)s: line %(lineno)d:\n%(message)s"
# LOG_FORMAT = "%(asctime)s [%(levelname)s] File %(pathname)s: line %(lineno)d: %(message)s"
LOG_FORMAT = "%(asctime)s [%(levelname)s] File %(filename)s: line %(lineno)d: %(message)s"

LOG_SEPARATOR = f"\n\n\n{'-'*50}\n\n\n"


# Compute Related
# -------------------------------------------------
TMP_INPUT_FILEPATHS_FILE = ".tmp_input_filepaths"
TMP_PROCESSED_FILEPATHS_FILE = ".tmp_processed_filepaths"