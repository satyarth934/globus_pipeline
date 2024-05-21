import argparse

import util_constants as c

import logging
import logger_config

argp_logger = logger_config.setup_logging(
    module_name=__name__,
    # level=logging.INFO,
    level=logging.DEBUG,
    logfile_dir=c.LOGFILE_DIR,
)


def parse_args():
    argp_logger.info("Entering parse_args()")

    parser = argparse.ArgumentParser(
        "Globus transfer-compute-transfer pipeline",
        fromfile_prefix_chars="@",  # helps read the arguments from a file.
    )

    requiredNamed = parser.add_argument_group("required named arguments")

    requiredNamed.add_argument(
        "--client_id",
        type=str,
        help="UUID of the Globus App that is being used for the process.",
        required=True,
    )

    # UUIDs
    requiredNamed.add_argument(
        "--src_collection_uuid",
        type=str,
        help="Globus UUID of the source collection.",
        required=True,
    )

    requiredNamed.add_argument(
        "--compute_collection_uuid",
        type=str,
        help="Globus UUID of the compute collection. This is where the data is transfered to be processed.",
        required=True,
    )

    requiredNamed.add_argument(
        "--dst_collection_uuid",
        type=str,
        help="Globus UUID of the destination collection. This is where the processed data is transferred.",
        required=True,
    )

    # Paths
    requiredNamed.add_argument(
        "--src_collection_path",
        type=str,
        help="Path of the data within source collection.",
        required=True,
    )

    requiredNamed.add_argument(
        "--compute_collection_src_path",
        type=str,
        help="Path where the data is to be stored within the compute collection where the data will be processed. This is the input directory for the compute process.",
        required=True,
    )

    requiredNamed.add_argument(
        "--compute_collection_dst_path",
        type=str,
        help="Path where processed data is temporarily stored till it is transferred to the destination collection.",
        required=True,
    )

    requiredNamed.add_argument(
        "--dst_collection_path",
        type=str,
        help="Path where the processed data is to be transferred in destination collection.",
        required=True,
    )

    args, unknown = parser.parse_known_args()

    return args
