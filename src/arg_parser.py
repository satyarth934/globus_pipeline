import os
import argparse
import collections
from dataclasses import dataclass

import util_constants as c

import logging
import logger_config

argp_logger = logger_config.setup_logging(
    module_name=__name__,
    # level=logging.INFO,
    level=logging.DEBUG,
    logfile_dir=c.LOGFILE_DIR,
)


@dataclass
class EnvArgsDataClass:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key.lower(), value)


def check_env_args():
    if all([(k in os.environ) for k in c.ENV_ARG_KEYS]):

        argp_logger.info("Getting args from ENV")

        args = EnvArgsDataClass(**{k.lower(): os.environ[k] for k in c.ENV_ARG_KEYS})

        return args
    
    return None


def parse_args():
    argp_logger.info("Entering parse_args()")

    args = check_env_args()

    if args is None:

        argp_logger.info("Getting args from argparser")

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

        requiredNamed.add_argument(
            "--file_type",
            type=str,
            help="Type of file that is to be processed. Currently only supports FITS and HDF5 files.",
            required=True,
        )

        args, unknown = parser.parse_known_args()

    return args
