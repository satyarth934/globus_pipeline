import json
import base64

import arg_parser
import util_constants as c
import authentication as auth
# import read_n_process_fits as rnp_fits

import logging
import logger_config

offauth_logger = logger_config.setup_logging(
    module_name=__name__,
    # level=logging.INFO,
    level=logging.DEBUG,
    logfile_dir=c.LOGFILE_DIR,
)


def store_to_file(data, file):
    try:
        with open(file, 'w') as f:
            f.write(data)
    except Exception as e:
        offauth_logger.error(f"Some issue writing the data to file!!\n{e}")


def main():
    args = arg_parser.parse_args()
    
    uuids = [args.__dict__[k] for k in dir(args) if k.endswith('_uuid')]
    
    # Get authentication token info (will be used for authentication)
    # -------------------------------------------------
    offauth_logger.info(f"Initiating offline authentication...")

    token_info = auth.get_authentication_token(client_id=args.client_id, uuids=uuids)

    # Store encoded token to file
    encoded_token_info = auth.encode_dict(token_info)
    store_to_file(data=encoded_token_info, file=c.OFFAUTH_TOKEN_FILE)
    offauth_logger.info(f"Encoded token info saved to {c.OFFAUTH_TOKEN_FILE}")
    
    # Print the on stdout
    offauth_logger.info(f"Here is the encoded token info: {encoded_token_info}")


if __name__ == "__main__":
    main()














