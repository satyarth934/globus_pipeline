import json
import base64


def encode_dict(data: dict):
    encoded_data = base64.b64encode(
        json.dumps(data).encode('utf-8')
    ).decode('utf-8')

    return encoded_data


def decode_dict(data):
    decoded_data = json.loads(base64.b64decode(data))

    return decoded_data


# TODO: Implement pulling data from SPIN secrets
def retrieve_token_info_from_secret():
    # TODO: Pull data from secret
    encoded_data_from_secret = ...

    # Decode data
    decoded_data = decode_dict(encoded_data_from_secret)

    # Return decoded data
    return decoded_data


def retrieve_token_info_from_file(filename):
    # Pull data from file
    with open(filename, 'r') as f:
        encoded_data_from_file = f.read()

    # Decode data
    decoded_data = decode_dict(encoded_data_from_file)

    # Return decoded data
    return decoded_data
