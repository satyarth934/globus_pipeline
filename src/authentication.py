import os
import sys
import time
import json
import base64
import globus_sdk

import util_constants as c
import authentication_util as auth_util

import logging
import logger_config

auth_logger = logger_config.setup_logging(
    module_name=__name__,
    level=logging.DEBUG,
    logfile_dir=c.LOGFILE_DIR,
)


# Deprecated
def authenticate_old(
    client_id,
    src_collection_uuid,
    compute_collection_uuid,
    dst_collection_uuid,
):
    auth_logger.warning("Function DEPRECATED!!!")

    native_auth_client = globus_sdk.NativeAppAuthClient(client_id)

    # Get GCS Data Access scopes for both mapped collections. These are needed to access any data on a mapped collection
    src_data_access = globus_sdk.scopes.GCSCollectionScopeBuilder(src_collection_uuid).data_access
    compute_data_access = globus_sdk.scopes.GCSCollectionScopeBuilder(compute_collection_uuid).data_access
    dst_data_access = globus_sdk.scopes.GCSCollectionScopeBuilder(dst_collection_uuid).data_access

    # Add scopes as dependencies for the transfer scope, allowing transfer operations to access data on all mapped collections.
    transfer_scope = globus_sdk.scopes.TransferScopes.make_mutable("all")
    transfer_scope.add_dependency(src_data_access)
    transfer_scope.add_dependency(compute_data_access)
    transfer_scope.add_dependency(dst_data_access)

    # As in the Platform_Introduction_Native_App_Auth notebook, do the Native App Grant Flow
    SCOPES = [
        "openid",
        "profile",
        "email",
        "urn:globus:auth:scope:auth.globus.org:view_identities",
        transfer_scope,
    ]
    # May need to be set to "login" below, if you need to authorize a specific identity for your collection
    PROMPT=None

    native_auth_client.oauth2_start_flow(requested_scopes=SCOPES)
    print(f"Login Here:\n\n{native_auth_client.oauth2_get_authorize_url(prompt=PROMPT)}")

    # print('Go to this URL and login:', client.oauth2_get_authorize_url())
    auth_code = input('Enter the authorization code here: ')
    token_response = native_auth_client.oauth2_exchange_code_for_tokens(auth_code)

    transfer_token = token_response.by_resource_server['transfer.api.globus.org']['access_token']

    authorizer = globus_sdk.AccessTokenAuthorizer(transfer_token)
    
    return authorizer


# Uses NativeApp
def get_authentication_token(
    client_id,
    uuids,
):
    native_auth_client = globus_sdk.NativeAppAuthClient(client_id)

    # Add scopes as dependencies for the transfer scope, allowing transfer operations to access data on all mapped collections.
    transfer_scope = globus_sdk.scopes.TransferScopes.make_mutable("all")

    # Get GCS Data Access scopes for both mapped collections. These are needed to access any data on a mapped collection
    for uuid in uuids:
        collection_data_access = globus_sdk.scopes.GCSCollectionScopeBuilder(uuid).data_access
        transfer_scope.add_dependency(collection_data_access)

    # As in the Platform_Introduction_Native_App_Auth notebook, do the Native App Grant Flow
    SCOPES = [
        "openid",
        "profile",
        "email",
        "urn:globus:auth:scope:auth.globus.org:view_identities",
        "urn:globus:auth:scope:transfer.api.globus.org:all offline_access",
        transfer_scope,
    ]
    # May need to be set to "login" below, if you need to authorize a specific identity for your collection
    PROMPT=None    

    native_auth_client.oauth2_start_flow(requested_scopes=SCOPES)
    print(f"Login Here:\n\n{native_auth_client.oauth2_get_authorize_url(prompt=PROMPT)}")

    # print('Go to this URL and login:', client.oauth2_get_authorize_url())
    auth_code = input('Enter the authorization code here: ')
    token_response = native_auth_client.oauth2_exchange_code_for_tokens(auth_code)

    # transfer_token = token_response.by_resource_server['transfer.api.globus.org']['access_token']
    # return transfer_token

    resource_token_response = token_response.by_resource_server['transfer.api.globus.org']

    return resource_token_response


# Uses ConfidentialApp
# 'Might not end up using ConfidentialApp' 
def get_authentication_token_ONHOLD(
    client_id,
    uuids,
):
    # client_id
    client_secret = os.environ['GLOBUS_CLIENT_SECRET']

    confidential_auth_client = globus_sdk.ConfidentialAppAuthClient(
        client_id, 
        client_secret,
    )

    # Add scopes as dependencies for the transfer scope, allowing transfer operations to access data on all mapped collections.
    transfer_scope = globus_sdk.scopes.TransferScopes.make_mutable("all")

    # Get GCS Data Access scopes for both mapped collections. These are needed to access any data on a mapped collection
    for uuid in uuids:
        collection_data_access = globus_sdk.scopes.GCSCollectionScopeBuilder(uuid).data_access
        transfer_scope.add_dependency(collection_data_access)

    # As in the Platform_Introduction_Native_App_Auth notebook, do the Native App Grant Flow
    SCOPES = [
        "openid",
        "profile",
        "email",
        "urn:globus:auth:scope:auth.globus.org:view_identities",
        "urn:globus:auth:scope:transfer.api.globus.org:all offline_access",
        transfer_scope,
    ]
    # May need to be set to "login" below, if you need to authorize a specific identity for your collection
    PROMPT=None    

    confidential_auth_client.oauth2_start_flow(
        redirect_uri='http://localhost:5000',
        requested_scopes=SCOPES,
    )
    # print(f"Login Here:\n\n{confidential_auth_client.oauth2_get_authorize_url(prompt=PROMPT)}")

    # # print('Go to this URL and login:', client.oauth2_get_authorize_url())
    # auth_code = input('Enter the authorization code here: ')
    # token_response = confidential_auth_client.oauth2_exchange_code_for_tokens(auth_code)

    token_response = confidential_auth_client.oauth2_client_credentials_tokens()

    # transfer_token = token_response.by_resource_server['transfer.api.globus.org']['access_token']
    # return transfer_token

    resource_token_response = token_response.by_resource_server['transfer.api.globus.org']

    return resource_token_response


def get_authorizer(token):
    authorizer = globus_sdk.AccessTokenAuthorizer(token)
    
    auth_logger.debug("Returning AccessTokenAuthorizer!")
    return authorizer


def get_refresh_authorizer(
        client_id,
        access_token_info,
        on_refresh_call=None,
):
    """
    Returns a RefreshTokenAuthorizer object that can be used for authentication.

    Args:
        client_id (str): The client ID for the NativeAppAuthClient.
        access_token_info (dict): A dictionary containing the access token information.
            This dictionary should include the 'refresh_token', 'expires_at_seconds', and
                'access_token' keys.
        on_refresh_call (callable, optional): A callback which is triggered any time this authorizer fetches a new access_token. The on_refresh callable is invoked on the OAuthTokenResponse object resulting from the token being refreshed. It should take only one argument, the token response object. This is useful for implementing storage for Access Tokens, as the on_refresh callback can be used to update the Access Tokens and their expiration times. Defaults to None.

    Returns:
        globus_sdk.RefreshTokenAuthorizer: The RefreshTokenAuthorizer object.

    """

    # Create a NativeAppAuthClient instance
    auth_client = globus_sdk.NativeAppAuthClient(client_id=client_id)
    
    # # [ONHOLD] Use this with ConfidentialApp 
    # # 'Might not end up using ConfidentialApp' 
    # client_secret = os.environ['GLOBUS_CLIENT_SECRET']
    # auth_client = globus_sdk.ConfidentialAppAuthClient(
    #     client_id, 
    #     client_secret,
    # )

    # Use the RefreshTokenAuthorizer to automatically handle refreshing
    authorizer = globus_sdk.RefreshTokenAuthorizer(
        refresh_token=access_token_info['refresh_token'], 
        auth_client=auth_client,
        access_token=access_token_info['access_token'],
        expires_at=access_token_info['expires_at_seconds'],
        on_refresh=on_refresh_call,
    )

    auth_logger.debug("Returning RefreshTokenAuthorizer!")
    return authorizer


def store_token_to_file(
        token, 
        token_filepath,
    ):
    auth_logger.debug(f"Storing token to file: {token_filepath}!")

    # Dump the token dictionary to a JSON file
    with open(token_filepath, "w") as file:
        json.dump(token, file)


def store_token_to_cache(
        token, 
        token_cache_filepath=c.TOKEN_CACHE_PATH,
    ):

    # # Create a dictionary with the token as the value and "token" as the key
    # token_dict = {
    #     "token": token['access_token'],
    #     "expires_at": time.time() + token['expires_in'],
    # }

    auth_logger.debug("Storing token to cache!")

    # # Dump the token dictionary to a JSON file
    # with open(token_cache_filepath, "w") as file:
    #     json.dump(token, file)
    store_token_to_file(
        token=token,
        token_filepath=token_cache_filepath,
    )




def retrieve_token_from_cache(filepath=c.TOKEN_CACHE_PATH):
    if not os.path.exists(filepath):
        return None

    with open(filepath, "r") as file:
        token_dict = json.load(file)
    
    return token_dict


# # Main authentication function
# def authenticate_WORKS(
# # def authenticate(
#         # args,
#         client_id: str,
#         uuids: list,
#         force_new_token: bool=False,
# ):
#     auth_logger.info(f"Authenticating...")

#     token_info = retrieve_token_from_cache()
#     auth_logger.debug(f"Pulled token from cache.")

#     curr_time = time.time()
#     if force_new_token or \
#         (token_info is None) or \
#         (curr_time >= token_info["expires_at_seconds"]):

#         if force_new_token:
#             auth_logger.debug("Force generating new token!")
#         elif token_info is None:
#             auth_logger.debug("No cached token!")
#         elif curr_time >= token_info["expires_at_seconds"]:
#             auth_logger.debug("Cached token expired!")

#         token_info = get_authentication_token(client_id=client_id, uuids=uuids)

#         store_token_to_cache(token=token_info)
    
#     try:
#         # authorizer = get_authorizer(token=token_info["access_token"])
#         authorizer = get_refresh_authorizer(
#             client_id=client_id,
#             access_token_info=token_info,
#             on_refresh_call=store_token_to_cache,
#         )
        
#         auth_logger.debug("Authentication successful!")
#         return authorizer
    
#     except Exception as e:
#         # Incase of unknown authentication issue!
#         auth_logger.debug(f"Exception: {e}")
#         raise ValueError("Issue with Authentication!!")
    

# # TODO: To be tested!!!!
# def authenticate_from_encoded_token_info(
#         # args,
#         client_id: str,
# ):
#     auth_logger.info(f"Authenticating...")

#     # if not in container:
#     token_info = auth_util.retrieve_token_info_from_file(
#         filename=c.OFFAUTH_TOKEN_FILE,
#     )
#     auth_logger.debug(f"Pulled token from file.")
    
#     # # elif in container:
#     # token_info = retrieve_encoded_token_from_secret()
#     # auth_logger.debug(f"Pulled token from file.")
    

#     curr_time = time.time()
#     if (token_info is None) or \
#         (curr_time >= token_info["expires_at_seconds"]):

#         if token_info is None:
#             auth_logger.debug("Pulled token is None!")
#         elif curr_time >= token_info["expires_at_seconds"]:
#             auth_logger.debug("Pulled token is EXPIRED!")

#         # token_info = get_authentication_token(client_id=client_id, uuids=uuids)

#     # Store token to cache if it is a valid token
#     store_token_to_cache(token=token_info)
    
#     try:
#         # authorizer = get_authorizer(token=token_info["access_token"])
#         authorizer = get_refresh_authorizer(
#             client_id=client_id,
#             access_token_info=token_info,
#             on_refresh_call=store_token_to_cache,
#         )
        
#         auth_logger.debug("Authentication successful!")
#         return authorizer
    
#     except Exception as e:
#         # Incase of unknown authentication issue!
#         auth_logger.debug(f"Exception: {e}")
#         raise ValueError("Issue with Authentication!!")


# TODO: To be tested!!!!
def authenticate(client_id: str):
    auth_logger.info(f"Authenticating...")

    if os.path.exists(c.TOKEN_CACHE_PATH):
        # Retrieve token info from cache
        token_info = retrieve_token_from_cache()
        auth_logger.debug("" if token_info is None else f"Pulled token from cache.")
    
    elif os.path.exists(c.OFFAUTH_TOKEN_FILE):
        # Retrieving token info from locally stored encoded data
        token_info = auth_util.retrieve_token_info_from_file(
            filename=c.OFFAUTH_TOKEN_FILE,
        )
        auth_logger.debug(f"Pulled from encoded token info.")
    
    elif os.path.exists(c.SPIN_SECRETS_TOKEN_FILE):
        # Retrieving token info from spin secrets encoded data
        token_info = auth_util.retrieve_token_info_from_file(
            filename=c.SPIN_SECRETS_TOKEN_FILE,
        )
        auth_logger.debug(f"Pulled from spin secrets encoded token info.")
    else:
        # EXIT Scenario 1
        auth_logger.error(f"Could not find authentication token-info in cache, offline_authentication file or secrets. Update the token_info value using offline authentication code!!")
        sys.exit("Update the token_info value using offline authentication code!!")

    # EXIT Scenario 2
    if token_info is None:
        auth_logger.debug("Pulled token is None!")
        auth_logger.error("Pulled token is None!! Update the token_info value using offline authentication code!!")
        sys.exit("Update the token_info value using offline authentication code!!")

    # EXIT Scenario 3
    curr_time = time.time()
    if (curr_time >= token_info["expires_at_seconds"]):
        auth_logger.debug("Pulled token EXPIRED!")
        auth_logger.error("Pulled token is EXPIRED!! Update the token_info value using offline authentication code!!")
        sys.exit("Update the token_info value using offline authentication code!!")
    
    # Valid Token Scenarios
    # -------------------------------------------------
    store_token_to_cache(token=token_info)

    # if (token_info is None) or \
    #     (curr_time >= token_info["expires_at_seconds"]):
    #     # # Retrieving token info from encoded data
    #     # # if not in container:
    #     # token_info = auth_util.retrieve_token_info_from_file(
    #     #     filename=c.OFFAUTH_TOKEN_FILE,
    #     # )
    #     # auth_logger.debug(f"Pulled from encoded token info.")
        
    #     # # elif in container:
    #     # token_info = retrieve_encoded_token_from_secret()
    #     # auth_logger.debug(f"Pulled token from file.")

    #     # # EXIT Senarios - if encoded token is also None or Expired
    #     # # -------------------------------------------------
    #     # if token_info is None:
    #     #     auth_logger.debug("Pulled token is None!")
    #     #     auth_logger.error("Pulled token is None!! Update the token_info value using offline authentication code!!")
    #     #     sys.exit("Update the token_info value using offline authentication code!!")

    #     if (curr_time >= token_info["expires_at_seconds"]):
    #         auth_logger.debug("Pulled token EXPIRED!")
    #         auth_logger.error("Pulled token is EXPIRED!! Update the token_info value using offline authentication code!!")
    #         sys.exit("Update the token_info value using offline authentication code!!")

    #     # Valid Token Scenarios
    #     # -------------------------------------------------
    #     store_token_to_cache(token=token_info)

    try:
        # authorizer = get_authorizer(token=token_info["access_token"])
        authorizer = get_refresh_authorizer(
            client_id=client_id,
            access_token_info=token_info,
            on_refresh_call=store_token_to_cache,
        )
        
        auth_logger.debug("Authentication successful!")
        return authorizer
    
    except Exception as e:
        # Incase of unknown authentication issue!
        auth_logger.debug(f"Exception: {e}")
        raise ValueError("Issue with Authentication!!")

































# # TODO: To be tested
# def is_token_valid(token_info):
#     # Check if token_info exists
#     if token_info is None:
#         return False
    
#     # Check if all the required fields in token_info exist
#     valid_token_key_list = [
#         "scope",
#         "access_token",
#         "refresh_token",
#         "token_type",
#         "expires_at_seconds",
#         "resource_server",
#     ]

#     if not set(valid_token_key_list).issubset(token_info.keys()):
#         missing_keys = list(set(valid_token_key_list) - set(token_info.keys()))
#         auth_logger.warning(f"Token info missing the following required values: {missing_keys}")
#         return False
    
#     # Check if token_info is expired
#     curr_time = time.time()
#     if token_info['expires_at_seconds'] < curr_time:
#         return False

#     # Return True if all the other cases are False.
#     return True


# # TODO: To be tested
# # Only testable when running inside the container
# def get_valid_token_from_spin_secrets(
#         client_id: str,
#         uuid: str,
# ):
#     import json
#     import base64
#     from kubernetes import client, config

#     # Load Kubernetes configuration
#     config.load_kube_config()

#     # Initialize the Kubernetes API client
#     v1 = client.CoreV1Api()

#     # Name and namespace of your secret
#     secret_name = 'auth-token-info'     # TODO: store in util_constants.py
#     namespace = 'globus-namespace'      # TODO: store in util_constants.py

#     # Fetch the secret
#     secret = v1.read_namespaced_secret(secret_name, namespace)
#     encoded_data = secret.data['token-info']  # TODO: store in util_constants.py

#     # Decode the base64-encoded string
#     decoded_data = base64.b64decode(encoded_data).decode('utf-8')

#     # Convert the JSON string back to a dictionary
#     token_info = json.loads(decoded_data)

#     # print("Decoded dictionary:")
#     # print(token_info)

#     if is_token_valid(token_info=token_info):
#         return token_info
#     else:
#         return None


# # TODO: To be tested
# def get_valid_token_from_cache(args):
#     token_info = retrieve_token_from_cache()

#     if is_token_valid(token_info=token_info):
#         return token_info
#     else:
#         return None


# # TODO: To be tested
# def authentication(
#         client_id: str,
#         uuids: list,
#         # interactive: bool=False,
# ):
#     auth_logger.info("Authenticating...")

#     # Check from SPIN secrets
#     token_info = get_valid_token_from_spin_secrets()

#     # Check from cache
#     if token_info is None:
#         token_info = get_valid_token_from_cache()

#     # # Generate a new token using a separate code. Do not mix them!
#     # if token_info is None:
#     #     token_info = generate_new_token()

#     # Ask user to authenticate if none are valid
#     if token_info is None:
#         raise Exception("Valid authentication does not exist. The stored token might be expired or invalid. Generate a new token offline and copy-paste it in SPIN secrets (is using on SPIN container) or cache (if using outside SPIN).")