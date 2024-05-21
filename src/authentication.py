import os
import time
import json
import globus_sdk

import util_constants as c

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

    # Dump the token dictionary to a JSON file
    with open(token_cache_filepath, "w") as file:
        json.dump(token, file)


def retrieve_token_from_cache(filepath=c.TOKEN_CACHE_PATH):
    if not os.path.exists(filepath):
        return None

    with open(filepath, "r") as file:
        token_dict = json.load(file)
    
    return token_dict


# Main authentication function
def authenticate(
        # args,
        client_id: str,
        uuids: list,
        force_new_token: bool=False,
):
    auth_logger.info(f"Authenticating...")

    token_info = retrieve_token_from_cache()
    auth_logger.debug(f"Pulled token from cache.")

    curr_time = time.time()
    if force_new_token or \
        (token_info is None) or \
        (curr_time >= token_info["expires_at_seconds"]):

        if force_new_token:
            auth_logger.debug("Force generating new token!")
        elif token_info is None:
            auth_logger.debug("No cached token!")
        elif curr_time >= token_info["expires_at_seconds"]:
            auth_logger.debug("Cached token expired!")

        token_info = get_authentication_token(client_id=client_id, uuids=uuids)

        store_token_to_cache(token=token_info)
    
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