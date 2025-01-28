"""Module contains utils for common AWS functionality (excluding s3)"""
from collections.abc import Iterator


def get_secrets_manager_client():
    import boto3  # delay this import so Airflow has less to load at top level DAG
    session = boto3.session.Session()
    sm = session.client("secretsmanager", region_name="us-east-1")
    return sm


def get_ssm_client():
    import boto3  # delay this import so Airflow has less to load at top level DAG
    ssm = boto3.client("ssm", region_name="us-east-1")
    return ssm


def get_param_store_value(name: str, decrypted=True) -> str:
    """Returns the value from AWS parameter store for given key.

    Args:
        name (str): The parameter name to return value for from parameter store.
        decrypted (bool): Default: True. True returns decrypted values for secure string parameters.
            This flag is ignored for String and StringList parameter types.

    Raises:
        ParameterNotFound: if given key does not exist.
        AccessDeniedException: if AWS IAM policies aren't granted for authenticated user to access param.
            or if the given key is in a parameter domain that does not exist.
    """
    response = get_ssm_client().get_parameter(Name=name, WithDecryption=decrypted)
    return response["Parameter"]["Value"].strip()


def get_param_store_values(names: list[str], decrypted=True) -> dict[str, str]:
    """Returns dict of all values retrieved from AWS parameter store for given keys as Dict of keys: values

    Args:
        names (List(str)): List of parameter names to return values for.
        decrypted (bool): Default: True. True returns decrypted values for secure string parameters.
            This flag is ignored for String and StringList parameter types.

    Raises:
        ParameterNotFound: if any one of the given keys does not exist.
        AccessDeniedException: if AWS IAM policies aren't granted for authenticated user to access a param.
            or if any given key is in a parameter domain that does not exist.
    """
    response = get_ssm_client().get_parameters(Names=names, WithDecryption=decrypted)
    values = {p["Name"]: p["Value"] for p in response["Parameters"]}
    # ssm will return only the keys that it can find and doesn't raise. Thus, manually raising to avoid confusion.
    for name in names:
        if values.get(name, None) is None:
            get_param_store_value(name)  # Will raise consistent error messaging for missing key
    return values


def get_secret(name: str) -> str:
    """
    Returns secret from AWS secrets manager for given name.

    Raises:
        ClientError (AccessDeniedException) if name contains path caller doesn't have access to.
        ClientError (ResourceNotFoundException) if caller has permission to retrieve from name path,
            but name does not exist.
    """
    response = get_secrets_manager_client().get_secret_value(SecretId=name)
    return response['SecretString']


def list_secrets(filters: list[dict] = None, max_results: int = 1000) -> Iterator:
    """
    Returns paginated Iterator containing metadata for existing secrets from AWS Secrets Manager.
    See boto3 sm list_secrets method docs for what the response dict contains.
    For example you can retrieve the secret name using secret["Name"]
    Use get_secret function above to retrieve actual secret string.

    Args:
        filters (List[dict]): List of Filters to apply to the boto3 list_secrets() call. See boto3 docs for specifics.
        max_results (int): The maximum number of results to return.

    """
    paginator = get_secrets_manager_client().get_paginator('list_secrets')
    for page in paginator.paginate(PaginationConfig={'MaxItems': max_results}, Filters=filters or []):
        for secret in page['SecretList']:
            yield secret
