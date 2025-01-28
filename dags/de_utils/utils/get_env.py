from de_utils.enums import Env
import os
from functools import lru_cache


@lru_cache(maxsize=1)
def get_env() -> Env:
    """Returns the enum (Env.PROD, Env.DEV, Env.TOOLING) associated with the environment code is running in.
    Should be smart enough to detect what platform code is running in.
    Currently tested for MWAA and Kubernetes Pod Runs running in AWS accounts prod, dev, and tooling.
    If the env cannot be determined, returns Env.DEV."""
    import boto3
    from de_utils.constants import PROD_ACCOUNT_ID, TOOLING_ACCOUNT_ID, DEV_ACCOUNT_ID
    client = boto3.client("sts")
    try:
        account_id = client.get_caller_identity()["Account"]
        if account_id == str(PROD_ACCOUNT_ID):
            return Env.PROD
        elif account_id == str(TOOLING_ACCOUNT_ID):
            return Env.TOOLING
        elif account_id == str(DEV_ACCOUNT_ID):
            return Env.DEV
    except Exception:
        pass  # continue on to try backup way to get env

    env = os.getenv("AIRFLOW__CUSTOM__ENV")  # MWAA env variable
    if not env:
        return Env.DEV
    else:
        return Env(env)
