"""Disable users based on TS-1716
DAG should run the latest/most current run only, thus setting catchup=False.
"""
from airflow_utils import dag
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.constants import (SQL_SCRIPTS_DIR, FRAUD_DISABLER_IMAGE, FRAUD_DISABLER_BASE_KUBERNETES_ENV,
                                        FRAUD_DISABLER_ARGUMENTS, FRAUD_DISABLER_CONTAINER_RESOURCES)
from kubernetes.client.models import V1EnvVar
from datetime import datetime, timedelta
from de_utils.constants import SF_BUCKET


@dag(
    start_date=datetime(2023, 10, 5, 12, 30),
    schedule="*/30 * * * *",
    default_args={
        "owner": "Dheeraj",
        "retry_delay": timedelta(minutes=10),
        "retries": 3
    },
    template_searchpath=f"{SQL_SCRIPTS_DIR}/antifraud",
    max_active_runs=1,
    catchup=False
)
def device_level_account_creation_disabler():
    s3_dest_key = "antifraud/device-account-creation-limiting-disable/usernames_{{ ts }}"

    sf_dump = SnowflakeToS3DeferrableOperator(
        task_id="dump_usernames_s3",
        sql="device_level_account_creation_disabler.sql",
        s3_bucket=SF_BUCKET,
        s3_key=s3_dest_key,
        file_format="(TYPE = CSV COMPRESSION = NONE EMPTY_FIELD_AS_NULL = FALSE NULL_IF = (''))",
        copy_options="OVERWRITE = TRUE SINGLE = TRUE HEADER = FALSE, MAX_FILE_SIZE =4900000000",
    )

    disable = KubernetesPodOperator(
        task_id="disable_users",
        name="disable_users",
        image=FRAUD_DISABLER_IMAGE,
        arguments=FRAUD_DISABLER_ARGUMENTS,
        env_vars=FRAUD_DISABLER_BASE_KUBERNETES_ENV + [
            V1EnvVar("TN_REASON", "device-account-creation-limiting-disable"),
            V1EnvVar("TN_FILE", f"s3://{SF_BUCKET}/{s3_dest_key}"),
            V1EnvVar("TN_REVOKE", "FALSE"),
            V1EnvVar("TN_RESERVE_NUMBER", "FALSE"),
            V1EnvVar("TN_ACCOUNT_STATUS", "HardDisabled")
        ],
        container_resources=FRAUD_DISABLER_CONTAINER_RESOURCES
    )

    sf_dump >> disable


device_level_account_creation_disabler()

if __name__ == "__main__":
    device_level_account_creation_disabler().test(execution_date="2023-10-05 11:00:00")
