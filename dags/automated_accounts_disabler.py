"""DAG runs pipeline to disable automated account creations based on embrace, leanplum and adtracker signals.
Excludes accounts that show some real user behaviour even though it may be fraud.
The main purpose is to prevent these dormant accounts from accumulating for future use.
"""
from airflow_utils import dag
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.sensors.external_task_deferred_sensor import ExternalTaskDeferredSensor
from dag_dependencies.constants import (
    DAG_DEFAULT_CATCHUP, SQL_SCRIPTS_DIR, FRAUD_DISABLER_CONTAINER_RESOURCES,
    FRAUD_DISABLER_IMAGE, FRAUD_DISABLER_ARGUMENTS, FRAUD_DISABLER_BASE_KUBERNETES_ENV
)
from de_utils.constants import SF_BUCKET, SF_WH_LARGE
from de_utils.slack.alerts import alert_on_failure
from kubernetes.client.models import V1EnvVar
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 1, 2),
    schedule=None,  # triggered by dbt_downstream_dags_triggerer DAG
    default_args={
        "owner": "Dheeraj",
        "retry_delay": timedelta(minutes=1),
        "retries": 1,
        "on_failure_callback": alert_on_failure,
    },
    template_searchpath=f"{SQL_SCRIPTS_DIR}/antifraud",
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP,
)
def automated_accounts_disabler():
    s3_dest_key_v2 = "antifraud/automated_accounts_v2/usernames_{{ ts }}"

    wait_user_summary = ExternalTaskDeferredSensor(
        task_id="wait_user_summary",
        external_dag_id="update_registration_fraud_summary",
        execution_date_fn=lambda dt: dt.replace(hour=7, minute=15),
        timeout=3600 * 3  # wait for up to 3 hours
    )

    sf_dump2 = SnowflakeToS3DeferrableOperator(
        task_id="dump_usernames_s3_v2",
        sql="automated_accounts_disabler_v2.sql",
        warehouse=SF_WH_LARGE,
        s3_bucket=SF_BUCKET,
        s3_key=s3_dest_key_v2,
        file_format=r"(TYPE = CSV COMPRESSION = NONE EMPTY_FIELD_AS_NULL = FALSE NULL_IF = (''))",
        copy_options="OVERWRITE = TRUE HEADER = FALSE SINGLE = TRUE MAX_FILE_SIZE =4900000000"
    )

    disable = KubernetesPodOperator(
        task_id="disable_automated_accounts",
        name="disable_automated_accts",
        image=FRAUD_DISABLER_IMAGE,
        arguments=FRAUD_DISABLER_ARGUMENTS,
        env_vars=FRAUD_DISABLER_BASE_KUBERNETES_ENV + [
            V1EnvVar("TN_REASON", "automated-accounts-scheduled_v2"),
            V1EnvVar("TN_FILE", f"s3://{SF_BUCKET}/{s3_dest_key_v2}"),
            V1EnvVar("TN_REVOKE", "FALSE"),
            V1EnvVar("TN_RESERVE_NUMBER", "FALSE"),
            V1EnvVar("TN_ACCOUNT_STATUS", "HardDisabled")
        ],
        container_resources=FRAUD_DISABLER_CONTAINER_RESOURCES
    )

    wait_user_summary >> sf_dump2 >> disable


automated_accounts_disabler()

if __name__ == "__main__":
    automated_accounts_disabler().test(execution_date='2023-11-15')
