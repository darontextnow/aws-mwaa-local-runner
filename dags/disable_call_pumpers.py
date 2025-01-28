"""Disable call pumpers
adapted from:
https://github.com/Enflick/textnow-mono/blob/67e7d1c36d3103c7cfaa26c4cb3d30979b61ac1f/
services/sketchy/disable/cmd/disablecli/call_pumping.go
"""
from airflow_utils import dag, task
from kubernetes.client.models import V1EnvVar
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.constants import (
    SQL_SCRIPTS_DIR, DAG_DEFAULT_CATCHUP, FRAUD_DISABLER_BASE_KUBERNETES_ENV,
    FRAUD_DISABLER_ARGUMENTS, FRAUD_DISABLER_IMAGE, FRAUD_DISABLER_CONTAINER_RESOURCES
)
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import ENV_ENUM, SF_BUCKET
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 9, 25),
    schedule="3 10 * * *",
    default_args={
        "owner": "Eric",
        "retry_delay": timedelta(minutes=60),
        "retries": 3,
        "on_failure_callback": alert_on_failure
    },
    template_searchpath=f"{SQL_SCRIPTS_DIR}/antifraud",
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def disable_call_pumpers():
    s3_dest_key = "antifraud/call_pumpers/usernames_{{ ts }}"
    s3_dest = f"s3://{SF_BUCKET}/{s3_dest_key}"

    dump_users_to_s3 = SnowflakeToS3DeferrableOperator(
        task_id="dump_usernames_s3",
        sql="disable_call_pumpers_source_data.sql",
        s3_bucket=SF_BUCKET,
        s3_key=s3_dest_key,
        file_format="(TYPE = CSV COMPRESSION = NONE EMPTY_FIELD_AS_NULL = FALSE NULL_IF = (''))",
        copy_options="OVERWRITE = TRUE SINGLE = TRUE HEADER = FALSE, MAX_FILE_SIZE =4900000000"
    )

    verify_data_exists = S3KeySensor(
        task_id="verify_data_exists",
        bucket_key=s3_dest_key,
        bucket_name=SF_BUCKET,
        soft_fail=True,
        timeout=1,
        poke_interval=1
    )

    disable = KubernetesPodOperator(
        task_id="disable_call_pumpers",
        name=f"disable_callpump_{str(datetime.now().timestamp()).split('.')[0]}",
        image=FRAUD_DISABLER_IMAGE,
        arguments=FRAUD_DISABLER_ARGUMENTS,
        env_vars=FRAUD_DISABLER_BASE_KUBERNETES_ENV + [
            V1EnvVar("TN_REASON", "Call pumping - scheduled"),
            V1EnvVar("TN_FILE", s3_dest),
            V1EnvVar("TN_REVOKE", "FALSE"),
            V1EnvVar("TN_RESERVE_NUMBER", "FALSE"),
            V1EnvVar("TN_ACCOUNT_STATUS", "HardDisabled")
        ],
        container_resources=FRAUD_DISABLER_CONTAINER_RESOURCES
    )

    @task
    def send_slack_alert(s3_dest, task=None):
        """This function carries out the following tasks:
        1. check row count to usernames to be disabled
        2. Report the prefixes to Slack
        """
        from de_utils.slack import send_message
        from de_utils.slack.tokens import get_de_alerts_token
        from de_utils.aws import S3Path

        user_count = S3Path(s3_dest).read().count('\n')
        # alert Slack with users who have been disabled
        msg = f"{user_count} call pumpers were disabled - list of usernames in <{s3_dest}|S3 here>"
        send_message(env=ENV_ENUM,
                     channel="call_pumping_alerts",
                     message=msg,
                     get_auth_token_func=get_de_alerts_token,
                     username="airflow",
                     icon_emoji=":loudspeaker:")

        task.log.info("Slack alert message: ", msg)

    dump_users_to_s3 >> verify_data_exists >> disable >> send_slack_alert(s3_dest)


disable_call_pumpers()

if __name__ == "__main__":
    disable_call_pumpers().test(execution_date='2023-09-25')
