"""Disable users based on request from Kamlesh
https://textnow.slack.com/archives/C043VMZ27FZ/p1681832129885809
DAG should run the latest/most current run only, thus setting catchup=False.
"""
from airflow_utils import dag
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.constants import (FRAUD_DISABLER_BASE_KUBERNETES_ENV, FRAUD_DISABLER_IMAGE,
                                        FRAUD_DISABLER_ARGUMENTS, FRAUD_DISABLER_CONTAINER_RESOURCES)
from de_utils.constants import SF_BUCKET
from kubernetes.client.models import V1EnvVar
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 10, 31),
    schedule="40 */4 * * *",
    default_args={
        "owner": "Dheeraj",
        "retry_delay": timedelta(minutes=10),
        "retries": 3
    },
    max_active_runs=1,
    catchup=False
)
def spam_calling_disabler():
    s3_dest_key = "antifraud/spam-calling-device-disable/usernames_{{ ts }}"

    sf_dump = SnowflakeToS3DeferrableOperator(
        task_id="dump_usernames_s3",
        sql="""
            WITH daily_devices AS (
                SELECT DISTINCT
                    date_utc,
                    username,
                    adid,
                    COUNT(distinct adid) OVER (PARTITION BY date_utc, username) AS num_associated_devices
                from dau.user_device_master
                WHERE (date_utc >= GETDATE() - INTERVAL '3 days')
            ), 

            call_info AS (
                SELECT DISTINCT 
                    cc.created_at::DATE AS date_utc,
                    username,
                    COUNT(*) AS num_calls
                FROM party_planner_realtime.callcompleted cc
                LEFT JOIN analytics.users u ON (cc.user_id_hex = u.user_id_hex)
                WHERE 
                  (date_utc >= getdate() - INTERVAL '3 days')
                  AND ("payload.call_direction" = 'CALL_DIRECTION_OUTBOUND')
                  AND ("client_details.client_data.client_platform" != 'WEB')
                GROUP BY 1, 2
            ),

            spam_users AS (
                SELECT DISTINCT 
                    ci.date_utc,
                    ci.username,
                    account_status,
                    num_calls AS num_calls_by_users, -- # of calls have been made by the account on the day
                    num_associated_devices, -- # of devices the account have been used on the day
                    adid,
                    -- # of potential calls have been made by the device on the day
                    SUM(CASE WHEN (adid IS NULL OR adid = '') THEN 0 ELSE num_calls END) 
                        OVER (PARTITION BY ci.date_utc, adid) AS total_associated_calls
                FROM call_info ci
                LEFT JOIN daily_devices dd ON (ci.date_utc = dd.date_utc) AND (ci.username = dd.username)
                LEFT JOIN core.users u ON (ci.username = u.username)
                QUALIFY (total_associated_calls >= 200)
            )

            -- some runs will not have any users and downstream disable task will fail as no s3 object will be written
            -- so add a dummy user who is already disabled
            SELECT DISTINCT username FROM spam_users WHERE (account_status != 'HARD_DISABLED')
            UNION ALL SELECT 'dummy_spam_user'
        """,
        s3_bucket=SF_BUCKET,
        s3_key=s3_dest_key,
        file_format="(TYPE = CSV COMPRESSION = NONE EMPTY_FIELD_AS_NULL = FALSE NULL_IF = (''))",
        copy_options="OVERWRITE = TRUE SINGLE = TRUE HEADER = FALSE, MAX_FILE_SIZE =4900000000",
        poll_interval=30  # usually quick loads, thus keeping poll interval small
    )

    disable = KubernetesPodOperator(
        task_id="disable_users",
        name="disable_users",
        image=FRAUD_DISABLER_IMAGE,
        arguments=FRAUD_DISABLER_ARGUMENTS,
        env_vars=FRAUD_DISABLER_BASE_KUBERNETES_ENV + [
            V1EnvVar("TN_REASON", "spam-calling-device-linked-disable"),
            V1EnvVar("TN_FILE", f"s3://{SF_BUCKET}/{s3_dest_key}"),
            V1EnvVar("TN_REVOKE", "FALSE"),
            V1EnvVar("TN_RESERVE_NUMBER", "FALSE"),
            V1EnvVar("TN_ACCOUNT_STATUS", "HardDisabled")
        ],
        container_resources=FRAUD_DISABLER_CONTAINER_RESOURCES
    )

    sf_dump >> disable


spam_calling_disabler()

if __name__ == "__main__":
    spam_calling_disabler().test(execution_date="2023-11-01 04:00:00")
