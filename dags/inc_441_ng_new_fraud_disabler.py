"""Disable scammers from West Africa based on LP session data"""
from airflow_utils import dag
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.constants import (FRAUD_DISABLER_BASE_KUBERNETES_ENV, FRAUD_DISABLER_IMAGE,
                                        FRAUD_DISABLER_ARGUMENTS, FRAUD_DISABLER_CONTAINER_RESOURCES)
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import SF_BUCKET
from kubernetes.client.models import V1EnvVar
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 1, 2),
    schedule="15 7 * * *",
    default_args={
        "owner": "Ross",
        "retry_delay": timedelta(minutes=60),
        "retries": 3,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=False
)
def inc_441_ng_new_fraud_disabler():
    s3_dest_key = "antifraud/INC_441_lp_session_ng_gh/usernames_{{ ts }}"

    sf_dump = SnowflakeToS3DeferrableOperator(
        task_id="dump_usernames_s3",
        sql="""
            WITH bad_sessions AS (
                SELECT 
                    username,
                    SUM(LEAST(600, DATEDIFF('seconds', event_time, next_event_time))) total_duration_applifecycle 
                FROM prod.analytics_staging.applifecycle_events
                WHERE
                    (event = 'APP_LIFECYCLE_FOREGROUNDED')
                    AND (next_event = 'APP_LIFECYCLE_BACKGROUNDED')
                    AND date_utc BETWEEN CURRENT_DATE - INTERVAL '3 days' AND CURRENT_DATE
                    AND (client_country_code IN ('NG', 'GH') 
                        OR tz_code IN ('Africa/Lagos', 'Africa/Accra', 'Africa/Porto-Novo', 'WAT'))
                    AND LENGTH(username) > 0
                GROUP BY 1
                HAVING total_duration_applifecycle > 120
            )
            SELECT username
            FROM bad_sessions
            JOIN core.users USING (username)
            WHERE
                (username NOT IN ('preccyjay', 'maaryaam7272', 'jefehunter420', 'jefepimp69', 
                                  'saintrhemmys', 'navidfa6'))
                AND (account_status != 'HARD_DISABLED')
                AND (username NOT IN (SELECT username FROM public.username_exclusion_list_ts))
        """,
        s3_bucket=SF_BUCKET,
        s3_key=s3_dest_key,
        file_format=r"(TYPE = CSV COMPRESSION = NONE EMPTY_FIELD_AS_NULL = FALSE NULL_IF = (''))",
        copy_options="OVERWRITE = TRUE SINGLE = TRUE HEADER = FALSE MAX_FILE_SIZE =4900000000",
        poll_interval=30
    )

    disable = KubernetesPodOperator(
        task_id="disable_ng_gh_lp_session_users",
        name="disable_ng_gh",
        image=FRAUD_DISABLER_IMAGE,
        arguments=FRAUD_DISABLER_ARGUMENTS,
        env_vars=FRAUD_DISABLER_BASE_KUBERNETES_ENV + [
            V1EnvVar("TN_REASON", "inc-441-ng-gh-applifecycle-sessions"),
            V1EnvVar("TN_FILE", f"s3://{SF_BUCKET}/{s3_dest_key}"),
            V1EnvVar("TN_REVOKE", "FALSE"),
            V1EnvVar("TN_RESERVE_NUMBER", "FALSE"),
            V1EnvVar("TN_ACCOUNT_STATUS", "HardDisabled")
        ],
        container_resources=FRAUD_DISABLER_CONTAINER_RESOURCES
    )

    sf_dump >> disable


inc_441_ng_new_fraud_disabler()
