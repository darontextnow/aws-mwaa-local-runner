"""Disable web registrations that escape device linking at registration
but show up later on bad devices 
depends on analytics_staging.dau_user_device_history
"""
from airflow_utils import dag
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.constants import (
    FRAUD_DISABLER_IMAGE,
    FRAUD_DISABLER_ARGUMENTS,
    FRAUD_DISABLER_BASE_KUBERNETES_ENV,
    FRAUD_DISABLER_CONTAINER_RESOURCES
)
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import SF_BUCKET, SF_WH_LARGE
from kubernetes.client.models import V1EnvVar
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 1, 2),
    schedule=None,  # triggered by dbt_downstream_dags_triggerer DAG
    default_args={
        "owner": "Dheeraj",
        "retry_delay": timedelta(minutes=60),
        "retries": 3,
        "sla": timedelta(hours=24),
        "on_failure_callback": alert_on_failure
    },
    catchup=False
)
def inc_441_ng_web_reg_bad_device():
    s3_dest_key = "antifraud/INC_441_web_reg_bad_device/usernames_{{ ts }}"

    sf_dump = SnowflakeToS3DeferrableOperator(
        task_id="dump_usernames_s3",
        sql="""
            WITH dis_users AS (
                SELECT DISTINCT username 
                FROM core.disabled
                JOIN core.users ON (global_id = user_id_hex)
                WHERE
                    (LOWER(disable_reason) LIKE 'inc%441%nig%' 
                    OR LOWER(disable_reason) LIKE 'inc%441%ng%')
                    AND (account_status != 'ENABLED')
                    AND (disabled_at > '2021-11-06')
                    AND (username NOT LIKE 'qa%')
                    AND (username NOT LIKE 'lss%')
                    AND (username NOT LIKE '%test%')
            ),
            bad_devices AS (
                SELECT adid, COUNT(DISTINCT username) num_users 
                FROM analytics_staging.dau_user_device_history
                WHERE
                    (username IN (SELECT * FROM dis_users))
                    AND (last_date_seen >= '2020-01-01')
                    AND (adid NOT IN ('bc9cf1574a5ff632b974a44dbb4597bf', '02f4d4aa94665ce37b70333c33e747ac'))
                    AND (array_to_string(sources_info,',') NOT IN ('core.sessions (android)','core.sessions (ios)'))
                GROUP BY 1
                HAVING (num_users > 1)
            ),
            previous_disable AS (
                SELECT DISTINCT username 
                FROM core.disabled
                JOIN core.users ON (global_id = user_id_hex)
                WHERE (disable_reason LIKE 'inc-441%')
            )
            SELECT DISTINCT a.username
            FROM analytics_staging.dau_user_device_history a
            JOIN core.registrations b ON (a.username = b.username)
            JOIN core.users c ON (a.username = c.username)
            LEFT JOIN previous_disable p ON (a.username = p.username)
            WHERE 
                (last_date_seen > '{{ ts }}'::TIMESTAMP - INTERVAL '7 days')
                AND (b.client_type = 'TN_WEB')
                AND (a.adid IN (SELECT adid FROM bad_devices))
                AND (c.account_status != 'HARD_DISABLED')
                AND (a.username NOT IN ('maaryaam7272', 'penny093'))
                AND (p.username IS NULL)
        """,
        warehouse=SF_WH_LARGE,
        s3_bucket=SF_BUCKET,
        s3_key=s3_dest_key,
        file_format=r"(TYPE = CSV COMPRESSION = NONE EMPTY_FIELD_AS_NULL = FALSE NULL_IF = (''))",
        copy_options="OVERWRITE = TRUE SINGLE = TRUE HEADER = FALSE MAX_FILE_SIZE =4900000000",
    )

    disable = KubernetesPodOperator(
        task_id="disable_web_reg_bad_device_users",
        name="disable_ng_gh",
        image=FRAUD_DISABLER_IMAGE,
        arguments=FRAUD_DISABLER_ARGUMENTS,
        env_vars=FRAUD_DISABLER_BASE_KUBERNETES_ENV + [
            V1EnvVar("TN_REASON", "inc-441-ng-web-reg-bad-device-scheduled"),
            V1EnvVar("TN_FILE", f"s3://{SF_BUCKET}/{s3_dest_key}"),
            V1EnvVar("TN_REVOKE", "FALSE"),
            V1EnvVar("TN_RESERVE_NUMBER", "FALSE"),
            V1EnvVar("TN_ACCOUNT_STATUS", "HardDisabled")
        ],
        container_resources=FRAUD_DISABLER_CONTAINER_RESOURCES
    )

    sf_dump >> disable


inc_441_ng_web_reg_bad_device()
