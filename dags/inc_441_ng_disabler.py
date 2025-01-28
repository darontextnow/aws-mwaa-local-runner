"""Disable users based on NG scammers device links as soon as core.registrations is updated.
It also depends on analytics_staging.dau_user_device_history
"""
from airflow_utils import dag
from dag_dependencies.sensors.external_task_deferred_sensor import ExternalTaskDeferredSensor
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.constants import (DAG_DEFAULT_CATCHUP, FRAUD_DISABLER_IMAGE, FRAUD_DISABLER_BASE_KUBERNETES_ENV,
                                        FRAUD_DISABLER_ARGUMENTS, FRAUD_DISABLER_CONTAINER_RESOURCES)
from de_utils.constants import SF_BUCKET, SF_WH_LARGE
from kubernetes.client.models import V1EnvVar
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 6, 20, 9, 25),
    schedule="25 1,5,9,13,17,21 * * *",  # every 4 hours coordinated with dbt_snowflake_4h DAG (running 10m later)
    default_args={
        "owner": "dheeraj",
        "retry_delay": timedelta(minutes=1),
        "retries": 3,
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def inc_441_ng_disabler():

    s3_key = "antifraud/INC_441_device_links/usernames_{{ ts }}"

    wait_for_registrations = ExternalTaskDeferredSensor(
        task_id="wait_for_registrations",
        external_dag_id="dbt_snowflake_4h",
        execution_date_fn=lambda dt: dt.replace(minute=15),  # dbt_snowflake_4h runs 15m after same hour as this runs
        timeout=3600 * 4  # wait for 4 hours
    )

    sf_dump = SnowflakeToS3DeferrableOperator(
        task_id="dump_usernames_s3",
        sql="""
            WITH dis_users AS (
                SELECT DISTINCT username
                FROM core.disabled
                JOIN core.users ON (global_id = user_id_hex)
                WHERE 
                    (
                    (LOWER(disable_reason) LIKE 'inc%441%nig%')
                    OR (LOWER(disable_reason) LIKE 'inc%441%ng%')
                    OR (LOWER(disable_reason) LIKE 'inc%718%')
                    OR (LOWER(disable_reason) LIKE '%swatting%')
                    OR (LOWER(disable_reason) LIKE '%shooting%')
                    OR disable_reason ilike '%spam%call%'
                    OR disable_reason ilike '%bank%scam%'
                    OR disable_reason ilike '%spam%dial%'
                    OR disable_reason ilike '%robo%dial%'
                    OR disable_reason ilike '%robo%call%'
                    OR disable_reason ilike '%call%pump%'
                    OR disable_reason ilike '%911%abuse%'
                    OR disable_reason ilike '%third%party%'
                    OR disable_reason ilike '%traceback%'
                    OR disable_reason ilike '%emergency%'
                    OR disable_reason ilike '%toll%fraud%'
                    OR disable_reason ilike '%outbound_call%'
                    OR disable_reason ilike '%law%enforce%'
                    OR disable_reason ilike 'risky%short%call%'
                    OR disable_reason ilike '%harassment%'
                    OR disable_reason ilike '%impersonation%'
                    OR disable_reason ilike '%bank%fraud%'
                    OR disable_reason ilike '%call%pumping%'
                    OR disable_reason ilike '%megapersonal%'
                    OR disable_reason ilike '%adult%link%'
                    OR disable_reason ilike '%crypto%'
                    OR disable_reason like '%PB%' --Pig Butchering scam
                    OR disable_reason like '% SB' -- Scam baiter
                    ) 
                    AND (account_status != 'ENABLED')
                    AND (username NOT LIKE 'efqa_%')
                    AND (username NOT LIKE 'lss%')
            ),
            recent_disables AS (
                SELECT DISTINCT 
                    adid, 
                    COUNT(*) num, 
                    SUM(CASE WHEN account_status='HARD_DISABLED' THEN 1 ELSE 0 END) num_disabled 
                FROM core.registrations a
                JOIN core.users USING (username)
                WHERE (a.created_at > CURRENT_DATE - INTERVAL '2 days')
                GROUP BY 1
                HAVING (num > 2) AND (num_disabled * 2 > num)
            ),
            automated_disables AS (
                SELECT global_id,disable_reason,disabled_at
                FROM core.disabled 
                WHERE (disable_reason LIKE 'automated%accounts%')
                QUALIFY ROW_NUMBER() OVER(PARTITION BY global_id ORDER BY disabled_at DESC) = 1
            ),
            automated_disable_adid AS (
                SELECT c.adid,count(*) num_users
                FROM automated_disables a
                JOIN core.users b ON (a.global_id = b.user_id_hex)
                JOIN core.registrations c ON (b.username = c.username)
                WHERE 
                    (account_status = 'HARD_DISABLED')
                    AND (c.created_at > '2022-05-01')
                GROUP BY 1
                HAVING (num_users > 2)
            ),
            devices AS (
                SELECT DISTINCT adid
                FROM analytics_staging.dau_user_device_history
                WHERE 
                    (username IN (SELECT * FROM dis_users))
                    AND (last_date_seen >= '2020-01-01')
                    AND (adid NOT IN ('bc9cf1574a5ff632b974a44dbb4597bf', 
                                      '02f4d4aa94665ce37b70333c33e747ac', 
                                      'a82275a6322f43979296700d4b30ea73'))
                    AND (array_to_string(sources_info,',') NOT IN ('core.sessions (android)','core.sessions (ios)'))
                    AND (adid NOT IN (SELECT DISTINCT adid FROM core.ng_fp_enable))

                UNION SELECT adid
                FROM recent_disables
            ),
            idfvs AS (
                SELECT DISTINCT idfv 
                FROM core.registrations
                WHERE
                    (username IN (SELECT * FROM dis_users))
                    AND (created_at >= '2021-01-01')
                    AND (nvl(adid,'NA') NOT IN ('bc9cf1574a5ff632b974a44dbb4597bf', 
                                                '02f4d4aa94665ce37b70333c33e747ac', 
                                                'a82275a6322f43979296700d4b30ea73'))
                    AND (nvl(adid,'NA') NOT IN (SELECT DISTINCT adid FROM core.ng_fp_enable))
                    AND (LEN(idfv) > 10)
            ),
            previous_disable AS (
                SELECT DISTINCT username 
                FROM core.disabled
                JOIN core.users ON (global_id = user_id_hex)
                WHERE (disable_reason LIKE 'inc-441%')
            )

            SELECT DISTINCT a.username
            FROM core.registrations a
            JOIN core.users b ON (a.username = b.username)
            LEFT JOIN previous_disable c ON (a.username = c.username)
            WHERE 
                (a.created_at > '{{ ts }}'::TIMESTAMP - interval '48 hours')
                AND (account_status != 'HARD_DISABLED')
                AND (client_type IN ('TN_ANDROID', 'TN_IOS_FREE', '2L_ANDROID'))
                AND (
                    (adid IN (SELECT * FROM devices))
                    OR (idfv IN (SELECT idfv FROM idfvs))
                    OR (adid IN (SELECT adid FROM automated_disable_adid))
                )
                AND (c.username IS NULL)
        """,
        warehouse=SF_WH_LARGE,
        s3_bucket=SF_BUCKET,
        s3_key=s3_key,
        file_format=r"(TYPE = CSV COMPRESSION = NONE EMPTY_FIELD_AS_NULL = FALSE NULL_IF = (''))",
        copy_options="OVERWRITE = TRUE SINGLE = TRUE HEADER = FALSE MAX_FILE_SIZE =4900000000"
    )

    disable = KubernetesPodOperator(
        task_id="disable_ng_device_links",
        name="disable_ng_device_links",
        image=FRAUD_DISABLER_IMAGE,
        arguments=FRAUD_DISABLER_ARGUMENTS,
        env_vars=FRAUD_DISABLER_BASE_KUBERNETES_ENV + [
            V1EnvVar("TN_REASON", "inc-441-ng-device-links-scheduled"),
            V1EnvVar("TN_FILE", f"s3://{SF_BUCKET}/{s3_key}"),
            V1EnvVar("TN_REVOKE", "FALSE"),
            V1EnvVar("TN_RESERVE_NUMBER", "FALSE"),
            V1EnvVar("TN_ACCOUNT_STATUS", "HardDisabled")
        ],
        container_resources=FRAUD_DISABLER_CONTAINER_RESOURCES
    )

    wait_for_registrations >> sf_dump >> disable


inc_441_ng_disabler()

if __name__ == "__main__":
    inc_441_ng_disabler().test(execution_date="2025-01-14 01:25:00")
