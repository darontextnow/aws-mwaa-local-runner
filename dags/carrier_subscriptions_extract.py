"""DAG extracts data from various tables in MySQL subscription DB and updates Snowflake tables.

DAG designed to self catch up on next run should any hourly run fail.
"""
from airflow_utils import dag
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from de_utils.constants import SF_WH_SMALL
from datetime import datetime, timedelta


@dag(
        start_date=datetime(2023, 8, 23),
        schedule="20 * * * *",
        default_args={
            "owner": "DE Team",
            "retry_delay": timedelta(minutes=5),
            "retries": 3
        },
        max_active_runs=1,
        catchup=False
)
def carrier_subscriptions_extract():
    mysql_conn_id = "carrier_subscription_service"

    # there is a bug that updates rows even after they are deleted, we cap updated_at by deleted_at whenever possible
    sql = """
    SELECT 
        TRIM(COALESCE(device_id, '')) AS device_id,
        TRIM(COALESCE(iccid, '')) AS iccid,
        mdn,
        msid,
        nai,
        activation_status,
        carrier_plan_id,
        textnow_plan_id,
        ip_address,
        imsi,
        carrier,
        created_at,
        GREATEST(updated_at, COALESCE(deleted_at, '1990-01-01 00:00:00')) AS updated_at,
        deleted_at
    FROM subscriptions
    WHERE NOT (TRIM(COALESCE(device_id, '')) = '' AND TRIM(COALESCE(iccid, '')) = '')
    """
    # not a lot of data right now, just truncate and reload until they address more bugs
    _, carrier_subscription_load = mysql_to_snowflake_task_group(
        mysql_conn_id=mysql_conn_id,
        sql=sql,
        target_table="carrier_subscriptions",
        target_schema="core",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.TRUNCATE
    )

    insert_history = SnowflakeAsyncDeferredOperator(
        task_id="subscriptions_history_snowflake",
        sql="""INSERT INTO core.carrier_subscriptions_history
            SELECT 
                s.device_id,
                s.iccid,
                s.mdn,
                s.msid,
                s.nai,
                s.activation_status,
                s.carrier_plan_id,
                s.textnow_plan_id,
                s.ip_address,
                s.imsi,
                s.carrier,
                s.created_at,
                s.updated_at,
                s.deleted_at,
                '{{ data_interval_start }}'::TIMESTAMP AS record_injected_at
            FROM (
                SELECT 
                    TRIM(COALESCE(device_id, '')) AS device_id,
                    TRIM(COALESCE(iccid, '')) AS iccid,
                    TRIM(COALESCE(mdn, '')) AS mdn,
                    msid,
                    nai,
                    TRIM(COALESCE(activation_status, '')) AS activation_status,
                    TRIM(COALESCE(carrier_plan_id, '')) AS carrier_plan_id,
                    textnow_plan_id,
                    ip_address,
                    TRIM(COALESCE(imsi, '')) AS imsi,
                    carrier,
                    created_at,
                    updated_at,
                    deleted_at,
                    ROW_NUMBER() OVER (PARTITION BY iccid ORDER BY created_at DESC) AS rank
                FROM prod.core.carrier_subscriptions
                WHERE
                    (TRIM(COALESCE(iccid, '')) <> '')
                    AND (TRIM(COALESCE(activation_status, '')) <> '')
                    AND (TRIM(COALESCE(carrier_plan_id, '')) <> '')
            ) s
            LEFT JOIN (
                SELECT 
                    TRIM(COALESCE(iccid, '')) AS iccid,
                    MAX(updated_at) AS updated_at
                FROM prod.core.carrier_subscriptions_history
                GROUP BY 1
            ) h ON 
                (s.iccid = h.iccid)
                AND (s.updated_at <= h.updated_at)
            WHERE 
                (s.rank = 1)
                AND (s.updated_at > '{{ prev_data_interval_end_success or data_interval_start }}'::TIMESTAMP)
                AND (h.updated_at IS NULL)
    """,
        warehouse=SF_WH_SMALL
    )

    # not a lot of data right now, just truncate and reload until they address more bugs
    collect_devices, load_devices = mysql_to_snowflake_task_group(
        mysql_conn_id=mysql_conn_id,
        sql="SELECT id, iccid, msl, inventory_uuid, created_at, updated_at FROM devices",
        target_table="carrier_subscription_devices",
        target_schema="core",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.TRUNCATE
    )

    # not a lot of data right now, just truncate and reload
    collect_preactivation, _ = mysql_to_snowflake_task_group(
        mysql_conn_id=mysql_conn_id,
        sql="SELECT iccid, mdn, user, exceptioned, created_at, updated_at FROM pwg_preactivation",
        target_table="pwg_preactivation",
        target_schema="core",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.TRUNCATE
    )

    carrier_subscription_load >> insert_history >> collect_devices >> load_devices >> collect_preactivation


carrier_subscriptions_extract()

if __name__ == "__main__":
    carrier_subscriptions_extract().test()
