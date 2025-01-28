"""DAG extracts data from MySQL User Shards Databases and loads it into Snowflake.

Should DAG not run for any particular period, there is no need to catchup. Just run latest.
"""
from airflow_utils import dag
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from de_utils.constants import SF_WH_SMALL, MYSQL_SHARDS_CONN_IDS
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 8, 29),
    schedule="35 * * * *",
    default_args={
        "owner": "Eric",
        "retry_delay": timedelta(minutes=5),
        "retries": 3
    },
    max_active_runs=1,
    catchup=False
)
def data_devices_extract():

    _, sf_load = mysql_to_snowflake_task_group(
        mysql_conn_id=MYSQL_SHARDS_CONN_IDS,
        sql="""
            SELECT 
                '{{ params.shard_id }}' AS shard,
                CURRENT_TIMESTAMP,
                id,
                username,
                product_id,
                esn,
                msl,
                mdn,
                msid,
                suspended,
                throttle_level,
                in_use,
                subscription_id,
                updated_at,
                created_at,
                uuid,
                ip_address,
                imsi
            FROM data_devices
            WHERE 
                (COALESCE(updated_at, created_at) >= '{{ data_interval_start }}')
        """,
        target_table="data_devices",
        target_schema="core",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE
    )

    insert_history = SnowflakeAsyncDeferredOperator(
        task_id='populate_data_devices_history',
        sql="""INSERT INTO core.data_devices_history
            SELECT DISTINCT
                d."@shard",
                d.data_device_id,
                d.username,
                d.product_id,
                d.esn,
                d.msl,
                NULLIF(d.mdn, '') AS mdn,
                d.msid,
                d.suspended,
                d.throttle_level,
                d.in_use,
                d.subscription_id,
                COALESCE(d.updated_at, d.created_at) AS updated_at,
                d.created_at,
                NULLIF(d.uuid, '') AS uuid,
                d.ip_address,
                d.imsi,
                "@loaded_utc" AS snapshot_utc
            FROM core.data_devices d
            LEFT JOIN core.data_devices_history h ON 
                (d."@shard" = h."@shard") 
                AND (d.data_device_id = h.data_device_id) 
                AND (COALESCE(d.updated_at, d.created_at) = COALESCE(h.updated_at, h.created_at))
            WHERE 
                (COALESCE(d.updated_at, d.created_at) >= '{{ data_interval_start }}'::TIMESTAMP_NTZ)
                AND (h.updated_at IS NULL)
        """,
        warehouse=SF_WH_SMALL
    )

    sf_load >> insert_history


data_devices_extract()

if __name__ == "__main__":
    data_devices_extract().test()
