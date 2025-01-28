from airflow_utils import dag
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP
from de_utils.constants import MYSQL_SHARDS_CONN_IDS, SF_WH_LARGE
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 11, 8),
    schedule="35 2 * * *",
    default_args={
        "owner": "Eric",
        "retry_delay": timedelta(minutes=5),
        "retries": 3
    },
    max_active_runs=1,
    max_active_tasks=2,  # so dump from mysql to s3 won't overload Airflow workers.
    catchup=DAG_DEFAULT_CATCHUP
)
def server_sessions_extract():

    mysql_to_snowflake_task_group(
        mysql_conn_id=MYSQL_SHARDS_CONN_IDS,
        sf_warehouse=SF_WH_LARGE,
        deferrable_load=True,
        sql="""
            SELECT '{{ params.shard_id }}' AS shard,
                CURRENT_TIMESTAMP AS loaded_utc,
                id AS session_id,
                username,
                type AS client_type,
                last_access,
                lifetime,
                data_device_id,
                os_version,
                app_version,
                user_agent,
                ip,
                identity_provider,
                mdn,
                mdn_expiry,
                callkit_push_token
            FROM sessions
            WHERE (last_access >= '{{ data_interval_start }}')
              AND (last_access <  '{{ data_interval_end }}')
        """,
        sensor_sql="""
            SELECT 1 FROM sessions
            WHERE last_access >= '{{ data_interval_end }}' LIMIT 1
        """,
        target_table="server_sessions",
        target_schema="core",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE,
        merge_match_where_expr="(staging.last_access > tgt.last_access)"  # do not overwrite newer data with old data
    )


server_sessions_extract()

if __name__ == "__main__":
    server_sessions_extract().test(execution_date="2024-06-03")
