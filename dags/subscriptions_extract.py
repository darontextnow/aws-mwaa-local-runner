from airflow_utils import dag
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP
from de_utils.constants import MYSQL_SHARDS_CONN_IDS, SF_WH_SMALL
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 11, 13, 13, 50),
    schedule="50 * * * *",
    default_args={
        "owner": "Eric",
        "retry_delay": timedelta(minutes=5),
        "retries": 3
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def subscriptions_extract():

    _, sf_load = mysql_to_snowflake_task_group(
        mysql_conn_id=MYSQL_SHARDS_CONN_IDS,
        sql="""
            SELECT '{{ params.shard_id }}' AS shard,
                current_timestamp,
                id,
                username,
                plan_id,
                status,
                warned,
                throttling_enabled,
                period_start,
                period_end,
                sub_id,
                last_updated,
                created_at,
                suspend_usage,
                suspend_times,
                throttle_time,
                hold_until,
                recurring_bill_id,
                billing_item_id,
                sandvine_version
            FROM subscriptions
            WHERE (last_updated >= '{{ data_interval_start }}')
                AND (last_updated < '{{ data_interval_end }}')
        """,
        sensor_sql="""SELECT 1 FROM subscriptions WHERE (last_updated >= '{{ data_interval_end }}') LIMIT 1""",
        target_table="subscriptions",
        target_schema="core",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE,
        merge_match_where_expr="(staging.last_updated > tgt.last_updated)"  # don't overwrite newer data with old data
    )

    insert_history = SnowflakeAsyncDeferredOperator(
        task_id="subscriptions_history_snowflake",
        sql="""
            INSERT INTO core.subscriptions_history
            SELECT distinct
                s.id,
                s.username,
                s.plan_id,
                s.status,
                s.warned,
                s.throttling_enabled,
                s.period_start,
                s.period_end,
                s.sub_id,
                s.last_updated,
                s.created_at,
                s.suspend_usage,
                s.suspend_times,
                s.throttle_time,
                s.hold_until,
                s.recurring_bill_id,
                s.billing_item_id,
                s.sandvine_version,
                "@loaded_utc" AS snapshot_utc
            FROM prod.core.subscriptions s
            LEFT JOIN (
                SELECT username, max(last_updated) AS last_updated
                FROM prod.core.subscriptions_history
                GROUP BY username
            ) h ON s.username = h.username AND s.last_updated <= h.last_updated
            WHERE s.last_updated > '{{ ts }}'
              AND h.last_updated is null
        """,
        warehouse=SF_WH_SMALL
    )

    sf_load >> insert_history


subscriptions_extract()

if __name__ == "__main__":
    subscriptions_extract().test(execution_date="2023-11-10 11:50:00")
