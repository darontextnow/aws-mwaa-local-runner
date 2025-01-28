"""DAG collects identities tables data from MySQL user shards and loads it into Snowflake.

Should DAG not run for any particular period, it will catch up on next run using previous successful run's timestamp.
Thus, only need to run one run for all missing hours.
"""
from airflow_utils import dag
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from de_utils.constants import MYSQL_SHARDS_CONN_IDS, SF_WH_SMALL
from de_utils.slack.alerts import alert_on_failure
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 7, 19, 13, 40, 0),
    schedule="40 * * * *",
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=5),
        "retries": 3,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=False
)
def identities_extract():

    mysql_to_snowflake_task_group(
        mysql_conn_id=MYSQL_SHARDS_CONN_IDS,
        sql="""SELECT 
                '{{params.shard_id}}' AS shard, id, username, provider, identifier, created_at, updated_at, deleted_at
            FROM identities
            WHERE
                (updated_at > TIMESTAMP('{{ prev_data_interval_end_success or data_interval_start }}'))
                AND (updated_at <=  TIMESTAMP('{{ data_interval_end }}'))
        """,
        sensor_sql="SELECT 1 from identities WHERE (updated_at >= TIMESTAMP('{{ data_interval_end }}')) LIMIT 1",
        sensor_timeout=7200,
        target_table="identities",
        target_schema="core",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE,
        deferrable_load=False,
        load_poll_interval=30,
        merge_update_columns=["username", "provider", "identifier", "created_at", "updated_at", "deleted_at"],
        merge_match_where_expr="(staging.updated_at > tgt.updated_at)",  # ensure we do not overwrite newer data
        sf_warehouse=SF_WH_SMALL
    )


identities_extract()

if __name__ == "__main__":
    identities_extract().test(execution_date="2024-06-01")
