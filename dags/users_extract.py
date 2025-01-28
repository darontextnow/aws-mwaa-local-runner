"""DAG extracts data from MySQL User Shards Databases and loads it into Snowflake.

Should DAG not run for any particular period, it will catch up on next run using previous successful run's timestamp.
Thus, only need to run one run for all missing hours.

The user_id_hex at the end consists of 4 sub hex strings in the following order:
    envId=3, shardId=shard_id, typeId=1, localId=shard_row_id
    This is the case for all prod users, for more info refer to this script:
    https://github.com/Enflick/textnow-mono/blob/master/lib/types/global_id.go
"""
from airflow_utils import dag, task
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from de_utils.constants import SF_CONN_ID, MYSQL_SHARDS_CONN_IDS, SF_WH_MEDIUM, SF_WH_XSMALL
from de_utils.slack.alerts import alert_on_failure
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 7, 18, 4, 15, 0),
    schedule="15 * * * *",
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=5),
        "retries": 3,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=False
)
def users_extract():
    _, users_load = mysql_to_snowflake_task_group(
        mysql_conn_id=MYSQL_SHARDS_CONN_IDS,
        snowflake_conn_id=SF_CONN_ID,
        sf_warehouse=SF_WH_MEDIUM,
        deferrable_load=True,
        sql="""
            SELECT 
                '{{ params.shard_id }}' AS shard,
                current_timestamp AS loaded_utc,
                username,
                email,
                facebook_id,
                first_name,
                last_name,
                NULLIF(dob, '0000-00-00') AS dob,
                CASE gender WHEN '1' THEN 'M' WHEN '2' THEN 'F' END AS gender,
                NULLIF(expiry, '0000-00-00') AS expiry,
                is_forward,
                email_verified,
                ringtone,
                signature,
                show_text_previews,
                NULLIF(last_update, '0000-00-00') AS last_update,
                NULLIF(incentivized_share_date_twitter, '0000-00-00') AS incentivized_share_date_twitter,
                NULLIF(incentivized_share_date_fb, '0000-00-00') AS incentivized_share_date_fb,
                phone_number_status,
                NULLIF(phone_assigned_date, '0000-00-00') AS phone_assigned_date,
                NULLIF(phone_last_unassigned, '0000-00-00') AS phone_last_unassigned,
                forwarding_status,
                NULLIF(forwarding_expiry, '0000-00-00') AS forwarding_expiry,
                forwarding_number,
                voicemail,
                NULLIF(voicemail_timestamp, '0000-00-00') AS voicemail_timestamp,
                credits,
                stripe_customer_id,
                archive_mask,
                NULLIF(purchases_timestamp, '0000-00-00') AS purchases_timestamp,
                NULLIF(created_at, '0000-00-00') AS created_at,
                account_status,
                NULLIF(timestamp, '0000-00-00') AS timestamp,
                CONCAT(
                    lpad(hex(3), 3, '0'), '-',
                    lpad(hex(cast(substring_index('{{ params.shard_id }}', '_', -1) as int)), 2, '0'), '-',
                    lpad(hex(1), 3, '0'), '-',
                    lpad(lower(hex(id)), 9, '0')    
                ) AS user_id_hex,
                FALSE as is_employee
            FROM users
            WHERE 
                (timestamp > TIMESTAMP('{{ prev_data_interval_end_success or data_interval_start }}'))
                AND (timestamp <= TIMESTAMP('{{ data_interval_end }}'))
                AND (users.username <> '')
        """,
        sensor_sql="SELECT 1 FROM users WHERE (timestamp >= TIMESTAMP('{{ data_interval_end }}')) LIMIT 1",
        sensor_timeout=7200,
        target_table="users",
        target_schema="core",
        transform_sql="""
            SELECT s.* EXCLUDE is_employee, NVL(f.is_employee, FALSE) AS is_employee
            FROM staging s
            LEFT JOIN analytics_staging.user_features_updates f ON s.username = f.username
        """,
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE,
        merge_match_where_expr="(staging.timestamp > tgt.timestamp)"  # ensures we don't overwrite newer data with old
    )

    @task.short_circuit(retry_delay=timedelta(minutes=45))
    def check_for_completed_day(task=None, data_interval_start=None):
        """Returns True if this DagRun is the last DagRun for the day.
        Thus, only running update on features at end of day after all hours have processed successfully.
        """
        if data_interval_start.hour == 23:
            return True
        task.log.info("It is not hour 23 yet (end of day). Returning False so features load will not run.")
        return False

    truncate_features = SnowflakeAsyncDeferredOperator(
        task_id="truncate_features_table",
        sql="TRUNCATE TABLE IF EXISTS analytics_staging.user_features_updates"
    )

    features_collect, features_load = mysql_to_snowflake_task_group(
        mysql_conn_id=MYSQL_SHARDS_CONN_IDS,
        snowflake_conn_id=SF_CONN_ID,
        sf_warehouse=SF_WH_XSMALL,
        deferrable_load=False,
        sql="""
                SELECT
                    username,
                    COALESCE(features.`value` = 'b:1;', FALSE) AS is_employee
                    -- Only users who's is_employee checkbox on Angel has been used atleast once show up in this table
                FROM features
                WHERE (features.`key` = 'IS_EMPLOYEE')
        """,
        target_table="user_features_updates",
        target_schema="analytics_staging",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND
    )

    emp_refresh = SnowflakeAsyncDeferredOperator(
        task_id="refresh_sf_table",
        sql="""
            UPDATE core.users tgt
            SET tgt.is_employee = src.is_employee 
            FROM analytics_staging.user_features_updates src
            WHERE tgt.username = src.username;
        """
    )

    users_load >> check_for_completed_day() >> truncate_features >> features_collect
    features_load >> emp_refresh


users_extract()

if __name__ == "__main__":
    users_extract().test(execution_date="2024-06-05")
