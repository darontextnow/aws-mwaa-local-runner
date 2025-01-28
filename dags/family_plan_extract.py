from airflow_utils import dag
from dag_dependencies.sensors.sql_deferrable_sensor import SqlDeferrableSensor
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from de_utils.constants import SF_CONN_ID, MYSQL_CONN_ID_CENTRAL
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 10, 4),
    schedule="20 1 * * *",
    default_args={
        "owner": "Eric",
        "retry_delay": timedelta(minutes=5),
        "retries": 3
    },
    max_active_runs=1
)
def family_plan_extract():
    source_configs = [
        {
            "sql": """
                SELECT id, owner, updated_at, created_at FROM family_plans
                WHERE (updated_at BETWEEN '{{ data_interval_start.strftime("%Y-%m-%dT%H:%M:%S") }}'
                  AND '{{ data_interval_end.strftime("%Y-%m-%dT%H:%M:%S") }}')
            """,
            "target_table": "family_plans"
        },
        {
            "sql": "SELECT id, family_plan_id, username, updated_at, created_at FROM family_plan_members",
            "target_table": "family_plan_members",
            "load_type": S3ToSnowflakeDeferrableOperator.LoadType.TRUNCATE
        },
        {
            "sql": """
                SELECT id, family_plan_id, username, status, preauth_receipt_id,
                       old_plan_id, old_sub_status, updated_at, created_at
                FROM family_plan_invites
                WHERE (updated_at BETWEEN '{{ data_interval_start.strftime("%Y-%m-%dT%H:%M:%S") }}'
                  AND '{{ data_interval_end.strftime("%Y-%m-%dT%H:%M:%S") }}')
            """,
            "target_table": "family_plan_invites"
        }
    ]

    # family_plan tables do not have much data, so checking another table that is updated more frequently
    wait_phone_number_logs = SqlDeferrableSensor(
        task_id="wait_phone_number_logs",
        conn_id=MYSQL_CONN_ID_CENTRAL,
        sql="""
            SELECT 1 FROM phone_number_logs
            WHERE (created_at >= '{{ data_interval_start.strftime("%Y-%m-%dT%H:%M:%S") }}')
            ORDER BY id DESC LIMIT 1
        """,
        timeout=3600
    )

    previous_task = wait_phone_number_logs
    for config in source_configs:
        collect, load = mysql_to_snowflake_task_group(
            mysql_conn_id=MYSQL_CONN_ID_CENTRAL,
            snowflake_conn_id=SF_CONN_ID,
            sql=config["sql"],
            target_table=config["target_table"],
            target_schema="core",
            load_type=config.get("load_type", S3ToSnowflakeDeferrableOperator.LoadType.MERGE)
        )

        previous_task >> collect >> load
        previous_task = load


family_plan_extract()

if __name__ == "__main__":
    family_plan_extract().test(execution_date="2023-10-04")
