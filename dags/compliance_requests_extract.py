"""DAG extracts data from MySQL compliance Databases and loads it into Snowflake.

Should DAG not run for any particular period, it will catch up on next run using previous successful run's timestamp.
Thus, only need to run one run for all missing hours.
"""
from airflow_utils import dag
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 8, 24),
    schedule="45 1 * * *",
    default_args={
        "owner": "Eric",
        "retry_delay": timedelta(minutes=5),
        "retries": 3
    },
    max_active_runs=1,
    catchup=False
)
def compliance_requests_extract():

    mysql_to_snowflake_task_group(
        mysql_conn_id="compliance_db",
        sql="""SELECT * 
            FROM compliances
            WHERE
                (updated_at > TIMESTAMP('{{ prev_data_interval_end_success or data_interval_start }}'))
                AND (updated_at <= TIMESTAMP('{{ data_interval_end }}'))
        """,
        target_table="compliance_requests",
        target_schema="core",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE
    )


compliance_requests_extract()

if __name__ == "__main__":
    compliance_requests_extract().test(execution_date="2023-08-24")
