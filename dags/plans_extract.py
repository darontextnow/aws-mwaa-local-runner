"""Extracts entire plans table from MySQL tn_central DB and truncates and loads into Snowflake core.plans table."""
from airflow_utils import dag
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from de_utils.constants import MYSQL_CONN_ID_CENTRAL
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 10, 24),
    schedule="10 2 * * *",
    default_args={
        "owner": "Eric",
        "retry_delay": timedelta(minutes=5),
        "retries": 3
    },
    max_active_runs=1,
    catchup=False
)
def plans_extract():

    mysql_to_snowflake_task_group(
        mysql_conn_id=MYSQL_CONN_ID_CENTRAL,
        sql="SELECT * FROM plans",
        target_table="plans",
        target_schema="core",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.TRUNCATE
    )


plans_extract()

if __name__ == "__main__":
    plans_extract().test(execution_date="2023-10-24 01:25:00")
