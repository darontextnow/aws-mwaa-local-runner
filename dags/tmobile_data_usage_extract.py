from airflow_utils import dag
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from de_utils.constants import MYSQL_CONN_ID_SPRINT_AMS
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 11, 12),
    schedule="50 1 * * *",
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=5),
        "retries": 3
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def tmobile_data_usage_extract():

    mysql_to_snowflake_task_group(
        mysql_conn_id=MYSQL_CONN_ID_SPRINT_AMS,
        sql="""
            SELECT * FROM pwg_usage_records
            WHERE (reported_at >= TIMESTAMP('{{ macros.ds_add(ds, -3) }}'))
            ORDER BY id
        """,
        target_table="tmobile_pwg_data_usage",
        target_schema="core",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE
    )


tmobile_data_usage_extract()

if __name__ == "__main__":
    tmobile_data_usage_extract().test(execution_date="2023-11-09")
