"""Dag collects messages table data from MySQL and loads it into Snowflake.
this is a test dag to check the data load performance
"""
from airflow_utils import dag
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from de_utils.constants import MYSQL_CONN_ID_ARCHIVE
from de_utils.slack.alerts import alert_on_failure
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 8, 20, 13, 40, 0),
    schedule="40 * * * *",
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=5),
        "retries": 3,
        "on_failure_callback": alert_on_failure,
    },
    max_active_runs=1,
    catchup=False
)
def messages_extract():

    sql = """
            SELECT id, message_id, username, device_id, direction, contact_value, contact_type,
                contact_name, date, message_type, message, `read`, deleted 
            FROM tndb_prod_archive_5.messages
            WHERE id > 158681058469 AND id <= 158686058469 AND direction = 2
    """

    mysql_to_snowflake_task_group(
        mysql_conn_id=MYSQL_CONN_ID_ARCHIVE,
        sql=sql,
        target_table="messages",
        target_schema="raw",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
        deferrable_load=False,
        load_poll_interval=30
    )


messages_extract()

if __name__ == "__main__":
    messages_extract().test(execution_date="2024-08-20 00:40:00")
