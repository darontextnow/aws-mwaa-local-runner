"""
This DAG transforms and loads CDC data streamed from DynamoDB into Snowflake raw table into Snowflake reporting tables.

Should DAG not run for any particular period, it will catch up on next run using previous successful run's timestamp.
Thus, only need to run one run for all missing hours.
"""
from airflow_utils import dag
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from dag_dependencies.constants import SQL_SCRIPTS_DIR
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import ENV_VALUE, SF_WH_MEDIUM
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 8, 29, 5),
    schedule="40 * * * *",
    default_args={
        "owner": "Harish",
        "retry_delay": timedelta(minutes=10),
        "retries": 2,
        "on_failure_callback": alert_on_failure,
    },
    template_searchpath=f"{SQL_SCRIPTS_DIR}/dynamodb-sql-scripts",
    max_active_runs=2,
    params={"env": ENV_VALUE},
    catchup=False
)
def dynamodb_cdc_load():

    load_blocks = SnowflakeAsyncDeferredOperator(
        task_id="load_blocks",
        sql="blocks_dynamodb_cdc.sql"
    )

    load_email_addresses = SnowflakeAsyncDeferredOperator(
        task_id="load_email_addresses",
        sql="email_addresses_dynamodb_cdc.sql"
    )

    load_iap = SnowflakeAsyncDeferredOperator(
        task_id="load_iap",
        sql="iap_dynamodb_cdc.sql"
    )

    load_capabilities = SnowflakeAsyncDeferredOperator(
        task_id="load_capabilities",
        sql="capabilities_dynamodb_cdc.sql",
        warehouse=SF_WH_MEDIUM  # try to not use LARGE WH since this is an hourly running DAG
    )

    load_blocks >> load_email_addresses >> load_iap >> load_capabilities


dynamodb_cdc_load()

if __name__ == "__main__":
    dynamodb_cdc_load().test(execution_date="2023-08-28")
