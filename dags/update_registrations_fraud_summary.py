"""DAG updates registrations_fraud_summary table with latest summary data."""
from airflow_utils import dag
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP, SQL_SCRIPTS_DIR
from de_utils.constants import SF_WH_MEDIUM, SF_WH_SMALL
from de_utils.slack.alerts import alert_on_failure
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 1, 2),
    schedule="15 7 * * *",
    default_args={
        "owner": "Ross",
        "retry_delay": timedelta(minutes=60),
        "retries": 2,
        "sla": timedelta(hours=24),
        "on_failure_callback": alert_on_failure
    },
    template_searchpath=f"{SQL_SCRIPTS_DIR}/antifraud",
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def update_registration_fraud_summary():

    delete_new_user_summary = SnowflakeAsyncDeferredOperator(
        task_id="delete_new_user_summary",
        sql="DELETE FROM core.registration_fraud_summary WHERE (DATE(created_at) = '{{ macros.ds_add(ds, -2) }}')",
        warehouse=SF_WH_SMALL
    )

    insert_new_user_summary = SnowflakeAsyncDeferredOperator(
        task_id="insert_new_user_summary",
        sql="new_registrations_summary.sql",
        warehouse=SF_WH_MEDIUM
    )

    delete_new_user_summary >> insert_new_user_summary


update_registration_fraud_summary()
