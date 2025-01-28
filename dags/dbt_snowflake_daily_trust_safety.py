"""Runs all DBT models/ETL tagged as 'daily_trust_safety' in Snowflake."""

from airflow_utils import dag
from airflow_dbt_python.operators.dbt import DbtRunOperator
from dag_dependencies.constants import DBT_PROJECT_DIR
from de_utils.slack.alerts import alert_on_failure, alert_sla_miss_high_priority
from de_utils.constants import ENV_IS_LOCAL, DBT_SNOWFLAKE_CONN_ID
from datetime import datetime, timedelta

default_dbt_args = {
    "project_dir": DBT_PROJECT_DIR,
    "upload_dbt_project": False,
    "project_conn_id": DBT_SNOWFLAKE_CONN_ID,  # required arg, but not actually used for these dbt runs.
    "target": DBT_SNOWFLAKE_CONN_ID
}


@dag(
    start_date=datetime(2024, 6, 17),
    schedule=None,  # triggered by dbt_downstream_dags_triggerer DAG
    default_args={
        "owner": "DE Team",
        "retries": 0,  # disable retries, since each run takes a long time
        "depends_on_past": True,
        "on_failure_callback": alert_on_failure,
        "sla": timedelta(hours=4),
        **default_dbt_args
    },
    sla_miss_callback=alert_sla_miss_high_priority,
    max_active_runs=1,
    catchup=False
)
def dbt_snowflake_daily_trust_safety():

    DbtRunOperator(
        task_id="run_dbt",
        full_refresh=False,
        select=["tag:daily_trust_safety"],
        vars=("""{data_interval_start: "'{{ data_interval_start }}'", """
              """data_interval_end: "'{{ data_interval_start + macros.timedelta(days=1) }}'", """
              """ds: "'{{ data_interval_start.date() }}'", """
              "current_date: CURRENT_DATE, "
              "current_timestamp: CURRENT_TIMESTAMP}"),
        threads=4 if ENV_IS_LOCAL else 6,
        **default_dbt_args
    )


dbt_snowflake_daily_trust_safety()
