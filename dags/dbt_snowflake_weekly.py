"""Runs all DBT models/ETL tagged as 'weekly' in Snowflake."""

from airflow_utils import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    start_date=datetime(2024, 5, 13),
    schedule="15 14 * * MON",
    default_args={
        "owner": "DE Team",
        "retries": 0,  # no retries as typically a failure here is an issue in a model
        "depends_on_past": True,
        "on_failure_callback": alert_on_failure,
        "sla": timedelta(hours=4),
        **default_dbt_args
    },
    sla_miss_callback=alert_sla_miss_high_priority,
    max_active_runs=1,
    catchup=False
)
def dbt_snowflake_weekly():

    # DBT Run Task
    dbt_run = DbtRunOperator(
        task_id="run_dbt",
        full_refresh=False,
        select=["tag:weekly"],
        vars=("""{data_interval_start: "'{{ data_interval_start }}'", """
              """data_interval_end: "'{{ data_interval_end }}'", """
              """ds: "'{{ data_interval_start.date() }}'", """
              "current_date: CURRENT_DATE, "
              "current_timestamp: CURRENT_TIMESTAMP}"),
        threads=4 if ENV_IS_LOCAL else 6,
        **default_dbt_args
    )

    # Trigger Persona Prediction DAG
    trigger = TriggerDagRunOperator(
        task_id="trigger_persona_prediction",
        trigger_dag_id="persona_model",
        execution_date="{{ data_interval_start }}",
    )

    dbt_run >> trigger


dbt_snowflake_weekly()


if __name__ == "__main__":
    dbt_snowflake_weekly().test(execution_date="2024-05-09")
