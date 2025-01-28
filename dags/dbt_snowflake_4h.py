"""Runs all DBT models/ETL tagged as '4h' every 4 hours in Snowflake."""

from airflow_utils import dag
from airflow_dbt_python.operators.dbt import (
    DbtRunOperator,
    DbtDepsOperator,
    DbtSeedOperator
)
from dag_dependencies.sensors.sql_deferrable_sensor import SqlDeferrableSensor
from dag_dependencies.sensors.external_task_deferred_sensor import ExternalTaskDeferredSensor
from dag_dependencies.constants import DBT_PROJECT_DIR, DAG_DEFAULT_CATCHUP
from de_utils.slack.alerts import alert_on_failure, alert_sla_miss_high_priority
from de_utils.constants import ENV_IS_LOCAL, DBT_SNOWFLAKE_CONN_ID, SF_CONN_ID
from datetime import datetime, timedelta

default_dbt_args = {
    "project_dir": DBT_PROJECT_DIR,
    "upload_dbt_project": False,
    "project_conn_id": DBT_SNOWFLAKE_CONN_ID,  # required arg, but not actually used for these dbt runs.
    "target": DBT_SNOWFLAKE_CONN_ID
}


@dag(
    # base schedule off a 9:15am run so that it's finished when dbt_snowflake_daily runs.
    start_date=datetime(2024, 6, 20, 9, 15),
    schedule="15 1,5,9,13,17,21 * * *",
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=5),
        "retries": 0,  # disable retries as we may not want to run all tables again
        "on_failure_callback": alert_on_failure,
        "sla": timedelta(hours=4),
        **default_dbt_args
    },
    sla_miss_callback=alert_sla_miss_high_priority,
    catchup=DAG_DEFAULT_CATCHUP
)
def dbt_snowflake_4h():
    # adjust data intervals down 15 minutes so wait times on wait sensors are less rather than start DAG and wait.
    data_interval_start = "{{ data_interval_start - macros.timedelta(minutes=15) }}"
    data_interval_end = "{{ data_interval_end - macros.timedelta(minutes=15) }}"
    wait_firehose_registrations = SqlDeferrableSensor(
        task_id="wait_firehose_registrations",
        conn_id=SF_CONN_ID,
        sql=f"SELECT 1 FROM prod.firehose.registrations WHERE (created_at > '{data_interval_end}') LIMIT 1; ",
        timeout=3600
    )

    # Only run after adjust.registrations DQ Checks have passed.
    wait_adjust_registrations = ExternalTaskDeferredSensor(
        task_id="wait_adjust_registrations",
        external_dag_id="dq_checks_adjust_realtime_events",
        external_task_id="run_adjust_registrations_dq_checks",
        execution_date_fn=lambda dt: dt.replace(minute=5),  # dbt_snowflake_4h runs 5m before same hour as this runs
        timeout=3600
    )

    wait_core_messages = SqlDeferrableSensor(
        task_id="wait_core_messages",
        conn_id=SF_CONN_ID,
        sql=f"SELECT 1 FROM prod.core.messages WHERE (created_at > '{data_interval_end}') LIMIT 1; ",
        timeout=3600
    )

    # Ensure latest adtracker has run to completion before running sessions update
    wait_adtracker_load = ExternalTaskDeferredSensor(
        task_id="wait_adtracker_load",
        external_dag_id="adtracker_load",
        execution_date_fn=lambda dt: dt.replace(minute=10),
        timeout=3600
    )

    install_deps = DbtDepsOperator(task_id="install_deps")

    collect_seeds = DbtSeedOperator(task_id="collect_seeds", full_refresh=True)

    run_dbt = DbtRunOperator(
        task_id="run_dbt",
        full_refresh=False,
        select=["tag:4h"],
        vars=(f"""{{data_interval_start: "'{data_interval_start}'", """
              f"""data_interval_end: "'{data_interval_end}'", """
              f"""ds: "'{{{{ ds }}}}'", """
              f"current_date: CURRENT_DATE, "
              f"current_timestamp: CURRENT_TIMESTAMP}}"),
        threads=4 if ENV_IS_LOCAL else 6,
        **default_dbt_args
    )

    ([wait_firehose_registrations, wait_adjust_registrations, wait_core_messages, wait_adtracker_load] >>
     install_deps >> collect_seeds >> run_dbt)


dbt_snowflake_4h()

if __name__ == "__main__":
    dbt_snowflake_4h().test(execution_date="2024-04-11")
