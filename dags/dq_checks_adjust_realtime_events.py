"""DAG for running DQ Checks on tables, wherein the data is collected through adjust realtime callbacks.
Designed to run every 4 hour.
"""
from airflow_utils import dag
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP
from dag_dependencies.dq_checks_defs import AdjustInstallsRealtimeDQChecks
from dag_dependencies.dq_checks_defs import AdjustSessionsRealtimeDQChecks
from dag_dependencies.dq_checks_defs import AdjustRegistrationsRealtimeDQChecks
from dag_dependencies.operators.dq_checks_operator import DQChecksOperator
from de_utils.slack.alerts import alert_on_failure
from datetime import datetime


@dag(
        start_date=datetime(2024, 6, 20, 9, 5),
        schedule="5 1,5,9,13,17,21 * * *",  # run every 4 hour coordinated to run just before dbt_snowflake_4h
        max_active_runs=1,
        default_args={
            "owner": "Roopa",
            "retries": 0,
            "on_failure_callback": alert_on_failure
        },
        catchup=DAG_DEFAULT_CATCHUP
)
def dq_checks_adjust_realtime_events():
    # Heartbeat check for installs_with_pi data
    DQChecksOperator(
        task_id="run_adjust_installs_with_pi_dq_checks",
        dq_checks_instance=AdjustInstallsRealtimeDQChecks(),
        run_date="{{ ts }}"
    )
    # Heartbeat check for sessions_with_pi data
    DQChecksOperator(
        task_id="run_adjust_sessions_with_pi_dq_checks",
        dq_checks_instance=AdjustSessionsRealtimeDQChecks(),
        run_date="{{ ts }}"
    )
    # Heartbeat check for registrations data
    DQChecksOperator(
        task_id="run_adjust_registrations_dq_checks",
        dq_checks_instance=AdjustRegistrationsRealtimeDQChecks(),
        run_date="{{ ts }}"
    )


dq_checks_adjust_realtime_events()

if __name__ == "__main__":
    dq_checks_adjust_realtime_events().test(execution_date="2024-01-02 04:00:00")
