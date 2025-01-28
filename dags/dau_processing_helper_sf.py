"""
This DAG serves to re-run daily DAU processing ETL starting from the start of the lookback window (3 days).

Each day of DAU processing must be ran as a separate day as the next day uses previous day's data.
Thus, clearing previous 3 dag runs here so they will rerun in order one day at a time.
This DAG triggers the runs.

Running DQ Checks on core.messages, core.registrations, and leanplum_sessions tables before clearing DAG runs
to ensure these tables is ready for use.
"""
from airflow_utils import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dag_dependencies.sensors.sql_deferrable_sensor import SqlDeferrableSensor
from dag_dependencies.constants import DAG_RUN_DATE_1_DAY_LAG
from dag_dependencies.dq_checks_defs import CoreMessagesDQChecks
from dag_dependencies.dq_checks_defs import CoreRegistrationsDQChecks
from dag_dependencies.dq_checks_defs import CoreUsersDQChecks
from dag_dependencies.operators.dq_checks_operator import DQChecksOperator
from de_utils.slack.alerts import alert_on_failure, alert_sla_miss_high_priority
from de_utils.constants import SF_CONN_ID
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 8, 15),
    schedule="0 7 * * *",
    default_args={
        "owner": "DE Team",
        "on_failure_callback": alert_on_failure,
        "sla": timedelta(hours=1)
    },
    sla_miss_callback=alert_sla_miss_high_priority,
    max_active_runs=1,
    catchup=False
)
def dau_processing_helper_sf():

    wait_upstream_sessions = SqlDeferrableSensor(  # make sure sessions table populated by dbt_snowflake_4h is current
        task_id="wait_upstream_sessions",
        conn_id=SF_CONN_ID,
        sql="""SELECT 1 FROM prod.core.sessions
               WHERE (DATE(created_at) = '{{ ds }}'::DATE + INTERVAL '1 DAY')
               --5K is normal well into the future. More than 10K means it's ran at least once for current day.
               HAVING (COUNT(*) > 10000)
        """,
        timeout=3600
    )

    run_messages_dqc = DQChecksOperator(
        task_id="run_core_messages_dq_checks",
        dq_checks_instance=CoreMessagesDQChecks(),
        run_date=DAG_RUN_DATE_1_DAY_LAG
    )

    run_registrations_dqc = DQChecksOperator(
        task_id="run_core_registrations_dq_checks",
        dq_checks_instance=CoreRegistrationsDQChecks(),
        run_date=DAG_RUN_DATE_1_DAY_LAG
    )

    run_users_dqc = DQChecksOperator(
        task_id="run_core_users_dq_checks",
        dq_checks_instance=CoreUsersDQChecks(),
        run_date=DAG_RUN_DATE_1_DAY_LAG
    )

    # TODO: Build DQ Checks for party planner applifecyclechanged per DS-2726 to replace the deprecated lp DQ Checks.
    # run_lp_sessions_dqc = DQChecksOperator(
    #     task_id="run_leanplum_sessions_dq_checks",
    #     dq_checks_instance=LeanplumSessionsDQChecks(),
    #     run_date=DAG_RUN_DATE_1_DAY_LAG
    # )

    @task
    def clear_runs(data_interval_start=None):
        from airflow.models import DagBag
        dag_id = "dau_processing_sf"
        start = data_interval_start - timedelta(days=5)
        end = data_interval_start
        DagBag().dags[dag_id].clear(start_date=start, end_date=end)

    trigger = TriggerDagRunOperator(
        task_id="trigger",
        trigger_dag_id="dau_processing_sf",
        execution_date="{{ data_interval_start }}"
    )

    ((wait_upstream_sessions >> run_messages_dqc >> run_registrations_dqc >> run_users_dqc) >> clear_runs() >> trigger)


dau_processing_helper_sf()

if __name__ == "__main__":
    dau_processing_helper_sf().test_dq_checks(execution_date="2025-01-06")
