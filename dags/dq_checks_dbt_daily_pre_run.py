"""DAG DQ Checks on multiple tables used as source tables for dbt daily run."""
from airflow_utils import dag
from dag_dependencies.constants import DAG_RUN_DATE_1_DAY_LAG, DAG_DEFAULT_CATCHUP
from dag_dependencies.dq_checks_defs import (CoreIAPDQChecks, SandvineAppDomainDQChecks, SandvineAppUsageDQChecks,
                                             SingularSpendDataDQChecks)
from dag_dependencies.operators.dq_checks_operator import DQChecksOperator
from de_utils.slack.alerts import alert_on_failure
from datetime import datetime


@dag(
        start_date=datetime(2024, 7, 11),
        schedule="45 8 * * *",
        default_args={
            "owner": "Daron",
            "retries": 0,
            "on_failure_callback": alert_on_failure
        },
        max_active_runs=1,
        max_active_tasks=1,  # run one DQ Check task at a time to not overload Snowflake queue
        catchup=DAG_DEFAULT_CATCHUP
)
def dq_checks_daily_dbt_daily_pre_run():

    # core.iap table is populated by data streamed from Dynamo DB to s3 and loaded hourly by dynamodb_cdc_extract DAG.
    # Only need to check once a day prior to dbt daily run.
    DQChecksOperator(
        task_id="run_core_iap_dq_checks",
        dq_checks_instance=CoreIAPDQChecks(),
        run_date=DAG_RUN_DATE_1_DAY_LAG
    )

    # Runs DQ Checks on core.sandvine_app_domain and core.sandvine_app_usage table once a day.
    # These tables are populated by data from s3 and loaded into the tables realtime using snowpipes.
    DQChecksOperator(
        task_id="run_core_sandvine_app_domain_dq_checks",
        dq_checks_instance=SandvineAppDomainDQChecks(),
        run_date=DAG_RUN_DATE_1_DAY_LAG,
        on_failure_callback=alert_on_failure
    )

    DQChecksOperator(
        task_id="run_core_sandvine_app_usage_dq_checks",
        dq_checks_instance=SandvineAppUsageDQChecks(),
        run_date=DAG_RUN_DATE_1_DAY_LAG,
        on_failure_callback=alert_on_failure
    )

    # Singular Adspend data is received by snowflake data sharing.
    # Before the dbt model picks up the data from the singular table,
    # we need to run the dq check and make sure data is good
    DQChecksOperator(
        task_id="run_adspend_dq_checks",
        dq_checks_instance=SingularSpendDataDQChecks(),
        run_date=DAG_RUN_DATE_1_DAY_LAG
    )


dq_checks_daily_dbt_daily_pre_run()

if __name__ == "__main__":
    dq_checks_daily_dbt_daily_pre_run().test_dq_checks()
