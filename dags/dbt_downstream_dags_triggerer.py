"""DAG for running all DAGs downstream of dbt_snowflake_daily DAG and the subsequent DQ Checks.
Since dbt_snowflake_daily runtime can vary significantly it's hard to schedule all these downstream DAGs accordingly.
Thus, this triggerer DAG helps organize all downstream DAGs in one wait process and coordinates the runs so that
we don't have too many running at the same time which overwhelms SF queue.
"""
from airflow_utils import dag
from dag_dependencies.operators.trigger_dagrun_operator import TriggerDagRunOperator
from dag_dependencies.sensors.external_task_deferred_sensor import ExternalTaskDeferredSensor
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP
from de_utils.slack.alerts import alert_on_failure, alert_sla_miss_high_priority
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 6, 24),
    schedule="10 11 * * *",
    default_args={
        "owner": "Daron",
        "sla": timedelta(hours=2)
        #  "on_failure_callback": alert_on_failure  # don't alert here. triggered DAG will send alert if required.
    },
    sla_miss_callback=alert_sla_miss_high_priority,
    max_active_runs=1,
    max_active_tasks=3,  # many queries are ran by the DAGs below. Thus, limiting to run only 3 tasks at a time.
    catchup=DAG_DEFAULT_CATCHUP
)
def dbt_downstream_dags_triggerer():

    wait_dbt_daily = ExternalTaskDeferredSensor(
        task_id="wait_for_dbt_daily",
        external_dag_id="dbt_snowflake_daily",
        execution_date_fn=lambda dt: dt.replace(hour=10, minute=0),
        on_failure_callback=alert_on_failure,
        timeout=60 * 60 * 2  # 2 hours
    )

    wait_firebase_dq_checks = ExternalTaskDeferredSensor(
        task_id="wait_firebase_dq_checks",
        external_dag_id="dq_checks_dbt_daily_post_run",
        external_task_id="run_firebase_queries_dq_checks",
        execution_date_fn=lambda dt: dt.replace(hour=11, minute=0),
        on_failure_callback=alert_on_failure,
        timeout=60 * 60 * 2  # 2 hours
    )

    # Trigger all downstream dependent DAGS (in priority order) to be triggered after daily DBT run DQ Checks.
    # Values below represent (priority_weight value, sla, force_status_to_success) per task.
    # Higher priority_weights run first.
    # force_status_to_success = True means status of task will be success even if the triggered task fails.
    dags = {
        "dbt_snowflake_daily_features": (6, timedelta(hours=3), False),
        "ltv_d1_models": (5, timedelta(hours=2, minutes=30), False),
        "ltv_active_days_models": (5, timedelta(hours=3), True),
        "adjust_send_post_install_events": (4, timedelta(hours=2, minutes=30), True),
        "firebase_post_install_events": (4, timedelta(hours=3), False),
        "automated_accounts_disabler": (3, timedelta(hours=3), True),
        "inc_441_ng_web_reg_bad_device": (3, timedelta(hours=3), True),
        "braze_cdi_import_daily": (2, timedelta(hours=3), True),
        "fraud_models": (1, timedelta(hours=3), True),
        "dbt_snowflake_daily_trust_safety": (1, timedelta(hours=3), True),
        "existing_user_trust_score": (1, timedelta(hours=4), True)
    }

    triggers = {}
    for dag_id, (priority_weight, sla, force_status_to_success) in dags.items():
        timeout = 5 if dag_id == "existing_user_trust_score" else 3  # don't let a task get stuck long only waiting
        triggers[dag_id] = TriggerDagRunOperator(
            task_id=f"trigger_{dag_id}",
            trigger_dag_id=dag_id,
            execution_date="{{ data_interval_start }}",
            priority_weight=priority_weight,  # execute triggers in priority order (high numbers first)
            sla=sla,
            wait_for_completion=True,  # jobs will run based on max_active_tasks attr and not overwhelm worker resources
            force_status_to_success=force_status_to_success,
            execution_timeout=timedelta(hours=timeout)
        )

    # add specific dependencies between trigger tasks
    triggers["dbt_snowflake_daily_features"] >> triggers["ltv_d1_models"]  # ltv_d1_models must run after features
    triggers["dbt_snowflake_daily_features"] >> triggers["ltv_active_days_models"]  # models must run after features
    triggers["ltv_d1_models"] >> triggers["firebase_post_install_events"]  # firebase must run after ltv_d1_models
    triggers["ltv_d1_models"] >> triggers["automated_accounts_disabler"]  # disabler must run after ltv_d1_models
    triggers["dbt_snowflake_daily_trust_safety"] >> triggers["automated_accounts_disabler"]  # disabler depends on dbt
    triggers["ltv_d1_models"] >> triggers["adjust_send_post_install_events"]  # adjust send must run after ltv_d1_models
    # run one dbt features task at a time as they are both very heavy on SF resources
    triggers["dbt_snowflake_daily_features"] >> triggers["dbt_snowflake_daily_trust_safety"]
    triggers["dbt_snowflake_daily_trust_safety"] >> triggers["existing_user_trust_score"]  # existing depends on dbt
    triggers["dbt_snowflake_daily_features"] >> triggers["existing_user_trust_score"]  # existing depends on dbt

    wait_dbt_daily >> list(triggers.values())

    # add dependencies between any DQ Checks and their trigger tasks
    wait_firebase_dq_checks >> triggers["firebase_post_install_events"]  # don't run firebase if DQ Checks fail


dbt_downstream_dags_triggerer()

if __name__ == "__main__":
    dbt_downstream_dags_triggerer().test(execution_date="2024-06-23")
