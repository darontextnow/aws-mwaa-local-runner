"""Runs all DBT models/ETL tagged as 'daily' in Snowflake.

For reruns that you want to start in middle of the run, you can update the value in Airflow UI Admin -> Variables
    for key "dbt_daily_dag_run_models_arg" with the model name you want to start at followed by the plus (+) symbol.

To run only one model, you can update the value in Airflow UI Admin -> Variables
    for key "dbt_daily_dag_run_models_arg" with the model name you want to run.
"""

from airflow_utils import dag, task
from airflow_dbt_python.operators.dbt import (
    DbtRunOperator,
    DbtDepsOperator,
    DbtSeedOperator
)
from dag_dependencies.sensors.external_task_deferred_sensor import ExternalTaskDeferredSensor
from dag_dependencies.constants import DBT_PROJECT_DIR
from de_utils.slack.alerts import alert_on_failure, alert_sla_miss_high_priority
from de_utils.constants import ENV_IS_LOCAL, DBT_SNOWFLAKE_CONN_ID
from datetime import datetime, timedelta

DBT_RUN_MODELS_ARGS_KEY = "dbt_daily_dag_run_models_arg"

default_dbt_args = {
    "project_dir": DBT_PROJECT_DIR,
    "upload_dbt_project": False,
    "project_conn_id": DBT_SNOWFLAKE_CONN_ID,  # required arg, but not actually used for these dbt runs.
    "target": DBT_SNOWFLAKE_CONN_ID
}


@dag(
    start_date=datetime(2024, 1, 2),
    schedule="0 10 * * *",
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
def dbt_snowflake_daily():
    # Only run after RFM segmentation run has finished. RFM DAG has same schedule, don't need to set `execution_delta`
    wait_rfm_segmentation = ExternalTaskDeferredSensor(
        task_id="wait_for_rfm_segmentation",
        external_dag_id="rfm_segmentation",
        execution_date_fn=lambda dt: dt.replace(hour=9, minute=50),
        timeout=7200,
        sla=timedelta(hours=1)
    )

    # Only run after all dbt_snowflake_4h DAG runs for previous day have finished.
    wait_dbt_snowflake_4h = ExternalTaskDeferredSensor(
        task_id="wait_dbt_snowflake_4h",
        external_dag_id="dbt_snowflake_4h",
        execution_date_fn=lambda dt: dt.replace(hour=21, minute=15),  # this should be the last run for previous day
        timeout=7200,
        sla=timedelta(hours=1)
    )

    # Only run after DQ checks on upstream DAU tables have run and not failed
    wait_dau_dq_checks = ExternalTaskDeferredSensor(
        task_id="wait_dau_dq_checks",
        external_dag_id="dau_processing_sf",
        external_task_id="run_dbt_dau_source_tables_dq_checks",
        execution_delta=timedelta(hours=3),  # dau_processing_sf runs at 7
        timeout=7200,
        sla=timedelta(hours=1)
    )

    install_deps = DbtDepsOperator(task_id="install_deps")

    collect_seeds = DbtSeedOperator(task_id="collect_seeds", full_refresh=True)

    @task
    def run_dbt(**context):
        from airflow.models import Variable
        log = context["task"].log
        models = Variable.get(DBT_RUN_MODELS_ARGS_KEY, "")
        log.info(f"The current value for '{DBT_RUN_MODELS_ARGS_KEY}' variable is {models}.")
        log.info("Resetting models variable in UI so next dbt run will run all models.")
        desc = ("Enter the models arg to be used for the next run of dbt_snowflake_daily DAG.\n"
                "The value can be a comma separated list of model names to be ran, i.e. model, model, ...\n"
                "Use the plus sign (+) after a model name to run all models starting from a particular model.\n"
                "Leave blank to run all models.")
        Variable.set(DBT_RUN_MODELS_ARGS_KEY, "", description=desc)  # reset Variable value to default

        select = ["tag:daily"]
        if models:
            select = [f"tag:daily,{model}" for model in models.replace(" ", "").split(",")]

        DbtRunOperator(
            task_id="run_dbt",
            full_refresh=False,
            select=select,
            vars=(f"""{{data_interval_start: "'{context['data_interval_start']}'", """
                  f"""data_interval_end: "'{context['data_interval_end']}'", """
                  f"""ds: "'{context['ds']}'", """
                  f"current_date: CURRENT_DATE, "
                  f"current_timestamp: CURRENT_TIMESTAMP}}"),
            threads=4 if ENV_IS_LOCAL else 6,
            **default_dbt_args
        ).execute(context)

    ([wait_rfm_segmentation, wait_dbt_snowflake_4h, wait_dau_dq_checks] >>
     install_deps >> collect_seeds >> run_dbt())


dbt_snowflake_daily()

if __name__ == "__main__":
    dbt_snowflake_daily().test(execution_date="2024-07-01")
