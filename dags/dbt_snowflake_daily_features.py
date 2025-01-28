"""Runs all DBT models/ETL tagged as 'daily_features' in Snowflake.

For reruns that you want to start in middle of the run, you can update the value in Airflow UI Admin -> Variables
    for key "dbt_daily_features_dag_run_models_arg" with the model name you want to start at
    followed by the plus (+) symbol.

To run only one model, you can update the value in Airflow UI Admin -> Variables
    for key "dbt_daily_features_dag_run_models_arg" with the model name you want to run.
"""

from airflow_utils import dag, task
from airflow_dbt_python.operators.dbt import DbtRunOperator
from dag_dependencies.constants import DBT_PROJECT_DIR, DAG_RUN_DATE_1_DAY_LAG
from dag_dependencies.operators.dq_checks_operator import DQChecksOperator
from dag_dependencies.dq_checks_defs.user_features_summary import UserFeaturesSummaryDQChecks
from dag_dependencies.dq_checks_defs.user_set_features_summary import UserSetFeaturesSummaryDQChecks
from de_utils.slack.alerts import alert_on_failure, alert_sla_miss_high_priority
from de_utils.constants import ENV_IS_LOCAL, DBT_SNOWFLAKE_CONN_ID
from datetime import datetime, timedelta

DBT_RUN_MODELS_ARGS_KEY = "dbt_daily_features_dag_run_models_arg"

default_dbt_args = {
    "project_dir": DBT_PROJECT_DIR,
    "upload_dbt_project": False,
    "project_conn_id": DBT_SNOWFLAKE_CONN_ID,  # required arg, but not actually used for these dbt runs.
    "target": DBT_SNOWFLAKE_CONN_ID
}


@dag(
    start_date=datetime(2024, 4, 14),
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
def dbt_snowflake_daily_features():

    @task
    def run_dbt(**context):
        from airflow.models import Variable
        log = context["task"].log
        models = Variable.get(DBT_RUN_MODELS_ARGS_KEY, "")
        log.info(f"The current value for '{DBT_RUN_MODELS_ARGS_KEY}' variable is {models}.")
        log.info("Resetting models variable in UI so next dbt run will run all models.")
        desc = ("Enter the models arg to be used for the next run of dbt_snowflake_daily_features DAG.\n"
                "The value can be a comma separated list of model names to be ran, i.e. model, model, ...\n"
                "Use the plus sign (+) after a model name to run all models starting from a particular model.\n"
                "Leave blank to run all models.")
        Variable.set(DBT_RUN_MODELS_ARGS_KEY, "", description=desc)  # reset Variable value to default

        select = ["tag:daily_features"]
        if models:
            select = [f"tag:daily_features,{model}" for model in models.replace(" ", "").split(",")]

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

    run_ufs_dq_checks = DQChecksOperator(
        task_id="run_user_features_summary_dq_checks",
        dq_checks_instance=UserFeaturesSummaryDQChecks(),
        run_date=DAG_RUN_DATE_1_DAY_LAG
    )

    run_usfs_dq_checks = DQChecksOperator(
        task_id="run_user_set_features_summary_dq_checks",
        dq_checks_instance=UserSetFeaturesSummaryDQChecks(),
        run_date=DAG_RUN_DATE_1_DAY_LAG
    )

    run_dbt() >> run_ufs_dq_checks >> run_usfs_dq_checks


dbt_snowflake_daily_features()

if __name__ == "__main__":
    dbt_snowflake_daily_features().test(execution_date="2024-06-15")
