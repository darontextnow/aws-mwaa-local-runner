"""This Dag loads sketchy registration logs into Snowflake.

Notes:
    Not parsing sketchy_details:ga_id as it is 100% NULL in source data.
"""
from airflow_utils import dag
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.operators.dq_checks_operator import DQChecksOperator
from dag_dependencies.dq_checks_defs import SketchyRegistrationLogsDQChecks
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import DATA_LAKE_BUCKET_LEGACY, STAGE_DATA_LAKE_LEGACY, AWS_CONN_ID_GROWTH, ENV_VALUE
from datetime import datetime


@dag(
    start_date=datetime(2023, 10, 11, 8),
    schedule="0 * * * *",
    default_args={
        "owner": "DE Team",
        "retries": 1,
        "on_failure_callback": alert_on_failure,
    },
    max_active_runs=2,
    catchup=DAG_DEFAULT_CATCHUP
)
def sketchy_registration_logs_load():
    next_execution_dt = "{{ (data_interval_start + macros.timedelta(hours=1)).date() }}"
    next_execution_hr = "{{ '{:01d}'.format((data_interval_start + macros.timedelta(hours=1)).hour) }}"

    wait_next_hour = S3KeySensor(
        task_id="wait_next_hour",
        aws_conn_id=AWS_CONN_ID_GROWTH,  # Allows us to run this in local testing.
        bucket_key=f"incoming/sketchy_analysis/date={next_execution_dt}/hour={next_execution_hr}/*",
        bucket_name=DATA_LAKE_BUCKET_LEGACY,
        wildcard_match=True,
        deferrable=True,
        timeout=3600 * 3
    )

    load_to_sf = S3ToSnowflakeDeferrableOperator(
        task_id="load_s3_log_files",
        stage=STAGE_DATA_LAKE_LEGACY,
        table="sketchy_registration_logs",
        schema="core",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
        s3_loc=("incoming/sketchy_analysis/date={{ data_interval_start.date() }}/"
                "hour={{ '{:01d}'.format(data_interval_start.hour) }}/"),
        use_variant_staging=True,
        file_format="(TYPE = JSON NULL_IF = (''))",
        pre_exec_sql=[f"""DELETE FROM {ENV_VALUE}.core.sketchy_registration_logs 
                          WHERE source_file_time = '{{{{ data_interval_start }}}}'::TIMESTAMP_NTZ"""],
        transform_sql="sketchy_registration_logs_load_transform_sql.sql"
    )

    run_dq_checks = DQChecksOperator(
        task_id="run_sketchy_registration_logs_dq_checks",
        dq_checks_instance=SketchyRegistrationLogsDQChecks(),
        run_date="{{ data_interval_start }}"
    )

    wait_next_hour >> load_to_sf >> run_dq_checks


sketchy_registration_logs_load()

if __name__ == "__main__":
    sketchy_registration_logs_load().test(execution_date='2025-01-15 00:00:00')
