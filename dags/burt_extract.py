"""This DAG extracts data from the Burt Analytics ingested S3 folder to our adops.report snowflake table"""
from airflow_utils import dag
from dag_dependencies.dq_checks_defs.burt_report import BurtReportDQChecks
from dag_dependencies.operators.dq_checks_operator import DQChecksOperator
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from dag_dependencies.constants import DAG_RUN_DATE_2_DAY_LAG
from de_utils.constants import ENV_VALUE, UPLOADS_BUCKET
from de_utils.slack.alerts import alert_on_failure
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 12, 5),
    schedule="15 15 * * *",
    default_args={
        "owner": "Harish",
        "retry_delay": timedelta(minutes=3),
        "retries": 3,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=False
)
def burt_extract():

    burt_report_loc = "{{ data_interval_end | ds }}/burt_report_{{ data_interval_end | ds }}.csv"
    stage_uploads_sf = f"{ENV_VALUE}.public.S3_TN_UPLOADS_BURT"

    verify_file_exists = S3KeySensor(
        task_id="verify_file_exists",
        bucket_key=f"burt/{burt_report_loc}",
        bucket_name=UPLOADS_BUCKET,
        wildcard_match=True,
        deferrable=True
    )

    load_burt_report_snowflake = S3ToSnowflakeDeferrableOperator(
        task_id="load_burt_report_snowflake",
        table="burt_report",
        schema="core",
        stage=stage_uploads_sf,
        s3_loc=burt_report_loc,
        file_format="(TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1  FIELD_OPTIONALLY_ENCLOSED_BY = '\"')",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
        pre_exec_sql=[f"""
            DELETE FROM {ENV_VALUE}.core.burt_report
            WHERE (date BETWEEN '{{{{ data_interval_end | ds }}}}'::DATE - INTERVAL '30 DAYS'
                AND '{{{{ data_interval_end | ds }}}}'::DATE - INTERVAL '1 DAY' )
        """]
    )

    run_burt_report_dq_checks = DQChecksOperator(
        task_id="run_burt_report_dq_checks",
        dq_checks_instance=BurtReportDQChecks(),
        run_date=DAG_RUN_DATE_2_DAY_LAG
    )

    verify_file_exists >> load_burt_report_snowflake >> run_burt_report_dq_checks


burt_extract()

if __name__ == "__main__":
    burt_extract().test(execution_date="2024-10-19")
    # burt_extract().test_dq_checks(execution_date="2024-10-18")
