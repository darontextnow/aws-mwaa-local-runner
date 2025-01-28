"""DAG uses SFTP to pull extract Inteliquent CDR data and load it into Snowflake.
There is a two day-lag on source data availability. Thus, using prev_data_interval_start_success for extraction.
"""
from airflow_utils import dag, task
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP, AIRFLOW_STAGING_KEY_PFX
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import SF_BUCKET
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 10, 15),
    schedule="30 18 * * *",
    default_args={
        "owner": "Kannan",
        "retry_delay": timedelta(minutes=5),
        "retries": 3,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def inteliquent_cdr_extract():
    prev_date = "{{ (prev_data_interval_start_success or data_interval_start).date() }}"
    s3_dir = f"{AIRFLOW_STAGING_KEY_PFX}/billingreconciler/inteliquent/CDR/{prev_date}"

    @task(retry_delay=timedelta(hours=3))
    def get_files(s3_dir: str, prev_data_interval_start_success=None, data_interval_start=None):
        from tempfile import NamedTemporaryFile
        from de_utils.aws import S3Path
        from airflow.providers.sftp.hooks.sftp import SFTPHook
        sftp_hook = SFTPHook(ssh_conn_id='iqnt_cdr')
        str_date = (prev_data_interval_start_success or data_interval_start).strftime('%Y%m%d')

        # Get list of files that have the current date in it from these two directories
        int_files = sftp_hook.list_directory('INT/')
        cdr_files = sftp_hook.list_directory('CDR/')

        file_list = [file for file in int_files + cdr_files if str_date in file]
        if not file_list:
            raise ValueError('Files not available for retrieval')

        for file in file_list:
            with NamedTemporaryFile("w") as f:
                sftp_hook.retrieve_file(f"INT/{file}" if 'INT' in file else f"CDR/{file}", f.name)
                S3Path(f"s3://{SF_BUCKET}/{s3_dir}{file}").upload_file(f.name)

    snowflake_load = S3ToSnowflakeDeferrableOperator(
        task_id="load_CDR_snowflake",
        table="inteliquent_cdr",
        schema="core",
        s3_loc=s3_dir,
        file_format=("(TYPE = CSV COMPRESSION = GZIP DATE_FORMAT = 'YYYYMMDD' EMPTY_FIELD_AS_NULL = TRUE "
                     "REPLACE_INVALID_CHARACTERS = TRUE) ON_ERROR = SKIP_FILE_10 "),
        copy_options="TRUNCATECOLUMNS = TRUE",  # Removed "ON_ERROR = CONTINUE" on 10/10/2023 to see if it works w/out
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND
    )

    get_files(s3_dir) >> snowflake_load


inteliquent_cdr_extract()

if __name__ == "__main__":
    inteliquent_cdr_extract().test(execution_date="2023-10-16")
