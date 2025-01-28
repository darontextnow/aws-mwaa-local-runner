"""DAG processes all raw event files dumped by Adjust in tn-adjust-events-stream bucket
and makes them immediately queryable by the adjust.raw_events table.
"""
from airflow_utils import dag, task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from de_utils.slack.alerts import alert_sla_miss_high_priority, alert_on_failure
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP
from de_utils.constants import DATA_LAKE_BUCKET, \
    ADJUST_APP_TOKEN_TN_IOS_FREE, ADJUST_APP_TOKEN_TN_ANDROID, ADJUST_APP_TOKEN_2L_ANDROID
from datetime import datetime, timedelta

SRC_BUCKET = "tn-adjust-events-stream"


@dag(
    start_date=datetime(2024, 3, 20),
    schedule="10 * * * *",
    default_args={
        "owner": "Daron",
        "retry_delay": timedelta(minutes=1),
        "retries": 1,
        "on_failure_callback": alert_on_failure,
        "sla": timedelta(hours=1)
    },
    sla_miss_callback=alert_sla_miss_high_priority,
    max_active_runs=3,
    catchup=DAG_DEFAULT_CATCHUP
)
def adjust_raw_events_processing():

    sensors = []
    # Ensure latest hourly files have come in from all three apps before processing.
    for app_id in [ADJUST_APP_TOKEN_TN_IOS_FREE, ADJUST_APP_TOKEN_TN_ANDROID, ADJUST_APP_TOKEN_2L_ANDROID]:
        input_loc = (f"s3://{SRC_BUCKET}/{app_id}_{{{{ data_interval_start.strftime('%Y-%m-%dT%H') }}}}"
                     f"0000_*.csv.gz")
        sensors.append(S3KeySensor(
            task_id=f"wait_for_{app_id}",
            bucket_key=input_loc,
            wildcard_match=True,
            deferrable=True
        ))

    @task
    def process_raw_event_files(task=None, data_interval_start=None):
        """Task moves raw files from bucket where Adjust dumps them to tn-data-lake-prod bucket with partitioned keys"""
        from de_utils.aws import S3Path
        from concurrent.futures.thread import ThreadPoolExecutor
        from itertools import repeat
        import s3fs
        s3_fs = s3fs.S3FileSystem()  # keep this out of top level code to avoid airflow/celery hanging issues.
        src_timestamp = data_interval_start.strftime('%Y-%m-%dT%H')

        with ThreadPoolExecutor() as mover:
            for _result in mover.map(move_event_file, repeat(s3_fs), repeat(src_timestamp),
                                     repeat(task.log), S3Path(f"s3://{SRC_BUCKET}/").iterdir()):
                pass  # this actually causes any raises in worker task to raise here

    # refresh the table so all events in files processed by above task can now be read.
    sf_refresh = SnowflakeAsyncDeferredOperator(
        task_id="refresh_sf_table",
        sql="ALTER EXTERNAL TABLE IF EXISTS adjust.raw_events REFRESH;"
    )

    sensors >> process_raw_event_files() >> sf_refresh


def move_event_file(s3_fs, src_timestamp, log, path):
    if path.key.startswith(ADJUST_APP_TOKEN_TN_ANDROID):
        client_type = "TN_ANDROID"
    elif path.key.startswith(ADJUST_APP_TOKEN_TN_IOS_FREE):
        client_type = "TN_IOS_FREE"
    elif path.key.startswith(ADJUST_APP_TOKEN_2L_ANDROID):
        client_type = "2L_ANDROID"
    else:
        raise ValueError(f"Unknown app_id or file name found in {SRC_BUCKET} bucket. File name: {path.key}")

    date_utc = path.key[13:23]
    # Validate logic for date_utc actually consistently returns a date
    try:
        datetime.strptime(date_utc, "%Y-%m-%d")
    except Exception:
        msg = (f"The filename: {path.key} found in bucket: {SRC_BUCKET} is not in the expected format."
               f" Unable to parse date out of this file name.")
        raise ValueError(msg)

    if src_timestamp not in path.key:
        msg = f"Raw event file '{path}' will be skipped as it is not in time period for current run: {src_timestamp}"
        log.warning(msg)
        return

    fname = path.key[24:]  # only need everything after the timestamp to be a unique filename
    dst = f"s3://{DATA_LAKE_BUCKET}/adjust_raw_events/date_utc={date_utc}/client_type={client_type}/{fname}"
    log.info(f"Moving file: {path} to: {dst}")
    s3_fs.move(str(path), dst, on_error="raise")


adjust_raw_events_processing()

if __name__ == "__main__":
    adjust_raw_events_processing().test(execution_date="2024-03-20")
