"""Parses raw json files streamed to s3 from firehose and loads data into SF core.adtracker table.

There are many issues currently with this raw data which makes it very difficult to parse.
See https://textnow.atlassian.net/browse/DS-2308 for status on corrupt source s3 files.
This DAG applies two different parsing methods and then combines them into one DISTINCT dataset.
This is a workaround for now until we have a fix for DS-2308 and can apply just standard JSON parsing.

Also, note that these source s3 files have timestamp column in two different formats:
(string format and epoch int format) which require specific parsing for each format.

For current list of issues with this dataset, see https://textnow.atlassian.net/browse/DS-2305

See the following Confluence article on data column defs/schema:
https://textnow.atlassian.net/wiki/spaces/MNT/pages/11823148312/AdTracker+Ad+Events+Schema
"""
from airflow_utils import dag, task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from dag_dependencies.operators.dq_checks_operator import DQChecksOperator
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP
from dag_dependencies.dq_checks_defs import AdtrackerDQChecks
from de_utils.slack.alerts import alert_on_failure, alert_sla_miss_high_priority
from de_utils.constants import AWS_CONN_ID_GROWTH, SF_CONN_ID, SF_WH_MEDIUM
from datetime import datetime, timedelta
import pytz

cached_dbo = None  # will not be populated until DAG runs. Should be reset with DagBag reloads.


@dag(
        start_date=datetime(2023, 9, 21, 6, 0, 0),
        schedule="10 * * * *",
        default_args={
            "owner": "Daron",
            "retry_delay": timedelta(minutes=3),
            "retries": 3,
            "on_failure_callback": alert_on_failure,
            "sla": timedelta(hours=1)
        },
        sla_miss_callback=alert_sla_miss_high_priority,
        max_active_runs=3,
        catchup=DAG_DEFAULT_CATCHUP
)
def adtracker_load():
    start_hour = "{{ data_interval_start.strftime('%Y/%m/%d/%H') }}"
    input_key = f"adtracker-prod{start_hour}"
    next_dir = ("s3://growth-firehose/adtracker-prod"
                "{{ (data_interval_start + macros.timedelta(hours=1)).strftime('%Y/%m/%d/%H/') }}*.gz")
    variant_table = f"analytics_staging.adtracker_variant_temp_{start_hour.replace('/', '_')}"
    json_table = f"analytics_staging.adtracker_json_temp_{start_hour.replace('/', '_')}"

    wait_hour_complete = S3KeySensor(
        aws_conn_id=AWS_CONN_ID_GROWTH,
        task_id="wait_hour_complete",
        bucket_key=next_dir,
        wildcard_match=True,
        deferrable=True
    )

    @task
    def stage_as_variant(input_key: str, variant_table: str, task=None):
        """Loads source data as a single JSON string into one variant column in staging table.
        Then, uses Snowflake functions to parse the individual columns from the JSON string.
        This method returns the most rows successfully from the source files over any other method to date.
        However, there are many records in the source files that do not have newlines between records and these
            all end up getting skipped.
        """
        dbo = get_dbo()
        dbo.execute(f"""CREATE OR REPLACE TRANSIENT TABLE {variant_table}(data VARIANT)""")
        sql = f"""
            COPY INTO {variant_table} FROM @prod.public.S3_GROWTH_FIREHOSE_LEGACY/{input_key}
            FILE_FORMAT=(TYPE = CSV FIELD_DELIMITER = NONE ESCAPE = NONE ESCAPE_UNENCLOSED_FIELD = NONE
                FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            ON_ERROR = CONTINUE RETURN_FAILED_ONLY = TRUE"""
        results = dbo.execute(sql, warehouse=SF_WH_MEDIUM).fetchall()
        if len(results) > 0:
            msg = "The following is a list of files and details that contained parsing errors during copy:"
            for row in results:
                msg += "\n    " + str(row)
            task.log.info(msg)

    @task
    def stage_as_json(input_key: str, json_table: str, task=None):
        """Loads source data by parsing the columns out from the JSON records into a staging table.
        This method returns the least number of rows as it stops parsing at the first row where issues are found.
        However, this method does recover some records lost by the load_source_data_as_variant method.
        Note: Should be able to only use this method if we get a fix to https://textnow.atlassian.net/browse/DS-2308
              However, backfills would require both.
        """
        dbo = get_dbo()
        dbo.execute(f"CREATE OR REPLACE TRANSIENT TABLE {json_table} LIKE prod.core.adtracker")
        dbo.execute(f"ALTER TABLE {json_table} DROP COLUMN cpm")  # Must use variant as this isn't always a number
        dbo.execute(f"ALTER TABLE {json_table} ADD cpm VARIANT")
        sql = f"""
            COPY INTO {json_table} 
            FROM @prod.public.S3_GROWTH_FIREHOSE_LEGACY/{input_key}
            FILE_FORMAT=(TYPE = JSON NULL_IF = (''))
            MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE' ON_ERROR = CONTINUE RETURN_FAILED_ONLY = TRUE"""
        results = dbo.execute(sql, warehouse=SF_WH_MEDIUM).fetchall()
        if len(results) > 0:
            msg = "The following is a list of files and details that contained parsing errors during copy:"
            for row in results:
                msg += "\n    " + str(row)
            task.log.info(msg)

    # Union together data from above two tasks and insert it into Snowflake. Using UNION ALL to suppress dupes.
    combine_and_load = SnowflakeAsyncDeferredOperator(
        task_id="combine_and_load",
        sql=f"""INSERT INTO core.adtracker 
                SELECT DISTINCT
                    NULLIF(PARSE_JSON(data):username::STRING, '') AS username,
                    NULLIF(PARSE_JSON(data):adid::STRING, '') AS adid,
                    TO_TIMESTAMP_NTZ(NULLIF(PARSE_JSON(data):timestamp::VARCHAR, '')) AS timestamp,
                    NULLIF(PARSE_JSON(data):type::STRING, '') AS type,
                    NULLIF(PARSE_JSON(data):client_type::STRING, '') AS client_type,
                    NULLIF(PARSE_JSON(data):client_version::STRING, '') AS client_version,
                    NULLIF(PARSE_JSON(data):ad_format::STRING, '') AS ad_format,
                    NULLIF(PARSE_JSON(data):ad_id::STRING, '') AS ad_id,
                    NULLIF(PARSE_JSON(data):ad_name::STRING, '') AS ad_name,
                    NULLIF(PARSE_JSON(data):ad_network::STRING, '') AS ad_network,
                    NULLIF(PARSE_JSON(data):ad_placement::STRING, '') AS ad_placement,
                    NULLIF(PARSE_JSON(data):ad_platform::STRING, '') AS ad_platform,
                    NULLIF(PARSE_JSON(data):ad_type::STRING, '') AS ad_type,
                    NULLIF(PARSE_JSON(data):line_item_id::STRING, '') AS line_item_id,
                    NULLIF(PARSE_JSON(data):cpm::STRING, '')::FLOAT AS cpm,
                    NULLIF(PARSE_JSON(data):experiment_id::STRING, '') AS experiment_id,
                    NULLIF(PARSE_JSON(data):experiment_name::STRING, '') AS experiment_name,
                    NULLIF(PARSE_JSON(data):experiment_variant::STRING, '') AS experiment_variant,
                    NULLIF(PARSE_JSON(data):keywords::STRING, '') AS keywords,
                    NULLIF(PARSE_JSON(data):gaid::STRING, '') AS gaid,
                    NULLIF(PARSE_JSON(data):idfa::STRING, '') AS idfa,
                    NULLIF(PARSE_JSON(data):android_id::STRING, '') AS android_id,
                    NULLIF(PARSE_JSON(data):ip_address::STRING, '') AS ip_address,
                    NULLIF(PARSE_JSON(data):mac_address::STRING, '') AS mac_address,
                    PARSE_JSON(data):timestamp_epoch::INTEGER AS timestamp_epoch,
                    NULLIF(PARSE_JSON(data):user_id::STRING, '') AS user_id,
                    NULLIF(PARSE_JSON(data):ad_request_id::STRING, '') AS ad_request_id,
                    NULLIF(PARSE_JSON(data):ad_report_kvs::STRING, '') AS ad_report_kvs,
                    NULLIF(PARSE_JSON(data):ad_unit_id::STRING, '') AS ad_unit_id,
                    NULLIF(PARSE_JSON(data):ad_screen::STRING, '') AS ad_screen,
                    NULLIF(PARSE_JSON(data):is_filtered::STRING, '') AS is_filtered,
                    NULLIF(PARSE_JSON(data):error_code::STRING, '') AS error_code,
                    NULLIF(PARSE_JSON(data):idfv::STRING, '') AS idfv,
                    CURRENT_TIMESTAMP AS inserted_at,
                    'adtracker_load DAG variant' AS inserted_by,
                    TO_DATE(NULLIF(PARSE_JSON(data):timestamp::VARCHAR, '')) AS date_utc
                FROM {variant_table} 
                UNION SELECT DISTINCT 
                    NULLIF(username, ''),
                    NULLIF(adid, ''),
                    -- source files have timestamp in both string format and as epoch integer format
                    -- Snowflake is incorrectly parsing this timestamp column by default when timestamp is epoch format.
                    --The following converts timestamp back to epoch and then properly parses it.
                    TO_TIMESTAMP_NTZ(DATE_PART(epoch_second, timestamp)/
                        CASE WHEN LEN(DATE_PART(epoch_second, timestamp)) = 13 THEN 1000 ELSE 1 END) AS timestamp,
                    NULLIF(type, ''),
                    NULLIF(client_type, ''),
                    NULLIF(client_version, ''),
                    NULLIF(ad_format, ''),
                    NULLIF(ad_id, ''),
                    NULLIF(ad_name, ''),
                    NULLIF(ad_network, ''),
                    NULLIF(ad_placement, ''),
                    NULLIF(ad_platform, ''),
                    NULLIF(ad_type, ''),
                    NULLIF(line_item_id, ''),
                    NULLIF(cpm, '')::FLOAT AS cpm,
                    NULLIF(experiment_id, ''),
                    NULLIF(experiment_name, ''),
                    NULLIF(experiment_variant, ''),
                    NULLIF(keywords, ''),
                    NULLIF(gaid, ''),
                    NULLIF(idfa, ''),
                    NULLIF(android_id, ''),
                    NULLIF(ip_address, ''),
                    NULLIF(mac_address, ''),
                    timestamp_epoch,
                    NULLIF(user_id, ''),
                    NULLIF(ad_request_id, ''),
                    NULLIF(ad_report_kvs, ''),
                    NULLIF(ad_unit_id, ''),
                    NULLIF(ad_screen, ''),
                    is_filtered,
                    NULLIF(error_code, ''),
                    NULLIF(idfv, ''),
                    CURRENT_TIMESTAMP AS inserted_at,
                    'adtracker_load DAG variant' AS inserted_by,
                    -- see note above on timestamp parsing
                    DATE(TO_TIMESTAMP_NTZ(DATE_PART(epoch_second, timestamp)/
                        CASE WHEN LEN(DATE_PART(epoch_second, timestamp)) = 13 THEN 1000 ELSE 1 END)) AS date_utc
                FROM {json_table}""",
        warehouse=SF_WH_MEDIUM
    )

    @task
    def drop_temp_tables(variant_table: str, json_table: str):
        """We can't use actual TEMP tables in SF here as tasks do not share DB connections.
        Thus, dropping the physical tables created and shared between tasks above to clean up.
        Using IF EXISTS here in case task reruns and a previous run already dropped one or more tables.
        """
        get_dbo().execute_in_parallel([
            f"DROP TABLE IF EXISTS {variant_table};",
            f"DROP TABLE IF EXISTS {json_table}"
        ])

    @task.short_circuit(retry_delay=timedelta(minutes=45))
    def check_for_completed_day(task=None, data_interval_start=None):
        """Returns True if this DagRun is the last DagRun for the day.
        Thus, only running DQ Checks at end of day after all hours have processed successfully.
        """
        from airflow.models import DagRun
        from airflow.utils.state import State
        begin_day = datetime.combine(data_interval_start.date(), datetime.min.time(), tzinfo=pytz.UTC)
        end_period = data_interval_start - timedelta(hours=1)
        if data_interval_start.hour == 23:
            dag_runs = DagRun.find(
                dag_id=task.dag_id,
                execution_start_date=begin_day,
                execution_end_date=end_period  # Does not include hour 23 as it is still RUNNING
            )
            for dag_run in dag_runs:
                task.log.info(f"Task Run for execution_date: {dag_run.execution_date} has state: {dag_run.state}")
                if dag_run.state != State.SUCCESS:
                    msg = ("Adtracker run for hour 23 completed."
                           f"\nHowever, not all 24 hours for this execution date '{data_interval_start.date()}'"
                           f" have completed successfully."
                           f"\nNot running DQ Checks until all 24 hours have processed successfully.")
                    raise RuntimeError(msg)
            task.log.info("All 23 hourly runs succeeded for the day. Returning True so DQ Checks will run.")
            return True
        task.log.info("It is not hour 23 yet (end of day). Returning False so DQ Checks will not run.")
        return False  # do not run dq checks if it's not the end of the day

    run_dq_checks = DQChecksOperator(
        task_id="run_adtracker_dq_checks",
        dq_checks_instance=AdtrackerDQChecks(),
        run_date="{{ ds }}"
    )

    stages = [stage_as_variant(input_key, variant_table), stage_as_json(input_key, json_table)]
    (wait_hour_complete >> stages >> combine_and_load >>
     drop_temp_tables(variant_table, json_table) >> check_for_completed_day() >> run_dq_checks)


def get_dbo():
    """Returns the same dbo object to all tasks."""
    from de_utils.tndbo import get_dbo as _get_dbo
    global cached_dbo
    if cached_dbo:
        return cached_dbo
    cached_dbo = _get_dbo(SF_CONN_ID)
    return cached_dbo


adtracker_load()

if __name__ == "__main__":
    adtracker_load().test_dq_checks(execution_date="2024-04-29")
