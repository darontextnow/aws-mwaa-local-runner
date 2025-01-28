"""This DAG loads msgspam logs from S3 into Snowflake"""
from airflow_utils import dag
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.operators.dq_checks_operator import DQChecksOperator
from dag_dependencies.dq_checks_defs import MsgSpamLogsDQChecks
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP
from de_utils.slack.alerts import alert_on_failure
from datetime import datetime
from de_utils.constants import SF_BUCKET, ENV_VALUE


@dag(
    start_date=datetime(2023, 9, 26),
    schedule="15 * * * *",
    default_args={
        "owner": "DE Team",
        "retries": 3,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=3,
    catchup=DAG_DEFAULT_CATCHUP
)
def msg_spam_logs_load():
    next_execution_dt = "{{ (data_interval_start + macros.timedelta(hours=1)).date() }}"
    next_execution_hr = "{{ '{:02d}'.format((data_interval_start + macros.timedelta(hours=1)).hour) }}"
    source_file_time = "{{ data_interval_start - macros.timedelta(minutes=15)}}"  # Should be datetime on the hour

    wait_next_hour = S3KeySensor(
        task_id="wait_next_hour",
        bucket_key=("logs/kafka-connect-ds/k8s-container.trust-and-safety.message-spam/"
                    f"date={next_execution_dt}/hour={next_execution_hr}/*.gz"),
        bucket_name=SF_BUCKET,
        wildcard_match=True,
        deferrable=True,
        timeout=3600 * 3
    )

    load_to_sf = S3ToSnowflakeDeferrableOperator(
        task_id="load_s3_log_files",
        table="cm_msg_spam_logs",
        schema="core",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
        s3_loc=("logs/kafka-connect-ds/k8s-container.trust-and-safety.message-spam/"
                "date={{ data_interval_start.date() }}/hour={{ '{:02d}'.format(data_interval_start.hour) }}/"),
        use_variant_staging=True,
        file_format="(TYPE = CSV FIELD_DELIMITER = NONE ESCAPE = NONE ESCAPE_UNENCLOSED_FIELD = NONE)",
        pre_exec_sql=[f"DELETE FROM {ENV_VALUE}.core.cm_msg_spam_logs WHERE source_file_time = '{source_file_time}'"],
        transform_sql=f"""
            SELECT
                log:"@timestamp"::TIMESTAMP_NTZ AS event_time,
                log:account_id::STRING AS account_id,
                log:action::STRING AS action,
                log:attachment_url::STRING AS attachment_url,
                log:caller::STRING AS caller,
                log:detail::STRING AS detail,
                log:direction::STRING AS direction,
                NULLIF(log:enforced, '')::BOOLEAN AS enforced,
                log:from::STRING AS "from",
                log:id::STRING AS id,
                log:level::STRING AS level,
                log:logger::STRING AS logger,
                log:md5::STRING AS md5,
                log:msg::STRING AS msg,
                log:reason::STRING AS reason,
                log:to::VARIANT AS "to",
                log:verdict::STRING AS verdict,
                log:version::STRING AS version,
                '{source_file_time}' AS source_file_time,
                log:client_ip::STRING AS client_ip
            FROM (
                SELECT 
                    PARSE_JSON(
                        REGEXP_SUBSTR(
                            PARSE_JSON(data):log
                        ,'{{.*',1,1,'e')
                    ) AS log
                FROM staging
                WHERE (data LIKE '%cloudmark decision%')
            )
        """
    )

    run_dq_checks = DQChecksOperator(
        task_id="run_core_cm_msg_spam_logs_dq_checks",
        dq_checks_instance=MsgSpamLogsDQChecks(),
        run_date=source_file_time
    )

    wait_next_hour >> load_to_sf >> run_dq_checks


msg_spam_logs_load()

if __name__ == "__main__":
    msg_spam_logs_load().test(execution_date="2023-09-28 00:15:00")
