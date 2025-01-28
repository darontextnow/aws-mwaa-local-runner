"""
Simple extract and load job for copying data from disabled mysql db.

Frequency: hourly incremental extraction

Inputs:
    Mysql: disabled.disabled

Outputs:
    S3: tn-snowflake-prod/airflow_staging_data/disabled_extract/YYYY-MM-DD/

Upstream DAGs: None

Rerun: Rerunning the dag would overwrite the disabled data from that execution date onwards.
    Should not be a risk as disabled mysql seems to be an insert only table.
"""
from airflow_utils import dag
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP
from de_utils.constants import ENV_VALUE
from de_utils.slack.alerts import alert_on_failure
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 6, 28),
    schedule="5 * * * *",
    default_args={
        "owner": "Ganesan",
        "retry_delay": timedelta(minutes=5),
        "retries": 3,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def disabled_extract():

    _, load_disabled = mysql_to_snowflake_task_group(
        mysql_conn_id="disabled_mysql",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
        sql="""
            SELECT id, global_id, disable_reason, requesting_service_name, disabled_status, disabled_at
            FROM disabled
            WHERE
                (disabled_at >= TIMESTAMP('{{ data_interval_start }}'))
                AND (disabled_at < TIMESTAMP('{{ data_interval_end }}'))
        """,
        pre_exec_sql=[f"""
            DELETE FROM {ENV_VALUE}.core.disabled 
            WHERE 
                (disabled_at >= '{{{{ data_interval_start }}}}')
                AND (disabled_at < '{{{{ data_interval_end }}}}')
        """],
        target_table="disabled",
        target_schema="core",
        transform_sql="""
            SELECT 
                id, BIN_USER_ID_TO_HEX_USER_ID(global_id), 
                disable_reason, requesting_service_name, disabled_status, disabled_at
            FROM staging
        """
    )

    collect_appeal_forms, load_appeal_forms = mysql_to_snowflake_task_group(
        mysql_conn_id="disabled_mysql",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
        sql="""
            SELECT 
                id, submission_ip, submission_created_at, created_at, disable_reason, requesting_service_name,
                disabled_at, submission_primary_email, submission_alternate_email, submission_last_access,
                submission_extra_info, submission_phone_number, submission_location_city, 
                submission_location_state, submission_location_country, submission_reported_error_message,
                submission_reported_login_method, submission_reported_vpn_use, submission_reported_primary_use,
                username, user_id_hex, CURRENT_TIMESTAMP() AS inserted_at
            FROM disable_appeal_forms
            WHERE
                (created_at >= TIMESTAMP('{{ data_interval_start }}'))
                AND (created_at < TIMESTAMP('{{ data_interval_end }}'))
        """,
        pre_exec_sql=[f"""
            DELETE FROM {ENV_VALUE}.core.disable_appeal_forms 
            WHERE
                (created_at >= '{{{{ data_interval_start }}}}')
                AND (created_at < '{{{{ data_interval_end }}}}')
        """],
        target_table="disable_appeal_forms",
        target_schema="core",
        transform_sql="""
            SELECT 
                id, submission_ip, submission_created_at, created_at, disable_reason, requesting_service_name,
                disabled_at, submission_primary_email, submission_alternate_email, submission_last_access,
                submission_extra_info, submission_phone_number, submission_location_city, 
                submission_location_state, submission_location_country, submission_reported_error_message,
                submission_reported_login_method, submission_reported_vpn_use, submission_reported_primary_use,
                username, user_id_hex, inserted_at
            FROM staging
        """
    )

    collect_reenabled, _ = mysql_to_snowflake_task_group(
        mysql_conn_id="disabled_mysql",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
        sql="""
            SELECT 
                id, user_id_bin, reenabled_reason, requesting_service_name, reset_password, clear_limit_history, 
                reenabled_at, disable_immune_until, CURRENT_TIMESTAMP() AS inserted_at
            FROM reenabled
            WHERE
                (reenabled_at >= TIMESTAMP('{{ data_interval_start }}'))
                AND (reenabled_at < TIMESTAMP('{{ data_interval_end }}'))
        """,
        pre_exec_sql=[f"""
            DELETE FROM {ENV_VALUE}.core.reenabled 
            WHERE 
                (reenabled_at >= '{{{{ data_interval_start }}}}')
                AND (reenabled_at < '{{{{ data_interval_end }}}}')
        """],
        target_table="reenabled",
        target_schema="core",
        transform_sql="""
            SELECT 
                id, BIN_USER_ID_TO_HEX_USER_ID(user_id_hex) as user_id_hex, reenabled_reason, 
                requesting_service_name, reset_password, clear_limit_history, reenabled_at, disable_immune_until, 
                inserted_at
            FROM staging
        """
    )

    load_disabled >> collect_appeal_forms >> load_appeal_forms >> collect_reenabled


disabled_extract()

if __name__ == "__main__":
    disabled_extract().test(execution_date="2023-06-26")
