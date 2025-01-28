"""This DAG updates ip related info hourly.
Depends on maxmind and ipinfo data
"""
from airflow_utils import dag
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP, SQL_SCRIPTS_DIR
from de_utils.constants import SF_WH_SMALL
from de_utils.slack.alerts import alert_on_failure
from datetime import datetime, timedelta


@dag(
        start_date=datetime(2023, 10, 11, 8),
        schedule="0 * * * *",
        default_args={
            "owner": "Dheeraj",
            "retry_delay": timedelta(minutes=15),
            "retries": 2,
            "sla": timedelta(hours=2)
        },
        template_searchpath=f"{SQL_SCRIPTS_DIR}/antifraud",
        max_active_runs=1,
        on_failure_callback=alert_on_failure,
        catchup=DAG_DEFAULT_CATCHUP
)
def ip_info_update():

    create_shared_temp_table = SnowflakeAsyncDeferredOperator(
        task_id="ip_install_reg_login_ips_temp",
        sql="ip_install_reg_login_ips_temp.sql",
        warehouse=SF_WH_SMALL

    )

    update_ip_geo = SnowflakeAsyncDeferredOperator(
        task_id="update_ip_geo",
        sql="ip_geo_update_hourly.sql",
        warehouse=SF_WH_SMALL
    )

    update_ip_asn = SnowflakeAsyncDeferredOperator(
        task_id="update_ip_asn",
        sql="ip_asn_update_hourly.sql",
        warehouse=SF_WH_SMALL
    )

    update_ip_privacy = SnowflakeAsyncDeferredOperator(
        task_id="update_ip_privacy",
        sql="ip_privacy_update_hourly.sql",
        warehouse=SF_WH_SMALL
    )

    drop_shared_temp_table = SnowflakeAsyncDeferredOperator(
        task_id="drop_shared_temp_table",
        sql="DROP TABLE IF EXISTS analytics_staging.ip_install_reg_login_ips_temp_{{ ts_nodash }}"
    )

    trigger = TriggerDagRunOperator(
        task_id="trigger_trust_score_dag",
        trigger_dag_id="new_user_trust_score",
        execution_date="{{ data_interval_start }}",
    )

    create_shared_temp_table >> update_ip_geo >> update_ip_asn >> update_ip_privacy >> drop_shared_temp_table
    update_ip_asn >> trigger


ip_info_update()

if __name__ == "__main__":
    ip_info_update().test(execution_date="2023-08-29")
