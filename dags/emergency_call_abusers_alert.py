"""
load_911_call_abusers task:
    1. filter users from the view which tracks live call counts from PP CallCompleted
        (analytics.vw_get_emergency_call_abusers) using certain criteria
    2. check duplicate users that have been previously filtered and alerted and remove them (done on the view SQL)
    3. load the latest filtered users to table (insert statement in this task)
generate_911_alerts task:
    4. read from snowflake table with alerts (updated in the above task)
        and report all the latest filtered alerts to Slack (#911_abuse_alerts)

On Call Support:
    - If there are any failures which have been fixed tasks need to be cleared & re-run
    - Subsequent runs don't depend on previous runs, so we can let them continue
"""
from airflow_utils import dag, task
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from de_utils.slack.alerts import alert_on_failure, alert_sla_miss_high_priority
from de_utils.constants import ENV_ENUM, SF_CONN_ID, SF_WH_SMALL
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 4, 23),
    schedule="1/10 * * * *",  # every 10 mins
    default_args={
        "owner": "Satish",
        "retry_delay": timedelta(minutes=5),
        "retries": 1,
        "on_failure_callback": alert_on_failure,
        "sla": timedelta(minutes=30)
    },
    sla_miss_callback=alert_sla_miss_high_priority,
    max_active_runs=1,
    catchup=False
)
def emergency_call_abusers_alert():
    load_911_call_abusers = SnowflakeAsyncDeferredOperator(
        task_id="load_emergency_call_abusers",
        sql="""
            INSERT INTO analytics.emergency_call_abusers_alert
            WITH new_alerts AS (
                SELECT
                    "client_details.client_data.user_data.username" AS user_id,
                    ARRAY_AGG(DISTINCT "payload.destination") AS numbers_called,
                    COUNT(1) AS total_calls,
                    AVG("payload.call_duration.seconds") AS avg_call_duration,
                    MAX(created_at) AS latest_call_time,
                    ARRAY_AGG(DISTINCT "payload.trunk_carrier") AS trunk_carriers_used
                FROM prod.party_planner_realtime.callcompleted
                WHERE
                    ("payload.call_direction" = 'CALL_DIRECTION_OUTBOUND')
                    AND (created_at BETWEEN '{{ ts }}'::TIMESTAMP - INTERVAL '50 min' 
                        AND '{{ ts }}'::TIMESTAMP + INTERVAL '10 min')
                    AND ("payload.destination" IN ('+1911', '+911', '911', '112', '+112', 
                                                '+1112', '+1933', '+933', '933'))
                GROUP BY 1
                HAVING (total_calls >= 5)
            )
            SELECT 
                n.user_id,
                n.numbers_called,
                n.total_calls,
                n.avg_call_duration,
                n.latest_call_time,
                '{{ ts }}' AS inserted_at,
                n.trunk_carriers_used
            FROM new_alerts n
            LEFT JOIN analytics.emergency_call_abusers_alert o ON 
                (n.user_id = o.user_id) 
                AND (n.latest_call_time = o.latest_call_time)
            WHERE (o.user_id IS NULL);
        """,
        warehouse=SF_WH_SMALL
    )

    @task
    def generate_911_alerts(task=None, ts=None):
        from de_utils.tndbo import get_dbo
        from de_utils.slack import send_message
        from de_utils.slack.tokens import get_de_alerts_token

        alert_sql = f""" 
            SELECT user_id, numbers_called, total_calls, avg_call_duration, trunk_carriers_used 
            FROM analytics.emergency_call_abusers_alert 
            WHERE (inserted_at = '{ts}');
        """
        alert_prefixes_df = get_dbo(SF_CONN_ID).read(alert_sql)
        task.log.info('Alert Prefix dataframe result:\n' + alert_prefixes_df.to_string(index=False))

        # alert Slack with those prefixes that are new
        if not alert_prefixes_df.empty:
            slack_msg_header = """911 abuse alert:\n"""
            tbl = "```" + alert_prefixes_df.to_string(index=False) + "```"
            send_message(
                env=ENV_ENUM,
                channel="911_abuse_alerts",
                message=slack_msg_header + tbl,
                get_auth_token_func=get_de_alerts_token,
                username="airflow",
                icon_emoji=":alert:"
            )

            task.log.info('Slack alert message: ' + slack_msg_header + tbl)

    load_911_call_abusers >> generate_911_alerts()


emergency_call_abusers_alert()

if __name__ == "__main__":
    emergency_call_abusers_alert().test(execution_date="2024-04-01")
