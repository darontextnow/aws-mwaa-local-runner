from airflow_utils import dag, task
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from de_utils.slack.alerts import alert_on_failure, alert_sla_miss_high_priority
from de_utils.constants import ENV_ENUM, ENV_VALUE, SF_CONN_ID, SF_WH_SMALL
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 10, 3),
    schedule="*/10 * * * *",  # every 10 mins
    default_args={
        "owner": "Kannan",
        "retry_delay": timedelta(minutes=5),
        "retries": 1,
        "on_failure_callback": alert_on_failure,
        "sla": timedelta(minutes=30)
    },
    sla_miss_callback=alert_sla_miss_high_priority,
    max_active_runs=1,
    catchup=False
)
def call_pumping_prefixes_alert():
    """
    load_call_pumping_prefix task:
        1. filter prefixes using certain criteria.
        2. check duplicate prefixes that have been previously filtered.
        3. load the latest filtered prefixes to table.
    slack_call_pumping_prefix task:
        4. read from snowflake table and report the latest filtered prefixes to Slack.
    auto_markup task:
        5. Auto markup the IA/SD prefixes if the cost is greater than $25
        6. Report the auto markup prefixes to Slack
    """
    load_call_pumping_prefix = SnowflakeAsyncDeferredOperator(
        task_id="load_call_pumping_prefix",
        sql="""INSERT INTO analytics.call_pumping_prefixes_alert
            SELECT 
                CURRENT_DATE, prefix, total_calls, call_duration, tn_call_cost, user_call_cost, unique_numbers,
                min_markup, avg_markup, max_term_rate, avg_term_rate, '{{ ts }}' AS created_at,
                avg_call_duration_sec, connected_calls, connect_rate, unique_callers
            FROM analytics.vw_get_call_pumping_prefixes; 
        """,
        warehouse=SF_WH_SMALL
    )    

    @task
    def generate_alerts(task=None, ts=None):
        """
        This task does the following:
            1. get the latest filtered prefixes from snowflake table
            2. Report the prefixes to Slack
        """
        from de_utils.tndbo import get_dbo
        from de_utils.slack import send_message
        from de_utils.slack.tokens import get_de_alerts_token

        alert_sql = f""" 
            SELECT 
                prefix, tn_call_cost AS cost, unique_numbers AS unique_called_numbers, unique_callers, total_calls,
                connected_calls, connect_rate, call_duration AS call_duration_sec, avg_call_duration_sec, min_markup 
            FROM analytics.call_pumping_prefixes_alert 
            WHERE (created_at = '{ts}');
        """
        alert_prefixes_df = get_dbo(SF_CONN_ID).read(alert_sql)
        task.log.info('Alert Prefix dataframe result:\n' + alert_prefixes_df.to_string(index=False))

        # alert Slack with those prefixes that are new
        if not alert_prefixes_df.empty:
            slack_msg_header = """Call pumping alerts:\n"""
            tbl = "```" + alert_prefixes_df.to_string(index=False) + "```"
            send_message(
                env=ENV_ENUM,
                channel="call_pumping_alerts",
                message=slack_msg_header + tbl,
                get_auth_token_func=get_de_alerts_token,
                username="airflow",
                icon_emoji=":loudspeaker:"
            )

            task.log.info('Slack alert message: ' + slack_msg_header + tbl)

    @task
    def auto_markup_prefix(task=None, ts=None):
        """
        This task does the following:
            1. Auto markup the IA/SD prefixes if the cost is greater than $25
            2. Report the auto markup prefixes to Slack
        """
        from de_utils.tndbo import get_dbo
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        from de_utils.slack import send_message
        from de_utils.slack.tokens import get_de_alerts_token

        markup_sql = f"""
            SELECT DISTINCT SUBSTRING(c.prefix, 2, 7) AS prefix 
            FROM analytics.call_pumping_prefixes_alert c
            INNER JOIN support.npa_codes n ON SUBSTRING(c.prefix, 3, 3) = n.npa_id
            WHERE (created_at = '{ts}') AND (n."location" IN ('IA','SD')) AND (c.prefix LIKE '+1%');
        """
        markup_prefixes_df = get_dbo(SF_CONN_ID).read(markup_sql)
        task.log.info('Markup dataframe result:\n ' + markup_prefixes_df.to_string(index=False))

        # auto markup and report to Slack if markup_prefixes_df is not empty.
        if not markup_prefixes_df.empty:
            mysql_hook = MySqlHook(mysql_conn_id=f"{ENV_VALUE}_mysql_phone_write")
            markup_queries = []
            for idx, row in enumerate(markup_prefixes_df["prefix"].to_numpy(), start=1):
                dial_code = str(row) + str(idx)
                insert_query = f"""
                        INSERT IGNORE INTO call_rates_v3 (
                            dial_code, rate, term_provider_id, call_rate_deck_id, min_time,inc_time,markup, 
                            description, effective
                        )
                        VALUES (
                            {dial_code}, -- dial_code
                            0.1, -- rate
                            10, -- term_provider_id
                            9, -- call_rate_deck_id - 9 is fraud
                            6, -- min_time
                            6, -- inc_time
                            400, -- markup
                            "call pumping markup from airflow", -- description
                            now() -- effective
                        );
                        """
                task.log.info("Marking up dial code: " + dial_code)
                markup_queries.append(insert_query)

                mysql_hook.run(markup_queries)

            slack_markup_msg_header = """Auto markup is completed for below prefixes:\n"""
            tbl = "```" + markup_prefixes_df.to_string(index=False) + "```"
            slack_msg = slack_markup_msg_header + tbl
            task.log.info('slack markup msg:' + slack_msg)
            send_message(
                env=ENV_ENUM,
                channel="call_pumping_alerts",
                message=slack_msg,
                get_auth_token_func=get_de_alerts_token,
                username="airflow",
                icon_emoji=":loudspeaker:"
            )

    load_call_pumping_prefix >> generate_alerts() >> auto_markup_prefix()


call_pumping_prefixes_alert()

if __name__ == "__main__":
    call_pumping_prefixes_alert().test(execution_date="2023-10-03")
