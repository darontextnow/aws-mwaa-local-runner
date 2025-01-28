"""This DAG processes phone number requests and then calls Twilio to lookup the carrier information.
This currently assumes that all requests from the same username and phone number are under the same set of requests.
This is because users can and do submit multiple requests for the same phone number across days or weeks, and they
should be treated as just one request. However, this also ignores the fact that it is possible for users to
legitimately port in, port out, and then submit a new port in request for the same phone number. This version ignores
that scenario because there's no simple way of separating these two scenarios right now, but the backend number porting
automation may hold some clues to how to achieve that.
"""
from airflow_utils import dag, task
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP, AIRFLOW_STAGING_KEY_PFX
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from de_utils.constants import SF_CONN_ID, MYSQL_CONN_ID_PORTAGE, SF_BUCKET
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 10, 24, 17),
    schedule='*/30 * * * *',
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=3),
        "retries": 3
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def number_porting():
    s3_key = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}//data"  # replaces previous runs value

    _, update = mysql_to_snowflake_task_group(
        mysql_conn_id=MYSQL_CONN_ID_PORTAGE,
        sql="""SELECT
                requests.id as port_request_id,
                username,
                number as phone_number,
                requests.created_at as request_time,
                requests.updated_at,
                state,
                zip,
                status,
                provider,
                NULL as old_service_id,
                NULL as old_status,
                provider_id
            FROM requests
            LEFT JOIN addresses ON (requests.address_id = addresses.id)
            WHERE (requests.updated_at >  '{{ data_interval_start }}')
              AND (requests.updated_at <= '{{ data_interval_end }}')
        """,
        target_table="port_requests",
        target_schema="support",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE
    )

    @task
    def fetch_carriers(s3_key: str, task=None):
        from de_utils.tndbo import get_dbo
        import pandas as pd
        from twilio.rest import Client
        from airflow.models import Variable

        df = get_dbo(SF_CONN_ID).read("""
            SELECT DISTINCT pr.username, RIGHT(pr.phone_number, 10) AS phone_number, CURRENT_TIMESTAMP AS lookup_time
            FROM support.port_requests pr
            LEFT JOIN support.port_twilio pt ON 
                (pr.username = pt.username)
                AND (RIGHT(pr.phone_number, 10) = pt.phone_number)
            WHERE (pt.username IS NULL)""")
        if df.empty:
            task.log.info("No data was retrieved. Thus, skipping Snowflake load.")
            return False

        # Do twilio lookup and merge with df
        numbers = list(df.phone_number.unique())
        credentials = Variable.get("twilio_credentials", deserialize_json=True)
        twilio_client = Client(**credentials)
        lookup = dict(zip(numbers, [fetch_twilio(twilio_client, x) for x in numbers]))
        lookup_df = pd.DataFrame(lookup).transpose().reset_index()[['index', 'name', 'type']]
        lookup_df.columns = ['phone_number', 'carrier', 'line_type']
        df = df.merge(lookup_df, on='phone_number')[['lookup_time', 'username', 'phone_number', 'carrier', 'line_type']]
        df.to_csv(f"s3://{SF_BUCKET}/{s3_key}", index=False, header=False, sep='\t')
        return True

    snowflake_load = S3ToSnowflakeDeferrableOperator(
        task_id="load_data_snowflake",
        table="port_twilio",
        schema="support",
        s3_loc=s3_key,
        file_format=r"(TYPE = CSV FIELD_DELIMITER = '\t' DATE_FORMAT = AUTO)",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE
    )

    update >> fetch_carriers(s3_key) >> snowflake_load


def fetch_twilio(client, value):
    from twilio.base.exceptions import TwilioRestException
    try:
        result = client.lookups.v1.phone_numbers(value).fetch(country_code="US", type='carrier').carrier
    except TwilioRestException:
        return {
            "error_code": "Lookup Error",
            "mobile_country_code": None,
            "mobile_network_code": None,
            "name": None,
            "type": None
        }
    return result


number_porting()

if __name__ == "__main__":
    number_porting().test(execution_date="2023-10-23")
