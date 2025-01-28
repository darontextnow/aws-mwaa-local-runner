from airflow_utils import dag, task
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP
from de_utils.constants import MYSQL_CONN_ID_CENTRAL, MYSQL_CONN_ID_PHONE, SF_CONN_ID
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 10, 25, 12, 50),
    schedule="50 * * * *",
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=5),
        "retries": 3
    },
    max_active_runs=1,  # Only 1 run at a time so bookmark works correctly.
    catchup=DAG_DEFAULT_CATCHUP
)
def phone_numbers_extract():

    # core.phone_number_logs
    _, load_phone_number_logs = mysql_to_snowflake_task_group(
        mysql_conn_id=MYSQL_CONN_ID_CENTRAL,
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
        sql="""
            SELECT * FROM phone_number_logs
            WHERE
              id > {{ ti.xcom_pull(task_ids="phone_number_logs_set_bookmark", key="max_id", include_prior_dates=True) }}
            ORDER BY id 
            LIMIT 5000000""",
        target_table="phone_number_logs",
        target_schema="core"
    )

    @task
    def phone_number_logs_set_bookmark(ti=None, task=None):
        # retrieve MAX id from phone_number_logs table - this is the id added by task above.
        from de_utils.tndbo import get_dbo
        max_id = get_dbo(SF_CONN_ID).execute("SELECT MAX(id) FROM core.phone_number_logs").fetchone()[0]
        task.log.info(f"Max id retrieved from MySQL phone_number_logs this run was: {max_id}")
        task.log.info("Pushing max id to xcom for next run.")
        ti.xcom_push(key="max_id", value=max_id)

    # core.phone_numbers
    collect_phone_numbers, load_phone_numbers = mysql_to_snowflake_task_group(
        mysql_conn_id=MYSQL_CONN_ID_PHONE,
        sql="""SELECT
              pn.id,
              pn.created_at,
              CONCAT('+', c.country_code, area_code, phone_number) AS phone_number,
              pn.last_update,
              pn.reserved,
              pn.premium,
              e.environment_name,
              pn.assignment_count,
              pn.extra_score,
              pn.extra_score_expiry,
              ac.state,
              c.code as country_code,
              pn.did_provider_id
            FROM phone_numbers pn
            JOIN area_codes ac ON pn.area_code_id = ac.id
            JOIN countries_v2 c ON ac.country_id = c.id
            LEFT JOIN environments e ON (pn.environment_id = e.id)
            WHERE 
                (pn.last_update >= '{{ data_interval_start }}')
                AND (pn.last_update <  '{{ data_interval_end }}')
        """,
        # filtering by environment_id = 3 helps greatly reduce query runtime for this sensor_sql
        sensor_sql="""
            SELECT 1
            FROM phone_numbers
            WHERE (environment_id = 3) AND (last_update >= '{{ data_interval_end }}')
            LIMIT 1
        """,
        target_table="phone_numbers",
        target_schema="core",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE)

    # core.phone_number_inventory
    # Notes from Robert Robinson:
    # There are essentially 3 primary states a phone number we own can be in.
    # 1) unassigned (no environment_id in the row for phone_numbers, no row with
    #    the phone_number_id matching in the reservations table)
    # 2) reserved (environment_id in the row for phone_numbers and a corresponding
    #    row with the phone_number_id matching in the reservations table). We tens
    #    of thousands of phone numbers we own but won't give to users. Some are numbers
    #    used for other purposes (e911, proxy calling, group messaging, etc.). Some are
    #    numbers we are offering to users in the multi-number select workflow, but will
    #    shortly return back to state 1
    # 3) assigned (environment_id in the row for phone_numbers, and no corresponding row
    #    with the phone_number_id matching in the reservations table). This is given to a given TN user
    # There are regions with overlaying area codes (e.g. 415/628 for San Francisco)
    # and we group them together
    collect_inventory, _ = mysql_to_snowflake_task_group(
        mysql_conn_id=MYSQL_CONN_ID_PHONE,
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
        sql="""SELECT
            (SELECT max(last_update) FROM phone_numbers) AS timestamp,
            CASE WHEN area_codes.region IS NULL THEN area_codes.area_code ELSE concat("R", area_codes.region) END 
                AS area,
            countries_v2.code AS country,
            COUNT(*) AS owned,
            SUM(CASE WHEN phone_numbers.id IN (SELECT phone_number_id FROM reservations) THEN 1 ELSE 0 END) AS reserved,
            SUM(CASE WHEN environment_id IS NULL AND phone_numbers.id NOT IN (SELECT phone_number_id FROM reservations)
                THEN 1 ELSE 0 END) AS available
            FROM phone_numbers
            JOIN area_codes ON (phone_numbers.area_code_id = area_codes.id)
            JOIN countries_v2 ON (countries_v2.id = area_codes.country_id)
            GROUP BY area;
        """,
        target_table="phone_number_inventory",
        target_schema="core"
    )

    (load_phone_number_logs >> phone_number_logs_set_bookmark() >>
     collect_phone_numbers >> load_phone_numbers >> collect_inventory)


phone_numbers_extract()

if __name__ == "__main__":
    phone_numbers_extract().test(execution_date="2023-10-24")
