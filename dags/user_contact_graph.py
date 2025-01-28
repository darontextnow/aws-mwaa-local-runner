from airflow_utils import dag
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from dag_dependencies.constants import SQL_SCRIPTS_DIR
from de_utils.constants import SF_WH_SMALL, SF_WH_MEDIUM
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 1, 2),
    schedule="45 7 * * *",
    default_args={
        "owner": "Dheeraj",
        "retry_delay": timedelta(minutes=15),
        "retries": 2,
        "sla": timedelta(hours=2)
    },
    template_searchpath=f"{SQL_SCRIPTS_DIR}/features_daily",
    max_active_runs=1
)
def user_contact_graph():

    create_shared_tmp_table = SnowflakeAsyncDeferredOperator(
        task_id="create_shared_tmp_table",
        sql="user_contact_graph_shared_tmp_table.sql",
        warehouse=SF_WH_MEDIUM
    )

    merge_user_contact_graph = SnowflakeAsyncDeferredOperator(
        task_id="update_user_contact_graph",
        sql="user_contact_graph.sql",
        warehouse=SF_WH_MEDIUM
    )

    delete_daily_contact_age = SnowflakeAsyncDeferredOperator(
        task_id="delete_daily_contact_age",
        sql="DELETE FROM core.daily_contact_age WHERE (contact_day = '{{ ds }}'::DATE)",
        warehouse=SF_WH_SMALL
    )

    insert_daily_contact_age = SnowflakeAsyncDeferredOperator(
        task_id="insert_daily_contact_age",
        sql="""INSERT INTO core.daily_contact_age
            SELECT DISTINCT
                a.user_id_hex,
                contact_day,
                a.normalized_contact,
                DATEDIFF('days', earliest_contact_day, '{{ ds }}'::DATE) contact_age
            FROM (
                SELECT * FROM analytics_staging.tmp_user_contact_graph_pp_call_mess_contacts_last_4_days
                WHERE (contact_day = '{{ ds }}'::DATE)
            ) a
            JOIN core.user_contact_graph b ON 
                (a.user_id_hex = b.user_id_hex) 
                AND (a.normalized_contact = b.normalized_contact)
        """,
        warehouse=SF_WH_MEDIUM
    )

    drop_shared_tmp_table = SnowflakeAsyncDeferredOperator(
        task_id="drop_shared_tmp_table",
        sql="DROP TABLE IF EXISTS analytics_staging.tmp_user_contact_graph_pp_call_mess_contacts_last_4_days"
    )

    (create_shared_tmp_table >> merge_user_contact_graph >>
     delete_daily_contact_age >> insert_daily_contact_age >> drop_shared_tmp_table)


user_contact_graph()

if __name__ == "__main__":
    user_contact_graph().test(execution_date="2024-06-18")
