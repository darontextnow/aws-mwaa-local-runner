"""DAG extracts data from several tables in MySQL Angel DB and loads them into Snowflake.

Note: only running sensor check on MySQL user_notes table for updated records. The assumption is
    that all other tables will also have been updated if user_notes is current.

"""
from airflow_utils import dag
from dag_dependencies.sensors.sql_deferrable_sensor import SqlDeferrableSensor
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP
from de_utils.constants import SF_CONN_ID
from de_utils.slack.alerts import alert_on_failure
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 11, 12),
    schedule="10 * * * *",
    default_args={
        "owner": "DE Team",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "catchup": DAG_DEFAULT_CATCHUP,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def angel_db_extract():
    source_configs = [

        {
            "sql": """
                SELECT 
                    id AS user_notes_id,
                    username,
                    who AS agent_email,
                    body AS note,
                    pinned,
                    created_at,
                    updated_at
                FROM user_notes
                WHERE (updated_at BETWEEN TIMESTAMP('{{ data_interval_start }}') 
                    AND TIMESTAMP('{{ data_interval_end }}'))""",
            "target_table": "user_notes",
            "pkey_cols": ["user_notes_id"]
        },
        {
            "sql": """
                SELECT 
                    id AS user_note_attribute_id,
                    user_note_id,
                    attribute_key,
                    attribute_value,
                    created_at,
                    updated_at
                FROM user_note_attributes
                WHERE (updated_at BETWEEN TIMESTAMP('{{ data_interval_start }}') 
                    AND TIMESTAMP('{{ data_interval_end }}'))""",
            "target_table": "user_notes_attributes",
            "pkey_cols": ["id"]
        },
        {
            "sql": """
                SELECT 
                    id AS user_note_part_id,
                    user_note_id,
                    part_key,
                    part_value,
                    created_at,
                    updated_at
                FROM user_note_parts
                WHERE (updated_at BETWEEN TIMESTAMP('{{ data_interval_start }}') 
                    AND TIMESTAMP('{{ data_interval_end }}'))""",
            "target_table": "user_notes_parts",
            "pkey_cols": ["id"]
        },
        {
            "sql": """
                SELECT 
                    dun.user_note_id,
                    d.id AS disposition_id,
                    d.name AS disposition_name,
                    db.id AS disposition_bucket_id,
                    db.name AS disposition_bucket_name,
                    d.created_at AS created_at,
                    d.updated_at AS updated_at,
                    d.deleted_at AS deleted_at
                FROM dispositions_user_notes dun
                LEFT JOIN dispositions d ON (dun.disposition_id = d.id)
                LEFT JOIN disposition_buckets db ON (d.disposition_bucket_id = db.id)
                WHERE (d.updated_at BETWEEN TIMESTAMP('{{ data_interval_start }}') 
                    AND TIMESTAMP('{{ data_interval_end }}'))""",
            "target_table": "dispositions",
            "pkey_cols": ["user_note_id", "disposition_id"]
        },
        {
            "sql": """
                SELECT 
                    a.id AS agent_id,
                    a.email AS agent_email,
                    ag.group_id AS agent_group_id,
                    g.name AS agent_group_name,
                    gr.role_id AS agent_role_id,
                    r.name AS agent_role_name,
                    g.created_at AS created_at,
                    g.updated_at AS updated_at
                FROM agents a
                LEFT JOIN agents_groups ag ON (a.id = ag.agent_id)
                LEFT JOIN `groups` g ON (ag.group_id = g.id)
                LEFT JOIN groups_roles gr ON (ag.group_id = gr.group_id)
                LEFT JOIN roles r ON (gr.role_id = r.id)
                WHERE (g.updated_at BETWEEN TIMESTAMP('{{ data_interval_start }}') 
                    AND TIMESTAMP('{{ data_interval_end }}'))""",
            "target_table": "agents",
            "pkey_cols": ["agent_id", "group_id"]
        }
    ]

    wait_user_notes = SqlDeferrableSensor(
        task_id="wait_angel_user_notes",
        conn_id="mysql_angel",
        sql="SELECT 1 FROM user_notes WHERE (updated_at >= TIMESTAMP('{{ data_interval_end }}')) LIMIT 1",
        timeout=3600
    )

    previous_task = wait_user_notes
    for config in source_configs:
        group_id = f"{config['target_table']}_mysql_to_sf_transfer"
        collect, load = mysql_to_snowflake_task_group.override(group_id=group_id)(
            mysql_conn_id="mysql_angel",
            sql=config["sql"],
            target_table=config["target_table"],
            snowflake_conn_id=SF_CONN_ID,
            target_schema="angel",
            load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE,
            merge_match_where_expr="(staging.updated_at > tgt.updated_at)"
        )

        previous_task >> collect >> load
        previous_task = load


angel_db_extract()

if __name__ == "__main__":
    angel_db_extract().test()
