"""DAG extracts data from MySQL billing_service Database and loads it into Snowflake tables in billing_service schema.

Should DAG not run for any particular period, it will catch up on next run using previous successful run's timestamp.
Thus, only need to run one run for all missing hours.
"""
from airflow_utils import dag
from dag_dependencies.sensors.sql_deferrable_sensor import SqlDeferrableSensor
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from de_utils.constants import MYSQL_CONN_ID_CENTRAL
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 8, 16),
    schedule="20 * * * *",
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=3),
        "retries": 3
    },
    max_active_runs=1,
    catchup=False
)
def billing_service_extract():
    source_configs = [
        {
            "sql": """
            SELECT id, customer_id, recurring_bill_id, state, created_at, updated_at, deleted_at, payment_attempts
            FROM invoices
            WHERE (updated_at > TIMESTAMP('{{ prev_data_interval_end_success or data_interval_start }}'))
              AND (updated_at <= TIMESTAMP('{{ data_interval_end }}'))
            """,
            "target_table": "invoices"
        },
        {
            "sql": """
            SELECT id, invoice_id, `name`, description, amount, created_at, updated_at, deleted_at
            FROM invoice_items
            WHERE (updated_at > TIMESTAMP('{{ prev_data_interval_end_success or data_interval_start }}'))
              AND (updated_at <= TIMESTAMP('{{ data_interval_end }}'))
            """,
            "target_table": "invoice_items"
        },
        {
            "sql": """
            SELECT id, customer_id, invoice_id, `category`, created_at, updated_at, deleted_at
            FROM receipts
            WHERE (updated_at > TIMESTAMP('{{ prev_data_interval_end_success or data_interval_start }}'))
              AND (updated_at <= TIMESTAMP('{{ data_interval_end }}'))
            """,
            "target_table": "receipts"
        },
        {
            "sql": """
            SELECT
                id, receipt_id, payment_option_id, payment_method, identifier,
                amount, description, success, created_at, updated_at, deleted_at
            FROM receipt_items
            WHERE (updated_at > TIMESTAMP('{{ prev_data_interval_end_success or data_interval_start }}'))
              AND (updated_at <= TIMESTAMP('{{ data_interval_end }}'))
            """,
            "target_table": "receipt_items"
        },
        {
            "sql": """
            SELECT id, receipt_item_id, `key`, `value`, created_at, updated_at, deleted_at
            FROM receipt_item_properties
            WHERE (updated_at > TIMESTAMP('{{ prev_data_interval_end_success or data_interval_start }}'))
              AND (updated_at <= TIMESTAMP('{{ data_interval_end }}'))
            """,
            "target_table": "receipt_item_properties"
        },
        {
            "sql": """
            SELECT id, customer_id, payment_provider_id, identifier, created_at, updated_at, deleted_at
            FROM payment_options
            WHERE (updated_at > TIMESTAMP('{{ prev_data_interval_end_success or data_interval_start }}'))
              AND (updated_at <= TIMESTAMP('{{ data_interval_end }}'))
            """,
            "target_table": "payment_options"
        },
        {
            "sql": "SELECT id, `name`, created_at, updated_at, deleted_at FROM payment_providers",
            "target_table": "payment_providers",
            "load_type": S3ToSnowflakeDeferrableOperator.LoadType.TRUNCATE
        },
        {
            "sql": """
            SELECT id, identifier, created_at, updated_at, deleted_at
            FROM customers
            WHERE (updated_at > TIMESTAMP('{{ prev_data_interval_end_success or data_interval_start }}'))
              AND (updated_at <= TIMESTAMP('{{ data_interval_end }}'))
            """,
            "target_table": "customers"
        },
        {
            "sql": """
            SELECT username, order_id, invoice_id, created_at, updated_at
            FROM user_orders
            WHERE (updated_at > TIMESTAMP('{{ prev_data_interval_end_success or data_interval_start }}'))
              AND (updated_at <= TIMESTAMP('{{ data_interval_end }}'))
            """,
            "target_table": "user_orders",
            "mysql_conn_id": MYSQL_CONN_ID_CENTRAL
        }
    ]

    wait_invoices = SqlDeferrableSensor(
        task_id="wait_invoices",
        conn_id="tn_billing_service",
        sql="SELECT 1 FROM invoices WHERE (updated_at >= TIMESTAMP('{{ data_interval_end }}')) LIMIT 1",
        timeout=3600
    )

    previous_task = wait_invoices
    for config in source_configs:
        collect, load = mysql_to_snowflake_task_group(
            mysql_conn_id=config.get("mysql_conn_id", "tn_billing_service"),
            sql=config["sql"],
            target_table=config["target_table"],
            target_schema="billing_service",
            load_type=config.get("load_type", S3ToSnowflakeDeferrableOperator.LoadType.MERGE)
        )
        previous_task >> collect >> load  # run one task at a time to avoid worker resources overwhelm
        previous_task = load


billing_service_extract()

if __name__ == "__main__":
    billing_service_extract().test(execution_date="2023-08-13")
