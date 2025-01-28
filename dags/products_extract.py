from airflow_utils import dag
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from de_utils.constants import MYSQL_CONN_ID_INVENTORY, SF_WH_SMALL
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 10, 24, 1, 25),
    schedule="20 2 * * *",
    default_args={
        "owner": "Eric",
        "retry_delay": timedelta(minutes=5),
        "retries": 3
    },
    max_active_runs=1,
    catchup=False
)
def products_extract():
    # the query has an extra filter on product_set.id
    # this is to counter a database entry error that assigns product 253 to two different product_sets
    _,  sf_load = mysql_to_snowflake_task_group(
        mysql_conn_id=MYSQL_CONN_ID_INVENTORY,
        sql="""
            SELECT p.*, s.name AS product_set
            FROM products p
            LEFT JOIN (
                SELECT product_id, MAX(product_set_id) AS product_set_id 
                FROM product_variants
                GROUP BY product_id
            ) v ON p.id = v.product_id
            LEFT JOIN product_sets s ON (v.product_set_id = s.id)
        """,
        target_table="products",
        target_schema="core",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.TRUNCATE
    )

    register_products_history = SnowflakeAsyncDeferredOperator(
        task_id="register_products_history_snowflake",
        sql="""INSERT INTO core.products_history
            SELECT p.* FROM prod.core.products p
            LEFT JOIN (
              SELECT id, MAX(updated_at) last_updated
              FROM prod.core.products_history h
              GROUP BY id
            ) h ON (p.id = h.id)
            WHERE (h.id IS NULL) OR (p.updated_at > h.last_updated);
        """,
        warehouse=SF_WH_SMALL
    )

    sf_load >> register_products_history


products_extract()

if __name__ == "__main__":
    products_extract().test(execution_date="2023-10-24 01:25:00")
