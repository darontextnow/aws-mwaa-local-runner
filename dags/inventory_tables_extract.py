"""DAG extracts data from several tables in MySQL Inventory Production DB and loads them into Snowflake.

DAG is designed to auto catchup should there be a failure in any given hourly run. Thus, no need to rerun a failed hour.
Note: only running sensor check on MySQL orders table for updated records. The assumption is
    that all other tables will also have been updated if orders is current.

"""
from airflow_utils import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dag_dependencies.sensors.sql_deferrable_sensor import SqlDeferrableSensor
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from de_utils.constants import MYSQL_CONN_ID_INVENTORY
from de_utils.slack.alerts import alert_on_failure
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 7, 25, 4, 0, 0),
    schedule="50 */2 * * *",
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=5),
        "retries": 3,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=False
)
def inventory_tables_extract():
    source_configs = [
        {
            "sql": """
                SELECT
                    i.id AS item_id, i.uuid, i.esn, i.iccid, i.mdn,
                    CASE i.activation_state
                        WHEN 0 THEN 'Pending Request'
                        WHEN 1 THEN 'Activated'
                        WHEN 2 THEN 'Suspended'
                        WHEN 3 THEN 'Inactive'
                    END AS activation_state,
                    i.created_at, i.updated_at, i.product_id, p.name AS product_name,
                    p.model, p.value, p.price, p.sale_price, p.web_store_enabled,
                    CASE p.webstore_quality
                        WHEN 0 THEN 'New'
                        WHEN 1 THEN 'Refurbished'
                        WHEN 2 THEN 'Used'
                        WHEN 3 THEN 'Gently Used'
                    END AS webstore_quality,
                    i.state_id, s.name AS state_name
                FROM items i
                INNER JOIN products p ON (p.id = i.product_id)
                INNER JOIN states s ON (s.id = i.state_id)
                WHERE (i.updated_at >  TIMESTAMP('{{ prev_data_interval_end_success or data_interval_start }}'))
                  AND (i.updated_at <= TIMESTAMP('{{ data_interval_end }}'))
            """,
            "target_table": "items"
        },
        {
            "sql": """
                SELECT DISTINCT
                    o.id, original_order_id, charge_id,
                    CASE
                        WHEN o.status = 1 THEN 'pending'
                        WHEN o.status = 2 THEN 'ready'
                        WHEN o.status = 3 THEN 'in_progress'
                        WHEN o.status = 4 THEN 'fulfilled'
                        WHEN o.status = 5 THEN 'shipped'
                        WHEN o.status = 6 THEN 'cancelled'
                        ELSE 'unknown'
                    END AS status,
                    type, o.created_at, o.updated_at, shipped_on,
                    CAST(shipping_fee as INT) AS shipping_fee,
                    shipping_error, refcode AS username, user_agent,
                    express_ship, include_return_label,
                    activation_readiness_type,
                    parent_return_id, campaign, ip_address
                FROM orders o
                LEFT JOIN stripe_order_configs s ON (o.id = s.order_id)
                WHERE (o.updated_at >  TIMESTAMP('{{ prev_data_interval_end_success or data_interval_start }}'))
                  AND (o.updated_at <= TIMESTAMP('{{ data_interval_end }}'))
            """,
            "target_table": "orders_data"
        },
        {
            "sql": """
                SELECT
                    i.id, esn, msl, mdn, msid, device_key, iccid, imsi, uuid,
                    device_config_id, subscription_id, product_id, s.name AS state,
                    i.created_at, i.updated_at, ic.title AS item_classification,
                    i.qc_count, suspended,recovery_mode, cdma_fallback, trued_up,
                    throttle_level, tethering, activation_state, ip_address
                FROM items i
                LEFT JOIN states s ON (i.state_id = s.id)
                LEFT JOIN item_classifications ic ON (i.item_classification_id = ic.id)
                WHERE (i.updated_at >  TIMESTAMP('{{ prev_data_interval_end_success or data_interval_start }}'))
                  AND (i.updated_at <= TIMESTAMP('{{ data_interval_end }}'))
            """,
            "target_table": "items_data"
        },
        {
            "sql": """
                SELECT * FROM order_items
                WHERE (updated_at >  TIMESTAMP('{{ prev_data_interval_end_success or data_interval_start }}'))
                  AND (updated_at <= TIMESTAMP('{{ data_interval_end }}'))   
            """,
            "target_table": "order_items"
        },
        {
            "sql": """
                SELECT
                    id, order_id, product_id, quantity, created_at, updated_at,
                    CAST(plan_price as INT) AS plan_price, CAST(device_price as INT) AS device_price
                FROM order_products
                WHERE (updated_at >  TIMESTAMP('{{ prev_data_interval_end_success or data_interval_start }}'))
                  AND (updated_at <= TIMESTAMP('{{ data_interval_end }}'))   
            """,
            "target_table": "order_products"
        },
        {
            "sql": """
                SELECT
                    fri.id, fri.fraud_test,fri.score, fri.detail, fri.succeeded,
                    fr.fraud_order_id, fo.order_id, fp.name AS order_type
                FROM fraud_results fr
                JOIN fraud_result_items fri ON (fr.id = fri.fraud_result_id)
                JOIN fraud_processes fp ON (fr.fraud_process = fp.id)
                JOIN fraud_orders fo ON (fr.fraud_order_id = fo.id)
            """,
            "target_table": "screen_results",
            "load_type": S3ToSnowflakeDeferrableOperator.LoadType.TRUNCATE
        },
        {
            "sql": """
                SELECT id,
                    order_id, created_at, updated_at, gift_from_email,
                    gift_from_number, gift_to_email, gift_to_number
                FROM gifts
                WHERE (updated_at >  TIMESTAMP('{{ prev_data_interval_end_success or data_interval_start }}'))
                  AND (updated_at <= TIMESTAMP('{{ data_interval_end }}'))   
            """,
            "target_table": "gifts"
        },
        {
            "sql": """
                SELECT 
                    id, 
                    item_id, 
                    event, 
                    created_at AS event_ts, 
                    SUBSTRING(object, POSITION('esn: ' IN object) + 6, 
                        POSITION('msl' IN object) - POSITION('esn: ' IN object) - 8) AS esn,
                    SUBSTRING(object, POSITION('mdn: ' IN OBJECT) + 6, 
                        POSITION('msid' IN object) - POSITION('mdn: ' IN object) - 8) AS mdn,
                    state, 
                    SUBSTRING(object, POSITION('product_id: ' IN object) + 12, 
                        POSITION('created_at:' IN object) - POSITION('product_id: ' IN object) - 13) AS product_id,
                    SUBSTRING(object, POSITION('suspended: ' IN OBJECT) + 11, 
                        POSITION('qc_count' IN object) - POSITION('suspended: ' IN object) - 12) AS suspended,
                    SUBSTRING(object, POSITION('device_key: ' IN OBJECT) + 12, 
                        POSITION('iccid:' IN object) - POSITION('device_key: ' IN object) - 13) AS device_key,
                    SUBSTRING(object, POSITION('iccid: ' IN OBJECT) + 8, 
                        POSITION('recovery_mode:' IN object) - POSITION('iccid: ' IN object) - 10) AS iccid,
                    SUBSTRING(object, POSITION('subscription_id: ' IN OBJECT) + 17, 
                        POSITION('cdma_fallback:' IN object) - POSITION('subscription_id: ' IN object) - 18) 
                        AS subscription_id,
                    SUBSTRING(object, POSITION('throttle_level: ' IN OBJECT) + 16, 
                        POSITION('tethering:' IN object) - POSITION('throttle_level: ' IN object) - 17) 
                        AS throttle_level,
                    SUBSTRING(object, POSITION('tethering: ' IN OBJECT) + 11, 
                        POSITION('item_classification_id:' IN object) - POSITION('tethering: ' IN object) - 12) 
                        AS tethering,
                    SUBSTRING(object, POSITION('uuid: ' IN OBJECT) + 6, 
                        POSITION('ip_address:' IN object) - POSITION('uuid: ' IN object) - 7) AS uuid,
                    activation_state,
                    SUBSTRING(object, POSITION('updated_at: ' IN OBJECT) + 12, 
                        POSITION('suspended:' IN object) - POSITION('updated_at: ' IN object) - 15) AS updated_at,
                    SUBSTRING(object, POSITION('imsi: ' IN OBJECT) + 7, 
                        POSITION('roaming:' IN object) - POSITION('imsi: ' IN object) - 9) AS imsi
                FROM item_versions
                WHERE (created_at >  TIMESTAMP('{{ prev_data_interval_end_success or data_interval_start }}'))
                  AND (created_at <= TIMESTAMP('{{ data_interval_end }}'))
            """,
            "target_table": "item_versions",
            "merge_match_where_expr": "(staging.event_ts > tgt.event_ts)"
        }
    ]

    wait_order = SqlDeferrableSensor(
        task_id="wait_orders",
        conn_id=MYSQL_CONN_ID_INVENTORY,
        sql="SELECT 1 FROM orders WHERE (updated_at >= TIMESTAMP('{{ data_interval_end }}')) LIMIT 1",
        timeout=3600,
        poke_interval=300
    )

    previous_task = wait_order
    for config in source_configs:
        group_id = f"{config['target_table']}_mysql_to_sf_transfer"
        collect, load = mysql_to_snowflake_task_group.override(group_id=group_id)(
            mysql_conn_id=MYSQL_CONN_ID_INVENTORY,
            sql=config["sql"],
            target_table=config["target_table"],
            target_schema="inventory",
            load_type=config.get("load_type", S3ToSnowflakeDeferrableOperator.LoadType.MERGE),
            merge_match_where_expr=config.get("merge_match_where_expr", "(staging.updated_at > tgt.updated_at)")
        )

        previous_task >> collect >> load
        previous_task = load

    trigger = TriggerDagRunOperator(
        task_id="trigger",
        trigger_dag_id="disable_web_reg_no_sim_order",
        execution_date="{{ data_interval_start }}",
    )

    previous_task >> trigger


inventory_tables_extract()

if __name__ == "__main__":
    inventory_tables_extract().test()
