"""Extracts from MySQL central DB data from receipts table and transforms it on load into Snowflake.

Currently only parsing receipt_types 2:IOS and 1:ANDROID.

Note: The following receipt types are no longer being parsed by this DAG as they were deprecated in 2018/2019:
    receipt_type == '4':  # receipt type '4' = CREDITS
    receipt_type == '5':  # receipt type '5' = AMAZON
    receipt_type == '7':  # receipt type '7' = WINDOWS

Parsing the raw content string from MySQL source is a little complicated.
It's completely different for ANDROID vs. IOS.
The IOS version does not use a standard JSON format, but it is JSON like. Thus, using SQL to convert it to JSON.
"""
from airflow_utils import dag
from dag_dependencies.task_groups import mysql_to_snowflake_task_group
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP
from de_utils.constants import MYSQL_CONN_ID_CENTRAL
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 10, 25),
    schedule="40 1 * * *",
    default_args={
        "owner": "Eric",
        "retry_delay": timedelta(minutes=5),
        "retries": 3
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def receipts_extract():

    mysql_to_snowflake_task_group(
        mysql_conn_id=MYSQL_CONN_ID_CENTRAL,
        sensor_sql="""SELECT 1 FROM receipts
            WHERE (created_at >= '{{ data_interval_end }}') 
            LIMIT 1
        """,
        sql="""
            SELECT id, content, type, username, created_at, is_valid, is_deducted, is_delayed
            FROM receipts
            WHERE (created_at >= '{{ data_interval_start }}' - INTERVAL 30 day)
              AND (created_at < '{{ data_interval_end }}')
        """,
        transform_sql="""
            SELECT 
                id, type, username, created_at, is_valid, is_deducted, is_delayed,
                CASE WHEN type = 2 THEN parsed:"transaction-id"::STRING ELSE parsed:orderId::STRING 
                    END AS order_id,
                CASE WHEN type = 2 THEN parsed:"app-item-id"::STRING ELSE parsed:packageName::STRING 
                    END AS app_name,
                CASE WHEN type = 2 THEN parsed:"product-id"::STRING ELSE parsed:productId::STRING 
                    END AS product_id,
                CASE WHEN type = 2 THEN parsed:"purchase-date-ms"::STRING ELSE parsed:purchaseTime::STRING 
                    END AS purchase_time_ms,
                CASE WHEN type = 2 THEN parsed:"expires-date"::STRING IS NOT NULL ELSE parsed:autoRenewing::STRING 
                    END AS auto_renewing,
                CASE WHEN type = 2 THEN parsed:"original-transaction-id"::STRING ELSE NULL 
                    END AS original_transaction_id
            FROM (
                SELECT id, type, username, created_at, is_valid, is_deducted, is_delayed,
                    CASE WHEN type = 2 THEN -- Must convert the IOS raw content string to standard JSON
                        PARSE_JSON(
                            REPLACE(
                                REPLACE(
                                    BASE64_DECODE_STRING(
                                        SPLIT(
                                            SPLIT(
                                                BASE64_DECODE_STRING(
                                                    REPLACE(content, '\\r\\n', '')  --must remove carriage returns
                                                ), -- decode base64 encoded string
                                            '"purchase-info" = "')[1], -- extract value of purchase-info key.
                                        '";')[0]), --split on ';' to easily remove everything after purchase-info value
                                ' = ', ':'), -- use standard json colon to separate key from value
                            ';\n', ',') -- use standard json comma to separate key:value pairs
                        ) -- now we should be able to parse the standard JSON string
                    ELSE PARSE_JSON(content) -- in the case of ANDROID it is already standard JSON
                    END AS parsed
                FROM staging
            )
        """,
        target_table="receipts",
        target_schema="core",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE,
        alt_staging_table_schema={
            "id": "NUMBER", "content": "STRING", "type": "NUMBER", "username": "STRING",
            "created_at": "TIMESTAMP_NTZ(9)",
            "is_valid": "BOOLEAN", "is_deducted": "BOOLEAN", "is_delayed": "BOOLEAN"
        }
    )


receipts_extract()

if __name__ == "__main__":
    receipts_extract().test(execution_date="2023-11-21")
