"""Disable users who register on web using payment token and dont purchase sim within 6 hours.

This DAG is triggered by successful completion of upstream inventory_tables_extract DAG.
DAG should run the latest/most current run only, thus setting catchup=False.
"""
from airflow_utils import dag
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.constants import (FRAUD_DISABLER_BASE_KUBERNETES_ENV, FRAUD_DISABLER_IMAGE,
                                        FRAUD_DISABLER_ARGUMENTS, FRAUD_DISABLER_CONTAINER_RESOURCES)
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import SF_BUCKET, SF_WH_MEDIUM
from kubernetes.client.models import V1EnvVar
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 5, 23),
    schedule=None,
    default_args={
        "owner": "Ross",
        "retry_delay": timedelta(minutes=5),
        "retries": 3,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=False
)
def disable_web_reg_no_sim_order():
    s3_dest_key = "antifraud/web-reg-no-sim-order/usernames_{{ ts }}"

    sf_dump = SnowflakeToS3DeferrableOperator(
        task_id="dump_usernames_s3",
        sql="""
            WITH ordering_users AS (
                SELECT DISTINCT o.username 
                FROM inventory.order_products AS p
                JOIN inventory.orders_data AS o ON (p.order_id = o.id)
                WHERE 
                  (p.product_id in (392, 428)) 
                  AND (o.created_at>current_date - interval '5 days')
            ),
            web_regs AS (
                SELECT u.username 
                FROM core.users u
                JOIN core.registrations AS r ON (u.username=r.username)
                LEFT JOIN (
                    SELECT * from core.disabled WHERE (disabled_at > current_timestamp - interval '6 hours')
                ) d ON (u.user_id_hex = d.global_id)
                WHERE
                  (u.created_at BETWEEN current_date - INTERVAL '5 days' AND current_timestamp - INTERVAL '6 hours')
                  AND (r.client_type='TN_WEB')
                  AND (account_status!='HARD_DISABLED')
                  AND (d.global_id IS NULL)
            )
            SELECT username 
            FROM web_regs
            LEFT JOIN ordering_users o USING (username)
            WHERE (o.username IS NULL)
        """,
        s3_bucket=SF_BUCKET,
        s3_key=s3_dest_key,
        file_format=r"(TYPE = CSV COMPRESSION = NONE EMPTY_FIELD_AS_NULL = FALSE NULL_IF = (''))",
        copy_options="OVERWRITE = TRUE SINGLE = TRUE HEADER = TRUE MAX_FILE_SIZE =4900000000",
        warehouse=SF_WH_MEDIUM
    )

    verify_data_exists = S3KeySensor(
        task_id="verify_data_exists",
        bucket_key=s3_dest_key,
        bucket_name=SF_BUCKET,
        soft_fail=True,
        timeout=1,
        poke_interval=1
    )

    disable = KubernetesPodOperator(
        task_id="disable_users",
        name="disable_users",
        image=FRAUD_DISABLER_IMAGE,
        arguments=FRAUD_DISABLER_ARGUMENTS,
        env_vars=FRAUD_DISABLER_BASE_KUBERNETES_ENV + [
            V1EnvVar("TN_REASON", "web-reg-no-sim-order"),
            V1EnvVar("TN_FILE", f"s3://{SF_BUCKET}/{s3_dest_key}"),
            V1EnvVar("TN_REVOKE", "FALSE"),
            V1EnvVar("TN_RESERVE_NUMBER", "FALSE"),
            V1EnvVar("TN_ACCOUNT_STATUS", "HardDisabled")
        ],
        container_resources=FRAUD_DISABLER_CONTAINER_RESOURCES
    )

    sf_dump >> verify_data_exists >> disable


disable_web_reg_no_sim_order()

if __name__ == "__main__":
    disable_web_reg_no_sim_order().test(execution_date='2023-08-01')
