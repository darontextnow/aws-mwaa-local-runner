"""The purpose of this DAG is to reenable some disabled users based on certain
conditions set by Trust & Safety: Data Science.

--- Reference: https://textnow.atlassian.net/wiki/spaces/DSA/pages/20918861906/Refactoring+Re-Enable+Jobs
"""
from airflow_utils import dag
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP, SQL_SCRIPTS_DIR, ECR_TOOLING_ROOT_URL
from de_utils.constants import SF_BUCKET
from de_utils.slack.alerts import alert_on_failure
from datetime import datetime, timedelta
from kubernetes.client.models import V1ResourceRequirements


@dag(
    start_date=datetime(2024, 11, 6),
    schedule="50 2 * * *",
    default_args={
        "owner": "Satish",
        "retry_delay": timedelta(minutes=5),
        "retries": 1,
        "on_failure_callback": alert_on_failure
    },
    template_searchpath=f"{SQL_SCRIPTS_DIR}/antifraud",
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def account_reenabler():
    s3_dest_key = "antifraud/account_reenabler/{{ ds }}/appeals.csv"

    sf_dump = SnowflakeToS3DeferrableOperator(
        task_id="dump_usernames_s3",
        sql="user_account_reenabler.sql",
        s3_bucket=SF_BUCKET,
        s3_key=s3_dest_key,
        file_format="(TYPE = CSV COMPRESSION = NONE FIELD_OPTIONALLY_ENCLOSED_BY = '\"'"
                    " EMPTY_FIELD_AS_NULL = FALSE NULL_IF = (''))",
        copy_options="OVERWRITE = TRUE HEADER = TRUE SINGLE = TRUE MAX_FILE_SIZE = 4900000000"
    )

    verify_data_exists = S3KeySensor(
        task_id="verify_data_exists",
        bucket_key=s3_dest_key,
        bucket_name=SF_BUCKET,
        soft_fail=True,
        timeout=1,
        poke_interval=1
    )

    reenable = KubernetesPodOperator(
        task_id="reenable_users",
        name="reenable_users",
        image=f"{ECR_TOOLING_ROOT_URL}/ds/account_reenabler:1",
        arguments=[s3_dest_key],
        container_resources=V1ResourceRequirements(requests={"cpu": "1", "memory": "1Gi"},
                                                   limits={"cpu": "2", "memory": "2Gi"}),
        security_context=dict(fsGroup=65534)
    )

    sf_dump >> verify_data_exists >> reenable


account_reenabler()

if __name__ == "__main__":
    account_reenabler().test(execution_date="2023-11-15")
