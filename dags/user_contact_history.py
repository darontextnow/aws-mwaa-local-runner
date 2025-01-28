"""
This DAG is running on the 0th minute of every hour since it accounts for hour window of user contact history from the
previous hour's 0th minute till the current minute and is being used to catch spammers and needs to be pushed to Dynamo

It also uses small amount of resources. If this were to change in the future we can adjust the time accordingly.
"""
from airflow_utils import dag
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.constants import SQL_SCRIPTS_DIR, AIRFLOW_STAGING_KEY_PFX, DAG_DEFAULT_CATCHUP, \
    ECR_TOOLING_ROOT_URL
from de_utils.constants import SF_BUCKET, SF_WH_MEDIUM
from kubernetes.client.models import V1ResourceRequirements
from de_utils.slack.alerts import alert_on_failure
from datetime import datetime, timedelta

DYNAMO_TABLE_NAME = "ds_user_contacts"


@dag(
    start_date=datetime(2024, 4, 9),
    schedule="0 * * * *",  # hourly job
    default_args={
        "owner": "Satish",
        "retry_delay": timedelta(minutes=15),
        "retries": 2,
        "sla": timedelta(hours=2),
        "on_failure_callback": alert_on_failure,
    },
    template_searchpath=f"{SQL_SCRIPTS_DIR}/features_hourly",
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def user_contact_history():
    s3_dest_key = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ ts }}}}/"

    sf_dump = SnowflakeToS3DeferrableOperator(
        task_id="get_transformed_dynamo_data",
        sql="user_contact_history.sql",
        warehouse=SF_WH_MEDIUM,
        s3_bucket=SF_BUCKET,
        s3_key=s3_dest_key,
        file_format="(TYPE = JSON  COMPRESSION = NONE STRIP_NULL_VALUES = TRUE)",
        copy_options="OVERWRITE = TRUE MAX_FILE_SIZE = 15000000",  # Max file size of 15MB
        session_parameters={"TIMESTAMP_NTZ_OUTPUT_FORMAT": 'YYYY-MM-DDTHH24:MI:SS'},
        poll_interval=30
    )

    agg_sf_dump = SnowflakeToS3DeferrableOperator(
        task_id="get_transformed_agg_dynamo_data",
        sql="user_contact_history_agg.sql",
        warehouse=SF_WH_MEDIUM,
        s3_bucket=SF_BUCKET,
        s3_key=f"{s3_dest_key}agg_",
        file_format="(TYPE = JSON  COMPRESSION = NONE STRIP_NULL_VALUES = TRUE)",
        copy_options="OVERWRITE = TRUE MAX_FILE_SIZE = 15000000",  # Max file size of 15MB
        session_parameters={"TIMESTAMP_NTZ_OUTPUT_FORMAT": 'YYYY-MM-DDTHH24:MI:SS'},
        poll_interval=30  # usually quick loads, thus keeping poll interval small
    )

    start_upload = KubernetesPodOperator(
        task_id="send_to_dynamo",
        name="dynamo-send-events",
        image=f"{ECR_TOOLING_ROOT_URL}/ds/dynamodb-push:latest",
        arguments=[s3_dest_key, "json", DYNAMO_TABLE_NAME],
        container_resources=V1ResourceRequirements(requests={"cpu": "1", "memory": "3Gi"},
                                                   limits={"cpu": "1", "memory": "4Gi"}),
        security_context=dict(fsGroup=65534)
    )

    [sf_dump, agg_sf_dump] >> start_upload


user_contact_history()

if __name__ == "__main__":
    user_contact_history().test(execution_date="2023-11-12")
