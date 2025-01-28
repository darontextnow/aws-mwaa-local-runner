from airflow_utils import dag
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.constants import (DAG_DEFAULT_CATCHUP, SQL_SCRIPTS_DIR, AIRFLOW_STAGING_KEY_PFX,
                                        ECR_TOOLING_ROOT_URL)
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import SF_BUCKET, SF_WH_SMALL
from kubernetes.client.models import V1ResourceRequirements
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 1, 2),
    schedule=None,  # triggered by dbt_downstream_dags_triggerer DAG
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=5),
        "retries": 2,
        "on_failure_callback": alert_on_failure
    },
    template_searchpath=f"{SQL_SCRIPTS_DIR}/firebase",
    max_active_runs=1,
    max_active_tasks=6,  # added this to not overwhelm worker resources since this runs at same time as other DAGs.
    catchup=DAG_DEFAULT_CATCHUP
)
def firebase_post_install_events():

    android_ios_events = {
        "sim_activation": ("sim_activation.sql", "sim_activation"),
        "sim_purchase": ("sim_purchase.sql", "sim_purchase"),
        "lp_sessions_out_call": ("pp_sessions_outbound_call.sql", "lp_sessions_gte4_out_call_d1"),
        "early_mover": ("early_mover.sql", "early_mover"),
        "synthetic_high_ltv": ("synthetic_high_ltv.sql", "synthetic_high_ltv"),
        "verified_registrations": ("verified_registrations.sql", "unique_verified_sign_up"),
        "early_mover_with_value": ("early_mover_with_value.sql", "early_mover_with_value")
    }

    for event_name, (script, firebase_name) in android_ios_events.items():
        s3_key = f"{AIRFLOW_STAGING_KEY_PFX}/firebase_events/{{{{ ds }}}}/{event_name}/gaids"

        collect = SnowflakeToS3DeferrableOperator(
            task_id=f"get_gaid_{event_name}",
            sql=script,
            s3_bucket=SF_BUCKET,
            s3_key=s3_key,
            file_format="(TYPE = CSV FIELD_DELIMITER = '\t' COMPRESSION = NONE "
                        "EMPTY_FIELD_AS_NULL = FALSE NULL_IF = (''))",
            copy_options="OVERWRITE = TRUE SINGLE = TRUE HEADER = TRUE MAX_FILE_SIZE =4900000000",
            poll_interval=30
        )

        send = KubernetesPodOperator(
            task_id=f"send_events_{event_name}",
            name="firebase2s-send-events",
            image=f"{ECR_TOOLING_ROOT_URL}/ds/firebase-s2s:latest",
            arguments=[
                "--firebase_event", firebase_name,
                "--s3_path", f"s3://{SF_BUCKET}/{s3_key}",
                "--validate_events", "'False'"  # set to True to run only a test run using GA4 validation server
            ],
            container_resources=V1ResourceRequirements(requests={"cpu": "2", "memory": "6Gi"},
                                                       limits={"cpu": "2", "memory": "8Gi"})
        )
        collect >> send

        if event_name in ("early_mover", "early_mover_with_value"):
            load_early_mover = S3ToSnowflakeDeferrableOperator(
                task_id=f"load_{event_name}",
                table="ua_events_sent_to_firebase",
                schema="analytics_staging",
                load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
                s3_loc=s3_key,
                transform_sql=f"SELECT gaid, '{event_name}', '{{{{ds}}}}' FROM staging",
                file_format="(TYPE = CSV FIELD_DELIMITER = '\t' PARSE_HEADER = TRUE "
                            "ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
                warehouse=SF_WH_SMALL
            )
            send >> load_early_mover


firebase_post_install_events()
