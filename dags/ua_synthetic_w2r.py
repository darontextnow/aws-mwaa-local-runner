from airflow_utils import dag
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP, ECR_TOOLING_ROOT_URL
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from de_utils.slack.alerts import alert_on_failure, alert_sla_miss_high_priority
from de_utils.constants import STAGE_SF, MLFLOW_URI
from kubernetes.client.models import V1ResourceRequirements, V1EnvVar
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 1, 2),
    schedule="25 10 * * *",
    default_args={
        "owner": "Eric",
        "retry_delay": timedelta(minutes=10),
        "retries": 1,
        "on_failure_callback": alert_on_failure,
        "sla": timedelta(hours=1)
    },
    sla_miss_callback=alert_sla_miss_high_priority,
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def ua_synthetic_w2r():

    for client_type in ["TN_ANDROID", "2L_ANDROID"]:
        for lookback in range(-6, -2):

            score = KubernetesPodOperator(
                task_id=f"score_{client_type}{lookback}",
                name=f"synw2r-{client_type.replace('_', '-')}{lookback}",
                image=f"{ECR_TOOLING_ROOT_URL}/ds/ua_synthetic_w2r:2.1",
                arguments=[
                    "python",
                    "predict.py",
                    "prod",
                    client_type,
                    f"--run-date={{{{ macros.ds_add(ds, {lookback}) }}}}",
                ],
                env_vars=[V1EnvVar("DYNACONF_MLFLOW_TRACKING_URI", MLFLOW_URI)],
                container_resources=V1ResourceRequirements(requests={"cpu": "1", "memory": "4Gi"},
                                                           limits={"cpu": "1", "memory": "4Gi"}),
                security_context=dict(fsGroup=65534)
            )

            snowflake_load = S3ToSnowflakeDeferrableOperator(
                task_id=f"loading_s3_snowflake_{client_type}{lookback}",
                table="ua_synthetic_w2r",
                schema="ua",
                load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
                pattern=f".*{client_type}[.]parquet",
                file_format="(TYPE = PARQUET)",
                copy_options="FORCE = TRUE",
                in_copy_transform_sql=f"""
                    SELECT DISTINCT
                        TO_DATE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', 2), '=', 2), 'YYYY-MM-DD') AS date_utc,
                        $1:"client_type" AS client_type,
                        $1:"adjust_id" AS adjust_id,
                        to_timestamp_ntz($1:"installed_at"::int,6) AS installed_at,
                        $1:"w2r_prob" AS w2r_prob
                    FROM @{STAGE_SF}/synthetic_w2r/date_utc={{{{ macros.ds_add(ds, {lookback}) }}}}/
                """,
                pre_exec_sql=[
                    f"""DELETE FROM ua.ua_synthetic_w2r 
                        WHERE 
                            (date_utc = '{{{{ macros.ds_add(ds, {lookback}) }}}}'::DATE)
                            AND (CLIENT_TYPE = '{client_type}')"""
                ]
            )

            score >> snowflake_load


ua_synthetic_w2r()
