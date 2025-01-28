"""This DAG makes synthetic high LTV predictions on D1."""
from airflow_utils import dag
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP, ECR_TOOLING_ROOT_URL, AIRFLOW_STAGING_KEY_PFX
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import ENV_VALUE, SF_BUCKET, MLFLOW_URI
from kubernetes.client.models import V1ResourceRequirements
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 1, 2),
    schedule=None,  # triggered by dbt_downstream_dags_triggerer DAG
    default_args={
        "owner": "Eric",
        "retry_delay": timedelta(minutes=5),
        "retries": 2,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def ltv_d1_models():
    s3_key = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ data_interval_start.date() }}}}/tn_android.csv"

    predict = KubernetesPodOperator(
        task_id="predict_TN_ANDROID_Paid",
        name="predict_TN_ANDROID_Paid",
        image=f"{ECR_TOOLING_ROOT_URL}/ds/ltv_active_days:v4.0",
        arguments=[
            "python", "predict_d1.py",
            "--model_uri=models:/D1 Top Active Days Model TN_ANDROID/Production",
            "--model2_uri=models:/D1 Active Days Model TN_ANDROID/Production",
            "--client_type=TN_ANDROID",
            "--from_date={{ macros.ds_add(ds, -2) }}",
            "--to_date={{ macros.ds_add(ds, 1) }}",
            f"--output_uri=s3://{SF_BUCKET}/{s3_key}",
            f"--mlflow_uri={MLFLOW_URI}"
        ],
        container_resources=V1ResourceRequirements(requests={"cpu": "4", "memory": "9Gi"},
                                                   limits={"cpu": "4", "memory": "9Gi"})
    )

    upload = S3ToSnowflakeDeferrableOperator(
        task_id="load_predictions",
        table="ltv_d1_prediction",
        schema="analytics",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
        s3_loc=s3_key,
        file_format="(TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='\"')",
        pre_exec_sql=[f"""DELETE FROM {ENV_VALUE}.analytics.ltv_d1_prediction
            WHERE (installed_at >= '{{{{ macros.ds_add(ds, -2) }}}}')
              AND (installed_at < '{{{{ macros.ds_add(ds, 1) }}}}')"""]
    )

    predict >> upload


ltv_d1_models()
