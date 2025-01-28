from airflow_utils import dag
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP, ECR_TOOLING_ROOT_URL, AIRFLOW_STAGING_KEY_PFX
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from kubernetes.client.models import V1ResourceRequirements, V1EnvVar
from de_utils.constants import ENV_VALUE, MLFLOW_URI
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 1, 2),
    schedule="50 9 * * *",  # needs to be in sync with ExternalTaskDeferredSensor in dbt.py
    default_args={
        "owner": "Eric",
        "retry_delay": timedelta(minutes=5),
        "retries": 2
    },
    max_active_runs=3,
    catchup=DAG_DEFAULT_CATCHUP
)
def rfm_segmentation():

    predict = KubernetesPodOperator(
        task_id="predict_rfm_segments",
        name="predict-rfm-segments",
        image=f"{ECR_TOOLING_ROOT_URL}/ds/rfm_segmentation:2024.10.14",
        arguments=[
            "python",
            "predict.py",
            ENV_VALUE,
            "{{ ds }}",
        ],
        env_vars=[V1EnvVar("SF_PASSWORD", "{{ var.value.sf_password }}"),
                  V1EnvVar("DYNACONF_MLFLOW_TRACKING_URI", MLFLOW_URI)],
        security_context=dict(fsGroup=65534),
        container_resources=V1ResourceRequirements(requests={"cpu": "2", "memory": "6Gi"},
                                                   limits={"cpu": "2", "memory": "6Gi"})
    )

    load = S3ToSnowflakeDeferrableOperator(
        task_id="load_to_snowflake",
        table="rfm_segments",
        schema="core",
        s3_loc=f"{AIRFLOW_STAGING_KEY_PFX}/rfm_segments/{{{{ ds }}}}/",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
        file_format="(TYPE = CSV DATE_FORMAT = AUTO SKIP_HEADER = 1)",
        pre_exec_sql=[f"DELETE FROM {ENV_VALUE}.core.rfm_segments WHERE (run_date = '{{{{ ds }}}}'::DATE)"],
    )

    predict >> load


rfm_segmentation()


if __name__ == "__main__":
    rfm_segmentation().test(execution_date="2023-11-28")
