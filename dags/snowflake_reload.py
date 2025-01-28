from airflow_utils import dag
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.constants import ECR_TOOLING_ROOT_URL
from kubernetes.client.models import V1ResourceRequirements, V1EnvVar
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 10, 30),
    schedule="10 1 * * *",
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=5),
        "retries": 3
    },
    max_active_runs=1,
    catchup=False
)
def snowflake_reload():

    KubernetesPodOperator(
        task_id="reload_failed_files",
        name="snowflake-errors-reload",
        image=f"{ECR_TOOLING_ROOT_URL}/ds/snowflake-utils:latest",
        env_vars=[
            V1EnvVar("AWS_ACCESS_KEY_ID", "{{ var.value.aws_access_key_id }}"),
            V1EnvVar("AWS_SECRET_ACCESS_KEY", "{{ var.value.aws_secret_access_key }}"),
            V1EnvVar("SF_PASSWORD", "{{ var.value.sf_password }}")
        ],
        arguments=["auto-reload"],
        security_context=dict(fsGroup=65534),
        container_resources=V1ResourceRequirements(requests={"memory": "128Mi"},
                                                   limits={"memory": "512Mi"})
    )


snowflake_reload()

if __name__ == "__main__":
    snowflake_reload().test(execution_date="2025-01-13 01:10:00")
