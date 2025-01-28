from airflow_utils import dag
from dag_dependencies.constants import ECR_TOOLING_ROOT_URL, DAG_DEFAULT_CATCHUP
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from de_utils.constants import SF_BUCKET
from de_utils.slack.alerts import alert_on_failure
from kubernetes.client.models import V1ResourceRequirements
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 10, 6),
    schedule=None,  # triggered by dbt_downstream_dags_triggerer DAG
    default_args={
        "owner": "Eric",
        "retry_delay": timedelta(minutes=1),
        "retries": 3,
        "on_failure_callback": alert_on_failure,
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def fraud_models():

    install_scoring = KubernetesPodOperator(
        task_id="install_scoring",
        name="install-fraud-scoring",
        image=f"{ECR_TOOLING_ROOT_URL}/ds/fraud_models:v8.0",
        arguments=[
            "python",
            "installs/score.py",
            "{{ macros.ds_add(ds, -2) }}",
            "{{ macros.ds_add(ds, 2) }}",
            "--csv", f"s3://{SF_BUCKET}/antifraud/install_scores/install_scores_{{{{ ds }}}}.csv.gz"
        ],
        security_context=dict(fsGroup=65534),
        container_resources=V1ResourceRequirements(requests={"memory": "4Gi"}, limits={"memory": "6Gi"})
    )

    install_score_load_sf = S3ToSnowflakeDeferrableOperator(
        task_id="load_install_scoring_sf",
        s3_loc="antifraud/install_scores/install_scores_{{ ds }}.csv.gz",
        table="installs",
        schema="fraud_alerts",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE,
        file_format="(TYPE = CSV COMPRESSION = GZIP SKIP_HEADER = 1)",
        copy_columns=["adjust_id", "score", "rule", "model_version"],
        merge_match_where_expr="(staging.score != tgt.score)",
        transform_sql="""SELECT 
                COALESCE(created_at, CURRENT_TIMESTAMP()) AS created_at,
                CURRENT_TIMESTAMP() AS last_updated_at,
                adjust_id,
                score,
                model_version,
                rule
            FROM staging
        """
    )

    install_scoring >> install_score_load_sf


fraud_models()

if __name__ == "__main__":
    fraud_models().test(execution_date="2023-10-06")
