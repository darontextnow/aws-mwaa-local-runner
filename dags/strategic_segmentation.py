"""This DAG is responsible for computing the Strategic Customer Segmentation.

1) First step computes engagement features for monthly DAUs
2) Second step computes customer segments using k-Means algorithm based on engagement features
   and a set of initial centroids
3) Third step uploads to Snowflake the strategic segments stored in S3 from the k-Means
   algorithm step.
4) Trigger the downstream DAG to run.
"""
from airflow_utils import dag
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP, ECR_TOOLING_ROOT_URL
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from de_utils.constants import ENV_VALUE, SF_BUCKET, STAGE_SF, SF_WH_MEDIUM, SF_WH_SMALL
from kubernetes.client.models import V1ResourceRequirements
from datetime import datetime, timedelta
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator


@dag(
    start_date=datetime(2023, 10, 1),
    schedule="40 15 1 * *",  # Runs monthly on first of the month, after corresponding dbt run
    default_args={
        "owner": "Diego",
        "retry_delay": timedelta(minutes=5),
        "retries": 2
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def strategic_segmentation():
    features_s3_loc = "segmentation/userset_features/{{ ds }}.csv.gz"
    segments_s3_loc = "segmentation/userset_scored/{{ ds }}.csv"

    featurization = SnowflakeToS3DeferrableOperator(
        task_id="segmentation_featurization_to_s3",
        sql="strategic_segmentation_featurization.sql",
        s3_bucket=SF_BUCKET,
        s3_key=features_s3_loc,
        file_format="(TYPE = CSV FIELD_DELIMITER = ',' COMPRESSION = GZIP)",
        copy_options="HEADER = TRUE OVERWRITE = TRUE SINGLE = TRUE MAX_FILE_SIZE =4900000000",
    )

    # Note initial_centroids are those centroids output from last month's run. final_centroids are current run output.
    predict = KubernetesPodOperator(
        task_id="calculate_strategic_segments",
        image=f"{ECR_TOOLING_ROOT_URL}/ds/strategic_segmentation:v1.0",
        name="calculate-strategic-segments",
        arguments=[
            "python",
            "segmentation_clustering.py",
            "--features", f"s3://{SF_BUCKET}/{features_s3_loc}",
            "--initial_centroids",
            f"s3://{SF_BUCKET}/segmentation/centroids/{{{{ ds }}}}.csv",
            "--final_centroids", f"s3://{SF_BUCKET}/segmentation/centroids/{{{{ data_interval_end.date() }}}}.csv",
            "--segments", f"s3://{SF_BUCKET}/{segments_s3_loc}"
        ],
        container_resources=V1ResourceRequirements(requests={"cpu": 1, "memory": "4Gi"},
                                                   limits={"cpu": 1, "memory": "6Gi"})
    )

    snowflake_upload = S3ToSnowflakeDeferrableOperator(
        task_id="load_segments_to_snowflake",
        table="segmentation",
        schema="analytics",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
        in_copy_transform_sql=f"""
            SELECT sc.$1, sc.$2, sc.$3, sc.$4, sc.$5, sc.$6, sc.$7, sc.$8, sc.$9, sc.$10, sc.$11, sc.$12,
                '{{{{ ds }}}}'::DATE
            FROM @{STAGE_SF}/{segments_s3_loc} sc
        """,
        file_format="(TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)",
        pre_exec_sql=["DELETE FROM {{ params.env }}.analytics.segmentation WHERE (score_date = '{{ ds }}'::DATE)"],
        params={"env": ENV_VALUE}
    )

    insert_monthly_attribute = SnowflakeAsyncDeferredOperator(
        task_id='insert_monthly_user_attribute',
        sql="braze/user_attribute_monthly.sql",
        warehouse=SF_WH_MEDIUM
    )

    insert_monthly_attribute_sync = SnowflakeAsyncDeferredOperator(
        task_id="sync_users_attributes",
        sql="""INSERT INTO braze_cloud_production.ingestion.users_attributes_sync (updated_at, external_id, payload)
            SELECT
                CURRENT_TIMESTAMP, 
                user_id_hex AS EXTERNAL_ID,
                OBJECT_CONSTRUCT('CurrentStrategicSegment', attr_value)::VARCHAR AS payload 
            FROM (
                SELECT user_id_hex, username, attr_name, attr_value
                FROM ua.user_attributes_braze_import
                WHERE 
                    (attr_name = ('CurrentStrategicSegment'))
                    AND (inserted_at = '{{ ts }}')
                )
        """,
        warehouse=SF_WH_SMALL
    )

    featurization >> predict >> snowflake_upload >> insert_monthly_attribute >> insert_monthly_attribute_sync


strategic_segmentation()

if __name__ == "__main__":
    strategic_segmentation().test(execution_date="2023-10-01")
