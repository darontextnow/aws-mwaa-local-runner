"""
* Author: Hao
* Date: 2024-11-22
* Purpose:
    This Airflow DAG automates the prediction process for persona cluster
    prediction and subsequently loads the results into Snowflake. The DAG
    uses the KubernetesPodOperator to perform predictions in a scalable and
    isolated manner. Prediction results are temporarily stored in an S3 bucket
    and then loaded into Snowflake using the S3ToSnowflakeDeferrableOperator.
* Dependencies:
    ! The predictions rely on feature data processed via dbt, so the DAG
    ! execute after the weekly dbt model run date.
Metadata:
    Model Location:  will add this detail later
"""
from datetime import datetime, timedelta
from kubernetes.client.models import V1ResourceRequirements
from airflow_utils import dag
from de_utils.constants import SF_BUCKET, SF_WH_SMALL
from de_utils.slack.alerts import alert_on_failure
from dag_dependencies.constants import (
    DAG_DEFAULT_CATCHUP,
    AIRFLOW_STAGING_KEY_PFX,
    ECR_TOOLING_ROOT_URL
)
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator


@dag(
    start_date=datetime(2024, 12, 1),
    schedule=None,  # DAG runs are triggered by the dbt_snowflake_weekly DAG
    default_args={
        "owner": "Hao",
        "retry_delay": timedelta(minutes=5),
        "retries": 3,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def persona_model():
    collect = SnowflakeToS3DeferrableOperator(
        task_id="collect_persona",
        sql="SELECT * FROM analytics.persona_features",
        s3_bucket=SF_BUCKET,
        s3_key=f"{AIRFLOW_STAGING_KEY_PFX}/persona/{{{{ ds }}}}/persona_features.csv",
        file_format=(
            "(TYPE = CSV COMPRESSION = NONE EMPTY_FIELD_AS_NULL = FALSE "
            "NULL_IF = ('') FIELD_OPTIONALLY_ENCLOSED_BY = '\"')"
        ),
        copy_options="OVERWRITE = TRUE SINGLE = TRUE HEADER = TRUE MAX_FILE_SIZE = 4900000000"
    )

    # Task to predict persona clusters using the collected features.
    input_uri = f"s3://{SF_BUCKET}/{AIRFLOW_STAGING_KEY_PFX}/persona/{{{{ ds }}}}/persona_features.csv"
    output_uri = f"s3://{SF_BUCKET}/{AIRFLOW_STAGING_KEY_PFX}/persona/{{{{ ds }}}}/persona_prediction.csv"
    predict = KubernetesPodOperator(
        task_id="predict_persona",
        name="predict_persona",
        image=f"{ECR_TOOLING_ROOT_URL}/ds/persona:v1.0",
        arguments=[
            "poetry",
            "run",
            "python",
            "run.py",
            f"--input_uri={input_uri}",
            f"--output_uri={output_uri}",
            "--running_date={{ ds }}"
        ],
        container_resources=V1ResourceRequirements(
            requests={"cpu": "4", "memory": "20Gi"},
            limits={"cpu": "4", "memory": "20Gi"}
        )
    )

    # Task to load prediction results into Snowflake.
    upload = S3ToSnowflakeDeferrableOperator(
        task_id="load_predictions",
        table="persona_cluster_assignment",
        schema="analytics",
        s3_loc=f"{AIRFLOW_STAGING_KEY_PFX}/persona/{{{{ ds }}}}/persona_prediction.csv",
        file_format="(TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='\"')",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
        pre_exec_sql=[
            "DELETE FROM analytics.persona_cluster_assignment WHERE update_date = '{{ ds }}'::DATE"
        ]
    )

    incremental_update = SnowflakeAsyncDeferredOperator(
        task_id="incremental_update",
        sql="""INSERT INTO ua.user_attributes_braze_import (user_id_hex, attr_name, attr_value, inserted_at )
                WITH user_attr_new AS (
                    SELECT user_id_hex, cluster_id AS attr_value 
                    FROM analytics.persona_cluster_assignment
                    WHERE update_date = '{{ ds }}'
                ),

                user_attr_imported AS (
                    SELECT user_id_hex, attr_name, attr_value
                    FROM ua.user_attributes_braze_import
                    WHERE attr_name = 'Persona'
                    QUALIFY DENSE_RANK() OVER ( PARTITION BY username ORDER BY inserted_at DESC) = 1
                )

                SELECT DISTINCT a.user_id_hex, 'Persona' AS attr_name, a.attr_value, '{{ ts }}'
                FROM user_attr_new a
                LEFT JOIN user_attr_imported b ON a.user_id_hex = b.user_id_hex 
                WHERE
                      (a.attr_value <> b.attr_value OR b.attr_name is NULL)
                      AND a.attr_value IS NOT NULL
           """,
        warehouse=SF_WH_SMALL
    )

    braze_upload = SnowflakeAsyncDeferredOperator(
        task_id="braze_upload",
        sql="""INSERT INTO braze_cloud_production.ingestion.users_attributes_sync (updated_at, external_id, payload)
            SELECT
                CURRENT_TIMESTAMP, 
                user_id_hex AS EXTERNAL_ID,
                OBJECT_CONSTRUCT(attr_name, TO_DECIMAL(attr_value))::VARCHAR AS payload 
            FROM ua.user_attributes_braze_import
                WHERE attr_name = 'Persona'
                   AND inserted_at = '{{ ts }}'
                   AND payload <> PARSE_JSON('{}') 
            ORDER BY user_id_hex
           """,
        warehouse=SF_WH_SMALL
    )

    # Setup task dependencies within the DAG.
    collect >> predict >> upload >> incremental_update >> braze_upload


persona_model()


if __name__ == "__main__":
    persona_model().test(execution_date="2024-12-01")
