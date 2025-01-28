from airflow_utils import dag
from dag_dependencies.sensors.external_task_deferred_sensor import ExternalTaskDeferredSensor
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP, SQL_SCRIPTS_DIR, ECR_TOOLING_ROOT_URL
from kubernetes.client.models import V1ResourceRequirements
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import SF_CONN_ID, SF_BUCKET, SF_WH_LARGE, MLFLOW_URI, SF_WH_SMALL
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 1, 2),
    schedule=None,  # triggered by dbt_downstream_dags_triggerer
    default_args={
        "owner": "Ross",
        "retry_delay": timedelta(minutes=30),
        "retries": 2,
        "sla": timedelta(hours=6),
        "on_failure_callback": alert_on_failure
    },
    template_searchpath=f"{SQL_SCRIPTS_DIR}/features_daily/existing_user_features",
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def existing_user_trust_score():
    dynamo_s3_key = "existing_user_trust_score/{{ ds }}/dynamodb_data/"

    # dependencies dbt_snowflake_daily_features, and dbt_snowflake_daily_trust_safety
    #    are handled in dbt_downstream_dags_triggerer

    wait_contact_graph = ExternalTaskDeferredSensor(
        task_id="wait_contact_graph",
        external_dag_id="user_contact_graph",
        execution_date_fn=lambda dt: dt.replace(hour=7, minute=45),
        timeout=3600 * 6  # timeout after 6 hours of waiting
    )

    create_tmp_tables = {}
    for script in ["user_snapshot", "active_users_metadata", "all_known_user_ips_90days"]:
        create_tmp_tables[script] = SnowflakeAsyncDeferredOperator(
            task_id=f"create_tmp_table_{script}",
            sql=f"{script}.sql",
            warehouse=SF_WH_LARGE
        )

    # Create these tmp tables in the following order as each one depends on the previous table
    (create_tmp_tables["user_snapshot"] >> create_tmp_tables["active_users_metadata"] >>
     create_tmp_tables["all_known_user_ips_90days"])

    previous_compute_features = None
    score_tasks = []
    for idx in [4, 3, 2, 1]:  # ordered so that part4 will run first as its dependent tmp table will be ready first

        delete_previous_run = SnowflakeAsyncDeferredOperator(
            task_id=f"delete_previous_run_part{idx}",
            sql=f"""DELETE FROM core.existing_user_trust_score_training_features_part{idx}
                WHERE (date_utc = '{{{{ ds }}}}'::DATE);""",
            warehouse=SF_WH_SMALL
        )

        compute_features = SnowflakeAsyncDeferredOperator(
            task_id=f"compute_features_part{idx}",
            sql=f"part{idx}.sql",
            warehouse=SF_WH_LARGE
        )

        delete_previous_run >> compute_features  # delete previous runs data (if exists) before running again

        if idx == 1:
            create_tmp_tables["active_users_metadata"] >> compute_features  # part1 needs this tmp table
        elif idx == 2:
            create_tmp_tables["all_known_user_ips_90days"] >> compute_features  # only part2 uses this tmp table
        else:
            create_tmp_tables["user_snapshot"] >> compute_features  # part3 and part4 need only this tmp table

        if idx in [2, 3]:
            wait_contact_graph >> compute_features  # part2 and part3 use contact_graph table
        if previous_compute_features:
            # run these long-running queries separately as a chain, so we don't overwhelm SF queue.
            previous_compute_features >> compute_features

        previous_compute_features = compute_features  # set previous_compute_features for next loop iteration

        score = KubernetesPodOperator(
            task_id=f"score_users_shard_0{idx}",
            name="existing_user_trust_score",
            image=f"{ECR_TOOLING_ROOT_URL}/ds/existing_user_trust_score_model:4",
            arguments=[
                "python",
                "predict_v3.py",
                "--date_utc={{ ds }}",
                f"--shard=0{idx}",
                f"--connection={SF_CONN_ID}",
                f"--mlflow_uri={MLFLOW_URI}",
                "--model1_name=Existing user likelihood of disable weighted V3",
                f"--s3_prefix={SF_BUCKET}/existing_user_trust_score"
            ],
            container_resources=V1ResourceRequirements(requests={"cpu": "4", "memory": "8.0Gi"},
                                                       limits={"cpu": "4", "memory": "8.0Gi"}),
            security_context=dict(fsGroup=65534)
        )
        score_tasks.append(score)

    compute_features >> score_tasks  # after last compute_features task runs, can run all scoring tasks in parallel

    load_scores = S3ToSnowflakeDeferrableOperator(
        task_id="load_trust_scores",
        table="existing_user_trust_scores",
        schema="analytics",
        s3_loc="existing_user_trust_score/{{ ds }}/",
        transform_sql="SELECT '{{ ds }}'::DATE AS date_utc, * EXCLUDE date_utc FROM staging",
        file_format=r"(TYPE = PARQUET)",
        copy_options="MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE
    )

    update_braze_segments = SnowflakeAsyncDeferredOperator(
        task_id="update_braze_segments",
        sql="""MERGE INTO braze_cloud_production.ingestion.existing_user_trust_score_segments tgt
            USING (
                SELECT 
                    user_id_hex AS external_user_id,
                    CASE WHEN likelihood_of_disable >= 0.9 THEN 'trustScoreLow'
                         WHEN likelihood_of_disable >= 0.5 AND likelihood_of_disable < 0.9 THEN 'trustScoreMediumLow'
                         WHEN likelihood_of_disable > 0.1 AND likelihood_of_disable < 0.5 THEN 'trustScoreMediumHigh'
                         ELSE 'trustScoreHigh'
                    END AS segment,
                    date_utc AS last_updated
                FROM prod.analytics.existing_user_trust_scores
                WHERE date_utc = '{{ ds }}'::DATE
                QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id_hex ORDER BY date_utc DESC) = 1
            ) src ON tgt.external_user_id = src.external_user_id
            WHEN MATCHED THEN
             UPDATE SET
               tgt.segment = src.segment,
               tgt.last_updated = src.last_updated
            WHEN NOT MATCHED THEN
             INSERT
               (external_user_id, segment, last_updated)
             VALUES
             (src.external_user_id, src.segment, src.last_updated)""",
        warehouse=SF_WH_SMALL
    )

    run_score_qa_task = KubernetesPodOperator(
        task_id="qa_task",
        name="existing_user_trust_score",
        image=f"{ECR_TOOLING_ROOT_URL}/ds/existing_user_trust_score_model:3",
        arguments=[
            "python",
            "checks.py",
            "--date_utc={{ ds }}",
            f"--connection={SF_CONN_ID}"
        ],
        security_context=dict(fsGroup=65534),
        container_resources=V1ResourceRequirements(requests={"cpu": "2", "memory": "4.0Gi"},
                                                   limits={"cpu": "2", "memory": "4.0Gi"})
    )

    get_dynamo_username_data = SnowflakeToS3DeferrableOperator(
        task_id="get_transformed_username_data",
        sql="""
            SELECT 
                OBJECT_CONSTRUCT(
                    'date_utc', OBJECT_CONSTRUCT('S', CAST(date_utc AS VARCHAR)),
                    'user_id', OBJECT_CONSTRUCT('S', user_id),
                    'user_attribute', OBJECT_CONSTRUCT('S', user_attribute),
                    'likelihood_of_disable', OBJECT_CONSTRUCT('N', CAST(likelihood_of_disable AS VARCHAR)),
                    'positive_factors', OBJECT_CONSTRUCT('S', positive_factors),
                    'negative_factors', OBJECT_CONSTRUCT('S', negative_factors),
                    'rolling_avg_score', OBJECT_CONSTRUCT('N', CAST(rolling_avg_score AS VARCHAR))
                ) AS json_data
            FROM (
                SELECT 
                    date_utc,
                    username AS user_id,
                    'existing_user_trust_score' AS user_attribute,
                    likelihood_of_disable,
                    positive_factors,
                    negative_factors,
                    AVG(LIKELIHOOD_OF_DISABLE) OVER (
                        PARTITION BY USERNAME 
                        ORDER BY DATE_UTC
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ) AS rolling_avg_score
                FROM prod.analytics.existing_user_trust_scores
                WHERE (date_utc > '{{ ds }}'::DATE - INTERVAL '7 DAY') 
                    AND (date_utc <= '{{ ds }}')
                    AND (username IS NOT NULL)

                UNION ALL SELECT 
                    date_utc,
                    user_id_hex AS user_id,
                    'existing_user_trust_score' AS user_attribute,
                    likelihood_of_disable,
                    positive_factors,
                    negative_factors,
                    AVG(LIKELIHOOD_OF_DISABLE) OVER (
                        PARTITION BY USERNAME 
                        ORDER BY DATE_UTC
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ) AS rolling_avg_score
                FROM prod.analytics.existing_user_trust_scores
                WHERE (date_utc > '{{ ds }}'::DATE - INTERVAL '7 DAY') 
                    AND (date_utc <= '{{ ds }}')
                    AND (user_id_hex IS NOT NULL)
            )
            WHERE (date_utc = '{{ ds }}');
        """,
        s3_bucket=SF_BUCKET,
        s3_key=dynamo_s3_key + 'data',
        file_format="(TYPE = JSON  COMPRESSION = NONE STRIP_NULL_VALUES = TRUE)",
        copy_options="OVERWRITE = TRUE MAX_FILE_SIZE = 15000000"  # Max file size of 15MB
    )

    start_upload = KubernetesPodOperator(
        task_id="send_to_dynamo",
        name="dynamo-ex-user-ts-events",
        image=f"{ECR_TOOLING_ROOT_URL}/ds/dynamodb-push:latest",
        arguments=[dynamo_s3_key, "json", 'data_science_user_attributes'],
        container_resources=V1ResourceRequirements(requests={"cpu": "1", "memory": "3Gi"},
                                                   limits={"cpu": "1", "memory": "4Gi"}),
        security_context=dict(fsGroup=65534)
    )

    score_tasks >> load_scores >> run_score_qa_task >> update_braze_segments
    run_score_qa_task >> get_dynamo_username_data >> start_upload


existing_user_trust_score()
