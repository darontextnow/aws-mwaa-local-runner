"""DAG runs a bunch of queries to create new_user_trust_scores and loads the results into both Snowflake and DynamoDB.
This DAG depends on:
    1. ip info update
    2. sketchy registration logs
    3. firehose and adjust registration events
All other usual tables in core schema.
"""
from airflow_utils import dag
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.sensors.external_task_deferred_sensor import ExternalTaskDeferredSensor
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP, SQL_SCRIPTS_DIR, ECR_TOOLING_ROOT_URL
from de_utils.constants import SF_CONN_ID, SF_BUCKET, SF_WH_SMALL, SF_WH_MEDIUM, MLFLOW_URI
from kubernetes.client.models import V1ResourceRequirements
from datetime import datetime, timedelta
from de_utils.slack.alerts import alert_on_failure


@dag(
    start_date=datetime(2023, 10, 11, 8),
    schedule=None,  # triggered by ip_info_update DAG which runs hourly
    default_args={
        "owner": "Ross",
        "retry_delay": timedelta(minutes=2),
        "retries": 2,
        "sla": timedelta(hours=2),
        "on_failure_callback": alert_on_failure
    },
    template_searchpath=f"{SQL_SCRIPTS_DIR}/features_hourly/new_user_trust_score",
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def new_user_trust_score():
    s3_key_v2 = "trust_score_v2/{{ ts }}"

    sketchy_sensor = ExternalTaskDeferredSensor(
        external_dag_id="sketchy_registration_logs_load",
        task_id="wait_sketchy",
        timeout=60 * 60 * 2
    )

    new_user_req_logs = SnowflakeAsyncDeferredOperator(
        task_id="update_new_user_req_logs",
        sql="new_user_request_logs.sql",
        warehouse=SF_WH_SMALL,
    )

    android_features_update = SnowflakeAsyncDeferredOperator(
        task_id="update_trust_score_features_android",
        sql="trust_score_android_features_v1.sql",
        warehouse=SF_WH_MEDIUM
    )

    create_bad_asns_shared_tmp = SnowflakeAsyncDeferredOperator(
        task_id="create_bad_asns_shared_tmp",
        sql="bad_asns_tmp.sql",
        warehouse=SF_WH_SMALL
    )

    create_cohort_shared_tmp = SnowflakeAsyncDeferredOperator(
        task_id="create_cohort_shared_tmp",
        sql="user_cohort_tmp.sql",
        warehouse=SF_WH_SMALL
    )

    create_user_ips_shared_tmp = SnowflakeAsyncDeferredOperator(
        task_id="create_user_ips_shared_tmp",
        sql="user_ips_tmp.sql",
        warehouse=SF_WH_SMALL
    )

    create_user_ips_info_android_tmp = SnowflakeAsyncDeferredOperator(
        task_id="create_user_ips_info_android_tmp",
        sql="user_ips_info_android_tmp.sql",
        warehouse=SF_WH_SMALL
    )

    create_user_ips_info_ios_tmp = SnowflakeAsyncDeferredOperator(
        task_id="create_user_ips_info_ios_tmp",
        sql="user_ips_info_ios_tmp.sql",
        warehouse=SF_WH_SMALL
    )

    create_user_req_logs_shared_tmp = SnowflakeAsyncDeferredOperator(
        task_id="create_user_req_logs_shared_tmp",
        sql="""CREATE TRANSIENT TABLE user_req_logs_tmp_{{ ts_nodash }} AS
            SELECT 
                a.username, request_ts, route_name, b.client_type, client_version, http_response_status,
                "X-TN-Integrity-Session", authentication_type, perimeterx, error_code, type, client_id
            FROM user_cohort_tmp_{{ ts_nodash }} a
            JOIN core.new_user_request_logs b ON (a.username = b.username)
            WHERE (b.insertion_time > '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 DAY');
        """,
        warehouse=SF_WH_MEDIUM
    )

    android_postreg_features_update = SnowflakeAsyncDeferredOperator(
        task_id="update_trust_score_postreg_features_android",
        sql="new_user_postreg_features_android.sql",
        warehouse=SF_WH_MEDIUM
    )

    #  - - - new, added on Jun 24 with iOS features
    ios_postreg_features_update = SnowflakeAsyncDeferredOperator(
        task_id="update_trust_score_postreg_features_ios",
        sql="new_user_postreg_features_ios.sql",
        warehouse=SF_WH_MEDIUM
    )

    ios_features_update = SnowflakeAsyncDeferredOperator(
        task_id="update_trust_score_features_ios",
        sql="trust_score_ios_features_v1.sql",
        warehouse=SF_WH_MEDIUM
    )

    update_new_user_snapshot = SnowflakeAsyncDeferredOperator(
        task_id="update_new_user_snapshot",
        sql="new_user_snapshot.sql",
        warehouse=SF_WH_SMALL
    )

    delete_events = SnowflakeAsyncDeferredOperator(
        task_id="delete_events",
        sql="""DELETE FROM core.new_user_embrace_events WHERE 
                (execution_time = '{{ data_interval_start }}'::TIMESTAMP_NTZ)
                OR (execution_time <= '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '3 MONTHS');
            DELETE FROM core.new_user_adtracker_events WHERE 
                (execution_time = '{{ data_interval_start }}'::TIMESTAMP_NTZ)
                OR (execution_time <= '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '3 MONTHS');
            DELETE FROM core.new_user_cm_msg_logs WHERE 
                (execution_time = '{{ data_interval_start }}'::TIMESTAMP_NTZ)
                OR (execution_time <= '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '3 MONTHS');
            DELETE FROM core.new_user_ui_events_android 
            WHERE (execution_time = '{{ data_interval_start }}'::TIMESTAMP_NTZ);
            DELETE FROM core.new_user_ui_events_ios WHERE (execution_time = '{{ data_interval_start }}'::TIMESTAMP_NTZ);
        """,
        warehouse=SF_WH_MEDIUM
    )

    new_user_embrace_events = SnowflakeAsyncDeferredOperator(
        task_id="new_user_embrace_events",
        sql="new_user_embrace_events.sql",
        warehouse=SF_WH_MEDIUM
    )

    new_user_adtracker_events = SnowflakeAsyncDeferredOperator(
        task_id="new_user_adtracker_events",
        sql="new_user_adtracker_events.sql",
        warehouse=SF_WH_MEDIUM
    )

    new_user_cm_msg_logs = SnowflakeAsyncDeferredOperator(
        task_id="new_user_cm_msg_logs",
        sql="new_user_cm_msg_logs.sql",
        warehouse=SF_WH_MEDIUM
    )

    new_user_ui_events_android = SnowflakeAsyncDeferredOperator(
        task_id="new_user_ui_events_android",
        sql="new_user_ui_events_android.sql",
        warehouse=SF_WH_MEDIUM
    )

    new_user_ui_events_ios = SnowflakeAsyncDeferredOperator(
        task_id="new_user_ui_events_ios",
        sql="new_user_ui_events_ios.sql",
        warehouse=SF_WH_MEDIUM
    )

    drop_tmp_tables = SnowflakeAsyncDeferredOperator(
        task_id="drop_tmp_tables",
        sql=("DROP TABLE IF EXISTS user_cohort_tmp_{{ ts_nodash }};"
             "DROP TABLE IF EXISTS user_ips_tmp_{{ ts_nodash }};"
             "DROP TABLE IF EXISTS user_ips_info_android_tmp_{{ ts_nodash }};"
             "DROP TABLE IF EXISTS user_ips_info_ios_tmp_{{ ts_nodash }};"
             "DROP TABLE IF EXISTS user_req_logs_tmp_{{ ts_nodash }};"
             "DROP TABLE IF EXISTS bad_asns_tmp_{{ ts_nodash }};")
    )

    score_users_android_v2 = KubernetesPodOperator(
        task_id="score_users_android_v2",
        arguments=[
            "python",
            "predict_v2.py",
            "--cohort_time={{ ts }}",
            "--table1_name=core.new_user_score_android_postreg_features",
            "--table2_name=core.trust_score_android_features_v1",
            f"--connection={SF_CONN_ID}",
            "--model_uri=models:/Android new user likelihood of disable V2/Production",
            f"--mlflow_uri={MLFLOW_URI}",
            f"--output_uri=s3://{SF_BUCKET}/{s3_key_v2}/android_users.parquet"
        ],
        name="new_user_trust_score_android_v2",
        image=ECR_TOOLING_ROOT_URL + "/ds/new_user_trust_score_model:7",
        container_resources=V1ResourceRequirements(requests={"cpu": 2, "memory": "4.0Gi"},
                                                   limits={"cpu": 2, "memory": "4.0Gi"}),
        security_context=dict(fsGroup=65534)
    )

    score_users_ios_v2 = KubernetesPodOperator(
        task_id="score_users_ios_v2",
        arguments=[
            "python",
            "predict_v2ios.py",
            "--cohort_time={{ ts }}",
            "--table1_name=core.trust_score_ios_features_v1",
            "--table2_name=core.new_user_score_ios_postreg_features",
            f"--connection={SF_CONN_ID}",
            "--model_uri=models:/new_user_trust_score_ios_v2/Production",
            f"--mlflow_uri={MLFLOW_URI}",
            f"--output_uri=s3://{SF_BUCKET}/{s3_key_v2}/ios_users.parquet"
        ],
        name="new_user_trust_score_ios_v2",
        image=ECR_TOOLING_ROOT_URL + "/ds/new_user_trust_score_model:6",
        container_resources=V1ResourceRequirements(requests={"cpu": 2, "memory": "4.0Gi"},
                                                   limits={"cpu": 2, "memory": "4.0Gi"}),
        security_context=dict(fsGroup=65534)
    )

    sf_load_v2 = S3ToSnowflakeDeferrableOperator(
        task_id="sf_load_trust_scores_v2",
        s3_loc=s3_key_v2,
        table="new_user_trust_scores_v2",
        schema="analytics",
        transform_sql="SELECT * EXCLUDE inserted_timestamp, '{{ ts }}' AS inserted_timestamp FROM staging",
        file_format=r"(TYPE = PARQUET)",
        copy_options="MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE
    )

    get_dynamo_data = SnowflakeToS3DeferrableOperator(
        task_id="get_transformed_dynamo_data",
        sql="get_transformed_dynamo_data.sql",
        s3_bucket=SF_BUCKET,
        s3_key=f'{s3_key_v2}/dynamodb_data/data_',
        file_format="(TYPE = JSON  COMPRESSION = NONE STRIP_NULL_VALUES = TRUE)",
        copy_options="OVERWRITE = TRUE MAX_FILE_SIZE = 15000000"  # Max file size of 15MB
    )

    start_upload = KubernetesPodOperator(
        task_id="send_to_dynamo",
        name="dynamo-new-user-ts-events",
        image=f"{ECR_TOOLING_ROOT_URL}/ds/dynamodb-push:latest",
        arguments=[f'{s3_key_v2}/dynamodb_data/', 'json', 'data_science_user_attributes'],
        container_resources=V1ResourceRequirements(requests={"cpu": "1", "memory": "3Gi"},
                                                   limits={"cpu": "1", "memory": "4Gi"}),
        security_context=dict(fsGroup=65534)
    )

    pre_postreg_features_updates = [
        sketchy_sensor >> new_user_req_logs >> update_new_user_snapshot >>
        delete_events >> new_user_ui_events_android >> new_user_ui_events_ios >>
        create_bad_asns_shared_tmp >> create_cohort_shared_tmp >> create_user_ips_shared_tmp >>
        create_user_req_logs_shared_tmp
    ]
    pre_postreg_features_updates >> create_user_ips_info_android_tmp >> android_postreg_features_update
    pre_postreg_features_updates >> create_user_ips_info_ios_tmp >> ios_postreg_features_update
    sketchy_sensor >> android_features_update >> android_postreg_features_update >> score_users_android_v2
    sketchy_sensor >> ios_features_update >> ios_postreg_features_update >> score_users_ios_v2
    (update_new_user_snapshot >> delete_events >>
     new_user_embrace_events >> new_user_adtracker_events >> new_user_cm_msg_logs)
    [score_users_android_v2, score_users_ios_v2] >> sf_load_v2 >> drop_tmp_tables >> get_dynamo_data >> start_upload


new_user_trust_score()

if __name__ == "__main__":
    new_user_trust_score().test(execution_date='2025-01-15')
