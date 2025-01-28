from airflow_utils import dag
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP
from dag_dependencies.constants import ECR_TOOLING_ROOT_URL
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.operators.dq_checks_operator import DQChecksOperator
from dag_dependencies.dq_checks_defs import DynamicThresholdForecastsDQChecks
from de_utils.constants import SF_BUCKET, ENV_VALUE
from de_utils.slack.alerts import alert_on_failure
from kubernetes.client.models import V1ResourceRequirements
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 7, 25),
    schedule="15 6 * * MON",
    default_args={
        "owner": "harish",
        "retry_delay": timedelta(minutes=15),
        "retries": 1,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def dynamic_threshold_prediction():
    s3_pfx = "dynamic_thresholds"
    run_date = "{{ data_interval_start.date() }}"

    input_data_sql = f"""
        WITH new_dqcheck_table_cte AS (
            SELECT 
                PARSE_JSON(params):run_date::TIMESTAMP AS run_timestamp, 
                a.name AS check_name,
                a.column_name, 
                b.table_name, 
                a.value,
                a.is_valid_alert,
                a.status
            FROM {ENV_VALUE}.core.dq_checks_details a
            INNER JOIN {ENV_VALUE}.core.dq_checks b ON a.check_id = b.check_id
            QUALIFY ROW_NUMBER() OVER(PARTITION BY run_timestamp, a.name, a.column_name, b.table_name 
                ORDER BY a.inserted_at DESC)=1),
        historical_dqcheck_table_cte AS (
            SELECT run_timestamp, check_name, column_name, table_name, value, is_valid_alert, False as status
            FROM {ENV_VALUE}.core.dq_checks_values_history
            QUALIFY ROW_NUMBER() OVER(PARTITION BY run_timestamp, check_name, column_name, table_name 
                ORDER BY inserted_at DESC)=1), 
        merge_table_cte AS (
            SELECT 
                COALESCE(n.run_timestamp, h.run_timestamp) AS run_timestamp,
                COALESCE(n.check_name, h.check_name) AS check_name,
                COALESCE(n.column_name, h.column_name) AS column_name,
                COALESCE(n.table_name, h.table_name) AS table_name,
                COALESCE(n.value, h.value) AS value,
                COALESCE(n.is_valid_alert, h.is_valid_alert) AS is_valid_alert,
                COALESCE(n.status, h.status) AS status
            FROM new_dqcheck_table_cte n FULL OUTER JOIN historical_dqcheck_table_cte h 
                ON n.run_timestamp = h.run_timestamp
                AND n.check_name = h.check_name 
                AND n.column_name = h.column_name
                AND n.table_name = h.table_name
        )
        SELECT run_timestamp, check_name, column_name, table_name, value, is_valid_alert, status
        FROM merge_table_cte where check_name= 'Total Count Check';
        """

    dump_input_data = SnowflakeToS3DeferrableOperator(
        task_id="load_input_dq_checks_details_to_s3",
        sql=input_data_sql,
        s3_bucket=SF_BUCKET,
        s3_key=f"{s3_pfx}/train_data/{run_date}/dq_checks_details_{run_date}.csv",
        file_format="(TYPE = CSV COMPRESSION = NONE FIELD_DELIMITER = ',')",
        copy_options="OVERWRITE = TRUE HEADER = TRUE SINGLE = TRUE MAX_FILE_SIZE =4900000000"
    )

    predict_thresholds = KubernetesPodOperator(
        task_id="predict_thresholds",
        image=f"{ECR_TOOLING_ROOT_URL}/ds/dynamic_thresholds_prediction:v1.0",
        name="predict_thresholds",
        arguments=["--run_date",
                   "{{ data_interval_start.strftime('%Y-%m-%d') }}",
                   "--s3_dir",
                   f"s3://{SF_BUCKET}/{s3_pfx}"
                   ],
        cmds=['python', 'dynamic_thresholds_prediction_pipeline.py'],
        container_resources=V1ResourceRequirements(requests={"cpu": 1, "memory": "1Gi"},
                                                   limits={"cpu": 1.5, "memory": "1.5Gi"})  # go with smallest
    )

    sf_load = S3ToSnowflakeDeferrableOperator(
        task_id="load_dynamic_thresholds_snowflake",
        table="dynamic_threshold_forecasts",
        schema="core",
        s3_loc=f"{s3_pfx}/predicted_data/{run_date}/",
        file_format="(TYPE = CSV DATE_FORMAT = AUTO SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"')",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE
    )

    run_dq_checks = DQChecksOperator(
        task_id="run_dynamic_threshold_forecasts_dq_checks",
        dq_checks_instance=DynamicThresholdForecastsDQChecks(),
        run_date="{{ data_interval_start }}"
    )

    dump_input_data >> predict_thresholds >> sf_load >> run_dq_checks


dynamic_threshold_prediction()

if __name__ == "__main__":
    dynamic_threshold_prediction().test(execution_date="2024-05-28")
