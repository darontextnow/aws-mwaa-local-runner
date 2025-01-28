"""This DAG is responsible for customer targeting model scoring."""

from airflow_utils import dag
from dag_dependencies.operators.dq_checks_operator import DQChecksOperator
from dag_dependencies.dq_checks_defs.sim_purchase_propensity import SimPurchasePropensityDQChecks
from dag_dependencies.constants import (
    DAG_DEFAULT_CATCHUP, ECR_TOOLING_ROOT_URL, DAG_RUN_DATE_2_DAY_LAG, SQL_SCRIPTS_DIR)
from dag_dependencies.sensors.external_task_deferred_sensor import ExternalTaskDeferredSensor
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from kubernetes.client.models import V1ResourceRequirements
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import SF_BUCKET, SF_WH_LARGE, MLFLOW_URI, SF_WH_SMALL
from datetime import datetime, timedelta
from de_utils.constants import SF_WH_MEDIUM


@dag(
    start_date=datetime(2023, 12, 31),
    schedule="0 15 * * 0",  # Run on every sunday
    default_args={
        "owner": "Eric",
        "retry_delay": timedelta(minutes=5),
        "retries": 2,
        "on_failure_callback": alert_on_failure
    },

    template_searchpath=f"{SQL_SCRIPTS_DIR}/braze",
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def targeting_models():
    # include latest 7 days of data from each source table
    report_date_filter = "(report_date BETWEEN '{{ ds }}'::DATE - INTERVAL '6 DAYS' AND '{{ ds }}'::DATE)"

    # the keys must match the subdirectories in the image (see targeting_models in data-science repo)
    model_specs = {
        'sim_propensity': {
            'table': "sim_purchase_propensity",
            'input_s3_loc': "targeting_models/user_features_report_date={{ ds }}/",
            'output_s3_loc': "targeting_models/sim_propensity_report_date={{ ds }}/",
            'input_sql': f"""
                SELECT *
                FROM user_features.user_features_summary
                WHERE
                    {report_date_filter}
                    AND (username IN (SELECT username FROM core.users 
                        WHERE (account_status NOT IN ('DISABLED', 'HARD_DISABLED'))))
                    AND (COALESCE("LAST(COUNTRY_CODE)", 'US') IN ('US', 'CA'))
            """
        },
        'returning_dau': {
            'table': "returning_dau_prediction",
            'input_s3_loc': "targeting_models/userset_features_report_date={{ ds }}/",
            'output_s3_loc': "targeting_models/returning_dau_report_date={{ ds }}/",
            'input_sql': f"""
                SELECT *
                FROM user_features.user_set_features_summary
                WHERE 
                    {report_date_filter}
                    AND (user_set_id NOT IN (select set_uuid from dau.bad_sets))
                    AND (COALESCE("LAST(COUNTRY_CODE)", 'US') IN ('US', 'CA'))
            """
        },
        'adfree_propensity': {
            'table': "adfree_purchase_propensity",
            'input_s3_loc': "targeting_models/adfree_user_features_report_date={{ ds }}/",
            'output_s3_loc': "targeting_models/adfree_propensity_report_date={{ ds }}/",
            'input_sql': f"""
                SELECT
                    f.*,
                    COALESCE(trust_score, 0) AS trust_score
                FROM user_features.user_features_summary f
                LEFT JOIN analytics.new_user_trust_scores s ON (f.username = s.username)
                WHERE 
                    {report_date_filter}
                    AND (username IN (SELECT username FROM core.users 
                        WHERE (account_status NOT IN ('DISABLED', 'HARD_DISABLED'))))
                    AND ("MODE(COUNTRY_CODE)" IN ('US', 'CA') OR "LAST(COUNTRY_CODE)" IN ('US', 'CA'))
            """
        },
        'data_attach_propensity': {
            'table': "data_attach_propensity",
            'input_s3_loc': "targeting_models/data_attach_user_features_report_date={{ ds }}/",
            'output_s3_loc': "targeting_models/data_attach_report_date={{ ds }}/",
            'input_sql': f"""
                SELECT
                    f.*,
                    COALESCE(trust_score, 0) AS trust_score,
                    DATEDIFF(day, a.act_date, report_date) AS days_since_act,
                    a.act_network,
                    a.act_product,
                    a.act_plan_name,
                    a.act_plan_family,
                    a.act_is_free,
                    s.plan_family as cur_plan_family,
                    s.is_free as cur_is_free,
                    s.plan_family as cur_plan_family,
                    s.is_free as cur_is_free
                FROM user_features.user_features_summary f
                LEFT JOIN analytics.new_user_trust_scores scores ON (f.username = scores.username)
                LEFT JOIN (
                    SELECT
                        username,
                        MIN(date_utc) AS act_date,
                        MIN_BY(network, date_utc) AS act_network,
                        MIN_BY(product_name, date_utc) AS act_product,
                        MIN_BY(plan_name, date_utc) AS act_plan_name,
                        MIN_BY(plan_family, date_utc) AS act_plan_family,
                        MIN_BY(is_free, date_utc) AS act_is_free
                    FROM analytics_staging.base_user_daily_subscription
                    WHERE (plan_name NOT LIKE 'Employee%')
                    GROUP BY username
                ) AS a ON (a.username = f.username) AND (a.act_date <= f.report_date)
                LEFT JOIN analytics_staging.base_user_daily_subscription s ON
                    (s.username = f.username)
                    AND (s.date_utc = f.report_date)
                    AND (s.plan_name not like 'Employee%')
                WHERE 
                    {report_date_filter}
                    AND (username IN (SELECT username FROM core.users 
                        WHERE (account_status NOT IN ('DISABLED', 'HARD_DISABLED'))))
                    AND ("MODE(COUNTRY_CODE)" IN ('US', 'CA') OR "LAST(COUNTRY_CODE)" IN ('US', 'CA'))
            """
        }
    }

    # Only run after the dbt_snowflake_daily_features job and DQ Checks for the last day of this DAG's week has finished
    wait_dbt = ExternalTaskDeferredSensor(
        task_id="wait_for_dbt_daily_features",
        external_dag_id="dbt_snowflake_daily_features",
        # execution_date_fn is set for run time for dbt_downstream_dags_triggerer DAG as it triggers daily_features
        execution_date_fn=lambda dt: dt.replace(hour=11, minute=10) + timedelta(days=6),  # +6 to run for last day of wk
        timeout=7200
    )

    run_spp_dq_checks = DQChecksOperator(
        task_id="test_for_predicted_scores_drift",
        dq_checks_instance=SimPurchasePropensityDQChecks(),
        run_date=DAG_RUN_DATE_2_DAY_LAG
    )

    insert_weekly_attribute = SnowflakeAsyncDeferredOperator(
        task_id='insert_weekly_user_attribute',
        sql="user_attribute_weekly.sql",
        warehouse=SF_WH_MEDIUM
    )

    insert_weekly_attributes_braze_sync = SnowflakeAsyncDeferredOperator(
        task_id="insert_users_attributes_weekly_sync",
        sql="""INSERT INTO braze_cloud_production.ingestion.users_attributes_sync (updated_at, external_id, payload)
            SELECT
                CURRENT_TIMESTAMP, 
                user_id_hex AS EXTERNAL_ID,
                OBJECT_CONSTRUCT ( 
                        'AdFreePurchasePropensity', TO_DECIMAL(adfreepurchasepropensity, 11, 10),
                        'DataPurchasePropensity', TO_DECIMAL(datapurchasepropensity, 11, 10)
                )::VARCHAR AS PAYLOAD 
            FROM (
                SELECT user_id_hex, username, attr_name, attr_value
                FROM ua.user_attributes_braze_import
                WHERE attr_name IN (
                        'AdFreePurchasePropensity',
                        'DataPurchasePropensity'
                )
                   AND inserted_at = '{{ ts }}'
            ) a 
            PIVOT (MAX(attr_value) FOR attr_name IN ( 
                'AdFreePurchasePropensity',
                'DataPurchasePropensity'
            )) AS p 
            (
             user_id_hex,
             username,
             AdFreePurchasePropensity,
             DataPurchasePropensity                 
            ) 
           WHERE payload <> PARSE_JSON('{}') 
           ORDER BY user_id_hex 
           """,
        warehouse=SF_WH_SMALL
    )

    for model, spec in model_specs.items():
        input_features = SnowflakeToS3DeferrableOperator(
            task_id=f"{model}_features_to_s3",
            sql=spec['input_sql'],
            s3_bucket=SF_BUCKET,
            s3_key=spec['input_s3_loc'],
            file_format="(TYPE = CSV EMPTY_FIELD_AS_NULL = FALSE NULL_IF = ('') COMPRESSION = GZIP)",
            copy_options="OVERWRITE = TRUE HEADER = TRUE MAX_FILE_SIZE = 10000000",
            warehouse=SF_WH_LARGE
        )

        predict = KubernetesPodOperator(
            task_id=f"score_{model}",
            name=f"score-{model.replace('_', '-')}",
            image=f"{ECR_TOOLING_ROOT_URL}/ds/targeting_models:v3.1",
            arguments=[
                f"{model}/score.sh",
                f"--source=s3://{SF_BUCKET}/" + spec['input_s3_loc'],
                f"--dest=s3://{SF_BUCKET}/" + spec['output_s3_loc'],
                f"--mlflow_uri={MLFLOW_URI}"
            ],
            container_resources=V1ResourceRequirements(requests={"cpu": "8", "memory": "20Gi"},
                                                       limits={"cpu": "8", "memory": "20Gi"})
        )

        load = S3ToSnowflakeDeferrableOperator(
            task_id=f"load_{model}_to_snowflake",
            table=spec["table"],
            schema="analytics",
            load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
            s3_loc=spec["output_s3_loc"],
            file_format="(TYPE = CSV COMPRESSION = GZIP SKIP_HEADER = 1)",
            pre_exec_sql=[f"""DELETE FROM analytics.{spec['table']} 
                WHERE (report_date = '{{{{ ds }}}}')"""]
        )

        (wait_dbt >> input_features >>
         predict >> load >> run_spp_dq_checks >> insert_weekly_attribute >>
         insert_weekly_attributes_braze_sync)


targeting_models()

if __name__ == "__main__":
    targeting_models().test(execution_date="2025-01-14")
