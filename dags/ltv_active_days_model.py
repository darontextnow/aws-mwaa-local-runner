"""This DAG is responsible for predicting active days for LTV."""
from airflow_utils import dag
from dag_dependencies.operators.dq_checks_operator import DQChecksOperator
from dag_dependencies.dq_checks_defs.ltv_features_activities import LTVFeaturesActivitiesDQChecks
from dag_dependencies.dq_checks_defs.ltv_features_devices import LTVFeaturesDevicesDQChecks
from dag_dependencies.dq_checks_defs.ltv_numbered_installs import LTVNumberedInstallsDQChecks
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import (DAG_RUN_DATE_1_DAY_LAG, DAG_DEFAULT_CATCHUP, ECR_TOOLING_ROOT_URL,
                                        AIRFLOW_STAGING_KEY_PFX)
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
def ltv_active_days_models():
    from_date = "{{ macros.ds_add(ds, -9) }}"
    to_date = '{{ macros.ds_add(ds, -6) }}'
    s3_dir = f"{AIRFLOW_STAGING_KEY_PFX}/ltv_active_days_prediction/{to_date}/"

    run_activities_tests = DQChecksOperator(
        task_id="run_ltv_features_activities_dq_checks",
        dq_checks_instance=LTVFeaturesActivitiesDQChecks(),
        run_date=DAG_RUN_DATE_1_DAY_LAG
    )

    run_devices_tests = DQChecksOperator(
        task_id="run_ltv_features_devices_dq_checks",
        dq_checks_instance=LTVFeaturesDevicesDQChecks(),
        run_date=DAG_RUN_DATE_1_DAY_LAG
    )

    numbered_installs_tests = DQChecksOperator(
        task_id="run_ltv_numbered_installs_dq_checks",
        dq_checks_instance=LTVNumberedInstallsDQChecks(),
        run_date=DAG_RUN_DATE_1_DAY_LAG
    )

    upload = S3ToSnowflakeDeferrableOperator(
        task_id="load_predictions",
        table="ltv_active_days_prediction",
        schema="analytics",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
        s3_loc=s3_dir,
        file_format="(TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)",
        pre_exec_sql=[f"DELETE FROM {ENV_VALUE}.analytics.ltv_active_days_prediction "
                      f"WHERE (date_utc >= '{from_date}') AND (date_utc <= '{to_date}')"]
    )

    for client_type in ["TN_ANDROID", "TN_IOS_FREE"]:
        for type in ["Paid", "Organic"]:

            predict = KubernetesPodOperator(
                task_id=f"predict_{client_type}_{type}",
                name=f"predict_{client_type}_{type}",
                image=f"{ECR_TOOLING_ROOT_URL}/ds/ltv_active_days:v4.0",
                arguments=[
                    "python", "predict.py",
                    "--network_model",
                    "--log=info",
                    f"--model_uri=models:/{client_type} {type} active days/Production",
                    f"--client_type={client_type}",
                    f"--is_organic={type == 'Organic'}",
                    "--from_date=" + from_date,
                    "--to_date=" + to_date,
                    f"--output_uri=s3://{SF_BUCKET}/{s3_dir}{client_type}_{type}.csv",
                    f"--mlflow_uri={MLFLOW_URI}"
                ],
                container_resources=V1ResourceRequirements(requests={"cpu": "4", "memory": "9Gi"},
                                                           limits={"cpu": "4", "memory": "9Gi"})
            )

            run_activities_tests >> run_devices_tests >> numbered_installs_tests >> predict >> upload


ltv_active_days_models()

if __name__ == "__main__":
    # ltv_active_days_models().test_dq_checks(execution_date="2024-03-11")
    ltv_active_days_models().test(execution_date="2025-01-09")
