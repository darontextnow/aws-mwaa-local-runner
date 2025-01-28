"""
This Airflow DAG is designed to automate the prediction process for demographic information (gender and ethnicity) and 
subsequently load the prediction results into Snowflake. The predictions are based on data for a specified date range, 
determined relative to the DAG's execution date. The process involves running containerized prediction models on 
Kubernetes and handling the data flow between S3 and Snowflake, ensuring that the predictions are stored and managed 
efficiently.

The DAG utilizes KubernetesPodOperator for running prediction tasks in a scalable and isolated manner.
The results are temporarily stored in an S3 bucket before being loaded into the Snowflake database
using the S3ToSnowflakeDeferrableOperator.

Author: Hao
Date: 2024-02-24
Model location: https://github.com/Enflick/data-science/tree/master/demo_prediction
Note: code is largely borrowed from 
https://github.com/Enflick/de-airflow/blob/master/de_airflow/dags/ltv_active_days_model.py
"""

from airflow_utils import dag
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import (DAG_DEFAULT_CATCHUP, AIRFLOW_STAGING_KEY_PFX,
                                        ECR_TOOLING_ROOT_URL, SQL_SCRIPTS_DIR)
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import SF_BUCKET
from kubernetes.client.models import V1ResourceRequirements
from datetime import datetime, timedelta

sqls = {
    "gender": "get_gender.sql",
    "ethnicity": "get_ethnicity.sql"
}


@dag(
    start_date=datetime(2024, 4, 15),
    schedule="45 8 * * *", 
    default_args={
        "owner": "Hao",  # The owner of the DAG.
        "retry_delay": timedelta(minutes=1),  # Time between retries.
        "retries": 3,  # Number of retries.
    },
    template_searchpath=f"{SQL_SCRIPTS_DIR}/demo_model",
    max_active_runs=1,  # Ensures that no more than one run is active at a time.
    on_failure_callback=alert_on_failure,  # Callback for failure alerts.
    catchup=DAG_DEFAULT_CATCHUP  # Whether to perform catchup scheduling.
)
def demo_model():
    # Date ranges in this DAG are based on the DAG's execution date or provided parameters
    # Define the S3 directory path for storing prediction results. borrowed from LTV DAG.
    s3_dir = f"{AIRFLOW_STAGING_KEY_PFX}/demo_pred/{{{{ macros.ds_add(ds, -1) }}}}/"

    # Loop through task types and create prediction and sensing tasks.
    for task_type in ["gender", "ethnicity"]:
        collect = SnowflakeToS3DeferrableOperator(
            task_id=f"collect_{task_type}",
            sql=sqls[task_type],
            s3_bucket=SF_BUCKET,
            s3_key=f"{s3_dir}{task_type}_source.csv",
            file_format=(
                "(TYPE = CSV COMPRESSION = NONE EMPTY_FIELD_AS_NULL = FALSE "
                "NULL_IF = ('') FIELD_OPTIONALLY_ENCLOSED_BY = '\"')"
            ),
            copy_options="OVERWRITE = TRUE SINGLE = TRUE HEADER = TRUE MAX_FILE_SIZE = 4900000000",
        )

        predict = KubernetesPodOperator(
            task_id=f"predict_{task_type}",
            name=f"predict_{task_type}",
            image=f"{ECR_TOOLING_ROOT_URL}/ds/demo_prediction:v1.3",
            arguments=[
                "python", "run.py",
                f"--prediction_type={task_type}",
                f"--input_uri=s3://{SF_BUCKET}/{s3_dir}{task_type}_source.csv",
                f"--output_uri=s3://{SF_BUCKET}/{s3_dir}{task_type}.csv",
                "--running_date={{ ds }}",
                "--apply_tn_mapping=FALSE"  # remove this line when JIRA ticket DS-2810 is resolved
            ],
            container_resources=V1ResourceRequirements(
                requests={"cpu": "4", "memory": "20Gi"},
                limits={"cpu": "4", "memory": "20Gi"})
        )

        # Task to load prediction results into Snowflake.
        upload = S3ToSnowflakeDeferrableOperator(
            task_id=f"load_{task_type}_predictions",
            table=f"demo_{task_type}_pred",
            schema="analytics",
            load_type=S3ToSnowflakeDeferrableOperator.LoadType.KEEP_EXISTING,
            s3_loc=f"{s3_dir}{task_type}.csv",
            file_format="(TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='\"')"  # File format specifics.
        )

        # Setup task dependencies within the DAG.
        collect >> predict >> upload


demo_model()


if __name__ == "__main__":
    demo_model().test(execution_date="2024-06-18")
