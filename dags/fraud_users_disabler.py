"""DAG Uses the manual disable utility which takes a file of usernames and generates disable requests in Kafka
Documentation: https://github.com/Enflick/textnow-mono/tree/master/services/sketchy/disable/cmd
The process / workflow is as follows:
    1. A list of usernames are generated with SQL queries or external process, and results are dumped to s3.
    2. Usernames are processed by the `disablemanual` binary of fraud_disabler image.
    3. Completed files will then be moved to s3://tn-data-science-prod/antifraud/automated/processed/
"""
from airflow_utils import dag, task
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.constants import (FRAUD_DISABLER_IMAGE, FRAUD_DISABLER_ARGUMENTS,
                                        FRAUD_DISABLER_BASE_KUBERNETES_ENV, FRAUD_DISABLER_CONTAINER_RESOURCES)
from de_utils.constants import SF_BUCKET
from kubernetes.client.models import V1EnvVar
from datetime import datetime, timedelta, timezone


@dag(
    start_date=datetime(2023, 11, 13, 12, 13),
    schedule="13 */3 * * *",  # every 3 hours at 13th minute
    default_args={
        "owner": "Eric",
        "retry_delay": timedelta(minutes=1),
        "retries": 3,
    },
    max_active_runs=1,
    catchup=False
)
def fraud_users_disabler():
    s3_dir = "antifraud/automated/candidates/{{ ts }}/"

    @task
    def generate_shared_callkit_token_user_list(s3_dir, task=None, ts=None):
        """Fetch list of users sharing callkit tokens with at least `min_count` other users
        in the past 24 hours and write the output to S3
        """
        from dag_dependencies.helpers.fraud_users_disabler_helper import find_users_sharing_callkit_tokens
        from pandas import DataFrame  # delay import to keep top level dag load efficient
        task.log.info("Fetching from ElasticSearch")
        usernames = find_users_sharing_callkit_tokens(
            datetime.now(timezone.utc) - timedelta(days=1),  # always 24 hour lookback
            datetime.now(timezone.utc),
            min_count=50
        )
        df = DataFrame(set(usernames))  # set to deduplicate
        s3_file = f"s3://{SF_BUCKET}/{s3_dir}shared_callkit_token_{ts}"
        task.log.info(f"Writing {len(df.index)} usernames to {s3_file}")
        df.to_csv(s3_file, index=False, header=False)

    @task
    def disable_lists(s3_dir: str, **context):
        """Loops through lists of usernames created by tasks above and process them for disabling."""
        import os
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        s3_hook = S3Hook()
        for key in s3_hook.list_keys(SF_BUCKET, prefix=s3_dir):
            name = os.path.basename(key)
            filename, _ = os.path.splitext(name)
            sanitized_reason = filename.translate(str.maketrans("_:+", "---"))  # Airflow task and k8s accept only `-`
            reason = filename.replace("_", " ")

            context["task"].log.info(f"Sending {key} to disable service. Reason: {reason}")

            disable = KubernetesPodOperator(
                task_id=f"disable_{sanitized_reason}",
                name=f"disable-{sanitized_reason}"[:20],
                image=FRAUD_DISABLER_IMAGE,
                arguments=FRAUD_DISABLER_ARGUMENTS,
                env_vars=FRAUD_DISABLER_BASE_KUBERNETES_ENV + [
                    V1EnvVar("TN_REASON", reason),
                    V1EnvVar("TN_FILE", f"s3://{SF_BUCKET}/{key}"),
                    V1EnvVar("TN_REVOKE", "TRUE"),
                    V1EnvVar("TN_RESERVE_NUMBER", "FALSE"),
                    V1EnvVar("TN_ACCOUNT_STATUS", "HardDisabled")
                ],
                container_resources=FRAUD_DISABLER_CONTAINER_RESOURCES,
                deferrable=False  # required here as deferring does not work when calling .execute() method
            )
            disable.execute(context)

            backup_key = key.replace("/candidates/", "/processed/")
            s3_hook.copy_object(
                key,
                backup_key,
                source_bucket_name=SF_BUCKET,
                dest_bucket_name=SF_BUCKET
            )
            s3_hook.delete_objects(SF_BUCKET, key)
            context["task"].log.info(f"{key} moved to {backup_key}")

    generate_shared_callkit_token_user_list(s3_dir) >> disable_lists(s3_dir)


fraud_users_disabler()

if __name__ == "__main__":
    fraud_users_disabler().test(execution_date="2025-01-09 00:13:00")
