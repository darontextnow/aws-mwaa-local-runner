"""The purpose of this DAG is to fight the pattern of new account creations
on bad devices belonging to spammers/scammers.
it looks for recent new accounts from firehose.registrations and adjust.registrations
then checks if device had users disabled in the past week
This will be a companion dag for the inc441 ng scammer DAG.
Note: Using catchup=False here as we only want to run latest run.
"""
from airflow_utils import dag
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from dag_dependencies.constants import (FRAUD_DISABLER_BASE_KUBERNETES_ENV, FRAUD_DISABLER_ARGUMENTS,
                                        FRAUD_DISABLER_IMAGE, FRAUD_DISABLER_CONTAINER_RESOURCES)
from de_utils.constants import SF_BUCKET
from kubernetes.client.models import V1EnvVar
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 11, 15, 10),
    schedule="*/30 * * * *",  # every 30 mins
    default_args={
        "owner": "Dheeraj",
        "retry_delay": timedelta(minutes=5),
        "retries": 1
    },
    max_active_runs=1,
    catchup=False
)
def bad_device_disabler():
    s3_dest_key = "antifraud/bad_device_new_users/usernames_{{ ts }}"

    sf_dump = SnowflakeToS3DeferrableOperator(
        task_id="dump_usernames_s3",
        sql="""
            WITH new_reg AS (
                SELECT * FROM (
                    SELECT a.username, b.adid, tracker_name
                    FROM firehose.registrations a
                    JOIN adjust.installs_with_pi b ON (a.idfv = b.idfv)
                    WHERE (a.created_at > '{{ data_interval_start }}'::TIMESTAMP - INTERVAL '1800 seconds')
                        AND (http_response_status = '200')
                        AND (a.idfv IS NOT NULL)
                    UNION SELECT username, adid, tracker_name
                    FROM adjust.registrations a
                    WHERE (created_at > '{{ data_interval_start }}'::TIMESTAMP - INTERVAL '1800 seconds')
                        AND (username IN (
                            SELECT DISTINCT username 
                            FROM firehose.registrations 
                            WHERE (created_at > CURRENT_DATE - INTERVAL '1 day') 
                                AND (http_response_status = '200')
                        ))
                        AND (username IS NOT NULL)
                    )
            ),
            bad_adid AS (
                SELECT 
                    adid, 
                    count(*) num_reg,
                    SUM(CASE WHEN account_status = 'HARD_DISABLED' THEN 1 ELSE 0 END) num_hard_dis
                FROM core.registrations a
                JOIN core.users b ON (a.username = b.username)
                WHERE (a.created_at > '{{ data_interval_start }}'::TIMESTAMP - INTERVAL '30 days')
                GROUP BY 1
                HAVING (num_hard_dis >= 2) AND (num_reg >= 2) AND (num_hard_dis::FLOAT/num_reg >= 0.5)
            )
            SELECT DISTINCT username FROM new_reg
            WHERE (adid IN (SELECT adid FROM bad_adid))
                AND (username NOT LIKE 'efqa_%')
        """,
        s3_bucket=SF_BUCKET,
        s3_key=s3_dest_key,
        file_format=r"(TYPE = CSV COMPRESSION = NONE EMPTY_FIELD_AS_NULL = FALSE NULL_IF = (''))",
        copy_options="OVERWRITE = TRUE HEADER = FALSE SINGLE = TRUE MAX_FILE_SIZE =4900000000",
        poll_interval=30  # usually a shorter run
    )

    # get seconds timestamp
    ts = str(datetime.now().timestamp()).split(".")[0]

    disable = KubernetesPodOperator(
        task_id="disable_ng_device_links",
        name=f"disable_ng_{ts}",
        image=FRAUD_DISABLER_IMAGE,
        arguments=FRAUD_DISABLER_ARGUMENTS,
        env_vars=FRAUD_DISABLER_BASE_KUBERNETES_ENV + [
            V1EnvVar("TN_REASON", "inc-441-ng-device-links-scheduled"),
            V1EnvVar("TN_FILE", f"s3://{SF_BUCKET}/{s3_dest_key}"),
            V1EnvVar("TN_REVOKE", "FALSE"),
            V1EnvVar("TN_RESERVE_NUMBER", "FALSE"),
            V1EnvVar("TN_ACCOUNT_STATUS", "HardDisabled")
        ],
        container_resources=FRAUD_DISABLER_CONTAINER_RESOURCES
    )

    sf_dump >> disable


bad_device_disabler()

if __name__ == "__main__":
    bad_device_disabler().test(execution_date="2023-11-15")
