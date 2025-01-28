from airflow_utils import dag, task
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.constants import AIRFLOW_STAGING_KEY_PFX
from de_utils.constants import SF_BUCKET, SF_WH_SMALL
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 10, 16),
    schedule="30 1 * * *",
    default_args={
        "owner": "Eric",
        "retry_delay": timedelta(minutes=3),
        "retries": 3
    },
    max_active_runs=1,
    catchup=False
)
def liftoff_retargeting():
    sf_s3_dir = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ ts }}}}/"
    sf_s3_key = sf_s3_dir + "android_data"
    sf_s3_ids_to_add = sf_s3_dir + "android_ids_to_add"
    sf_s3_ids_to_remove = sf_s3_dir + "android_ids_to_remove"
    # installs w/o reg login, created by calling https://analytics.liftoff.io/textnow/v1/audiences/create

    create_shared_temp_table = SnowflakeAsyncDeferredOperator(
        task_id="prepare_shared_temp_table",
        sql="""CREATE OR REPLACE TRANSIENT TABLE analytics_staging.liftoff_retargeting_installed_not_registered_tmp AS
            WITH t AS (
                SELECT DISTINCT adid
                FROM adjust.installs
                WHERE
                    (installed_at BETWEEN CURRENT_TIMESTAMP - INTERVAL '30 days' 
                        AND CURRENT_TIMESTAMP - INTERVAL '2 days')
                    AND (app_name = 'com.enflick.android.TextNow')
                    AND (network_name <> 'Untrusted Devices')
                EXCEPT SELECT DISTINCT adid
                FROM adjust.registrations
                WHERE 
                    (installed_at BETWEEN CURRENT_TIMESTAMP - INTERVAL '30 days' 
                        AND CURRENT_TIMESTAMP - INTERVAL '2 days')
                    AND (created_at BETWEEN CURRENT_TIMESTAMP - INTERVAL '30 days' 
                        AND CURRENT_TIMESTAMP - INTERVAL '2 days')
                    AND (app_name = 'com.enflick.android.TextNow')
                EXCEPT
                SELECT DISTINCT adid
                FROM adjust.sessions
                WHERE 
                    (installed_at BETWEEN CURRENT_TIMESTAMP - INTERVAL '30 days' 
                        AND CURRENT_TIMESTAMP - INTERVAL '2 days')
                    AND (created_at BETWEEN CURRENT_TIMESTAMP - INTERVAL '30 days' 
                        AND CURRENT_TIMESTAMP - INTERVAL '2 days')
                    AND (app_name = 'com.enflick.android.TextNow')
                    AND (username <> '')
                EXCEPT SELECT DISTINCT adid
                FROM adjust.logins
                WHERE 
                    (installed_at BETWEEN CURRENT_TIMESTAMP - INTERVAL '30 days' 
                        AND CURRENT_TIMESTAMP - INTERVAL '2 days')
                    AND (created_at BETWEEN CURRENT_TIMESTAMP - INTERVAL '30 days' 
                        AND CURRENT_TIMESTAMP - INTERVAL '2 days')
                    AND (app_name = 'com.enflick.android.TextNow')
            )
            SELECT LOWER(gps_adid) AS mobile_advertiser_id
            FROM t a
            JOIN adjust.installs_with_pi b ON (a.adid = b.adid)
            WHERE 
                (installed_at BETWEEN CURRENT_TIMESTAMP - INTERVAL '30 days' 
                    AND CURRENT_TIMESTAMP - INTERVAL '2 days')
                AND (app_name = 'com.enflick.android.TextNow')
                AND (gps_adid <> '')
                AND (gps_adid <> '00000000-0000-0000-0000-000000000000')
        """,
        warehouse=SF_WH_SMALL
    )

    update_table = SnowflakeAsyncDeferredOperator(
        task_id="update_liftoff_retargeting",
        sql="""UPDATE ua.liftoff_retargeting
            SET removed_at = CURRENT_TIMESTAMP
            FROM (
                SELECT mobile_advertiser_id FROM ua.liftoff_retargeting
                EXCEPT SELECT mobile_advertiser_id 
                FROM analytics_staging.liftoff_retargeting_installed_not_registered_tmp
            ) AS registered
            WHERE
                (liftoff_retargeting.mobile_advertiser_id = registered.mobile_advertiser_id)
                AND (removed_at > CURRENT_TIMESTAMP)
        """,
        warehouse=SF_WH_SMALL
    )

    insert_table = SnowflakeAsyncDeferredOperator(
        task_id="insert_liftoff_retargeting",
        sql="""INSERT INTO ua.liftoff_retargeting
            SELECT
                mobile_advertiser_id,
                CASE WHEN random() < 0.5 THEN 'holdout' ELSE 'retarget' END AS segment,
                CURRENT_TIMESTAMP AS added_at,
                CURRENT_TIMESTAMP + INTERVAL '30 days' AS removed_at
            FROM (
                SELECT mobile_advertiser_id FROM analytics_staging.liftoff_retargeting_installed_not_registered_tmp
                EXCEPT SELECT mobile_advertiser_id FROM ua.liftoff_retargeting
            )
        """,
        warehouse=SF_WH_SMALL
    )

    drop_temp_table = SnowflakeAsyncDeferredOperator(
        task_id="drop_temp_table",
        sql="DROP TABLE IF EXISTS analytics_staging.liftoff_retargeting_installed_not_registered_tmp",
        warehouse=SF_WH_SMALL
    )

    unload_android_targeting = SnowflakeToS3DeferrableOperator(
        task_id="unload_android_targeting",
        sql="""
            SELECT mobile_advertiser_id
            FROM ua.liftoff_retargeting
            WHERE (segment = 'retarget')
            AND (removed_at > CURRENT_TIMESTAMP)
        """,
        s3_bucket=SF_BUCKET,
        s3_key=sf_s3_key,
        file_format="(TYPE = CSV FIELD_DELIMITER = ',' COMPRESSION = GZIP  EMPTY_FIELD_AS_NULL = FALSE NULL_IF = (''))",
        copy_options="OVERWRITE = TRUE SINGLE = TRUE MAX_FILE_SIZE =4900000000"
    )

    @task
    def upload_android_targeting(s3_key: str, ts_nodash=None):
        from tempfile import NamedTemporaryFile
        from airflow.providers.sftp.hooks.sftp import SFTPHook
        from de_utils.aws import S3Path
        dest = f"uploads/android_{ts_nodash}.csv.gz"
        sftp_hook = SFTPHook(ssh_conn_id="liftoff_sftp")
        sftp_hook.create_directory("uploads")  # creates dir if it doesn't already exist.
        with NamedTemporaryFile("wb") as source_file:
            path = S3Path(f"s3://{SF_BUCKET}/{s3_key}")
            path.download(dst_path=source_file.name)
            sftp_hook.store_file(dest, source_file.name)
            path.rm()

    (create_shared_temp_table >> update_table >> insert_table >> drop_temp_table >>
     unload_android_targeting >> upload_android_targeting(s3_key=sf_s3_key))

    remove_audience = SnowflakeToS3DeferrableOperator(
        task_id="remove_audience",
        sql="""
            SELECT mobile_advertiser_id
            FROM ua.liftoff_retargeting
            WHERE (removed_at >= '{{ ts }}')
            AND (removed_at < CURRENT_TIMESTAMP)
            AND (segment = 'retarget')
        """,
        s3_bucket=SF_BUCKET,
        s3_key=sf_s3_ids_to_remove,
        file_format=r"(TYPE = CSV FIELD_DELIMITER = '\t' COMPRESSION = NONE"
                    r" EMPTY_FIELD_AS_NULL = FALSE NULL_IF = (''))",
        copy_options="OVERWRITE = TRUE  SINGLE = TRUE HEADER = FALSE MAX_FILE_SIZE =4900000000",
        poll_interval=30
    )

    collect_audience = SnowflakeToS3DeferrableOperator(
        task_id="collect_audience",
        sql="""
            SELECT mobile_advertiser_id
            FROM ua.liftoff_retargeting
            WHERE (added_at >= '{{ ts }}')
             AND (segment = 'retarget')
             AND (removed_at > CURRENT_TIMESTAMP OR removed_at IS NULL)
        """,
        s3_bucket=SF_BUCKET,
        s3_key=sf_s3_ids_to_add,
        file_format=r"(TYPE = CSV FIELD_DELIMITER = '\t' COMPRESSION = NONE"
                    r" EMPTY_FIELD_AS_NULL = FALSE NULL_IF = (''))",
        copy_options="OVERWRITE = TRUE  SINGLE = TRUE HEADER = FALSE MAX_FILE_SIZE =4900000000",
        poll_interval=30  # usually quick loads, thus keeping poll interval small
    )

    @task
    def add_retargeting_users(s3_key: str, task=None):
        from de_utils.aws import S3Path
        s3_path = S3Path(f"s3://{SF_BUCKET}/{s3_key}")
        modify_audience_users(op="add", s3_path=s3_path, log=task.log)

    @task
    def remove_users(s3_key: str, task=None):
        from de_utils.aws import S3Path
        s3_path = S3Path(f"s3://{SF_BUCKET}/{s3_key}")
        modify_audience_users(op="remove", s3_path=s3_path, log=task.log)

    remove = remove_users(s3_key=sf_s3_ids_to_remove)
    drop_temp_table >> collect_audience >> add_retargeting_users(s3_key=sf_s3_ids_to_add) >> remove
    drop_temp_table >> remove_audience >> remove


def send_post_to_liftoff_api(hook, op: str, batch, log):
    import json
    query_body = {
        "api_key": "c9e471ecc4",
        "audience_id": 1408,
        "ids": batch,
    }
    resp = hook.run(
        f"textnow/v1/audiences/{op}",
        data=json.dumps(query_body),
        headers={"Content-Type": "application/json"},
    )
    log.info(resp.json())
    resp.raise_for_status()
    return resp


def modify_audience_users(op, s3_path, log):
    """Call LiftOff's Audience API to dynamically update the audience
    https://docs.google.com/document/d/1BgK8-CL6gBZ7COrybUgklQh6L9eGc5L4wmwljGzeHzE
    """
    from airflow.providers.http.hooks.http import HttpHook
    import itertools
    from tempfile import NamedTemporaryFile

    liftoff_hook = HttpHook(method="POST", http_conn_id="liftoff_audience_api")
    batch_size = 15000
    with NamedTemporaryFile("wb") as f:
        s3_path.download(dst_path=f)
        with open(f.name, "r") as stream:
            batch = list(itertools.islice(stream, batch_size))
            while batch:
                send_post_to_liftoff_api(liftoff_hook, op, batch, log)
                batch = list(itertools.islice(stream, batch_size))
    s3_path.rm()  # cleanup, delete s3 path


liftoff_retargeting()

if __name__ == "__main__":
    liftoff_retargeting().test(execution_date="2023-10-16")
