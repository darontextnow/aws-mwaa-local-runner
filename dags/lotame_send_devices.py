"""DAG pulls all history of devices from analytics.vw_device_list_lotame and sends them to lotame SFTP server.
catchup is set to False as only most current run is needed since entire history is updated with each run.
"""
from airflow_utils import dag, task
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.constants import AIRFLOW_STAGING_KEY_PFX
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import SF_BUCKET, SF_WH_MEDIUM
from datetime import datetime


@dag(
    start_date=datetime(2023, 10, 8),
    schedule="15 1 * * SUN",
    default_args={
        "owner": "DE Team",
        "on_failure_callback": alert_on_failure,
    },
    max_active_runs=1,
    catchup=False
)
def lotame_send_devices():
    s3_key = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ ds_nodash }}}}/segmentmembership.json.gz"

    # query below manually concatenates a json like string together to match format required by lotame.
    unload_devices = SnowflakeToS3DeferrableOperator(
        task_id="unload_devices",
        sql="""SELECT TO_JSON(OBJECT_CONSTRUCT(
              'idtype', device_type, 
              'segments', to_array(segments),
              'userid', device_id
            )) AS c1
            FROM analytics.vw_device_list_lotame""",
        warehouse=SF_WH_MEDIUM,
        s3_bucket=SF_BUCKET,
        s3_key=s3_key,
        # using the following to allow json string from SELECT statement to print to file as is
        file_format="(TYPE = CSV COMPRESSION = GZIP FIELD_DELIMITER = '|')",
        copy_options="OVERWRITE = TRUE HEADER = FALSE SINGLE = TRUE MAX_FILE_SIZE =4900000000"
    )

    @task
    def prepare_and_send_files(s3_key: str, task=None, ds_nodash=None):
        from de_utils.aws import S3Path
        from tempfile import TemporaryDirectory
        import hashlib
        from airflow.providers.sftp.hooks.sftp import SFTPHook

        with TemporaryDirectory() as tmpdir:
            data_file = f"{tmpdir}/segmentmembership.json.gz"
            md5_file = f"{tmpdir}/segmentmembership.json.gz.md5"
            done_file = f"{tmpdir}/{ds_nodash}.done"

            task.log.info(f"Downloading data_file from S3 path: s3://{SF_BUCKET}/{s3_key}")
            S3Path(f"s3://{SF_BUCKET}/{s3_key}").download(dst_path=data_file)

            task.log.info("Creating md5 version of data_file")
            md5_sum = hashlib.md5(open(data_file, "rb").read()).hexdigest()
            open(md5_file, "w").write(md5_sum)

            task.log.info("Creating empty done file.")
            open(done_file, 'w')

            task.log.info("Sending three files to SFTP Server")
            dest = f"/mnt/textnow/textnow_mid/{ds_nodash}"
            sftp_hook = SFTPHook(ssh_conn_id="lotame_sftp")
            task.log.info(f"Creating dest dir '{dest}'")
            sftp_hook.create_directory(dest)
            task.log.info(f"Sending file '{data_file}' to SFTP Server")
            sftp_hook.store_file(dest + "/segmentmembership.json.gz", data_file)
            task.log.info(f"Sending file '{md5_file}' to SFTP Server")
            sftp_hook.store_file(dest + "/segmentmembership.json.gz.md5", md5_file)
            task.log.info(f"Sending file '{done_file}' to SFTP Server")
            sftp_hook.store_file(dest + f"/{ds_nodash}.done", done_file)

    unload_devices >> prepare_and_send_files(s3_key)


lotame_send_devices()

if __name__ == "__main__":
    # Note: Cannot test to send to SFTP server part of this DAG locally as the ssh private key location is set for MWAA.
    lotame_send_devices().test(execution_date="2023-10-08")
