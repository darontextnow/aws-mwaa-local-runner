"""Retrieves data from Adjust audience builder API and loads it into Snowflake."""
from airflow_utils import dag, task
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import AIRFLOW_STAGING_KEY_PFX
from de_utils.constants import SF_BUCKET, ENV_VALUE
from datetime import datetime, timedelta
from requests.exceptions import HTTPError
from de_utils.slack.alerts import alert_on_failure
import tenacity


@dag(
    start_date=datetime(2023, 7, 25),
    schedule="25 0 * * *",
    default_args={
        "owner": "Roopa",
        "retry_delay": timedelta(minutes=30),
        "retries": 3,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=False
)
def audience_builder_data_extract():
    dicof_files = {"installed_not_registered_in_last_3_months_group2": "0677cf23-86dc-4454-9f0e-e020050bfd19",
                   "installed_not_registered_in_last_3_months_group1": "eec0f527-2c23-4552-98ec-e7b41df50923"}
    for key in dicof_files:
        s3_dir = f"{AIRFLOW_STAGING_KEY_PFX}/adjust/audience_builder/{key}/{{{{ ds }}}}/"

        @task
        def collect_data(key: str, dicof_files: dict, s3_dir: str, **context):
            token = dicof_files[key]
            df = download_audience_builder_data(token, run_date=context["ds"])
            df.to_csv(f"s3://{SF_BUCKET}/{s3_dir}{token}.csv", index=False, sep=',')

        snowflake_load = S3ToSnowflakeDeferrableOperator(
            task_id=f'sf_load_{key}',
            table=key,
            schema='adjust',
            load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
            s3_loc=s3_dir,
            file_format=r"(TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)",
            pre_exec_sql=[f"DELETE FROM {ENV_VALUE.upper()}.ADJUST.{key} WHERE run_date = '{{{{ ds }}}}'"],
        )

        collect_data.override(task_id=f"collect_{key}")(key, dicof_files, s3_dir) >> snowflake_load


@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=2),
    stop=tenacity.stop_after_attempt(3),
    retry=tenacity.retry_if_exception_type(HTTPError),
    reraise=True
)
def get_resp(session, url):
    resp = session.get(url, stream=True, timeout=10)
    resp.raise_for_status()
    return resp


def download_audience_builder_data(token: str, run_date: str):
    import requests
    import pandas as pd
    sess = requests.Session()
    url = f"https://api.adjust.com/audience_builder/v1/data/{token}"
    response = get_resp(sess, url)
    df = pd.DataFrame()
    df["adid"] = pd.read_csv(response.raw)
    df["run_date"] = run_date
    df["inserted_timestamp"] = pd.to_datetime("now", utc=True).strftime("%Y-%m-%d %H:%M:%S")
    return df


audience_builder_data_extract()

if __name__ == "__main__":
    audience_builder_data_extract().test(execution_date="2023-07-23")
