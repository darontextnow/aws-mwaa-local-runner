from airflow_utils import dag, task
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP, AIRFLOW_STAGING_KEY_PFX
from de_utils.constants import GCP_CONN_ID_GROWTH, SF_BUCKET
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 10, 6),
    schedule="35 1 * * *",
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=3),
        "retries": 3
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def google_play_ratings_reports():
    s3_dir = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ ds }}}}/"

    @task.short_circuit
    def collect_report(s3_dir: str, task=None, data_interval_start=None, data_interval_end=None):
        from os import path
        import io
        import pandas as pd
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        gcs_bucket = "pubsite_prod_rev_04601070643487744872"
        gcp_hook = GCSHook(gcp_conn_id=GCP_CONN_ID_GROWTH)
        files_downloaded = 0
        for app_name in ["com.enflick.android.TextNow", "com.enflick.android.tn2ndLine"]:
            gcs_keys = gcp_hook.list(bucket_name=gcs_bucket, prefix=f"stats/ratings/ratings_{app_name}")
            for gcs_key in gcs_keys:
                if "country" not in gcs_key:
                    continue  # Only including files with country breakdowns
                update_time = gcp_hook.get_blob_update_time(bucket_name=gcs_bucket, object_name=gcs_key)
                if (data_interval_start <= update_time < data_interval_end) is False:
                    continue  # file was updated outside current DAG run dates. Only process files for current run.

                # Download and process data for SF load
                contents = gcp_hook.download(bucket_name=gcs_bucket, object_name=gcs_key)
                df = pd.read_csv(io.BytesIO(contents), thousands=",", encoding="utf-16")
                cleaned_df = clean_data(df)
                cleaned_df.to_csv(f"s3://{SF_BUCKET}/{s3_dir}/{path.basename(gcs_key)}", index=False)
                files_downloaded += 1

        task.log.info(f"{files_downloaded} files collected based on filter")
        if files_downloaded == 0:
            return False
        return True

    snowflake_load = S3ToSnowflakeDeferrableOperator(
        task_id="load_ratings_snowflake",
        table="google_play_ratings",
        schema="core",
        s3_loc=s3_dir,
        file_format="(TYPE = CSV DATE_FORMAT = AUTO SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"')",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE
    )

    collect_report(s3_dir) >> snowflake_load


def clean_data(df):
    """
    All fields in source data can have `NA` in them if they are null
    Only convert `NA` to null if the field is supposed to be float type
    """
    for col in ["Daily Average Rating", "Total Average Rating"]:
        df[col].replace("NA", None, inplace=True)
    df["Country"] = df["Country"].fillna("N/A")
    return df


google_play_ratings_reports()

if __name__ == "__main__":
    google_play_ratings_reports().test(execution_date="2023-10-05 01:35:00")
