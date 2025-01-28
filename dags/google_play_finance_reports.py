from airflow_utils import dag, task as _task
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP, AIRFLOW_STAGING_KEY_PFX
from de_utils.constants import GCP_CONN_ID_GROWTH, SF_BUCKET
from de_utils.slack.alerts import alert_on_failure
from datetime import datetime, timedelta

GCS_BUCKET = "pubsite_prod_rev_04601070643487744872"


@dag(
    start_date=datetime(2023, 10, 4),
    schedule="45 1 * * *",
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=3),
        "retries": 3,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def google_play_finance_reports():

    for report_type in ["sales", "earnings"]:
        s3_loc = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ ds }}}}/{report_type}"

        @_task.short_circuit(task_id=f"collect_{report_type}_reports")
        def collect_report(report_type: str,  s3_dir: str, task=None, data_interval_start=None, data_interval_end=None):
            report_data = collect_report_data(report_type, data_interval_start, data_interval_end)
            task.log.info(f"{len(report_data)} files collected based on filter")
            if not report_data:
                task.log.info(f"No data was collected for report type '{report_type}'. Skipping load to Snowflake.")
                return False

            cleaned_report_data = {gcs_key: clean_data(df, report_type) for gcs_key, df in report_data.items()}
            write_cleaned_report_data_to_s3(s3_dir, cleaned_report_data)
            return True

        snowflake_load = S3ToSnowflakeDeferrableOperator(
            task_id=f"load_{report_type}_reports",
            table=f"google_play_{report_type}",
            schema="core",
            s3_loc=s3_loc,
            file_format="(TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"')",
            load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE,
            params={"report_type": report_type}
        )

        collect_report(report_type, s3_loc) >> snowflake_load


def write_cleaned_report_data_to_s3(s3_dir: str, report_data: dict):
    import os
    for gcs_key, cleaned_df in report_data.items():
        s3_key = os.path.join(s3_dir, os.path.splitext(os.path.basename(gcs_key))[0])
        cleaned_df.to_csv(f"s3://{SF_BUCKET}/{s3_key}", index=False)


def collect_report_data(report_type: str, data_interval_start, data_interval_end):
    """Returns a dict containing each gcs_key retrieved and a Dataframe of the data read from the gcs_key"""
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    import io
    import pandas as pd

    gcp_hook = GCSHook(gcp_conn_id=GCP_CONN_ID_GROWTH)

    report_data = {}
    previous_mth = data_interval_end - timedelta(days=32)
    extra_pfx = 'report' if report_type == 'sales' else ''
    # It's normal for reports to be updated up to 5 to 6 days after the month ends.
    # Thus, filter for reports named with current runs yearmonth plus the yearmonth for previous month
    pfx = [f"{report_type}/{report_type}{extra_pfx}_{data_interval_end.year}{data_interval_end.month:02d}",
           f"{report_type}/{report_type}{extra_pfx}_{previous_mth.year}{previous_mth.month:02d}"]

    for gcs_key in gcp_hook.list(bucket_name=GCS_BUCKET, prefix=pfx):
        # this is not ideal b/c it loops through all objects, but it is the best we got
        update_time = gcp_hook.get_blob_update_time(bucket_name=GCS_BUCKET, object_name=gcs_key)
        if (data_interval_start <= update_time < data_interval_end) is False:
            continue  # file was updated outside current DAG run dates. Only process files for current run.
        contents = gcp_hook.download(bucket_name=GCS_BUCKET, object_name=gcs_key)
        report_data[gcs_key] = pd.read_csv(io.BytesIO(contents), compression="zip", thousands=",")

    return report_data


def split_ids(ids):
    """Google play order_ids are appended with subscription cycle numbers, we split that into two columns"""
    df = ids.astype('str').str.split(r"\.\.", expand=True).rename({0: "ID", 1: "Sub Cycle Number"}, axis=1)
    if "Sub Cycle Number" not in df.columns:
        df.insert(1, "Sub Cycle Number", None)
    return df


def clean_data(df, report_type: str):
    """
    This function does two things for each report_type:
        1. Ensure timestamp format is in compliance with RS requirements
        2. Split ids into ID and Subscription Cycle Number columns
        3. Return columns in desired sequence
    For reference:
        https://support.google.com/googleplay/android-developer/answer/6135870#export
    """
    import pandas as pd
    if report_type == "sales":
        df = df.drop("Order Charged Date", axis=1)
        # Split ids
        df = split_ids(df.pop("Order Number")).join(df)
        # Column sequence
        cols = ["ID", "Order Charged Timestamp", "Financial Status", "Device Model", "Sub Cycle Number",
                "SKU ID", "Product Title", "Product ID", "Product Type", "Currency of Sale", "Item Price",
                "Taxes Collected", "Charged Amount", "City of Buyer", "State of Buyer", "Postal Code of Buyer",
                "Country of Buyer"]
        return df[cols]

    elif report_type == "earnings":
        df = df.copy()
        # Combine Date and Time fields into a single timestamp
        df["Transaction Time"] = pd.to_datetime(
            # must remove the TimeZone from end of Transaction Time otherwise there is no format that works for this
            df["Transaction Date"] + " " + df["Transaction Time"].str.replace(r" [A-Z][A-Z][A-Z]$", "", regex=True),
            format="%b %d, %Y %I:%M:%S %p"  # specifying format is now required and greatly speeds up parsing
        )
        df.drop("Transaction Date", axis=1, inplace=True)
        # Split ids
        df = split_ids(df.pop("Description")).join(df)
        # Column sequence
        cols = ["ID", "Transaction Time", "Transaction Type", "Sub Cycle Number", "Tax Type", "Refund Type",
                "Sku Id", "Product Title", "Product id", "Product Type", "Hardware", "Buyer Country",
                "Buyer State", "Buyer Postal Code", "Buyer Currency", "Amount (Buyer Currency)",
                "Currency Conversion Rate", "Merchant Currency", "Amount (Merchant Currency)"]
        return df[cols]

    else:
        raise ValueError("Wrong report type provided")


google_play_finance_reports()

if __name__ == "__main__":
    google_play_finance_reports().test(execution_date="2023-10-04 01:45:00")
