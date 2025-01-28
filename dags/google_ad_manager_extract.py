"""Collects latest 7 days of Google Analytics self help portal data and loads it into Snowflake.

For more information regarding the format of Reporting API requests, refer to:
https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet#ReportRequest
"""
from airflow_utils import dag, task
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import AIRFLOW_STAGING_KEY_PFX, DAG_DEFAULT_CATCHUP
from de_utils.constants import GCP_CONN_ID_GROWTH, SF_BUCKET
from de_utils.slack.alerts import alert_on_failure
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 6, 10),
    schedule="50 6 * * *",
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=5),
        "retries": 3,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def google_ad_manager_extract():
    s3_key = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ ds }}}}/report.csv"

    @task
    def collect_report(s3_key, data_interval_start=None, task=None):
        start_date = data_interval_start.date() - timedelta(days=7)  # 7 day lookback
        end_date = data_interval_start.date()
        get_report(start_date, end_date, s3_key, task.log)

    snowflake_load = S3ToSnowflakeDeferrableOperator(
        task_id="load_admanager_report_snowflake",
        table="admanager_historical_data",
        schema="adops",
        s3_loc=s3_key,
        file_format="(TYPE = CSV COMPRESSION = GZIP SKIP_HEADER = 1)",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE
    )

    collect_report(s3_key) >> snowflake_load


def get_report(start_date: datetime, end_date: datetime, s3_key: str, log):
    import tempfile
    from de_utils.aws import S3Path
    from googleads import ad_manager, errors
    from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
    from googleads.oauth2 import GoogleRefreshTokenClient

    class GoogleServiceAccountClient(GoogleRefreshTokenClient):
        """Inherit the oauth2.GoogleRefreshTokenClient and overwrite the
        object variable 'creds' as we use the GoogleCloudBaseHook and gcp_conn_id set in airflow to get the credentials.
        calls the Refresh method to retrieve and set a new Access Token.
        """

        def __init__(self, credentials, **kwargs):
            """Initializes a GoogleRefreshTokenClient.

            Args:
            credentials: object of type google.oauth2.service_account.Credentials
            """
            super().__init__(None, None, None, **kwargs)
            self.creds = credentials
            self.Refresh()

    # Get Credentials GC hook
    log.info("Authenticating to Google and retrieving AdManagerClient")
    authed_http = GoogleBaseHook(gcp_conn_id=GCP_CONN_ID_GROWTH)._authorize()
    oauth2_client = GoogleServiceAccountClient(authed_http.credentials)
    client = ad_manager.AdManagerClient(
        oauth2_client,
        application_name='AdManager Historical Report',
        network_code=2897118,
        enable_compression=False
    )

    report_downloader = client.GetDataDownloader(version='v202402')
    report_query = {'reportQuery': {
        'dimensions': [
            'DATE',
            'AD_UNIT_NAME',
            'MOBILE_DEVICE_NAME'

        ],
        'adUnitView': 'FLAT',
        'columns': [
            'TOTAL_CODE_SERVED_COUNT',
            'TOTAL_INVENTORY_LEVEL_UNFILLED_IMPRESSIONS',
            'TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS',
            'TOTAL_LINE_ITEM_LEVEL_CLICKS',
            'TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE'
        ],
        'dimensionAttributes': [],
        'customFieldIds': [],
        'cmsMetadataKeyIds': [],
        'customDimensionKeyIds': [],
        'dateRangeType': 'CUSTOM_DATE',
        'startDate': start_date,
        'endDate': end_date,

        'reportCurrency': 'USD',
        'timeZoneType': 'PUBLISHER'
    }}

    log.info("Retrieving report and writing to S3")
    try:
        # Run the report and wait for it to finish.
        report_job_id = report_downloader.WaitForReport(report_query)
    except errors.AdManagerReportError as e:
        log.info(f'Failed to generate report. Error was: {e}')
        raise

    with tempfile.NamedTemporaryFile(mode="wb", suffix='.csv') as f:
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', f)
        f.flush()
        S3Path(f"s3://{SF_BUCKET}/{s3_key}").upload_file(f.name)
        log.info("Writing to s3 is complete")


google_ad_manager_extract()


if __name__ == "__main__":
    google_ad_manager_extract().test(execution_date="2024-07-21")
