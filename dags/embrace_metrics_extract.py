"""DAG for collecting Embrace Metrics last 24hrs data into snowflake tables. Designed to run each day."""
from airflow_utils import dag, task
from de_utils.constants import ENV_VALUE, SF_BUCKET
from de_utils.slack.alerts import alert_on_failure
from dag_dependencies.constants import AIRFLOW_STAGING_KEY_PFX
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP
from datetime import datetime, timedelta
from tenacity import retry, wait_random_exponential


@dag(
    start_date=datetime(2023, 8, 31),
    schedule="35 6 * * *",
    default_args={
        "owner": "Harish",
        "retry_delay": timedelta(minutes=10),
        "retries": 3,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def embrace_metrics_extract():
    # s3_transformed = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ ds }}}}/transformed"
    s3_dir = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ ds }}}}"
    app_id_client_mapper = {'DiFBd': 'TN_ANDROID',
                            'NUe92': '2L_ANDROID',
                            '53VwF': 'TN_IOS_FREE'}

    mandatory_metrics = ['crash_total', 'crashed_free_session_pct', 'crashed_session_pct', 'crashed_user_total',
                         'daily_crash_free_session_by_device_rate', 'daily_crash_free_session_rate',
                         'daily_crashed_users',
                         'daily_crashes_total', 'daily_sessions_by_device_total', 'daily_sessions_total', 'daily_users',
                         'hourly_crash_free_session_by_device_rate', 'hourly_crash_free_session_rate',
                         'hourly_crashed_users', 'hourly_crashes_total', 'hourly_sessions_by_device_total',
                         'hourly_sessions_total', 'hourly_users', 'sessions_by_device_model_total', 'sessions_total',
                         'user_total']

    failed_metrics = []

    @task
    def extract_metrics(s3_dir, **context):
        log = context["task"].log  # for logging
        log.info("Retrieving reports from embrace metrics API")
        start_time = context["data_interval_start"].replace(tzinfo=None) - timedelta(days=1)
        end_time = context["data_interval_start"].replace(tzinfo=None)
        log.info(f"Logging start_date: {start_time} and end_date: {end_time}")
        resp = get_resp()
        log.info("Successfully received the API response")
        metrics_list = resp.all_metrics()
        log.info("Successfully received metrics_list")
        step_in_sec = 60 * 60  # Define step in seconds

        for app_id, client_type in app_id_client_mapper.items():
            df_client_type = None
            s3_key = f"{s3_dir}/transformed/{client_type}.csv"  # transformed file s3 key
            for each_metric in metrics_list:
                try:
                    metric_data = collect_metric_data(each_metric, start_time, end_time, step_in_sec, app_id, resp)
                    log.info(f"Successfully collected metric:{each_metric} for client:{client_type}")
                    if len(metric_data) == 0:  # if the metric does not return data,then skip the transformation
                        failed_metrics.append([each_metric, client_type])
                        continue
                    df = transform_metric_data(metric_data, app_id, client_type)
                    df = df[['date_utc', 'client_type', 'metric_name', 'device_model', 'app_version', 'os_version',
                             'api_timestamp', 'value', 'inserted_at', 'inserted_by', 'app_id', 'status_code', 'country',
                             'domain', 'job']]
                    log.info(f"Data transformation completed for client: {client_type} and  Metric: {each_metric}")
                    df_client_type = df if df_client_type is None else df_client_type.append(df)
                    log.info(f"Data loaded to final df : {client_type} and  Metric: {each_metric}")
                except Exception as e:
                    log.info(f"Failure Exception for client: {client_type} and metric:{each_metric}", e.__str__())

            df_client_type.to_csv(f"s3://{SF_BUCKET}/{s3_key}", index=False, sep=',', header=False)
            log.info(f"File loaded to s3://{SF_BUCKET}/{s3_key}")

    @task
    def errored_metrics_alert(failed_metrics: list):
        if len(failed_metrics) > 0 and failed_metrics in mandatory_metrics:
            pfx = "No data was returned for the following metrics"
            raise ValueError(pfx + "\n".join(failed_metrics))

    # extract_metrics(s3_dir) >> snowflake_load >> errored_metrics_alert(failed_metrics)
    extract_metrics(s3_dir) >> errored_metrics_alert(failed_metrics)


def collect_metric_data(
        metric: str,
        start_time: datetime,
        end_time: datetime,
        step_in_sec: int,
        app_id: str,
        resp
) -> list[dict]:
    """Returns list of dictionary rows returned by Metrics API for a given metric"""
    metric_data = resp.custom_query_range(
        f"({metric}{{app_id='{app_id}'}})",
        start_time=start_time,
        end_time=end_time,
        step=step_in_sec
    )
    return metric_data


def transform_metric_data(metric_data, app_id, client_name):
    """Returns pandas dataframe containing transformed and flattened metrics data"""
    import pandas as pd
    df_list = []

    for row in metric_data:
        metric_dict = row['metric']
        values_list = row['values']
        for values in values_list:
            values_dict = {'timestamp': values[0], 'value': values[1]}
            # merge the values of metric column names, which represent dimensions,
            # with the corresponding unpacked measure values from the dict into a single dict
            row_dict = {**metric_dict, **values_dict}
            df_list.append(row_dict)

    df = pd.DataFrame(df_list)
    df.rename(columns={'__name__': 'metric_name'}, inplace=True)
    df.rename(columns={'timestamp': 'api_timestamp'}, inplace=True)
    df['epoch_column_temp'] = pd.to_datetime(df['api_timestamp'], unit='s')
    df['date_utc'] = df['epoch_column_temp'].dt.date
    df = df.drop('epoch_column_temp', axis=1)

    # add audit columns
    df['inserted_at'] = datetime.now()
    df['inserted_by'] = f"{ENV_VALUE}_batch_pipeline"
    df['client_type'] = client_name
    df['app_id'] = app_id
    df['status_code'] = None if 'status_code' not in df.columns else df['status_code']
    df['country'] = None if 'country' not in df.columns else df['country']
    df['domain'] = None if 'domain' not in df.columns else df['domain']
    df['job'] = None if 'job' not in df.columns else df['job']
    df['device_model'] = None if 'device_model' not in df.columns else df['device_model']
    df['app_version'] = None if 'app_version' not in df.columns else df['app_version']
    df['os_version'] = None if 'os_version' not in df.columns else df['os_version']
    df['api_timestamp'] = None if 'api_timestamp' not in df.columns else df['api_timestamp']

    return df


@retry(wait=wait_random_exponential(multiplier=1, max=60))
def get_resp():
    """Fetches Prometheus http api response with exponential retry if single request fails or times out."""
    from prometheus_api_client import PrometheusConnect  # delay import to keep top level DAG load efficient
    from de_utils.aws import get_secret  # delay this import so Airflow has less to load at top level DA
    uri = get_secret("airflow/connections/embrace_metrics_api")
    split = uri.split("?access_token=")
    return PrometheusConnect(url=split[0], headers={"Authorization": f"Bearer {split[1]}"})


embrace_metrics_extract()

if __name__ == "__main__":
    embrace_metrics_extract().test(execution_date="2023-08-28")
