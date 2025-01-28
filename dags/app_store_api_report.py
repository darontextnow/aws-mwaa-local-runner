"""Single DAG that Retrieves any appstore related API report data from appstore connect API and
updates values in Snowflake table. Currently this DAG extracts data from sales and performancemetrics endpoint.

Per Appstore API error returned when calling API too early for previous day's sales report data,
sales reports for previous day should be available from API by 5am Pacific time the next day.
However, this is often not true and reports come available later in the day.

Based on above report availability findings, this DAG starts shortly after 5am Pacific and will retry for
the sales report up to 8 hours.
Retrying every hour for up to 8 hours may help in the common event that the Appstore API is mal-functioning.

For JWT authentication:
    kid, iss and private key created in App Store Connect: https://appstoreconnect.apple.com/access/api
    Only account holder (Derek) has permission to generate the key ID / private key
    See docs for API at https://developer.apple.com/documentation/appstoreconnectapi/generating_tokens_for_api_requests
"""
from airflow_utils import dag, task
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP, AIRFLOW_STAGING_KEY_PFX
from dag_dependencies.dq_checks_defs.appstore_performance_metrics import AppstorePerformanceMetricsDQChecks
from dag_dependencies.operators.dq_checks_operator import DQChecksOperator
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import SF_BUCKET, ENV_VALUE, SF_CONN_ID
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2024, 10, 10),
    schedule="35 13 * * *",  # start close after 5am Pacific
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=60),
        "retries": 8,
        "on_failure_callback": alert_on_failure
    },
    catchup=DAG_DEFAULT_CATCHUP
)
def app_store_api_report():
    sales_s3_key = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ ds }}}}/appstore_sales.txt.gz"
    launch_metric_s3_key = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ ds }}}}/appstore_launch_metric.json"

    @task
    def collect_sales_report(sales_s3_key: str, ds=None, task=None):
        from de_utils.aws import S3Path
        logger = task.log
        logger.info("Collecting sales report data from app store API...")
        report_data = get_sales_report_data(report_date=ds)
        logger.info(f"Writing sales report data to s3://{SF_BUCKET}/{sales_s3_key}.")
        S3Path(f"s3://{SF_BUCKET}/{sales_s3_key}").upload_fileobj(report_data)

    load_sales_report = S3ToSnowflakeDeferrableOperator(
        task_id="load_sales_report_snowflake",
        table="appstore_sales",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
        schema="core",
        s3_loc=sales_s3_key,
        file_format=r"(TYPE = CSV COMPRESSION = GZIP FIELD_DELIMITER = '\t' DATE_FORMAT = AUTO SKIP_HEADER = 1)",
        pre_exec_sql=[f"DELETE FROM {ENV_VALUE}.core.appstore_sales WHERE begin_date = '{{{{ ds }}}}'"]
    )

    @task
    def collect_launch_metric_report(launch_metric_s3_key: str, ds=None, task=None):
        from pandas import DataFrame
        logger = task.log

        logger.info("Collecting all model names from embrace table...")
        device_models = get_device_models(ds)

        logger.info(f"Collecting launch metric report data from appstore API for these devices: {device_models}")
        metrics = ['LAUNCH']  # Include additional metrics in this list as needed
        launch_metric = get_launch_metric_report_data(ds, device_models, metrics, logger)
        logger.info(f"Writing launch metrics data to s3://{SF_BUCKET}/{launch_metric_s3_key}.")
        DataFrame(launch_metric).to_json(f"s3://{SF_BUCKET}/{launch_metric_s3_key}", orient="records")

    load_launch_metric_report = S3ToSnowflakeDeferrableOperator(
        task_id="load_launch_metric_report_snowflake",
        table="appstore_performance_metrics",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.APPEND,
        schema="core",
        s3_loc=launch_metric_s3_key,
        copy_options="MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
        file_format="(TYPE = JSON TIME_FORMAT = AUTO STRIP_OUTER_ARRAY = TRUE)",
        pre_exec_sql=[f"DELETE FROM {ENV_VALUE}.core.appstore_performance_metrics WHERE dag_run_date = '{{{{ ds }}}}'"]
    )

    run_appstore_performance_metrics_dq_checks = DQChecksOperator(
        task_id="run_appstore_performance_metrics_dq_checks",
        dq_checks_instance=AppstorePerformanceMetricsDQChecks(),
        run_date="{{ ds }}"
    )

    collect_launch_metric_report(launch_metric_s3_key) >> load_launch_metric_report >> \
        run_appstore_performance_metrics_dq_checks

    collect_sales_report(sales_s3_key) >> load_sales_report


def get_device_models(report_date: str):
    from de_utils.tndbo import get_dbo
    query = f"""
        SELECT DISTINCT model 
        FROM prod.core.embrace_data 
        WHERE 
            (TO_DATE(TO_TIMESTAMP(received_ts)) BETWEEN '{report_date}'::DATE - INTERVAL '1 DAY'
                AND '{report_date}'::DATE + INTERVAL '1 DAY')
            AND APP_ID = '53VwF'
            AND NULLIF(TRIM(model), '') IS NOT NULL
    """
    output = get_dbo(SF_CONN_ID).execute(query).fetchall()
    device_models = [model[0].replace(',', '_') for model in output]
    device_models.append('all_iphones')  # add all_iphones to get all iphones aggregated value
    return device_models


def get_auth_header(http_hook) -> dict:
    """Returns auth_header required by appstore API including jwt_token generated for auth to appstore API."""
    import jose.jwt  # delay import to keep top level dag load efficient
    from time import time
    extra = http_hook.get_connection("appstore_connect").extra_dejson
    headers = {
        "alg": "ES256",
        "kid": extra["kid"],
        "typ": "JWT"
    }
    payload = {
        "iss": extra["iss"],
        "exp": int(time()) + 120,
        "aud": "appstoreconnect-v1"
    }
    # replaced \n with ~ in secrets manager to get around issue.
    # \n required in private key, but would error when json.loads would parse extra attr. Thus, substitute it back in.
    private_key = extra["private_key"].replace("~", "\n")
    token = jose.jwt.encode(payload, private_key, algorithm="ES256", headers=headers)
    auth_header = {"Authorization": "Bearer " + token}
    return auth_header


def get_sales_report_data(report_date: str):
    """Returns raw gzipped content containing appstore report data returned by sales report endpoint"""
    from airflow.providers.http.hooks.http import HttpHook

    http_hook = HttpHook(http_conn_id="appstore_connect", method="GET")
    http_data = {
        "filter[reportDate]": report_date,
        "filter[reportType]": "SALES",
        "filter[vendorNumber]": "80088059",  # vendor id for TextNow Inc.
        "filter[frequency]": "DAILY",
        "filter[reportSubType]": "SUMMARY",
        "filter[version]": "1_0"
    }
    response = http_hook.run(endpoint="/v1/salesReports",
                             data=http_data,
                             headers=get_auth_header(http_hook),
                             extra_options={"stream": True, "check_response": False})
    if response.status_code == 200:
        return response.raw
    else:
        raise RuntimeError(f"Response from appstore API was: {response.text}")


def get_launch_metric_report_data(report_date: str, device_models: list, metrics: list, logger):
    """Returns list of json appstore launch metric data returned by perfPowerMetrics endpoint"""
    from airflow.providers.http.hooks.http import HttpHook
    import json

    http_hook = HttpHook(http_conn_id="appstore_connect", method="GET")
    rows = []
    for i, device_model in enumerate(device_models):
        logger.info(f"Processing device {i}/{len(device_models)}...")
        for metric_type in metrics:
            http_data = {
                "filter[metricType]": metric_type,
                "filter[deviceType]": device_model,
                "filter[platform]": "iOS"
            }

            response = http_hook.run(endpoint="/v1/apps/314716233/perfPowerMetrics",
                                     data=http_data,
                                     headers=get_auth_header(http_hook),
                                     extra_options={"stream": True, "check_response": False})
            if response.status_code == 200:
                data = json.loads(response.text)
                processed_json_data = process_response(data, report_date, logger)
                rows.extend(processed_json_data)
                logger.info(f"Data for device {device_model} and metric type {metric_type} is processed")
            else:
                logger.info(f"Failed for device model {device_model} and metric type {metric_type}: {response.text}")

    return rows


def process_response(data: dict, report_date: str, logger: None):
    processed_output = []
    product_data = data['productData']
    for product in product_data:
        platform = product.get('platform', '')

        for metric_category in product.get('metricCategories', []):
            for metric in metric_category.get('metrics', []):
                metrics_identifier = metric.get('identifier', '')

                for dataset in metric.get('datasets', []):
                    percentile = dataset.get('filterCriteria', {}).get('percentile', '')
                    device = dataset.get('filterCriteria', {}).get('device', '')

                    for point in dataset.get('points', []):
                        app_version = point.get('version', '')
                        points_value = point.get('value', '')

                        processed_output.append({
                            'platform': platform,
                            'metrics_identifier': metrics_identifier,
                            'percentile': percentile,
                            'device': device,
                            'app_version': app_version,
                            'points_value': points_value,
                            'dag_run_date': report_date,
                            'inserted_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'inserted_by': f"{ENV_VALUE}_batch_pipeline"
                        })

    return processed_output


app_store_api_report()


if __name__ == "__main__":
    # app_store_api_report().test(execution_date="2024-10-10")
    app_store_api_report().test_dq_checks(execution_date="2024-10-10")
