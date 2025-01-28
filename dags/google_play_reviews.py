from airflow_utils import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP, AIRFLOW_STAGING_KEY_PFX, DAG_RUN_DATE_1_DAY_LAG
from dag_dependencies.dq_checks_defs import GooglePlayReviewsDQChecks
from dag_dependencies.operators.dq_checks_operator import DQChecksOperator
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import SF_BUCKET
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
import tenacity
from googleapiclient.errors import HttpError
from datetime import datetime, timedelta, timezone


COLUMNS_MAPPING = {
    "reviewId": "id",
    "userName": "author_name",
    "content": "text",
    "score": "rating",
    "thumbsUpCount": "upvote_count",
    "reviewCreatedVersion": "app_version",
    "at": "last_modified"
}
MAIN_TABLE_COLS = [
    "id", "author_name", "title", "text", "last_modified", "rating", "language", "client_type",
    "os_version", "app_version", "app_version_code", "upvote_count", "device_type", "device_name",
    "device_manufacturer", "native_platform", "cpu_model", "cpu_make", "ram_in_mb", "screen_width_px",
    "screen_height_px", "screen_density_dpi", "opengl_version", "app_name"
]


@dag(
        start_date=datetime(2024, 5, 6),
        schedule="40 2 * * *",
        default_args={
            "owner": "Satish",
            "retry_delay": timedelta(minutes=10),
            "retries": 2,
            "on_failure_callback": alert_on_failure
        },
        max_active_runs=1,
        catchup=DAG_DEFAULT_CATCHUP
)
def google_play_reviews():
    comp_s3_dir = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/competitor/{{{{ data_interval_start.date() }}}}/"
    tn_s3_dir = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/textnow/{{{{ data_interval_start.date() }}}}/"

    @task
    def collect_competitor_reviews(app_name, app_id, s3_key, **context):
        log = context["task"].log
        # 1 day lookback
        start_date = context["data_interval_start"].replace(tzinfo=None) - timedelta(days=1)
        # adding +1 day to end_date so we include current days reviews if available.
        end_date = context["data_interval_start"].replace(tzinfo=None) + timedelta(days=1)
        log.info(f"Fetching reviews for app: {app_name} between dates: {start_date} & {end_date}")
        df = fetch_competitor_reviews(start_date, app_id)
        df = post_process(df, app_name, start_date, end_date)
        if df.empty:
            msg = f"No reviews were returned for app: {app_name} for dates: {start_date} & {end_date}."
            raise RuntimeError(msg)
        df.to_csv(f"s3://{SF_BUCKET}/{s3_key}{app_name}.csv", header=False, index=False)

    @task
    def collect_tn_reviews(client_type: str, s3_key: str, **context):
        log = context["task"].log
        client_map = {"TN_ANDROID": "com.enflick.android.TextNow",
                      "2L_ANDROID": "com.enflick.android.tn2ndLine"}

        log.info(f"Fetching reviews for client type '{client_type}'...")
        reviews = fetch_tn_reviews(package_name=client_map[client_type])
        log.info(f"Fetched {len(reviews)} reviews for client type '{client_type}'")
        log.info(f"Processing reviews for client type '{client_type}'...")
        processed_reviews = process_tn_reviews(reviews, client_type)
        log.info(f"Writing reviews for client type '{client_type}' to s3 bucket '{SF_BUCKET}', key '{s3_key}'.")
        write_data_to_s3(data=processed_reviews, s3_key=s3_key)

    load_competitor_reviews = S3ToSnowflakeDeferrableOperator(
        task_id="load_competitor_reviews",
        table="google_play_reviews",
        schema="core",
        s3_loc=comp_s3_dir,
        file_format=("(TYPE = CSV DATE_FORMAT = AUTO FIELD_OPTIONALLY_ENCLOSED_BY = '\"'"
                     " ESCAPE_UNENCLOSED_FIELD = NONE)"),
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.KEEP_EXISTING
    )

    load_tn_reviews = S3ToSnowflakeDeferrableOperator(
        task_id="load_tn_reviews",
        table="google_play_reviews",
        schema="core",
        s3_loc=tn_s3_dir,
        file_format="(TYPE = JSON STRIP_OUTER_ARRAY = TRUE)",
        copy_options="MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.KEEP_EXISTING,
    )

    run_dq_checks = DQChecksOperator(
        task_id="run_google_play_reviews_dq_checks",
        dq_checks_instance=GooglePlayReviewsDQChecks(),
        run_date=DAG_RUN_DATE_1_DAY_LAG
    )

    trigger_app_reviews = TriggerDagRunOperator(
        task_id="trigger_app_reviews_classification",
        trigger_dag_id="app_reviews_classification",
        execution_date="{{ data_interval_start }}",
    )

    reviews = []
    for app, app_id in {"TEXTFREE": "com.pinger.textfree",
                        "TEXTME": "com.textmeinc.textme",
                        "WHATSAPP": "com.whatsapp"}.items():
        reviews.append(collect_competitor_reviews.override(task_id=f"collect_{app}_reviews")(
            app_name=app, app_id=app_id, s3_key=comp_s3_dir))

    for client in ("TN_ANDROID", "2L_ANDROID"):
        reviews.append(collect_tn_reviews.override(task_id=f"collect_{client}_reviews")(
            client_type=client, s3_key=f"{tn_s3_dir}{client}.csv"))

    reviews >> load_competitor_reviews >> load_tn_reviews >> run_dq_checks >> trigger_app_reviews


# Helper Functions

def fetch_competitor_reviews(start_date: datetime, app_id):
    """Returns Pandas DataFrame containing results retrieved using google-play-scraper API"""
    from pandas import DataFrame
    from time import sleep
    from google_play_scraper import Sort, reviews

    continuation_token = None
    result = []
    while True:
        # get scraping result- see details at https://github.com/JoMingyu/google-play-scraper
        _result, continuation_token = reviews(
            app_id,
            continuation_token=continuation_token,
            lang="en",  # defaults to "en"
            country="us",  # defaults to "us"
            sort=Sort.NEWEST,  # sort order of results
            count=200
        )

        # append current result to previous iteration
        result += _result

        # if all the reviews have been scrapped, stop
        if continuation_token.token is None:
            break

        # if the start date has been reached (sort is from NEWEST to OLDEST), stop
        last_ele = _result[-1]
        last_ele_timestamp = last_ele["at"]
        if last_ele_timestamp < start_date:
            break

        sleep(1)  # sleep for a second to prevent from banning.

    return DataFrame(result)


def post_process(df, app_name: str, start_date: datetime, end_date: datetime):
    """Returns modified df after applying necessary alterations."""
    # filter with time
    mask = (df["at"] >= start_date) & (df["at"] < end_date)
    df = df.loc[mask]

    # drop columns not in columns mapping
    df = df.loc[:, COLUMNS_MAPPING.keys()]

    # rename columns name
    df.rename(columns=COLUMNS_MAPPING, inplace=True)

    df["app_name"] = app_name  # Add AppName column
    df["language"] = "en"  # Add Language since we're only pulling "en" currently

    # Add all additional columns including Client Type (Need Mapping of App Names) and TITLE
    for column in MAIN_TABLE_COLS:
        if column not in df.columns:
            df[column] = None

    # Filtering out rows where text column IS NULL as text is required downstream.
    df = df[df['text'].notnull()]

    # Reorder the columns and return
    return df[MAIN_TABLE_COLS]


@tenacity.retry(
        wait=tenacity.wait_exponential(multiplier=2),
        stop=tenacity.stop_after_attempt(3),
        retry=tenacity.retry_if_exception_type(HttpError),
        reraise=True
)
def send_request(req):
    import time
    resp = req.execute()
    time.sleep(1)  # sleep for a second to prevent from banning.
    return resp


def fetch_tn_reviews(package_name: str):
    """Returns up to 1000 reviews from Google API for most current week only."""
    from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
    from googleapiclient.discovery import build
    authed_http = GoogleBaseHook(gcp_conn_id="gcp_android")._authorize()
    svc = build("androidpublisher", "v3", http=authed_http, cache_discovery=False)
    reviews_resource = svc.reviews()
    reviews = []
    pagination_token = None
    while len(reviews) < 1000:
        req = reviews_resource.list(
            packageName=package_name,
            maxResults=200,
            token=pagination_token
        )
        resp = send_request(req)
        reviews += resp["reviews"]
        pagination_token = resp.get("tokenPagination", {}).get("nextPageToken")
        if pagination_token is None:
            break
    return reviews


def process_tn_reviews(reviews: list, client_type: str) -> list[dict]:
    """Returns Pandas DataFrame with processed reviews. Unpacks all desired column values from nested json."""
    processed_reviews = []

    for review in reviews:
        cols = {
            "id": review["reviewId"],
            "author_name": review.get("authorName"),
            "client_type": client_type
        }
        for c in review["comments"]:
            if not c.get("userComment"):
                continue
            comment = c["userComment"]
            # Review title, if present, will be separated from text by a tab character
            review_content = comment["text"].split("\t")

            cols["title"] = review_content[0] if len(review_content) > 1 and review_content[0] != "" else None
            cols["text"] = review_content[-1]
            cols["last_modified"] = int(comment["lastModified"]["seconds"])
            cols["rating"] = comment["starRating"]
            cols["language"] = comment.get("reviewerLanguage")
            cols["os_version"] = comment.get("androidOsVersion")
            cols["app_version"] = comment.get("appVersionName")
            cols["app_version_code"] = comment.get("appVersionCode")
            cols["upvote_count"] = comment.get("thumbsUpCount", 0)
            cols["app_name"] = "TEXTNOW" if client_type == "TN_ANDROID" else "2ND_LINE"

            meta = comment.get("deviceMetadata")
            if meta:
                cols["device_type"] = meta.get("deviceClass")
                cols["device_name"] = meta.get("productName")
                cols["device_manufacturer"] = meta.get("manufacturer")
                cols["native_platform"] = meta.get("nativePlatform")
                cols["cpu_model"] = meta.get("cpuModel")
                cols["cpu_make"] = meta.get("cpuMake")
                cols["ram_in_mb"] = meta.get("ramMb")
                cols["screen_width_px"] = meta.get("screenWidthPx")
                cols["screen_height_px"] = meta.get("screenHeightPx")
                cols["screen_density_dpi"] = meta.get("screenDensityDpi")
                cols["opengl_version"] = meta.get("glEsVersion")

        processed_reviews.append(cols)
    return processed_reviews


def write_data_to_s3(data: list[dict], s3_key):
    from pandas import DataFrame
    columns = ["id", "author_name", "title", "text", "last_modified", "rating", "language", "client_type",
               "os_version", "app_version", "app_version_code", "upvote_count", "device_type", "device_name",
               "device_manufacturer", "native_platform", "cpu_model", "cpu_make", "ram_in_mb",
               "screen_width_px", "screen_height_px", "screen_density_dpi", "opengl_version", "app_name"]
    df = DataFrame(data=data, columns=columns)
    df.to_json(f"s3://{SF_BUCKET}/{s3_key}", orient="records")
    return df  # returning for testing purposes.


google_play_reviews()

if __name__ == "__main__":
    execution_date = datetime.now(timezone.utc) - timedelta(days=1)
    google_play_reviews().test(execution_date=execution_date)
