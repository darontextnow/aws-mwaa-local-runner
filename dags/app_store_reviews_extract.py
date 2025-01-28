"""Retrieves IOS reviews from app store and loads into Snowflake."""
from airflow_utils import dag, task
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import AIRFLOW_STAGING_KEY_PFX
from de_utils.constants import SF_BUCKET
import tenacity
from datetime import datetime, timedelta
from requests.exceptions import HTTPError


@dag(
    start_date=datetime(2023, 7, 23),
    schedule="10 1 * * *",
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=5),
        "retries": 3
    },
    max_active_runs=1,
    catchup=False
)
def app_store_reviews_extract():
    s3_key = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ ds }}}}/TN_IOS_FREE.json"

    @task
    def collect_reviews(s3_key: str):
        from pandas import DataFrame
        # all country codes supported by Apple app store
        countries = ["CA", "MX", "US"]
        reviews = get_reviews(countries)
        DataFrame(reviews).to_json(f"s3://{SF_BUCKET}/{s3_key}", orient="records")

    snowflake_load = S3ToSnowflakeDeferrableOperator(
        task_id="load_reviews_snowflake",
        table="appstore_reviews",
        schema="core",
        s3_loc=s3_key,
        file_format="(TYPE = JSON TIME_FORMAT = AUTO STRIP_OUTER_ARRAY = TRUE)",
        copy_options="MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.KEEP_EXISTING
    )

    collect_reviews(s3_key) >> snowflake_load


def get_reviews(countries: list[str]) -> list[dict]:
    import requests
    import pytz

    session = requests.Session()
    reviews = []
    for country_code in countries:
        num_reviews = 0
        # Max page depth in iOS RSS feed is 10
        for page in range(1, 11):
            url = (f"https://itunes.apple.com/{country_code}/rss/customerreviews/page={page}/"
                   f"id=314716233/sortby=mostrecent/json")
            try:
                resp = send_request(session, url)
                review_feed = resp.json().get("feed")
            except HTTPError as e:
                # We only allow silent errors on 404, which indicates that data is absent for the given constraint
                if e.response.status_code == 404:
                    review_feed = {}
                else:
                    raise e

            # If no feed is available, end the pagination immediately
            if not review_feed:
                break

            entries = review_feed.get("entry", [])

            for entry in entries:
                pst = datetime.strptime(entry["updated"]["label"], "%Y-%m-%dT%H:%M:%S%z")
                review = {
                    "id": entry["id"]["label"],
                    "author_name": entry["author"]["name"]["label"],
                    "title": entry["title"]["label"],
                    "text": entry["content"]["label"],
                    "last_modified": pst.astimezone(pytz.timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S"),
                    "rating": int(entry["im:rating"]["label"]),
                    "country_code": country_code,
                    "client_type": "TN_IOS_FREE",
                    "app_version": entry["im:version"]["label"],
                    "upvote_count": int(entry["im:voteCount"]["label"])
                }
                reviews.append(review)
                num_reviews += 1

        if num_reviews > 0:
            print(f"Country: {country_code}, Number of Reviews: {num_reviews}")

    return reviews


@tenacity.retry(wait=tenacity.wait_exponential(multiplier=2),
                stop=tenacity.stop_after_attempt(3),
                retry=tenacity.retry_if_exception_type(HTTPError),
                reraise=True)
def send_request(session, url):
    import time
    resp = session.get(url)
    resp.raise_for_status()
    for retry in range(1, 10):
        # this itunes rss feed is not meant for what we're doing here. Online articles don't support this.
        # Sometimes this api returns html page instead of the json data.
        # Thus, applying a hack to retry up to 5 times to try to get json response instead of html code
        if resp.text.startswith("{"):
            return resp
        time.sleep(retry)  # wait before retrying request again
        resp = session.get(url)
        resp.raise_for_status()
    raise RuntimeError("Response from server is not json data. Rerun after a short wait.")


app_store_reviews_extract()

if __name__ == "__main__":
    app_store_reviews_extract().test(execution_date="2023-07-23")
