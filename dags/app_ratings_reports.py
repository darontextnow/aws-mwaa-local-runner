from airflow_utils import dag, task
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.constants import AIRFLOW_STAGING_KEY_PFX
from de_utils.constants import SF_BUCKET
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 7, 19),
    schedule="40 0 * * *",
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=3),
        "retries": 3
    },
    max_active_runs=1,
    catchup=False
)
def app_ratings_reports():
    s3_key = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ ds }}}}/data.csv"

    @task
    def collect_app_ratings(s3_key, **context):
        from pandas import DataFrame
        http_hook = get_http_hook()
        ratings = collect_ratings(http_hook, context["ds"])
        DataFrame(ratings).to_csv(f"s3://{SF_BUCKET}/{s3_key}", index=False, header=False)

    snowflake_load = S3ToSnowflakeDeferrableOperator(
        task_id="load_app_ratings_snowflake",
        table="app_ratings",
        schema="core",
        s3_loc=s3_key,
        file_format="(TYPE = CSV DATE_FORMAT = AUTO)",
        load_type=S3ToSnowflakeDeferrableOperator.LoadType.MERGE
    )

    collect_app_ratings(s3_key) >> snowflake_load


def calc_avg_rating(rating_dict: dict):
    """
    Sum/Count of ratings aren't always available so we calculate them ourselves

    :param rating_dict: dict
    :return: int, int
    """
    if not rating_dict:
        return None, None
    rating_sum, rating_cnt = 0.0, 0
    for i in range(1, 6):
        cnt = rating_dict[f"star_{i}_count"] or 0
        rating_sum += cnt * i
        rating_cnt += cnt
    # Avoid division by 0 error
    if rating_cnt == 0:
        return 0, 0
    return rating_sum / rating_cnt, rating_cnt


def get_http_hook():
    from airflow.providers.http.hooks.http import HttpHook
    return HttpHook(method="GET", http_conn_id="appannie_api")


def collect_ratings(http_hook, ds: str) -> list[list]:
    configs = [
        ("TN_ANDROID", "google-play", "20600000436866"),
        ("TN_IOS_FREE", "ios", "314716233")
    ]
    ratings = []
    for client_type, market, asset in configs:
        endpoint = f"v1.3/apps/{market}/app/{asset}/ratings"
        # Iterate through pages, collect data, write and then upload to S3
        resp = http_hook.run(endpoint)
        while True:
            resp.raise_for_status()
            results = resp.json()
            for rating in results["ratings"]:
                country = rating["country"]
                overall_avg, overall_cnt = calc_avg_rating(rating.get("all_ratings"))
                current_avg, current_cnt = calc_avg_rating(rating.get("current_ratings"))
                ratings.append([ds, client_type, asset, country, overall_avg, overall_cnt, current_avg, current_cnt])
            if results["next_page"]:
                resp = http_hook.run(results["next_page"])
            else:
                break

    return ratings


app_ratings_reports()

if __name__ == "__main__":
    app_ratings_reports().test(execution_date="2023-07-20")
