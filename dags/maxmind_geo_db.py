"""DAG unzips the maxmind's GEOIP and GEOLITE extract and loads GeoIP2-City-Blocks-IPv4, GeoIP2-City-Locations-en,
GeoLite2-ASN-Blocks-IPv6, 'GeoLite2-ASN-Blocks-IPv4' csv files into s3 and then to snowflake in analytics schema.
Every day this dag truncates and loads the file, so catch_up = False makes sense. Make sure always the latest load is
successful.
"""

from airflow_utils import dag, task
from de_utils.constants import SF_BUCKET
from de_utils.slack.alerts import alert_on_failure
from dag_dependencies.constants import AIRFLOW_STAGING_KEY_PFX
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 9, 6),
    schedule="20 09 * * *",
    default_args={
        "owner": "Roopa",
        "retry_delay": timedelta(minutes=10),
        "retries": 3,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=False
)
def maxmind_geo_db():
    list_of_files = ['GeoIP2-City-Blocks-IPv4', 'GeoIP2-City-Locations-en', 'GeoLite2-ASN-Blocks-IPv6',
                     'GeoLite2-ASN-Blocks-IPv4']
    url_list_of_files = {
        'https://download.maxmind.com/app/geoip_download?edition_id=GeoIP2-City-CSV&license_key=tCJzERraDGOnvvRw'
        '&suffix=zip': ['GeoIP2-City-Blocks-IPv4', 'GeoIP2-City-Locations-en'],
        'https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-ASN-CSV&license_key=tCJzERraDGOnvvRw'
        '&suffix=zip': ['GeoLite2-ASN-Blocks-IPv4', 'GeoLite2-ASN-Blocks-IPv6']}
    s3_dir = f'{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ ds }}}}'

    @task
    def collect_data(s3_dir: str, **context):
        from de_utils.aws import S3Path
        import io
        import zipfile
        import requests

        log = context["task"].log
        for url in url_list_of_files:
            response = requests.get(url, stream=True)
            with zipfile.ZipFile(io.BytesIO(response.content)) as zipobj:
                retrieved_file_list = zipobj.namelist()
                for elem in retrieved_file_list:
                    if elem.split('/')[1].split('.')[0] in list_of_files:
                        filename = elem.split('/')[1]
                        s3_path = S3Path(f's3://{SF_BUCKET}/{s3_dir}/{filename}')
                        s3_path.upload_fileobj(zipobj.open(elem))
                        log.info(f'File loaded to s3://{SF_BUCKET}/{s3_dir}/{filename}')

    for values in list_of_files:
        target_table = 'maxmind' + '_' + values.replace('-', '_').upper() if values[0:values.find(
            '-')] == "GeoIP2" else 'maxmindasn' + '_' + values.replace('-', '_').upper()

        snowflake_load = S3ToSnowflakeDeferrableOperator(
            task_id=f'sf_load_maxmind_{values}',
            table=target_table,
            schema='analytics',
            s3_loc=f'{s3_dir}/{values}.csv',
            transform_sql="SELECT * EXCLUDE run_date, COALESCE(run_date, CURRENT_DATE()) AS run_date FROM staging",
            file_format="(TYPE = CSV FIELD_DELIMITER = ',' ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE "
                        "FIELD_OPTIONALLY_ENCLOSED_BY='\"' SKIP_HEADER=1)",
            load_type=S3ToSnowflakeDeferrableOperator.LoadType.TRUNCATE
        )

        collect_data(s3_dir) >> snowflake_load


maxmind_geo_db()

if __name__ == "__main__":
    maxmind_geo_db().test(execution_date="2023-09-24")
