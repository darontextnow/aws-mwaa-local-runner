"""
This DAG generates a heartbeat metric to DataDog every hour.

A corresponding metric Monitor must be set up in DataDog
    https://app.datadoghq.com/monitors/manage
    https://app.datadoghq.com/dashboard/
If the heartbeat is 0 for any hour, it means the MWAA Prod environment is not running correctly.

DAG should run the latest/most current run only, thus setting catchup=False.
"""
from airflow_utils import dag, task
from datetime import datetime


@dag(
    start_date=datetime(2023, 6, 6),
    schedule="55 * * * *",  # run at min 55 of every hour
    default_args={
        'owner': "DE Team",
        'depends_on_past': False,
        'retries': 0
    },
    max_active_runs=1,
    catchup=False
)
def mwaa_airflow_heartbeat():

    @task
    def send_heartbeat():
        from de_utils.datadog import send_metric
        send_metric(metric_name="mwaa_airflow_heartbeat", value=1)

    send_heartbeat()


mwaa_airflow_heartbeat()
