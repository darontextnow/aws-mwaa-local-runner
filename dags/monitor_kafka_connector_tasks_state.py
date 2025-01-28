"""DAG pings all realtime-partyplanner-ds Kafka connector tasks hourly for their status
and raises a Slack alert if status = "FAILED"
"""
from airflow_utils import dag, task
from de_utils.slack.alerts import alert_on_failure
from datetime import datetime, timedelta
import requests
import json

BASE_URL = "https://realtime-partyplanner.prod.us-east-1.textnow.io/connectors/"


@dag(
    start_date=datetime(2024, 10, 23),
    schedule="48 * * * *",
    default_args={
        "owner": "Kannan",
        "retry_delay": timedelta(minutes=5),
        "retries": 0,
        "on_failure_callback": alert_on_failure,
    },
    max_active_runs=1,
    catchup=False
)
def monitor_kafka_connector_tasks_state():

    @task
    def get_task_states_and_alert_if_failed():
        failed_tasks = []
        for connector in get_connectors():
            for connector_task in get_connector_tasks(connector):
                task_id = connector_task["id"]["task"]
                status = get_connector_task_status(connector, task_id)
                print(f"Status for {connector}, task: {task_id}: {status}")
                if status["state"] == "FAILED":
                    failed_tasks.append(f"Status for {connector}, task: {task_id}: {status}")

        if failed_tasks:
            output = "\n".join(failed_tasks)
            raise RuntimeError(f"The following connector tasks have failed state:\n{output}")

    get_task_states_and_alert_if_failed()


monitor_kafka_connector_tasks_state()


def get_connectors() -> list:
    """Returns list of all connectors (names) for realtime_partyplanner kafka consumers"""
    response = requests.get(BASE_URL)
    connectors = response.json()
    return connectors


def get_connector_tasks(connector: str) -> json:
    """Returns json def of all tasks associated with the given connector"""
    url = f"{BASE_URL}{connector}/tasks/"
    response = requests.get(url)
    tasks = response.json()
    return tasks


def get_connector_task_status(connector: str, task_id: str) -> json:
    """Returns status of given connector and task as JSON string"""
    url = f"{BASE_URL}{connector}/tasks/{task_id}/status"
    response = requests.get(url)
    status = response.json()
    return status


if __name__ == "__main__":
    monitor_kafka_connector_tasks_state().test(execution_date="2024-10-24")
