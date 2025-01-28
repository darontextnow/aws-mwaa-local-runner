from de_utils.enums import Env


def send_metric(metric_name, value, current_time=None, env: Env = Env.PROD, **kwargs) -> None:
    """Send metric to datadog.

    Args:
        metric_name (str): The name the metric should be sent to datadog
        value (int): Usually a binary value such as 1.
        current_time (int, optional): The current seconds since epoch. Defaults to current.
        env (Env, optional) : Environment value to be used in the tag
    Returns:
        requests.Response: The response received from datadog
    """
    # delay the following imports to keep Airflow top level DAG loads efficient
    from de_utils.aws import get_secret
    import json
    from datetime import datetime
    import requests

    key = get_secret("airflow/variables/datadog_api_key")
    url = 'https://api.datadoghq.com/api/v1/series?api_key=' + key

    if not current_time:
        current_time = (datetime.now() - datetime(1970, 1, 1)).total_seconds()

    post_val = {
        "series": [
            {
                "metric": metric_name,
                "points": [[current_time, value]],
                "host": "airflow",
                "tags": ["team:dpe", "data_pipeline", f"env:{env.value}"]
            }
        ]
    }
    requests.post(url, data=json.dumps(post_val))
