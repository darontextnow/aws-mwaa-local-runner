from functools import lru_cache


@lru_cache(maxsize=1)
def get_de_alerts_token():
    """Returns token associated with Slack API App/Integration named DE DQ Check Alerts.
    This App/Integration was setup by Daron for DQ Check purposes.
    May be used for any alerts to be sent to data-alerts channel.
    """
    from de_utils.aws import get_secret  # delay this import so Airflow has less to load at top level DAG
    return get_secret(name="airflow/variables/slack_token_dqc")
