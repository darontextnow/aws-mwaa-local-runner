"""Module for sending messages to Slack channels"""
from de_utils.enums import Env
from collections.abc import Callable
from functools import lru_cache


def send_message(
        env: Env,
        channel: str,
        message: str,
        get_auth_token_func: Callable,  # do not retrieve token from Secrets Manager until needed.
        username: str,
        icon_emoji: str = None
):
    from slack_sdk.errors import SlackApiError
    if env != Env.PROD:
        print("Alert not sent due to not running in prod environment")
        return  # not sending alerts when not in prod environment

    try:
        response = get_client(auth_token=get_auth_token_func()).chat_postMessage(
            channel=channel,
            text=message,
            username=username,
            icon_emoji=icon_emoji,
            link_names=True,
            unfurl_links=False,
            unfurl_media=False
        )
        return response
    except SlackApiError as e:
        print(f"Could not send Slack DQ error to channel '{channel}'.\nError Type: {e.__class__.__name__}.\nError: {e}")
        raise


def send_message_with_file_attached(
        env: Env,
        filename: str,
        title: str,
        file_contents: str,
        channel: str,
        message: str,
        get_auth_token_func: Callable  # do not retrieve token from Secrets Manager until needed.
):
    if env != Env.PROD:
        print("Anomaly Alert not sent due to not running in prod environment")
        return  # not sending alerts when not in prod environment

    response = get_client(auth_token=get_auth_token_func()).files_upload_v2(
        filename=filename,
        content=file_contents,
        title=title,
        initial_comment=message,
        channels=channel,
        username="Airflow"
    )
    return response


@lru_cache(maxsize=1)
def get_client(auth_token: str):
    from slack_sdk import WebClient
    return WebClient(token=auth_token)
