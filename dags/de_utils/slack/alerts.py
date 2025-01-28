"""Module contains a variety of common alerts sent out by DE team."""
from de_utils.constants import ENV_ENUM
from de_utils.slack import send_message, send_message_with_file_attached
from de_utils.slack.tokens import get_de_alerts_token
from datetime import datetime

# The indent required for numbered lists for slack alerts to align in nicely formatted alert.
SLACK_ALERT_INDENT_NUMERIC_LIST = "                   "


# The following params are required by Airflow 2.
def alert_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    msg = f"""
        :red_circle: SLA Missed.
        *Dag*: {dag}
        *Tasks Missed SLAs*: {task_list}
        *Blocking Tasks*: {blocking_task_list}
    """
    send_message(env=ENV_ENUM, channel="#de-airflow-alerts", message=msg, get_auth_token_func=get_de_alerts_token,
                 username="MWAA Alert", icon_emoji=":weary:")


def alert_on_failure(context):
    """Used by dag on_failure_callback arg to raise dag failure alert in #de-airflow-alerts channel"""
    msg = f"""
        :red_circle: Task Failed. 
        *Task*: {context.get("task").task_id}  
        *Dag*: {context.get("task").dag_id} 
        *Execution Time*: {context.get("execution_date")}  
    """
    send_message(env=ENV_ENUM, channel="#de-airflow-alerts", message=msg, get_auth_token_func=get_de_alerts_token,
                 username="MWAA Alert", icon_emoji=":weary:")

    # Next line commented out until we decide what to do with downstream team alerting
    # alert_downstream_users_on_failure(context.get("task").dag_id)


def alert_ml_validation_on_failure(context):
    """Used by dag on_failure_callback arg to raise dag failure alert in #ml-validation-alerts"""
    msg = f"""
        :red_circle: Task Failed. 
        *Task*: {context.get("task").task_id}  
        *Dag*: {context.get("task").dag_id} 
        *Execution Time*: {context.get("execution_date")}  
    """
    send_message(env=ENV_ENUM, channel="#ml-validation-alerts", message=msg, get_auth_token_func=get_de_alerts_token,
                 username="MWAA Alert", icon_emoji=":weary:")


# primarily used by the de to send high priority sla missed alerts to the Slack channel #de-airflow-alerts
# The following params are required by Airflow 2.
def alert_sla_miss_high_priority(dag, task_list, blocking_task_list, slas, blocking_tis):
    msg = f"""
        :red_circle: SLA Missed.
        *Dag*: {dag}
        *Tasks Missed SLAs*: {task_list}
        *Blocking Tasks*: {blocking_task_list}
        """
    send_message(env=ENV_ENUM, channel="#de-airflow-alerts", message=msg, get_auth_token_func=get_de_alerts_token,
                 username="MWAA Alert", icon_emoji=":weary:")


# noinspection PyUnresolvedReferences
def alert_dq_check_failure(
        name: str,
        detail_name: str,
        table_name: str,
        column_name: str,
        message: str,
        check_code: "Link",
        query_run: "Link",
        todo: list[str],
        executed_ts: datetime = datetime.utcnow(),
        **kwargs
):
    """Alert specific to an individual DQ Check red failure."""
    msg = f"""
        :red_circle: DQ Check Failed. 
        *Name*: {name}
        *Detail Name*: {detail_name}
        *Table Name*: {table_name}
        *Column Name*: {column_name}
        *Error*: {message}
        *Check Code*: {check_code}
        *Query Run*: {query_run}
        *ToDo*: {_process_todos(todo)} 
        *Execution Time*: {executed_ts}  
    """
    send_message(env=ENV_ENUM, channel="#data-alerts", message=msg, get_auth_token_func=get_de_alerts_token,
                 username="MWAA DQ Checks", icon_emoji=":thinking_face:")


# noinspection PyUnresolvedReferences
def alert_dq_check_warning(
        name: str,
        detail_name: str,
        table_name: str,
        column_name: str,
        message: str,
        check_code: "Link",
        query_run: "Link",
        todo: list[str],
        executed_ts: datetime = datetime.utcnow(),
        **kwargs
):
    """Alert specific to an individual DQ Check yellow warning."""
    msg = f"""
        :large_yellow_circle: DQ Check Warning. 
        *Name*: {name}
        *Detail Name*: {detail_name}
        *Table Name*: {table_name}
        *Column Name*: {column_name}
        *Warning*: {message}
        *Check Code*: {check_code}
        *Query Run*: {query_run}
        *ToDo*: {_process_todos(todo)} 
        *Execution Time*: {executed_ts}  
    """
    send_message(env=ENV_ENUM, channel="#data-alerts", message=msg, get_auth_token_func=get_de_alerts_token,
                 username="MWAA DQ Checks", icon_emoji=":thinking_face:")


# noinspection PyUnresolvedReferences
def alert_anomalies_detected(
        name: str,
        table_name: str,
        column_name: str,
        filter_expr: str,
        file_contents: str,
        channel: str,
        **kwargs
):
    """Alert specific to anomaly data to send to appropriate alerts channel."""
    msg = (f"*Issue*: {name}\n"
           f"*table.column*: {table_name}.{column_name}\n"
           f"*Filter Expr*: {filter_expr}")

    send_message_with_file_attached(
        env=kwargs.get("env", ENV_ENUM),
        filename="anomaly_records.csv",
        title="anomaly_records.csv",
        file_contents=file_contents,
        channel=channel,
        message=msg,
        get_auth_token_func=get_de_alerts_token
    )


def _process_todos(todo: list[str]):
    """Returns str version of todos formatted for slack alert"""
    todo_str = ""
    for idx, e in enumerate(todo, start=1):
        if idx > 1:
            todo_str += f"\n{SLACK_ALERT_INDENT_NUMERIC_LIST}"
        todo_str += f"{idx}) {e}"
    return todo_str
