"""Module contains functions for common DAG functionality and testing use cases"""
from airflow.models import DagRun, TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from datetime import datetime
import pendulum


def get_default_context() -> Context:
    context = Context()
    context["dag_run"] = DagRun()
    context["task"] = TaskInstance(BaseOperator(task_id="default_task_id"), execution_date=datetime.now(), run_id="")
    context["data_interval_start"] = pendulum.now(tz="UTC")
    context["ti"] = TaskInstance(BaseOperator(task_id="default_task_id"), execution_date=datetime.now(), run_id="")
    return context
