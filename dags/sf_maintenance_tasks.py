"""
DAG runs regular maintenance tasks for our Snowflake environment.
Includes cost-optimized resuming and suspending of auto-clustering of clustered tables.
Other maintenance DAGS/Tasks can be added as needed.
"""
from airflow_utils import dag
from datetime import datetime, timedelta
from de_utils.slack.alerts import alert_on_failure
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from airflow.sensors.time_delta import TimeDeltaSensorAsync


@dag(
        start_date=datetime(2024, 12, 1),
        schedule="5 1 1,15 * *",  # run on 1st and 15th day of month at 1:05am
        default_args={
            'owner': 'DE Team',
            'retries': 0,
            "on_failure_callback": alert_on_failure
        },
        dagrun_timeout=timedelta(hours=5),
        catchup=False  # just ensure the suspend task runs if it fails after resume task.
)
def sf_maintenance_tasks():
    clustered_tables = [
        "party_planner_realtime.applifecyclechanged",
        "party_planner_realtime.messagedelivered",
        "core.adtracker",
        "core.users"
    ]

    resume_auto_clustering = SnowflakeAsyncDeferredOperator(
        task_id="resume_auto_clustering",
        sql=";".join([f"ALTER TABLE {table} RESUME RECLUSTER" for table in clustered_tables])
    )

    wait_3_hours = TimeDeltaSensorAsync(
        task_id="wait_three_hours",
        delta=timedelta(hours=3)
    )

    suspend_auto_clustering = SnowflakeAsyncDeferredOperator(
        task_id="suspend_auto_clustering",
        sql=";".join([f"ALTER TABLE {table} SUSPEND RECLUSTER" for table in clustered_tables])
    )

    resume_auto_clustering >> wait_3_hours >> suspend_auto_clustering


sf_maintenance_tasks()

if __name__ == '__main__':
    sf_maintenance_tasks().test(execution_date='2024-12-01 04:05:00')
