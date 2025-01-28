"""
DAG runs recommended maintenance tasks for MWAA Airflow.
Current version of this DAG follows guidelines from the following AWS MWAA article:
https://docs.aws.amazon.com/mwaa/latest/userguide/samples-database-cleanup.html

"""
from airflow_utils import dag, task
from datetime import datetime, timedelta

DEFAULT_MAX_AGE_IN_DAYS = 120  # adjusting to 120 days to give us 4 months of history and remove the rest.


@dag(
        start_date=datetime(2023, 6, 5),
        schedule="50 2 * * *",
        default_args={
            'owner': 'DE Team',
            'depends_on_past': False,
            'retries': 0,
        },
        dagrun_timeout=timedelta(hours=2),
        max_active_runs=1,
        catchup=False
)
def mwaa_airflow_maintenance():

    @task
    def cleanup_db():
        # moving all these imports out of top level greatly reduced load time on DagBag
        from airflow import settings
        from airflow.utils.dates import days_ago
        from airflow.jobs.job import Job
        from airflow.models import DagModel, DagRun, ImportError, Log, SlaMiss, RenderedTaskInstanceFields, TaskFail, \
            TaskInstance, TaskReschedule, Variable, XCom

        session = settings.Session()
        print("session: ", str(session))

        oldest_date = days_ago(int(Variable.get("max_metadb_storage_days", default_var=DEFAULT_MAX_AGE_IN_DAYS)))
        print("oldest_date: ", oldest_date)

        objects_to_clean = [
            [Job, Job.latest_heartbeat],
            [DagModel, DagModel.last_parsed_time],
            [DagRun, DagRun.execution_date],
            [ImportError, ImportError.timestamp],
            [Log, Log.dttm],
            [SlaMiss, SlaMiss.execution_date],
            [RenderedTaskInstanceFields, RenderedTaskInstanceFields.execution_date],
            [TaskFail, TaskFail.start_date],
            [TaskInstance, TaskInstance.execution_date],
            [TaskReschedule, TaskReschedule.execution_date],
            [XCom, XCom.execution_date]
        ]

        for x in objects_to_clean:
            query = session.query(x[0]).filter(x[1] <= oldest_date)
            print(str(x[0]), ": ", str(query.all()))
            query.delete(synchronize_session=False)

        session.commit()

        return "OK"

    cleanup_db()


mwaa_airflow_maintenance()
