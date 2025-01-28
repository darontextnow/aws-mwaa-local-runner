"""DAG collects latest updated data from various Snowflake tables and sends it to Adjust."""
from airflow_utils import dag
from dag_dependencies.constants import SQL_SCRIPTS_DIR, AIRFLOW_STAGING_KEY_PFX, ECR_TOOLING_ROOT_URL
from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from dag_dependencies.operators.snowflake_to_s3_deferrable_operator import SnowflakeToS3DeferrableOperator
from dag_dependencies.operators.kubernetes_pod_operator import KubernetesPodOperator
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import SF_BUCKET, SF_WH_LARGE, SF_WH_SMALL
from kubernetes.client.models import V1ResourceRequirements
from datetime import datetime, timedelta

FIELDS = ["app_token", "app_id", "adid", "event_token", "android_id", "gps_adid", "idfa",
          "idfv", "ip_address", "created_at_unix"]


class SnowflakeToS3DeferrableOperatorCustomSQL(SnowflakeToS3DeferrableOperator):
    # override execute method so we can concatenate all sql strings from files together.
    # Doing it here means this happens at runtime rather than every time the DAG is loaded.
    # Combining them as one query also avoids the skew when sending to Adjust API. Better control over parallelism.
    def execute(self, context):
        self._build_sql()

        # Resolve all Jinja templates in the query created above
        self.sql = self.render_template(self.sql, context)

        # Execute the query created above.
        super().execute(context)

    def _build_sql(self):
        """Builds an sql query from all queries in self.dag.template_searchpath."""
        import os
        from de_utils.tndbo.utils import snowflake_union_queries
        self.sql = f"SELECT {','.join(FIELDS)} FROM (\n"  # wrap queries to ensure expected column order
        sql_dir = self.dag.template_searchpath[0]  # directory with all .sql files to retrieve
        queries = [open(f"{sql_dir}/{sql_file}").read() for sql_file in os.listdir(sql_dir)]  # retrieve all queries
        self.sql += snowflake_union_queries(queries)  # union all queries together
        self.sql += ")"  # end of wrap


class SnowflakeAsyncDeferredOperatorWrapSQL(SnowflakeAsyncDeferredOperator):
    # override execute method to wrap sql in INSERT statement
    def execute(self, context):
        self.sql = "INSERT INTO analytics_staging.ua_events_sent_to_adjust " + self.sql
        super().execute(context)


@dag(
    start_date=datetime(2024, 2, 28),
    schedule=None,  # triggered by dbt_downstream_dags_triggerer DAG
    default_args={
        "owner": "Daron",
        "retry_delay": timedelta(minutes=5),
        "retries": 3,
        "on_failure_callback": alert_on_failure
    },
    template_searchpath=f"{SQL_SCRIPTS_DIR}/adjust_send_post_install_events",
    max_active_runs=1,
    catchup=False
)
def adjust_send_post_install_events():
    s3_key = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ ds }}}}/events.csv"

    collect = SnowflakeToS3DeferrableOperatorCustomSQL(
        task_id="get_events",
        sql="this will be replaced in custom execute method",
        s3_bucket=SF_BUCKET,
        s3_key=s3_key,
        # using the following to allow json string from SELECT statement to print to file as is
        file_format="(TYPE = CSV COMPRESSION = NONE FIELD_DELIMITER = '|' NULL_IF = ('') EMPTY_FIELD_AS_NULL = FALSE)",
        copy_options="OVERWRITE = TRUE HEADER = FALSE SINGLE = TRUE MAX_FILE_SIZE =4900000000",
        warehouse=SF_WH_LARGE
    )

    send = KubernetesPodOperator(
        task_id="send_events",
        name="adjusts2s-send-events",
        image=f"{ECR_TOOLING_ROOT_URL}/ds/adjust-s2s:latest",
        arguments=[f"s3://{SF_BUCKET}/{s3_key}"],
        # arguments=["test"],  # use this line instead of the above arguments to run a test only run.
        container_resources=V1ResourceRequirements(requests={"cpu": "8", "memory": "20Gi"},
                                                   limits={"cpu": "8", "memory": "20Gi"})
    )

    # load early mover data SF table ua_events_sent_to_adjust
    load_early_mover_data = SnowflakeAsyncDeferredOperatorWrapSQL(
        task_id="load_early-mover_data",
        sql="early_mover.sql",
        warehouse=SF_WH_SMALL
    )

    collect >> send >> load_early_mover_data


adjust_send_post_install_events()

if __name__ == "__main__":
    adjust_send_post_install_events().test(execution_date="2025-01-09")
