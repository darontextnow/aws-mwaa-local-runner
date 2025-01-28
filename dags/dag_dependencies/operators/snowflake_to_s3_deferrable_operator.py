from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from de_utils.constants import STAGE_SF, SF_CONN_ID


class SnowflakeToS3DeferrableOperator(SnowflakeAsyncDeferredOperator):
    """Unloads Snowflake sql query results to S3.

    NOTE: Default is to run in deferred mode. This is preferable for any runs longer than 30s.
        You can override this to run normally (non-deferred) by specifying:
        deferrable=False

    Args:
        sql (str): SQL statement whose results will be unloaded.
        s3_bucket (str): Bucket to which unloaded results will go.
        s3_key (str): The prefix of the unloaded results.
        file_format (str): file_format options FILE_FORMAT =(TYPE = csv, RECORD_DELIMITER = ',' )
        snowflake_conn_id (str): Snowflake connection id in Airflow.
        stage (str): reference to a specific snowflake stage.
        copy_options (str): OPTIONAL copy_options: overwrite = TRUE SINGLE = TRUE HEADER = TRUE
        warehouse (str): name of warehouse (will overwrite any warehouse defined in the connection's extra JSON)
        session_parameters (Dict[str]): You can set session-level parameters at the time you connect to Snowflake
        deferrable (bool): Default True. Specifies whether operator operates in deferrable mode.
        poll_interval (int): Time in seconds to wait between polling Snowflake for query status.
    """

    template_fields = ("sql", "s3_key")
    template_ext = (".sql",)
    ui_color = '#ededed'

    def __init__(
            self,
            *,
            sql: str,
            s3_bucket: str,
            s3_key: str,
            file_format: str,
            snowflake_conn_id: str = SF_CONN_ID,
            stage: str = STAGE_SF,
            copy_options: str | None = None,
            warehouse: str | None = None,
            session_parameters: dict | None = None,
            deferrable: bool = True,
            poll_interval: int = 60,
            **kwargs):

        super().__init__(sql=sql, snowflake_conn_id=snowflake_conn_id, poll_interval=poll_interval,
                         warehouse=warehouse,  **kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.stage = stage
        self.warehouse = warehouse
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.copy_options = copy_options
        self.session_parameters = session_parameters
        self.deferrable = deferrable

    def execute(self, context):
        normal_sql, deferrable_sql = self.get_sql(self.sql)

        self.log.info("Executing UNLOAD...")

        if normal_sql:
            self.run_queries_non_deferred(normal_sql)
        if deferrable_sql:
            # run last query (the COPY statement) in deferred mode
            self.split_statements = False
            self.sql = deferrable_sql
            super().execute(context)

        self.log.info("UNLOAD complete")

    def get_sql(self, sql: str) -> (list[str] | None, str | None):
        """Returns list of sql to run normally and sql to run as deferred with:
        All comments removed for safety.
        Split into a list in case there are multiple queries to run.
        Last query modified into COPY statement.
        """
        import re
        sql = re.sub("--.*", "", re.sub(r"(/\*[\s\S]*?\*/)", "", sql))
        sql_chain = sql.strip().rstrip(";").split(";")

        # replace last query (should be SELECT statement) with COPY statements
        unload_sql = f"""COPY INTO @{self.stage}/{self.s3_key} FROM ({sql_chain[-1].strip()})
                    file_format= {self.file_format} {self.copy_options or ""}"""

        normal_sql = None
        deferrable_sql = None
        if self.deferrable is False:
            normal_sql = sql_chain[:-1] + [unload_sql]
        else:
            if len(sql_chain) > 1:
                normal_sql = sql_chain[:-1]
            deferrable_sql = unload_sql
        return normal_sql, deferrable_sql

    def run_queries_non_deferred(self, sql_chain: list[str]):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        snowflake_hook = SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            session_parameters=self.session_parameters,
        )
        snowflake_hook.run(sql_chain)
