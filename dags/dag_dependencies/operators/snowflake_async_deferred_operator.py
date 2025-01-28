from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
import asyncio
from typing import Any, List, cast
from de_utils.constants import SF_CONN_ID
from snowflake.connector.constants import QueryStatus


class SnowflakeAsyncDeferredOperator(SQLExecuteQueryOperator):
    """Custom Operator to execute queries in Snowflake in async deferred mode.
    All queries will be sent to Snowflake at the same time for parallel execution.

    WARNING: do NOT use this operator for multiple queries chained together that depend on each other and that
    need to run within the same session. You can not run async with chained queries depending on each other.

    If any query in given sql list fails, the task will fail and the logs will contain error for only the first failure.

    Args:
        sql (str): The SQL statement(s) or string pointing to a .sql template file to be executed.
        snowflake_conn_id (str): The Airflow connection id containing the Snowflake connection string.
        warehouse (str): Optionally specify the Snowflake warehouse to run query in.
        poll_interval: the interval in seconds to poll the query.
        autocommit: (bool) if True, each command is automatically committed (default: True).
    """

    def __init__(
        self,
        *,
        sql: str,
        snowflake_conn_id: str = SF_CONN_ID,
        warehouse: str | None = None,
        poll_interval: int = 5,
        autocommit: bool = True,
        **kwargs
    ) -> None:
        if warehouse:
            hook_params = kwargs.get("hook_params", {})
            hook_params["warehouse"] = warehouse
            kwargs["hook_params"] = hook_params
        self.poll_interval = poll_interval
        self.query_ids = []  # initialize for use later
        self.dbo = None
        self.raw_conn = None

        super().__init__(
            sql=sql,
            conn_id=snowflake_conn_id,
            split_statements=SnowflakeAsyncDeferredOperator.sql_contains_multiple_statements(sql),
            autocommit=autocommit,
            **kwargs
        )

    def get_dbo(self):
        from de_utils.tndbo import get_dbo
        if self.dbo is None:
            self.dbo = get_dbo(self.conn_id)
        return self.dbo

    def get_raw_conn(self):
        return self.get_dbo().get_engine().raw_connection()

    # override to make execution deferrable and async
    def execute(self, context):
        """Executes query/queries asynchronously."""
        for stmt in self.get_statements():
            cursor = self.get_dbo().execute(
                sql=stmt,
                async_execute=True,
                autocommit=self.autocommit,
                warehouse=self.hook_params.get("warehouse", None)
            )
            self.query_ids.append(cursor["queryId"])

        self.log.info(f"List of query ids {self.query_ids}")

        if self.do_xcom_push:
            context["ti"].xcom_push(key="query_ids", value=self.query_ids)

        if len(self.query_ids) >= 1:
            for query_id in self.query_ids:
                if self.get_query_status(query_id) != QueryStatus.SUCCESS:
                    if context.get("skip_query_status_for_testing", False) is True:
                        return  # do not defer as this is a functional testing run
                    break  # go ahead and defer the operator until query is finished
            else:
                return  # no need to defer as all queries already finished running.

        # defer the operator until query run is finished
        self.defer(
            timeout=self.execution_timeout,
            trigger=SnowflakeTrigger(
                poll_interval=self.poll_interval,
                query_ids=self.query_ids,
                snowflake_conn_id=self.conn_id
            ),
            method_name="execute_complete",
        )

    def get_statements(self):
        stmts = [self.sql]
        if self.split_statements is True:
            hook = self.get_db_hook()
            stmts = hook.split_sql_string(self.sql)
        return stmts

    @staticmethod
    def sql_contains_multiple_statements(sql: str) -> bool:
        sql = sql[:-1] if sql.endswith(";") else sql
        split_sql = sql.split(";")
        if len(split_sql) > 1 and all(len(sql) > 4 for sql in split_sql):
            return True
        return False

    def get_query_status(self, query_id: str) -> QueryStatus:
        """Returns current query status as enum for given query_id."""
        status = self.get_raw_conn().get_query_status_throw_if_error(query_id)
        self.log.info(f"Status of query_id {query_id} run is: {status}")
        return status

    def execute_complete(self, context, event: dict[str, str | list[str]] | None = None) -> None:
        """
        Callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event:
            if "status" in event and event["status"] == "error":
                msg = f"{event['status']}: {event['message']}"
                raise RuntimeError(msg)
            elif "status" in event and event["status"] == "success":
                self.check_query_status(cast(List[str], event["statement_query_ids"]))
                self.log.info(f"{self.task_id} completed successfully.")
        else:
            self.log.info(f"{self.task_id} completed successfully.")

    def check_query_status(self, query_ids: list[str]) -> None:
        """Checks status of query execution for given query_ids and raise error if any query has failed."""
        for query_id in query_ids:
            status = self.get_raw_conn().get_query_status_throw_if_error(query_id)
            self.log.info(f"Status for query_id: {query_id} is: {status}")


class SnowflakeTrigger(BaseTrigger):
    """
    Fetch the status for the query ids passed.

    :param poll_interval:  polling period in seconds to check for the status
    :param query_ids: List of Query ids to run and poll for the status
    :param snowflake_conn_id: Reference to Snowflake connection id
    """

    def __init__(
        self,
        poll_interval: float,
        query_ids: list[str],
        snowflake_conn_id: str
    ):
        super().__init__()
        self.poll_interval = poll_interval
        self.query_ids = query_ids
        self.snowflake_conn_id = snowflake_conn_id
        self.raw_conn = None

    def get_raw_conn(self):
        from de_utils.tndbo import get_dbo
        if self.raw_conn is None:
            self.raw_conn = get_dbo(self.snowflake_conn_id).get_engine().raw_connection()
        return self.raw_conn

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes SnowflakeTrigger arguments and classpath."""
        return (
            "dag_dependencies.operators.snowflake_async_deferred_operator.SnowflakeTrigger",
            {
                "poll_interval": self.poll_interval,
                "query_ids": self.query_ids,
                "snowflake_conn_id": self.snowflake_conn_id,
            },
        )

    async def run(self):
        """Wait for the snowflake query to complete."""
        try:
            successful_query_ids: list[str] = []
            for query_id in self.query_ids:
                while True:
                    status = await self.get_query_status(query_id)
                    if status not in [QueryStatus.RUNNING, QueryStatus.QUEUED, QueryStatus.QUEUED_REPARING_WAREHOUSE,
                                      QueryStatus.RESUMING_WAREHOUSE, QueryStatus.RESTARTED]:
                        break  # break while loop as query has finished running
                    await asyncio.sleep(self.poll_interval)

                if status == QueryStatus.SUCCESS:
                    successful_query_ids.append(query_id)
                else:
                    yield TriggerEvent({"status": "error", "query_id": query_id})
                    return

            yield TriggerEvent(
                {
                    "status": "success",
                    "statement_query_ids": successful_query_ids,
                }
            )
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    async def get_query_status(self, query_id: str) -> QueryStatus:
        """Returns current query status as enum for given query_id."""
        status = self.get_raw_conn().get_query_status_throw_if_error(query_id)
        self.log.info(f"Status of query_id {query_id} run is: {status}")
        return status

    def _set_context(self, context):
        pass
