"""Implements a deferrable version of SqlSensor.

Currently tested to work with Snowflake and MySQL. May work with others also, but not tested.

Using this until airflow has its own version.
"""

from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.triggers.base import BaseTrigger, TriggerEvent
import asyncio
from datetime import timedelta
from typing import AsyncIterator
from typing import Any


class SqlDeferrableSensor(SqlSensor):
    """
    Runs a sql statement repeatedly (in deferred mode if first time doesn't return True)
    until a criteria is met. It will keep trying until
    success or failure criteria are met, or if the first cell returned from the query
    is not in (0, '0', '', None).
    Optional success and failure callables are called with the first cell returned
    from the query as the argument.
    If success callable is defined the sensor will keep retrying until the criteria is met.
    If failure callable is defined and the criteria is met the sensor will raise AirflowException.
    Failure criteria is evaluated before success criteria. A fail_on_empty boolean can also
    be passed to the sensor in which case it will fail if no rows have been returned.

    Args:
        poll_interval (float): Polling period in seconds to run sql and check for status.
        See SqlSensor class for additional available args
    """

    def __init__(
        self,
        *,
        poll_interval: float = 120,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.poll_interval = poll_interval

    # override to make it deferrable
    def execute(self, context) -> None:
        """Check for query result in DB by deferring using the trigger"""
        if not self.poke(context):
            self.defer(
                timeout=timedelta(seconds=self.timeout),
                trigger=SqlTrigger(
                    sql=self.sql,
                    poll_interval=self.poll_interval,
                    parameters=self.parameters,
                    success=self.success,
                    failure=self.failure,
                    fail_on_empty=self.fail_on_empty,
                    dag_id=context["dag"].dag_id,
                    task_id=context["task"].task_id,
                    run_id=context["dag_run"].run_id,
                    conn_id=self.conn_id,
                ),
                method_name=self.execute_complete.__name__,
            )

    def execute_complete(self, context, event: dict[str, str] | None = None) -> Any:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if "status" in event and event["status"] == "error":
                if self.soft_fail:
                    raise AirflowSkipException(event["message"])
                raise AirflowException(event["message"])
            self.log.info(event["message"])
        else:
            self.log.info("%s completed successfully.", self.task_id)


class SqlTrigger(BaseTrigger):
    """A trigger that fires after running a sql statement until records are returned"""

    def __init__(
        self,
        sql: str,
        dag_id: str,
        task_id: str,
        run_id: str,
        conn_id: str,
        parameters: str | None = None,
        success: str | None = None,
        failure: str | None = None,
        fail_on_empty: bool = False,
        poll_interval: float = 120
    ):
        super().__init__()
        self._sql = sql
        self._parameters = parameters
        self._success = success
        self._failure = failure
        self._fail_on_empty = fail_on_empty
        self._dag_id = dag_id
        self._task_id = task_id
        self._run_id = run_id
        self._conn_id = conn_id
        self._poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes SqlTrigger arguments and classpath."""
        return (
            "dag_dependencies.sensors.sql_deferrable_sensor.SqlTrigger",
            {
                "sql": self._sql,
                "parameters": self._parameters,
                "success": self._success,
                "failure": self._failure,
                "fail_on_empty": self._fail_on_empty,
                "dag_id": self._dag_id,
                "task_id": self._task_id,
                "run_id": self._run_id,
                "conn_id": self._conn_id,
                "poll_interval": self._poll_interval
            },
        )

    async def get_results(self):
        conn = BaseHook.get_connection(self._conn_id)
        hook = conn.get_hook()
        if not isinstance(hook, DbApiHook):
            raise AirflowException(
                f"The connection type is not supported by {self.__class__.__name__}. "
                f"The associated hook should be a subclass of `DbApiHook`. Got {hook.__class__.__name__}"
            )
        results = hook.get_records(self._sql, self._parameters)
        self.log.info(f"Raw query result = {results}")
        return results

    async def validate_result(self, result: list[tuple[Any]]) -> Any:
        """Validates query result and verifies if it returns a row"""
        if not result:
            if self._fail_on_empty:
                raise AirflowException("No rows returned, raising as per fail_on_empty flag")
            else:
                return False

        first_cell = result[0][0]
        if self._failure is not None:
            if callable(self._failure):
                if self._failure(first_cell):
                    raise AirflowException(f"Failure criteria met. self.failure({first_cell}) returned True")
            else:
                raise AirflowException(f"self.failure is present, but not callable -> {self._failure}")
        if self._success is not None:
            if callable(self._success):
                return self._success(first_cell)
            else:
                raise AirflowException(f"self.success is present, but not callable -> {self._success}")
        return bool(first_cell)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make an asynchronous connection to DB and defer until query returns a result"""
        try:
            while True:
                results = await self.get_results()
                if await self.validate_result(results):
                    yield TriggerEvent({"status": "success", "message": "Found expected markers."})
                    return
                else:
                    self.log.info(f"No success yet. Checking again in {self._poll_interval} seconds.")
                    await asyncio.sleep(self._poll_interval)
        except Exception as e:
            self.log.info(f"Error encountered. Error: {e}")
            yield TriggerEvent({"status": "error", "message": str(e)})
            return
