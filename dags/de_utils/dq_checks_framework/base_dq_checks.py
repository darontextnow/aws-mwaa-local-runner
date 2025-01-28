"""Base class for all implementations of data quality checks.
Each DQ Check should be a class that inherits from this class.
"""
from de_utils.dq_checks_framework.enums import Status
from de_utils.constants import ENV_ENUM, ENV_VALUE, SF_CONN_ID
from de_utils.enums import Env
from de_utils.dq_checks_framework.constants import DQ_CHECKS_DEFS_REPO_URL, SF_QUERY_ID_URL, REPORTING_TABLES_SCHEMA
from abc import ABC, abstractmethod
from datetime import datetime
import logging
from uuid import uuid4


class BaseDQChecks(ABC):
    """DQChecks abstract class for defining all aspects of DQ Checks.
    @DynamicAttrs  # This tag is added here to supress PyCharm unresolved attribute reference error.
                   # All the input aliases are attributes dynamically added to the class and Pycharm will complain.

    Note: name, description, and table_name class attributes are required by this abstract class.

    Args:
        params (dict): Optional. Any runtime sql or code parameters to pass into inputs or check code.
            Any params specified here will be combined with params supplied via the run_params arg
            from Airflow DQChecksOperator which allows for templated params.
        env (Env): The environment the code is running in. Defaults to env found on MWAA instances.
            Only override if needed for testing purposes.
    """
    def __init__(
            self,
            params: dict = None,
            env: Env = ENV_ENUM
    ):
        self._check_id = uuid4().hex  # GUID to use as unique id for this checks run. Used in reporting tables.
        self._params = params
        self._env = env
        self._log = logging.getLogger("dq_checks")  # Airflow operator will replace this with airflow logger

    @abstractmethod
    def name(self) -> str:
        return self._name

    @abstractmethod
    def description(self) -> str:
        return self._description

    @abstractmethod
    def table_name(self) -> str:
        return self._table_name

    @property
    def check_id(self) -> str:
        return self._check_id

    @property
    def params(self) -> dict:
        return self._params

    @property
    def env(self) -> Env:
        return self._env

    @property
    def log(self) -> logging.Logger:
        return self._log

    @log.setter
    def log(self, log: logging.Logger):
        """Allows us to replace Logger defined in init with Airflow or other environments Logger"""
        self._log = log

    @abstractmethod
    def get_inputs(self) -> list["BaseInput"]:
        """Must return a list of all the implementations of the BaseInput class needed
        by the checks defined in the get_checks method"""

    @abstractmethod
    def get_checks(self) -> list["Check"]:
        """Define your data quality check classes here."""

    def process_inputs(self, params: dict = None, timeout: int = 3000):
        """In parallel, fetches the data for all inputs and adds each input as instance attributes.

        Args:
            params (dict): An optional dictionary of params to use in query or code runs associated with inputs.
            timeout (int): The number of seconds to wait before a thread which processes a single input times out.
        """
        from concurrent.futures._base import TimeoutError
        from concurrent.futures import ThreadPoolExecutor
        self._inputs = self.get_inputs()
        if not self._inputs:
            return

        def process_input(inpt, parameters: dict):
            """The worker function for multi-thread process."""
            input_obj = inpt.get_input(params=parameters)
            return input_obj

        # Loop through each input and in parallel retrieve results as input_object
        with ThreadPoolExecutor(max_workers=len(self._inputs)) as executor:
            futures = []
            for idx in range(len(self._inputs)):
                self.log.info(f"Processing input named '{self._inputs[idx].name}'.")
                # Submit input to thread executor to run code or SQL in parallel
                futures.append(executor.submit(process_input, self._inputs[idx], params,))

            # Now loop through each thread/future and get the resulting InputObject returned.
            for idx, future in enumerate(futures):
                try:
                    result = future.result(timeout=timeout)
                except TimeoutError:
                    msg = (f"TimeoutError raised processing input '{self._inputs[idx].name}'. The timeout is currently"
                           f" set to {timeout} seconds.")
                    raise TimeoutError(msg)
                # Set attribute on this class to allow us to reference input by alias, i.e. self.alias
                if result:  # AnomalyDBOInput won't have a result and does not need to be accessed by alias
                    setattr(self, self._inputs[idx].alias, result)

    def run_checks(self, checks):
        executed_ts = datetime.utcnow()
        error = None  # For holding overall run error to include in results output
        check_errors = []  # For holding error(s) related to each check run
        try:
            for check in checks:
                # Setting some check attrs here so users don't have to manually pass it into checks
                check.parent_checks_name = self.name  # Set attr that can be used in errors and alerts
                check.parent_executed_ts = executed_ts  # Set attr that can be used in alerts
                if check.table_name is None:  # Do not override if already set by check class
                    check.table_name = self.table_name
                check.check_code = f"<{DQ_CHECKS_DEFS_REPO_URL}{self.__module__.split('.')[-1]}.py|Link>"
                query_id = self._get_query_id(check, self._inputs)
                check.query_run = "UNKNOWN" if not query_id else f"<{SF_QUERY_ID_URL}{query_id}|Link>"
                check._env = self._env

                # Retrieve dynamic thresholds and set attributes to be used in resolve_params below
                if check.use_dynamic_thresholds is True:
                    check.get_dynamic_thresholds(self.params["run_date"])

                # Resolve params here after above attrs are set so they can be resolved also.
                check.resolve_params()
                self._replace_params(check, self.params)

                self.log.info(f"Running check: {check.name}")
                check.run_check()
                if check.status == Status.RED:
                    msg = f"{check.name} check failed: {check.red_error}"
                    check.error = msg
                    check_errors.append(msg)
        except Exception as e:
            error = str(e)  # Capture error for outputting with results
            raise
        finally:
            self.log.info(f"Outputting results of checks ('{self.name}') to reporting tables.")
            self._output_results(executed_ts, self._inputs, checks, error)
            if error is None and check_errors:
                self._raise_red_errors(executed_ts, check_errors)

    def _get_query_id(self, check, inputs: list):
        """Tries to match up specific input to check and return query_id if found."""
        if len(inputs) == 1:
            return getattr(inputs[0], "query_id", None)

        check = check if not isinstance(check, list) else check[0]
        if check.input_alias:
            for input in self._inputs:
                if input.alias == check.input_alias:
                    return getattr(input, "query_id", None)

    # noinspection PyMethodMayBeStatic
    def _replace_params(self, check, params: dict):
        """Replaces parameters in check code with given params values."""
        c = check
        for k, v in params.items():
            v = str(v)
            c.red_expr = c.red_expr if not c.red_expr else c.red_expr.replace(f":{k}", v)
            c.red_error = c.red_error if not c.red_error else c.red_error.replace(f":{k}", v)
            c.yellow_expr = c.yellow_expr if not c.yellow_expr else c.yellow_expr.replace(f":{k}", v)
            c.yellow_warning = c.yellow_warning if not c.yellow_warning else c.yellow_warning.replace(f":{k}", v)
            if hasattr(c, "where_clause_expr"):
                c.where_clause_expr = c.where_clause_expr.replace(f":{k}", v)

    # noinspection PyTypeChecker
    def _output_results(self, executed_ts: datetime, inputs, checks, error: str):
        from de_utils.dq_checks_framework.results.snowflake_results_writer import SnowflakeResultsWriter
        snowflake_results_writer = SnowflakeResultsWriter(env=self.env)
        snowflake_results_writer.write_dq_checks(
            check_id=self.check_id,
            executed_ts=executed_ts,
            name=self.name,
            description=self.description,
            table_name=self.table_name,
            params=self.params,
            error=error
        )

        snowflake_results_writer.write_inputs(check_id=self.check_id, inputs=inputs)

        details_checks = [check for check in checks if check.write_details is True]
        anomalies_checks = [check for check in checks if check.write_anomaly_records is True]
        snowflake_results_writer.write_details(checks=details_checks, check_id=self.check_id)
        snowflake_results_writer.write_anomaly_records(checks=anomalies_checks, check_id=self.check_id)

    def _raise_red_errors(self, executed_ts: datetime, check_errors: list):
        """Raises all the red_errors at once so failure message will include all failures."""
        from de_utils.dq_checks_framework.errors import DQCheckErrors
        pfx = (f"DQ issues were raised during run of DQ Checks named: {self.name},"
               f" executed at: {executed_ts}.\nDetails:\n")
        raise DQCheckErrors(pfx + "\n".join(check_errors))
