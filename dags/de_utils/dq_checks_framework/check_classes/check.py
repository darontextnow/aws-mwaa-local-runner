from de_utils.slack.alerts import alert_dq_check_warning, alert_dq_check_failure
from de_utils.dq_checks_framework.enums import Status, AlertStatus, CheckType
from de_utils.dq_checks_framework.constants import (
    DQ_CHECK_CHANGES_LOG_URL,
    REPORTING_TABLES_SCHEMA,
    DQ_CHECK_FALSE_ALERT_UPDATER_URL
)
from de_utils.constants import SF_CONN_ID
from datetime import datetime, date
from collections.abc import Callable
# Next import is needed for some eval expressions, i.e. when comparing floats that may not be exactly the same.
import math  # Though this is not directly used in this mod, it is needed for some eval() expressions


class Check:
    """
    Check class to be used for most regular DQ checks.
    If a specific Check class does not exist for your check, then use this one.

    Args:
        name (str): The short name for the check. Used in reporting tables.
        description (str): The description (max 300 char) for the check. Used in reporting tables.
        column_name (str): The name of the column being checked. Used in reporting tables.
            If, check isn't for a specific column, use short description of columns or empty string.
        value (Any): The value returned for source used as the comparison value.
        todo (List[str]): A list of steps for DE on-call person to do/action items in event of alerts/failures.
        use_dynamic_thresholds (bool): Set to True if dynamic thresholds exist for this Check in the
            core.dynamic_threshold_forecasts table. The thresholds will be automatically retrieved from
            this table and applied to the red_expr and/or yellow_expr. You can access the threshold values
            via the parameters : lower_threshold and :upper_threshold. Example:
            red_expr = ":lower_threshold < value < :upper_threshold"
        get_previous_run_value (bool): Set to True in order to be able to use the :previous_value parameter
            in red and/or yellow expr. When True, the previous run's stored value will be retrieved for use
            as a comparison with this run.
            Note: previous_value will always be a str. You must convert it to other Python type as needed.
        red_expr (str): A string representation of the expression to evaluate which returns True or False.
            An expression returning False will raise a red failure and/or send a red alert.
            Example: "value > 0" or "0 < value < 1".
            Leave this blank if you don't want a red failure raised or alert sent.
        red_error (str): The error message to be used when raising a red failure and/or sending a red alert.
            Leave this blank if you don't want to raise a red failure.
        red_alert_func (Callable | Tuple[Callable]): The name of the alert function or Tuple of alert functions
            to use for sending a red alert.
            Default is set to alert_dq_check_failure. Set this to None if you do not want any alert to be sent.
        yellow_expr (str): A string representation of the expression to evaluate which returns True or False.
            An expression returning False will send a yellow alert.
            Example: "value > 0" or "0 < value < 1".
            Leave this blank if you don't want a yellow warning alert sent.
        yellow_warning (str): The message to be used when sending a yellow warning alert.
            Leave this blank if you don't want a yellow warning alert sent.
        yellow_alert_func (Callable | Tuple[Callable]): The name of the alert function or Tuple of alert functions
            to use for sending a yellow alert.
    """
    def __init__(
            self,
            name: str,
            description: str,
            column_name: str,
            value: any,
            todo: list[str],
            use_dynamic_thresholds: bool = False,
            get_previous_run_value: bool = False,
            red_expr: str = None,
            red_error: str = None,
            red_alert_func: Callable | tuple[Callable, ...] = alert_dq_check_failure,
            yellow_expr: str = None,
            yellow_warning: str = None,
            yellow_alert_func: Callable | tuple[Callable, ...] = alert_dq_check_warning
    ):
        self.parent_checks_name = None  # Will be set by BaseDQCheck class
        self.parent_executed_ts = None  # Will be set by BaseDQCheck class
        self.table_name = None  # Will be set by BaseDQCheck class
        self.check_code = None  # Will be set by BaseDQCheck class
        self.query_run = None  # Will be set by BaseDQCheck class
        self._env = None  # Will be set by BaseDQCheck class
        self.name = name
        self.description = description
        self.column_name = column_name
        self.value = value
        self.todo = todo
        self.use_dynamic_thresholds = use_dynamic_thresholds
        self.lower_threshold = None  # will be initialized later if use_dynamic_thresholds is True
        self.upper_threshold = None  # will be initialized later if use_dynamic_thresholds is True
        self.dq_check_changes_log_link = f"<{DQ_CHECK_CHANGES_LOG_URL}|DQ Check Changes Log>"
        self.get_previous_run_value = get_previous_run_value
        self.previous_run_value = None  # initialize so it can be set later
        self.red_expr = red_expr
        self.red_error = red_error
        self.red_alert_func = red_alert_func
        self.yellow_expr = yellow_expr
        self.yellow_warning = yellow_warning
        self.yellow_alert_func = yellow_alert_func
        self.status = Status.UNKNOWN
        self.alert_status = AlertStatus.UNKNOWN
        self.error = None  # Used for recording specific check red or yellow error in results
        self.write_details = True  # determines if this check should be output to reporting details table
        self.write_anomaly_records = False  # determines if this check should be output to reporting anomalies table
        self._validate_attrbs()
        self.input_alias = None
        self._set_input_alias()

    # Leaving this available to override if needed
    def get_dynamic_thresholds(self, run_timestamp: datetime | date):
        """Retrieves dynamic thresholds for check and attributes for use in resolve_params method.
        run_timestamp can be a date or datetime and should be sourced from run_date on the DAG.
        """
        import pytz
        from de_utils.dq_checks_framework.utils.dbo_utils import execute
        if isinstance(run_timestamp, datetime):  # note datetime is actually more specific than date is
            start = datetime.combine(run_timestamp.date(), datetime.min.time(), tzinfo=pytz.UTC)
            end = datetime.combine(run_timestamp.date(), datetime.max.time(), tzinfo=pytz.UTC)
            sql_filter = f"(run_timestamp BETWEEN '{start}' AND '{end}')"
        else:  # it must be a date
            sql_filter = f"(run_timestamp = '{run_timestamp}')"
        sql = f"""
            SELECT lower_threshold, upper_threshold 
            FROM {self._env.value}.core.dynamic_threshold_forecasts
            WHERE 
              {sql_filter}
              AND (check_name = '{self.name}')
              AND (table_name = '{self.table_name}')
              AND (column_name = '{self.column_name}')
        """
        row = execute(sql=sql, airflow_conn_id=SF_CONN_ID).fetchone()
        if row:
            self.lower_threshold = row[0]
            self.upper_threshold = row[1]
        else:
            msg = (f"No dynamic thresholds were found for dq check: {self.name}, table: {self.table_name},"
                   f" column: {self.column_name} for run_timestamp near: {run_timestamp}.")
            raise RuntimeError(msg)

    # Override this as needed in implementations that provide more params
    # noinspection PyMethodMayBeStatic
    def get_params_to_resolve(self) -> list[str]:
        """Returns list of parameter names to resolve.
        A param name can be any class instance attribute name with a colon(:) at the beginning.
        The resolve_params() method will do a .replace of param names with param values.
        Note: any params not found in class instance will be silently ignored by resolve_params() method.
        """
        return [":value", ":threshold", ":lower_threshold", ":upper_threshold",
                ":total_count", ":dups_count",
                ":column_name", ":table_name", ":filter_date_column_name", ":name",
                ":dq_check_changes_log_link",
                ":red_expr", ":yellow_expr", ":where_clause_expr"]

    # Override this as needed in implementations that provide more args
    # noinspection PyMethodMayBeStatic
    def get_attrs_to_resolve(self) -> list[str]:
        """Returns list of instance attribute names to be included in param resolution.
        Note: any attributes not found in class instance will be silently ignored by resolve_params() method.
        """
        return ["name", "description", "red_expr", "red_error", "yellow_expr", "yellow_warning", "todo"]

    def resolve_params(self):
        """Replaces param names from get_params_to_resolve() list with actual param values in each string attribute
        included in get_attrs_to_resolve() list.
        Running this from base_dq_checks class so that table_name will be resolved before this runs.
        """
        def replace_params(value: str):
            for param in self.get_params_to_resolve():
                if param in value:
                    param_val = getattr(self, param[1:], None)
                    value = value.replace(param, str(param_val))
            return value

        for attr in self.get_attrs_to_resolve():
            val = getattr(self, attr, None)
            if isinstance(val, (list, tuple)):  # the todo attr is a list of strings
                val = [replace_params(v) for v in val]
            elif val is not None:  # must use is not None here as if val passes up val == 0
                val = replace_params(val)
            setattr(self, attr, val)

    # Only override this if you really need to.
    def run_check(self):
        """Runs the check and sends alerts if alerts are triggered."""
        if self.get_previous_run_value is True:
            self.previous_run_value = self._get_previous_run_value()

        try:
            if self.red_expr:
                self._evaluate_check_expression(ctype=CheckType.RED)
            if self.yellow_expr and self.status != Status.RED:  # if red_expr didn't already fail
                self._evaluate_check_expression(ctype=CheckType.YELLOW)
        finally:
            self.status = Status.GREEN if self.status == Status.UNKNOWN else self.status
            self.alert_status = AlertStatus.NO_ALERT_SENT if self.alert_status == self.alert_status.UNKNOWN \
                else self.alert_status

    # Override if you need to use a custom alert with custom kwargs
    def get_alert_kwargs(self, ctype: CheckType) -> dict:
        """Returns the kwargs required as args to the alert_func."""
        kwargs = {"name": self.parent_checks_name,
                  "detail_name": self.name,
                  "table_name": self.table_name,
                  "column_name": self.column_name,
                  "message": self.red_error if ctype == CheckType.RED else self.yellow_warning,
                  "check_code": self.check_code,
                  "query_run": self.query_run,
                  "todo": self.todo,
                  "env": self._env,
                  "executed_ts": datetime.now()}
        return kwargs

    # Override if needed to send a custom alert.
    def send_alert(self, ctype: CheckType):
        """Sends alert(s)."""
        self._update_todo_list()
        alert_funcs = self.red_alert_func if ctype == CheckType.RED else self.yellow_alert_func
        if not alert_funcs:
            return
        alert_funcs = alert_funcs if isinstance(alert_funcs, tuple) else (alert_funcs,)
        for alert_func in alert_funcs:
            try:
                alert_func(**self.get_alert_kwargs(ctype=ctype))
                print(f"{ctype.value} Alert sent for DQ Check: {self.name}")
                print(f"Alert Details: {self.red_error if ctype == CheckType.RED else self.yellow_warning}")
                self.alert_status = AlertStatus.RED_ALERT_SENT \
                    if ctype == CheckType.RED else AlertStatus.YELLOW_WARNING_SENT
            except Exception:
                self.alert_status = AlertStatus.ALERT_FAILED_TO_SEND
                raise

    def _update_todo_list(self):
        """Add standard messaging to todo list."""
        from urllib.parse import quote_plus
        if self.use_dynamic_thresholds:
            self.todo.insert(0, "Note: thresholds were set by dynamic threshold model.")
            params = (f"tableName={quote_plus(self.table_name)}"
                      f"&executedAt={quote_plus(self.parent_executed_ts.strftime('%Y-%m-%d %H:%M:%S.%fZ'))}"
                      f"&columnName={quote_plus(self.column_name)}"
                      f"&checkName={quote_plus(self.name)}")
            # Note need to url encode the params else the link won't work in Slack.
            url = f"{DQ_CHECK_FALSE_ALERT_UPDATER_URL}?{params}"
            msg = f"If this is not a valid alert, <{url}|CLICK_HERE> to set is_valid_alert = False."
            self.todo.insert(1, msg)
        else:
            msg = f"Log any changes to DQ Checks in <{DQ_CHECK_CHANGES_LOG_URL}|DQ Check Changes Log>."
            self.todo.append(msg)

    def _validate_attrbs(self):
        if self.yellow_expr is None and self.red_expr is None:
            raise ValueError("Checks must have either or both a red_expr and yellow_expr defined.")
        if self.red_expr and not self.red_error:
            raise ValueError("red_error arg must be included when a red_expr is given.")
        if self.yellow_expr and not self.yellow_warning:
            raise ValueError("yellow_warning arg must be included when yellow_expr is given.")

    def _set_input_alias(self):
        """Sets the instance attribute input_alias by trying to parse out the alias name used in Check Class args
        from the module code. Having to do it this way as there is no other direct link between
        InputObject class and Check class.
        """
        from inspect import currentframe, getouterframes, getsourcelines, getmodule
        frame = currentframe()
        outer_frames = getouterframes(frame)
        caller_outer_frame = outer_frames[2]
        if "de_utils/dq_checks_framework/check_classes/" in caller_outer_frame.filename:
            caller_outer_frame = outer_frames[3]  # must back up one more frame to get beyond check class
        start_line = caller_outer_frame.positions.lineno - 1
        end_line = caller_outer_frame.positions.end_lineno
        lines = getsourcelines(getmodule(caller_outer_frame.frame))
        lines = lines[0][start_line:end_line]
        input_alias = None
        for idx, line in enumerate(lines):
            input_alias = self._get_alias(line)
            if input_alias:
                break
        self.input_alias = input_alias

    def _get_alias(self, line: str):
        """Returns alias based on known substring regexp patterns found from given line."""
        import re
        # Check case is typically value=self.alias[...]
        # MatchExpectedValuesCheck case is values=self.alias[...]
        # NoMissingValuesCheck and ColumnStatisticsCheck cases are input_object=self.alias,
        # NoDuplicateRowsCheck case is dups_count=self.alias[...]
        # TotalCountCheck and MissingValuesRateCheck cases are total_count=self.alias[...]
        # DistributionShiftPSI case is psi=self.alias[...]
        pattern = r".*?(value|values|input_object|dups_count|total_count|psi)=self.(.*?)(\[|,|\))"
        input_alias = re.findall(pattern, line)
        if input_alias:
            return input_alias[0][1]

    def _get_previous_run_value(self, lookback_days: int = 3) -> str:
        """Returns value from previous run of same check and inserts it into red and yellow expressions and errors.
        Look back period limited to 3 days (by default) for speed. Adjust if needed for checks that run seldom.
        The first run for any new DQCheck will not work perfectly since there is no value yet in table. Will run
            correctly with second run as value will get added by first run.
        """
        from de_utils.dq_checks_framework.utils.dbo_utils import execute
        sql = f"""
            SELECT value 
            FROM {self._env.value}.{REPORTING_TABLES_SCHEMA}.dq_checks_details d
            JOIN {self._env.value}.{REPORTING_TABLES_SCHEMA}.dq_checks c ON (d.check_id = c.check_id)
            WHERE (d.inserted_at > DATEADD(DAY, -{lookback_days}, CAST(CURRENT_TIMESTAMP AS TIMESTAMP_NTZ))) 
                AND (c.name = '{self.parent_checks_name}') AND (d.name = '{self.name}')
            ORDER BY executed_at DESC
            LIMIT 1
        """
        row = execute(sql, airflow_conn_id=SF_CONN_ID).fetchone()
        prev_val = None if row is None else str(row[0])
        self.red_expr = None if not self.red_expr else self.red_expr.replace(":previous_value", str(prev_val))
        self.red_error = None if not self.red_error else self.red_error.replace(":previous_value", str(prev_val))
        self.yellow_expr = None if not self.yellow_expr else self.yellow_expr.replace(":previous_value", str(prev_val))
        self.yellow_warning = None if not self.yellow_warning else self.yellow_warning\
            .replace(":previous_value", str(prev_val))
        return prev_val

    def _evaluate_check_expression(self, ctype: CheckType):
        """Helper function to evaluate a check red or yellow expression and send alerts if triggered."""
        expr = self.red_expr if ctype == CheckType.RED else self.yellow_expr
        try:
            if eval(expr) is False:
                self.status = Status.RED if ctype == CheckType.RED else Status.YELLOW
                self.error = self.red_error if ctype == CheckType.RED else self.yellow_warning
                self.send_alert(ctype=ctype)
                # Note failures will not be raised until after all checks have run.
        except TypeError as e:
            if self.previous_run_value == 'None':
                # Means check failed because there is no previous value yet in the reporting tables.
                self.status = Status.RUN_AGAIN
                msg = ("Failed to evaluate expression since this is the first time running this check and no"
                       " previous value existed for this check. There should now be a previous value. Thus,"
                       " simply run this check again.")
                self.yellow_warning = msg
                self.error = "Failed due to missing previous value for check. Simply run the check again."
                self.send_alert(ctype=CheckType.YELLOW)
            else:
                # Seeing TypeError from expressions comparing None to a number. This happens when no rows are returned.
                # Can't account for this in every DQ check. Best to not raise error, but process as a failed check.
                import traceback
                self.status = Status.RED  # Use RED here as this is basically a total failure and needs to fail.
                msg = (f"The check named '{self.name}' failed with TypeError trying to evaluate the"
                       f" expression '{expr}'.\nError: {e}\n{traceback.format_exc()}")
                self.error = msg
                self.send_alert(ctype=CheckType.RED)  # Send RED alert as this needs to be total failure.
        except Exception as e:
            import traceback
            self.status = Status.ERROR
            msg = (f"The check named '{self.name}' failed while trying to evaluate the expression '{expr}'."
                   f"\nError: {e}\n{traceback.format_exc()}")
            raise RuntimeError(msg)
