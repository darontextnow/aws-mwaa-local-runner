from airflow.models import BaseOperator
from dag_dependencies.dag_utils import get_default_context
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import ENV_IS_LOCAL
from datetime import date, datetime

RUN_PARAMS_DEFAULT = "{{ (dag_run.conf or {}).get('run_params') }}"  # Jinja template string
INCLUDE_CHECKS_DEFAULT = "{{ (dag_run.conf or {}).get('include_checks') }}"  # Jinja template string
EXCLUDE_CHECKS_DEFAULT = "{{ (dag_run.conf or {}).get('exclude_checks') }}"  # Jinja template string


class DQChecksOperator(BaseOperator):
    """Executes a Data Quality Checks Object.

    Args:
        dq_checks_instance (DQChecks instance): An instantiated implementation of BaseDQChecks class.
        run_date (str | date | datetime): The date/datetime to pass to DQ Checks to be the run_date. Best to use
            existing constants in de_utils.constants for this to keep all our DQ Checks consistent.
        include_checks (List[str]): Optional / Templated: Allows for optionally supplying a list of checks
            (format: "<check_name>-<column_name>") to be executed instead of the default of executing all checks
            within given dq_checks_instance.
            The checks can be specified by the combination of check_name and column_name separated by a dash.
            For example:  "Total Counts Check-count_ios_installs"
            Cannot be used in combination with exclude_checks arg.
        exclude_checks (list[str]): Optional / Templated: Allows for optionally supplying a list of checks
            (format: "<check_name>-<column_name>") to be excluded from executing instead of the default of
            executing all checks within given dq_checks_instance.
            Cannot be used in combination with include_checks arg.
        run_params (dict): Optional. Typically includes "run_date" if DQ Checks include a parameter called run_date.
            Any params specified here will be combined with params supplied directly to the DQChecks class.
        run_now_func (Callable) Optional: A reference to a callable function that receives Airflow context,
            current run_date, and run_params as args and should return True or False whether the DQChecksOperator
            execute method is called or not for the current execution and run_date. Example of a run_now_func def:
                def run_now(context, run_date, run_params):
                    if str(run_date).split("/")[-1] == 1:
                        return True  # meaning only run when run_date is the first day of any month
                    return False
    """

    template_fields = ("run_date", "include_checks", "exclude_checks", "run_params")

    def __init__(
            self,
            dq_checks_instance,
            run_date: str | date | datetime | None = None,  # None can come from template field
            include_checks: list[str] = INCLUDE_CHECKS_DEFAULT,
            exclude_checks: list[str] = EXCLUDE_CHECKS_DEFAULT,
            run_params: dict = RUN_PARAMS_DEFAULT,
            run_now_func: callable = None,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)

        if kwargs.get("retries", 0) > 0:
            print("It is intended that DQ Checks will fail occasionally."
                  " Thus, retries arg is being overriden and will be set to 0.")
        self.retries = 0
        self.run_date = str(run_date)
        self.include_checks = include_checks
        self.exclude_checks = exclude_checks
        self.run_params = run_params
        self.run_now_func = run_now_func
        self.on_failure_callback = alert_on_failure
        self.dq_checks_instance = dq_checks_instance
        dq_checks_instance.log = self.log  # replace Logger in dq_checks with airflow's logger

    def execute(self, context):
        self.log.info(f"Running task '{self.task_id}' for run_date = {self.run_date}")
        # next lines cover variations for how DAG is run including manual local runs which don't resolve templates.
        self.run_params = {} if self.run_params is None or self.run_params in ["None", RUN_PARAMS_DEFAULT] \
            else self.run_params
        self.include_checks = None if self.include_checks in ["None", INCLUDE_CHECKS_DEFAULT] else self.include_checks
        self.exclude_checks = None if self.exclude_checks in ["None", EXCLUDE_CHECKS_DEFAULT] \
            else self.exclude_checks
        if self.run_date and self.run_date != "None":  # "None" comes from templated fields
            # run_params will be passed down to the Checks class implementation for use in checks.
            if isinstance(self.run_date, datetime):
                self.run_params["run_date"] = self.run_date.strftime("%Y-%m-%dT%H:%M:%S")
            elif isinstance(self.run_date, date):
                self.run_params["run_date"] = self.run_date.strftime("%Y-%m-%d")
            else:
                self.run_params["run_date"] = str(self.run_date)
        self._update_params()

        if self.run_now_func and self.run_now_func(context, self.run_date, self.run_params) is False:
            self.log.info(f"Not running task '{self.task_id}' as run_now_func evaluated to False")
            return

        if self.include_checks:
            self.log.info(f"The following include_checks were supplied for this run: {self.include_checks}")
        if self.exclude_checks:
            self.log.info(f"The following exclude_checks were supplied for this run: {self.exclude_checks}")
        self.log.info("Processing DQ Checks inputs...")
        self.dq_checks_instance.process_inputs(params=self.dq_checks_instance.params)
        self.log.info("Retrieving DQ Checks to be ran for this run...")
        all_checks = self._get_all_checks()

        self._validate_checks(checks=[f"{check.name}-{check.column_name}" for check in all_checks])
        checks = self._get_checks(checks=all_checks)
        self.log.info("Running DQ Checks...")
        self.dq_checks_instance.run_checks(checks)

    def local_test_run(self, run_date: str | date):
        """
        Runs specific DQ checks task when in local env.
        Method does nothing if not in local env.

        Args:
            run_date (str | date | datetime): The run_date for this test run.
        """
        if ENV_IS_LOCAL is False:
            return self
        self.run_date = str(run_date)
        self.execute(context=get_default_context())

    def _update_params(self):
        """
        Combine any params supplied to dq_checks_instance with run_params supplied to DQChecksOperator.
        This allows for all params to be output to reporting tables and for Jinja Templating as an option.
        """
        params = dict(self.dq_checks_instance.params or {})
        params.update(self.run_params or {})
        self.dq_checks_instance._params = None if not params else params

    def _get_all_checks(self) -> list:
        """Retrieves all checks from DQChecks instance .get_checks method and flattens the list of checks."""
        all_checks = []
        for element in self.dq_checks_instance.get_checks():
            if isinstance(element, list):
                all_checks.extend(element)
            else:
                all_checks.append(element)
        return all_checks

    def _validate_checks(self, checks: list[str]):
        """
        Raises RuntimeError if any names included in include_checks or exclude_checks lists
        don't exist in the list of check names retrieved from dq_checks_instance.get_checks() method.
        """
        if self.include_checks and self.exclude_checks:
            raise ValueError("Use of both include_checks and exclude_checks args at the same time is not supported.")

        bad_names = None
        if self.include_checks:
            bad_names = list(set(self.include_checks).difference(checks))
        elif self.exclude_checks:
            bad_names = list(set(self.exclude_checks).difference(checks))
        if bad_names:
            msg = (f"The following checks supplied to DQChecksOperator for the current run of"
                   f" the '{self.dq_checks_instance.name}' class do not exist as Checks"
                   f" defined in the get_checks() method: {', '.join(bad_names)}")
            raise ValueError(msg)

    def _get_checks(self, checks: list) -> list:
        """Returns the list of checks to be included in this run based on resolving the include_checks list (if any)
        and the exclude_checks list (if any) with the list of Check objects returned from
        dq_checks_instance.get_checks() method.
        """
        new_checks = []
        for check in checks:
            if self.include_checks and f"{check.name}-{check.column_name}" not in self.include_checks:
                self.log.info(f"Check ('{check.name}-{check.column_name}') was not included in include_checks list"
                              f" and thus will not be ran.")
            elif self.exclude_checks and f"{check.name}-{check.column_name}" in self.exclude_checks:
                self.log.info(f"Check ('{check.name}-{check.column_name}') was included in exclude_checks list"
                              f" and thus will not be ran.")
            else:
                new_checks.append(check)
        return new_checks
