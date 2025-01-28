"""Module contains DE version of the DAG class with custom changes"""
from airflow.models.dag import (
    DAG as _DAG,
    DagStateChangeCallback,
    Iterable,
    NOTSET,
    ArgNotSet,
    Union,
    ScheduleArg,
    Timetable,
    SLAMissCallback
)
from airflow.decorators import task as _task
from airflow.configuration import conf
from airflow.utils.session import NEW_SESSION, provide_session
from dateutil.relativedelta import relativedelta
from de_utils.constants import ENV_IS_LOCAL
import jinja2
from datetime import datetime, timedelta
from collections.abc import Callable
import pathlib
import functools
from collections.abc import MutableMapping

current_dir = pathlib.Path(__file__).parent.resolve()
local_sql_scripts_path = f"{current_dir}/../dag_dependencies/sql-scripts"
MWAA_AIRFLOW_DAGS_DIR = "/usr/local/airflow/dags/"
default_template_searchpath = local_sql_scripts_path if ENV_IS_LOCAL \
    else f"{MWAA_AIRFLOW_DAGS_DIR}dag_dependencies/sql-scripts"


class DAG(_DAG):
    """
    DE custom version of DAG class.

    Changed default for render_template_as_native_obj arg from False to True so we can use sql file paths.

    Additional methods include:
        test() - method can be called on DAG instance to run the dag in local testing environment.
        test_dq_checks() - call this method on DAG instance to run the DQ Checks tasks included in the DAG
            in local testing environment.

    """

    # overrides method
    @provide_session
    def test(
            self,
            execution_date: str | datetime | None = None,
            run_conf: dict[str, any] | None = None,
            conn_file_path: str | None = None,
            variable_file_path: str | None = None,
            session=NEW_SESSION,
            tasks: dict = None  # added by Daron to enable test_dq_checks() method to work.
    ) -> None:
        """
        Execute one single DagRun for a given DAG and execution date.
        Overriding this built-in test method to:
            - add convert str execution_date to required datetime.
            - enable custom method test_dq_checks() which will only run DQ Check tasks.
            - skip all ExternalTaskDeferredSensor tasks, so we don't get stuck waiting on
              External DagRuns that never run.

        Args:
            execution_date (str or datetime): execution date for the DAG run.
            run_conf (dict): configuration to pass to newly created dagrun
            conn_file_path (str): file path to a connection file in either yaml or json
            variable_file_path (str): file path to a variable file in either yaml or json
            session (Session): database connection (optional)
            tasks (dict): for use by custom method test_dq_checks(). Leave this as default otherwise.
        """
        from airflow.configuration import secrets_backend_list
        from airflow.utils import timezone

        def add_logger_if_needed(ti):
            """
            Add a formatted logger to the taskinstance so all logs are surfaced to the command line instead
            of into a task file. Since this is a local test run, it is much better for the user to see logs
            in the command line, rather than needing to search for a log file.
            Args:
                ti: The taskinstance that will receive a logger

            """
            import logging
            import sys
            format = logging.Formatter("[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s")
            handler = logging.StreamHandler(sys.stdout)
            handler.level = logging.INFO
            handler.setFormatter(format)
            # only add log handler once
            if not any(isinstance(h, logging.StreamHandler) for h in ti.log.handlers):
                self.log.debug("Adding Streamhandler to taskinstance %s", ti.task_id)
                ti.log.addHandler(handler)

        if conn_file_path or variable_file_path:
            from airflow.secrets.local_filesystem import LocalFilesystemBackend
            local_secrets = LocalFilesystemBackend(
                variables_file_path=variable_file_path, connections_file_path=conn_file_path
            )
            secrets_backend_list.insert(0, local_secrets)

        execution_date = execution_date or timezone.utcnow()

        # next part added by Daron
        # convert str execution date to datetime as required by super.test() method.
        if isinstance(execution_date, str):
            date_parts = [int(part) for part in execution_date[:10].split("-")]
            if ":" in execution_date:
                time_parts = [int(part) for part in execution_date[12:].split(":")]
                execution_date = datetime(date_parts[0], date_parts[1], date_parts[2],
                                          time_parts[0], time_parts[1], time_parts[2], tzinfo=timezone.utc)
            else:
                execution_date = datetime(date_parts[0], date_parts[1], date_parts[2], tzinfo=timezone.utc)

        self.log.debug("Clearing existing task instances for execution date %s", execution_date)
        self.clear(
            start_date=execution_date,
            end_date=execution_date,
            dag_run_state=False,  # type: ignore
            session=session,
        )
        self.log.debug("Getting dagrun for dag %s", self.dag_id)
        from airflow.models.dagrun import DagRun
        from airflow.models.dag import _run_task, _get_or_create_dagrun
        from airflow.utils.types import DagRunType
        from airflow.utils.state import State

        dr: DagRun = _get_or_create_dagrun(
            dag=self,
            start_date=execution_date,
            execution_date=execution_date,
            run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),
            session=session,
            conf=run_conf,
        )

        if tasks:  # Added by Daron for dq_check specific testing
            for ti in dr.get_task_instances():
                if ti.task_id in tasks:
                    add_logger_if_needed(ti)
                    ti.task = tasks[ti.task_id]
                    _run_task(ti=ti, session=session)
        else:  # From built-in test method
            def defer(  # Daron added this for bypassing deferred mode when testing locally
                    *,
                    trigger=None,
                    method_name=None,
                    kwargs=None,
                    timeout=None,
            ):
                pass

            tasks = self.task_dict
            self.log.debug("starting dagrun")
            # Instead of starting a scheduler, we run the minimal loop possible to check
            # for task readiness and dependency management. This is notably faster
            # than creating a BackfillJob and allows us to surface logs to the user
            while dr.state == State.RUNNING:
                schedulable_tis, _ = dr.update_state(session=session)
                try:
                    for ti in schedulable_tis:
                        add_logger_if_needed(ti)
                        ti.task = tasks[ti.task_id]
                        ti.task.defer = defer  # Daron added this for bypassing deferred mode when testing locally
                        if ti.operator == "ExternalTaskDeferredSensor":
                            # Added by Daron so these tasks will not run,
                            # Simply sets ExternalTaskDeferredSensor task to Success so next task can run.
                            # Now we can test other task runs locally and not get stuck in
                            #     ExternalTaskDeferredSensor tasks.
                            ti._run_raw_task(mark_success=True, session=session)
                        else:
                            _run_task(ti=ti, session=session)
                except Exception:
                    self.log.info(
                        "Task failed. DAG will continue to run until finished and be marked as failed.",
                        exc_info=True,
                    )
        if conn_file_path or variable_file_path:
            # Remove the local variables we have added to the secrets_backend_list
            secrets_backend_list.pop(0)

    @provide_session
    def test_dq_checks(
            self,
            execution_date: str | datetime | None = None,
            run_conf: dict[str, any] | None = None,
            conn_file_path: str | None = None,
            variable_file_path: str | None = None,
            session=NEW_SESSION
    ) -> None:
        """
        Added to enable local testing of only DQCheckOperator Tasks included on a DAG.
        Execute one single DagRun of DQ Check tasks only for a given DAG and execution date.

        Args:
            execution_date (str or datetime): execution date for the DAG run.
            run_conf (dict): configuration to pass to newly created dagrun
            conn_file_path (str): file path to a connection file in either yaml or json
            variable_file_path (str): file path to a variable file in either yaml or json
            session (Session): database connection (optional)
        """
        # retrieve and use only those tasks that are DQ Check tasks.
        tasks = {}
        for tid, t_obj in self.task_dict.items():
            if t_obj.__class__.__name__ == "DQChecksOperator":
                tasks[tid] = t_obj
        if not tasks:
            raise RuntimeError("There were no DQ Check tasks found on this DAG to be tested.")
        self.test(execution_date, run_conf, conn_file_path, variable_file_path, session, tasks)


# dag decorator function copied from DAG class in airflow.model.dag for customization
# NOTE: Please keep the list of arguments in sync with DAG.__init__.
# Only exception: dag_id here should have a default value, but not in DAG.
def dag(
    dag_id: str = "",
    description: str | None = None,
    schedule: ScheduleArg = NOTSET,
    schedule_interval: Union[ArgNotSet, Union[None, str, timedelta, relativedelta]] = NOTSET,
    timetable: Timetable | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    full_filepath: str | None = None,
    template_searchpath: str | Iterable[str] | None = default_template_searchpath,
    template_undefined: type[jinja2.StrictUndefined] = jinja2.StrictUndefined,
    user_defined_macros: dict | None = None,
    user_defined_filters: dict | None = None,
    default_args: dict | None = None,
    concurrency: int | None = None,
    max_active_tasks: int = conf.getint("core", "max_active_tasks_per_dag"),
    max_active_runs: int = conf.getint("core", "max_active_runs_per_dag"),
    dagrun_timeout: timedelta | None = None,
    sla_miss_callback: None | SLAMissCallback | list[SLAMissCallback] = None,
    default_view: str = conf.get_mandatory_value("webserver", "dag_default_view").lower(),
    orientation: str = conf.get_mandatory_value("webserver", "dag_orientation"),
    catchup: bool = conf.getboolean("scheduler", "catchup_by_default"),
    on_success_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None,
    on_failure_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None,
    doc_md: str | None = None,
    params: MutableMapping | None = None,
    access_control: dict | None = None,
    is_paused_upon_creation: bool | None = None,
    jinja_environment_kwargs: dict | None = None,
    render_template_as_native_obj: bool = True,  # Airflow default is False, changed for our use case.
    tags: list[str] | None = None,
    owner_links: dict[str, str] | None = None,
    auto_register: bool = True,
    fail_stop: bool = False
) -> Callable[[Callable], Callable[..., DAG]]:
    """
    Python dag decorator which wraps a function into an Airflow DAG.

    Accepts kwargs for operator kwarg. Can be used to parameterize DAGs.

    :param dag_args: Arguments for DAG object
    :param dag_kwargs: Kwargs for DAG object.
    """

    def wrapper(f: Callable) -> Callable[..., DAG]:
        @functools.wraps(f)
        def factory(*args, **kwargs):
            # Generate signature for decorated function and bind the arguments when called
            # we do this to extract parameters, so we can annotate them on the DAG object.
            # In addition, this fails if we are missing any args/kwargs with TypeError as expected.
            from inspect import signature
            f_sig = signature(f).bind(*args, **kwargs)
            # Apply defaults to capture default values if set.
            f_sig.apply_defaults()

            # Initialize DAG with bound arguments
            with DAG(
                    dag_id or f.__name__,
                    description=description,
                    schedule_interval=schedule_interval,
                    timetable=timetable,
                    start_date=start_date,
                    end_date=end_date,
                    full_filepath=full_filepath,
                    template_searchpath=template_searchpath,
                    template_undefined=template_undefined,
                    user_defined_macros=user_defined_macros,
                    user_defined_filters=user_defined_filters,
                    default_args=default_args,
                    concurrency=concurrency,
                    max_active_tasks=max_active_tasks,
                    max_active_runs=max_active_runs,
                    dagrun_timeout=dagrun_timeout,
                    sla_miss_callback=sla_miss_callback,
                    default_view=default_view,
                    orientation=orientation,
                    catchup=catchup,
                    on_success_callback=on_success_callback,
                    on_failure_callback=on_failure_callback,
                    doc_md=doc_md,
                    params=params,
                    access_control=access_control,
                    is_paused_upon_creation=is_paused_upon_creation,
                    jinja_environment_kwargs=jinja_environment_kwargs,
                    render_template_as_native_obj=render_template_as_native_obj,
                    tags=tags,
                    schedule=schedule,
                    owner_links=owner_links,
                    auto_register=auto_register,
                    fail_stop=fail_stop,
            ) as dag_obj:
                # Set DAG documentation from function documentation if it exists and doc_md is not set.
                import sys
                if f.__doc__ and not dag_obj.doc_md:
                    dag_obj.doc_md = f.__doc__

                # Generate DAGParam for each function arg/kwarg and replace it for calling the function.
                # All args/kwargs for function will be DAGParam object and replaced on execution time.
                f_kwargs = {}
                for name, value in f_sig.arguments.items():
                    f_kwargs[name] = dag_obj.param(name, value)

                # set file location to caller source path
                back = sys._getframe().f_back
                dag_obj.fileloc = back.f_code.co_filename if back else ""

                # Invoke function to create operators in the DAG scope.
                f(**f_kwargs)

            # Return dag object such that it's accessible in Globals.
            return dag_obj

        # Ensure that warnings from inside DAG() are emitted from the caller, not here
        from airflow.models.dag import fixup_decorator_warning_stack
        fixup_decorator_warning_stack(factory)
        return factory

    return wrapper


task = _task  # keep this here in case we need to extend it
