from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.slack.alerts import SLACK_ALERT_INDENT_NUMERIC_LIST


class TotalCountCheck(Check):
    """
    Check to help ensure total counts for a table is within expected range.

    Args:
        filter_date_column_name (string): The name the timestamp or date column we can use to filter down history with.
        total_count (Number): The total count of records for current run_date.
        lower_threshold (Number): The minimum total count allowable/expected.
            Leave this empty when setting use_dynamic_thresholds = True.
        upper_threshold (Number): The maximum total count allowable/expected.
            Leave this empty when setting use_dynamic_thresholds = True.
        See Parent Class docstring for description of additional args.
    """
    def __init__(
            self,
            filter_date_column_name: str,
            total_count: int | float | complex,
            lower_threshold: int | float | complex = None,
            upper_threshold: int | float | complex = None,
            name="Total Count Check",
            description="Ensure total count of records for :table_name table remains in expected range",
            column_name="COUNT(*)",
            red_expr=":lower_threshold <= :total_count <= :upper_threshold",
            red_error="Total Count (:total_count) for :table_name table is not within expected range :red_expr",
            todo: list[str] = (
                (f"If total_count is abnormal for yesterday's date:\n"
                 f"{SLACK_ALERT_INDENT_NUMERIC_LIST}        a) Research source tables to find where issue lies.\n"
                 f"{SLACK_ALERT_INDENT_NUMERIC_LIST}        b) Notify upstream owner(s) if appropriate"
                 f" or rerun DAGs to repopulate.\n"
                 f"{SLACK_ALERT_INDENT_NUMERIC_LIST}        c) Notify downstream users of potential issue."),
                "If total_count looks correct, change threshold and log change in :dq_check_changes_log_link"
            ),
            *args,
            **kwargs
    ):
        super().__init__(name=name,
                         description=description,
                         column_name=column_name,
                         value=total_count,
                         todo=list(todo),
                         red_expr=red_expr,
                         red_error=red_error,
                         *args,
                         **kwargs)

        if self.use_dynamic_thresholds is False and (upper_threshold is None or lower_threshold is None):
            msg = "The lower_threshold and upper_threshold args must be set when use_dynamic_thresholds = False."
            raise ValueError(msg)

        # Set additional attributes needed for param resolution
        self.filter_date_column_name = filter_date_column_name
        self.total_count = total_count or 0  # convert None to 0 for those src table queries that return no results.
        self.lower_threshold = lower_threshold
        self.upper_threshold = upper_threshold
