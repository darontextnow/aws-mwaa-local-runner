from de_utils.dq_checks_framework.check_classes.check import Check

STATS = ["avg", "median", "min", "max", "stddev", "percentile"]


class ColumnStatisticsCheck(list):
    """
    Calculates summary statistics (avg, median, min, max, stddev, percentile) for a column to ensure values stay
    within expected given ranges. Leave the expr (expression) arg empty for any statistics not to be used.

    Attrs:
        name="Column Statistics Check"
        description=f"Check {stat} for column {column_name} meets given expectations",
        column_name=f"{column_name} {stat}"

    Args:
        input_object (BaseInput): The input object where stats were calculated using function below.
            Example: self.input0
        column_name (str): The name of the column to calculate statistics for.
        avg_expr (str): The expression to be evaluated for the avg stat value.
        median_expr (str): The expression to be evaluated for the median stat value.
        min_expr (str): The expression to be evaluated for the min stat value.
        max_expr (str): The expression to be evaluated for the max stat value.
        stddev_expr (str): The expression to be evaluated for the stddev stat value.
        percentile_expr (str): The expression to be evaluated for the percentile stat value.
        raise_red_alert (bool): Set to True to raise a RED failure and alert if any expression evaluates to False.
        raise_yellow_alert (bool): Set to True to raise a YELLOW warning if any expression evaluates to False.
        See Parent Class docstring for description of additional args.
    """
    def __init__(
            self,
            input_object,
            column_name: str,
            avg_expr: str = None,
            median_expr: str = None,
            min_expr: str = None,
            max_expr: str = None,
            stddev_expr: str = None,
            percentile_expr: str = None,
            raise_red_alert: bool = False,
            red_error: str = "The :stat value (:value) does not meet given expression :expr",
            raise_yellow_alert: bool = True,
            yellow_warning: str = "The :stat value (:value) does not meet given expression :expr",
            todo: list[str] = (
                    "Determine if this warning points to a potential large loss/duplication of data.",
                    "If data is missing/duplicated/erroneous, fix issue and rerun DAGs that populate table.",
                    # Commenting the following query out to cut down on Slack channel volume.
                    #(f"Else, run query below to determine if this is a one off or if check is regularly triggering:\n"
                    # f"   {SLACK_ALERT_INDENT_NUMERIC_LIST}SELECT check_name, executed_at, status, check_error\n"
                    # f"   {SLACK_ALERT_INDENT_NUMERIC_LIST}FROM prod.analytics.dq_checks\n"
                    # f"   {SLACK_ALERT_INDENT_NUMERIC_LIST}WHERE\n"
                    # f"   {SLACK_ALERT_INDENT_NUMERIC_LIST}  (executed_at > DATEADD('DAYS', '-90', CURRENT_DATE()))\n"
                    # f"   {SLACK_ALERT_INDENT_NUMERIC_LIST}  AND (check_name = ':name')\n"
                    # f"   {SLACK_ALERT_INDENT_NUMERIC_LIST}  AND (status <> 'GREEN')\n"
                    # f"   {SLACK_ALERT_INDENT_NUMERIC_LIST}ORDER BY executed_at DESC\n"
                    # f"   {SLACK_ALERT_INDENT_NUMERIC_LIST}LIMIT 200;"
                    # ),
                    "If it is a one off and overall integrity of table looks good, you can ignore this alert.",
                    "If this alert is triggered regularly, consider adjusting the threshold to match reality."
            ),
            *args,
            **kwargs
    ):
        checks = []
        for stat in STATS:
            expr = locals()[f"{stat}_expr"]
            if not expr:
                continue
            alias = f"""{stat}_{column_name.strip('"')}"""
            value = input_object[0][alias]
            red_expr = None if raise_red_alert is False else expr
            red_msg = red_error.replace(":stat", stat).replace(":expr", red_expr) if raise_red_alert else None
            yellow_expr = None if raise_yellow_alert is False else expr
            yellow_msg = yellow_warning.replace(":stat", stat).replace(":expr", yellow_expr) \
                if raise_yellow_alert else None

            checks.append(Check(
                name=f"Column Statistics Check",
                description=f"Check {stat} for column {column_name} meets given expectations",
                column_name=f"{column_name} {stat}",
                value=value,
                red_expr=red_expr,
                red_error=red_msg,
                yellow_expr=yellow_expr,
                yellow_warning=yellow_msg,
                todo=list(todo),
                *args,
                **kwargs
            ))
        # Add checks to parent list
        super().__init__(checks)

    @staticmethod
    def get_statistics_sql(
            column_name: str,
            percentile: float = None,
            where_filter: str = None
    ):
        """Returns SQL string will return count of null and empty rows for all given columns.
                Args:
                    column_name (str): The column name to retrieve statistics for.
                    percentile (float): Number between 0 and 1 to use to calculate percentile on column.
                        Leave blank to skip this statistic.
                    where_filter (str):
                """
        sql = ""
        for stat in STATS:
            expr = column_name
            alias = f'"{stat}_{column_name[1:]}' if column_name.startswith('"') else f"{stat}_{column_name}"
            if where_filter:
                expr = f"CASE WHEN {where_filter} THEN {column_name} ELSE NULL END"
            if stat == "percentile":
                if percentile:  # only add percentile if percentile arg was specified
                    sql += f"PERCENTILE_CONT({percentile}) WITHIN GROUP(ORDER BY {expr}) AS {alias},\n        "
            else:
                sql += f"{stat.upper()}({expr}) AS {alias},\n        "
        return sql[:-10]  # Removing last comma, newline, and 8 spaces
