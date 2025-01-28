from de_utils.dq_checks_framework.check_classes.check import Check


class NoMissingValuesCheck(list):
    """
    Check for raising alerts and failures whenever given column_names have missing values.
    Missing values can include NULLs, empty stings ('') or whatever the query includes as a missing value.
    Simply override arg defaults as needed to make name, description, errors more specific to the check.

    Args:
        input_object (BaseInput): The input object where a value (count of rows with missing values)
            for each given column in column_names arg can be found.
        column_names (str): A list of column names that are being evaluated for null or empty values.
        See Parent Class docstring for description of additional args.

    Example:
        NoMissingValuesCheck(
            input_object=self.input0,
            column_names=["username", "type"]
        ),
    """
    def __init__(
            self,
            input_object,
            column_names: list[str],
            name: str = "No Missing Values Check",
            description: str = "Ensure there are no missing values (i.e. NULL or empty ('')) in column: :column_name",
            red_expr: str = ":value == 0",
            red_error: str = ":column_name has :value rows with missing values",
            todo: list[str] = (
                "Check with data Owner(s) as to why suddenly there are missing values in :column_name.",
                "Notify downstream users if this could potentially affect reports/ml runs.",
                "If missing values are now expected, convert this check to a MissingValuesRateCheck."
            ),
            *args,
            **kwargs
    ):
        checks = []
        for col_name in column_names:
            check = Check(
                name=name,
                description=description,
                column_name=col_name,
                value=input_object[0][col_name + "_null_empty"],
                red_expr=red_expr,
                red_error=red_error,
                todo=list(todo),
                *args,
                **kwargs
            )
            check.resolve_params()  # rerun resolve_params() method so :column_name param is resolved
            checks.append(check)
        # Add checks to parent list
        super().__init__(checks)

    @staticmethod
    def get_null_and_empty_counts_sql(columns: list[str]):
        """Returns SQL string will return count of null and empty rows for all given columns.
        Args:
            columns (List[column_names]): The column names for each column to retrieve missing counts for.
        """
        sql = "".join([f"SUM(CASE WHEN NVL(CAST({col} AS STRING), '') = '' THEN 1 ELSE 0 END) AS {col}_null_empty,\n"
                       for col in columns])
        return sql[:-2]  # Removing last comma and newline

    @staticmethod
    def get_null_counts_sql(columns: list[str]) -> str:
        """Returns SQL string that returns count of null rows for all given columns.
        Args:
            columns (List[column_names]): The column names for each column to retrieve null counts for.
        """
        sql = "".join([f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS {col}_null,\n"
                       for col in columns])
        return sql[:-2]  # Removing last comma and newline

    @staticmethod
    def get_empty_counts_sql(columns: list[str]):
        """Returns SQL string will return count of empty ('') (STRING datatype only) rows for all given columns.
        Args:
            columns (List[column_names]): The column names for each column to retrieve empty counts for.
        """
        sql = "".join([f"SUM(CASE WHEN {col} = '' THEN 1 ELSE 0 END) AS {col}_empty,\n"
                       for col in columns])
        return sql[:-2]  # Removing last comma and newline
