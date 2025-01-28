from de_utils.dq_checks_framework.check_classes.check import Check


class MissingValuesRateCheck(Check):
    """
    Calculates the rate of missing values (missing_count/total_count) for a column as value to use in
    red and/or yellow expressions.

    Args:
        column_name (string): The name of column being evaluated.
        missing_count (Number): The numerator in percent of total calculation.
        total_count (Number): The total count. This count can never be 0 as it throws ZeroDivisionError.
        threshold (Number): The maximum value the rate of missing values can be.
        See Parent Class docstring for description of additional args.

    Raises: ValueError if total_count arg = 0
    """
    def __init__(
            self,
            column_name: str,
            missing_count: int | float | complex,
            total_count: int | float | complex,
            threshold: int | float | complex,
            name="Missing Values Rate Check",
            description="Ensure rate of missing values for :column_name column remains below :threshold",
            red_expr=":value < :threshold",
            red_error="Missing values rate for :column_name column (:value) is above acceptable rate of :threshold.",
            todo: list[str] = (
                    "Check with upstream owner(s) to see if the change in rate is expected.",
                    "If new rate can be expected, adjust the threshold for this DQ check.",
                    "Notify downstream users if the rate is significant and if it may affect reports/etc."
            ),
            *args,
            **kwargs
    ):
        if total_count == 0:
            msg = f"The total_count arg for MissingValuesRateCheck for column '{column_name}' cannot be zero (0)."
            raise ValueError(msg)

        # Set attributes needed for param resolution
        self.threshold = threshold
        value = missing_count/total_count
        super().__init__(name=name,
                         description=description,
                         column_name=column_name,
                         value=value,
                         todo=list(todo),
                         red_expr=red_expr,
                         red_error=red_error,
                         *args,
                         **kwargs)
