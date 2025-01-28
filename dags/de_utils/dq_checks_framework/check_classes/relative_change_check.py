from de_utils.dq_checks_framework.check_classes.check import Check


class RelativeChangeCheck(Check):
    """
    Calculates the relative change metric between given val1 and val2 and uses that as the 'value' in given
    red_expr and/or yellow_expr.
    Args:
        value1 (number): The first value. This will typically be the src system value when comparing source to target.
        value2 (number): The second value. Typically, the target system's value. Cannot be 0.
        Note a red_expr and/or yellow_expr must be supplied. Example: "0 <= :value <= 1"
        See Parent Class docstring for description of additional args.

    Raises:
        ValueError if val2 is 0 due to division by 0.
    """
    def __init__(
            self,
            value1: int | float,
            value2: int | float,
            red_error: str = (f"The relative change (:value) between value1 (:value1) and value2 (:value2)"
                              f" is outside acceptable red threshold."),
            yellow_warning: str = (f"The relative change (:value) between value1 (:value1) and value2 (:value2)"
                                   f" has reached the WARNING level."),
            todo: list[str] = (
                    "Determine if this alert points to a potential large loss/duplication of data.",
                    "If data is missing/duplicated/erroneous, fix issue and rerun DAGs that populate table.",
                    "If it is a one off and overall integrity of table looks good, you can ignore this alert.",
                    "If this alert is triggered regularly, consider adjusting the threshold to match reality."
            ),
            *args,
            **kwargs
    ):
        if value2 == 0:
            msg = "The value2 arg for RelativeChangeCheck cannot be zero (0). Division by zero error raised."
            raise ValueError(msg)

        rel_change = ((value1 - value2) / value2) * 100
        red_error = None if not red_error else red_error.replace(":value1", str(value1))\
            .replace(":value2", str(value2))
        yellow_warning = None if not yellow_warning else yellow_warning.replace(":value1", str(value1))\
            .replace(":value2", str(value2))

        super().__init__(value=rel_change,
                         red_error=red_error,
                         yellow_warning=yellow_warning,
                         todo=list(todo),
                         *args,
                         **kwargs)
