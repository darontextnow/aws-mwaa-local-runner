from de_utils.dq_checks_framework.check_classes.check import Check


class PercentValueToTotalCheck(Check):
    """
    Calculates percentage of total for given value and total as value to use in red and/or yellow expressions.

    Args:
        value (Number): The numerator in percent of total calculation.
        total (Number): The total to use as the denominator of percent of total calculation.
        See Parent Class docstring for description of additional args.

    Raises: ValueError if total arg = 0
    """
    def __init__(
            self,
            value: int | float | complex,
            total: int | float | complex,
            name: str = "Percent Value To Total Check",
            *args,
            **kwargs
    ):
        if total == 0:
            msg = f"The total arg for PercentValueOfTotalCheck class cannot be zero (0)."
            raise ValueError(msg)
        value = (value / total) * 100

        super().__init__(name=name, value=value, *args, **kwargs)
