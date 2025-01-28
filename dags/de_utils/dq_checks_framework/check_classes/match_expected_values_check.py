from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.enums import CheckType


class MatchExpectedValuesCheck(Check):
    """
    Check to ensure all values appearing in a column (as an array) match given list of expected values.

    Args:
        column_name (str): The name of the column being evaluated.
        values List[Any]: The array/list of values to compare with expected_values.
        expected_values (List[Any]): A list of all possible, acceptable values for the column.
        check_type: (CheckType Enum): CheckType.RED or CheckType.YELLOW. This replaces the red_expr and yellow_expr
            args and determines whether a red error or a yellow warning will be raised.
        See Parent Class docstring for description of args.
    """
    def __init__(
            self,
            column_name: str,
            values: list[any],
            expected_values: list[any],
            check_type: CheckType = CheckType.RED,
            name: str = "Values Match Expected List",
            description: str = "Ensure column values for :column_name column match with expected values list",
            red_error: str = "The values :values found in :column_name column are not in the expected_values list",
            yellow_warning: str = "The values :values found in :column_name column are not in the expected_values list",
            todo: list[str] = (
                    "Check with upstream owner(s) if new value(s) are expected.",
                    "If expected, add value(s) to expected_values list in DQ Check.",
                    "Notify downstream users of new value(s)."
            ),
            *args,
            **kwargs
    ):
        # The difference between the two lists should result in an empty list ([]) else raise error or warning
        expr = f"list(set({str(values)}).difference({str(expected_values)})) == []"
        values = str(list(set(values).difference(expected_values)))
        super().__init__(name=name,
                         description=description,
                         column_name=column_name,
                         value=values,
                         red_expr=None if check_type == CheckType.YELLOW else expr,
                         red_error=red_error.replace(':values', values),
                         yellow_expr=None if check_type == CheckType.RED else expr,
                         yellow_warning=yellow_warning.replace(':values', values),
                         todo=list(todo),
                         *args,
                         **kwargs)
