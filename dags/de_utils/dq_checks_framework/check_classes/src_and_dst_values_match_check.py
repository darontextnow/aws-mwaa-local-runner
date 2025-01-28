from de_utils.dq_checks_framework.check_classes.check import Check


class SrcAndDstValuesMatchCheck(list):
    """
    Check to ensure source value matches destination value for given column(s).
    In other words, it compares the value found in the source input_object with the value in the
        destination input_object for the given column(s) to ensure the values match.

    Args:
        src_input (InputObject): Example: self.input0.
        dst_input (InputObject): Example: self.input1.
        column_names (List[str] or str): The column name or List of column names to be compared/matched.
        raise_red_alert (bool): Set to True to raise a RED failure and alert if red_expr evaluates to False.
        raise_yellow_alert (bool): Set to True to raise a YELLOW warning if yellow_expr evaluates to False.
        See Parent Class docstring for description of args.
    """
    def __init__(
            self,
            src_input,
            dst_input,
            column_names: list[str] | str,
            raise_red_alert: bool = True,
            red_error: str = "Value ':src_value' from src_input does match the value ':dst_value' from dst_input.",
            raise_yellow_alert: bool = False,
            yellow_warning: str = "Value ':src_value' from src_input does match the value ':dst_value' from dst_input.",
            todo: list[str] = (
                    "When source and destination tables aren't matching, this typically means data is missing.",
                    "Determine what data is missing or why source and destination values are not matching.",
                    "If data is missing/duplicated/erroneous, fix issue and rerun DAGs that populate table."
            ),
            *args,
            **kwargs
    ):
        cols = [column_names] if type(column_names) == str else column_names
        checks = []
        for col in cols:
            src_val = src_input[0][col]
            dst_val = dst_input[0][col]
            expr = f"{src_val} == {dst_val}"
            if type(src_val) == float or type(dst_val) == float:
                # using math.isclose() function as recommended (PEP 485) for floats
                expr = f"math.isclose({src_val}, {dst_val}, abs_tol=0.01)"
            # Must resolve src_input values here or else parent class eval won't work
            red_expr = None if raise_red_alert is False else expr
            red_msg = None
            if raise_red_alert:
                red_msg = red_error.replace(":src_value", str(src_val)).replace(":dst_value", str(dst_val))
            yellow_expr = None if raise_yellow_alert is False else expr
            yellow_msg = None
            if raise_yellow_alert:
                yellow_msg = yellow_warning.replace(":src_value", str(src_val)).replace(":dst_value", str(dst_val))

            checks.append(
                Check(name=f"Src And Dst Values Match",
                      description=f"Ensure values match between two given inputs for column name: {col}",
                      column_name=col,
                      value=dst_val,
                      red_expr=red_expr,
                      red_error=red_msg,
                      yellow_expr=yellow_expr,
                      yellow_warning=yellow_msg,
                      todo=list(todo),
                      *args,
                      **kwargs)
            )
        # Add checks to parent list
        super().__init__(checks)
