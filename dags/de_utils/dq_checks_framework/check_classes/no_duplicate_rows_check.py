from de_utils.dq_checks_framework.check_classes.check import Check


class NoDuplicateRowsCheck(Check):
    """
    Check to help ensure total counts for a table is within expected range.

    Attrs:
        name defaults to "No Duplicate Rows"
        column_name="pkey columns"

    Args:
        dups_count (Number): The number of duplicate rows found in table.
        See Parent Class docstring for description of additional args.
    """
    def __init__(
            self,
            dups_count: int | float | complex,
            name="No Duplicate Rows",
            description="Ensure :table_name table has no duplicate rows.",
            red_expr=":dups_count == 0",
            red_error=":dups_count duplicate rows found in :table_name",
            todo: list[str] = (
                "Determine where the source of duplicates is from.",
                "Either fix the duplicates or notify the upstream owner(s) a fix is needed.",
                "Notify downstream users of issues."
            ),
            *args,
            **kwargs
    ):
        # Set attributes needed for param resolution
        self.dups_count = dups_count

        super().__init__(name=name,
                         description=description,
                         column_name="pkey columns",
                         value=dups_count,
                         todo=list(todo),
                         red_expr=red_expr,
                         red_error=red_error,
                         *args,
                         **kwargs)
