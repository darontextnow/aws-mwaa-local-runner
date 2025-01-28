from de_utils.dq_checks_framework.check_classes.check import Check


class DistributionShiftPSI(Check):
    """Population Stability Index is a measure of how much a population has shifted over time
    or between two different samples of a population in a single number.
    It does this by bucketing the two distributions and comparing the percents of items in each of the buckets.

    Use the static method get_psi_sql of this class to calculate the PSI for given column.

    Args:
        name (str): The short name for the check. Used in reporting tables.
        description (str): The description (max 300 char) for the check. Used in reporting tables.
        column_name (str): The name of the column being checked. Used in reporting tables.
        psi (float): The psi as returned by the input query.
        See Check Class for additional args.
    """
    def __init__(
            self,
            *,
            name: str = "Dist Shift PSI Check",
            description: str = "Ensure distribution shift in PSI remains within given threshold.",
            column_name: str,
            psi: float,
            yellow_expr: str | None = ":value < 0.1",
            yellow_warning: str | None = "PSI for column :column_name is out of given range :yellow_expr",
            **kwargs
    ):

        super().__init__(name=name, description=description, column_name=column_name,
                         value=psi, yellow_expr=yellow_expr, yellow_warning=yellow_warning, **kwargs)

    @staticmethod
    def get_psi_sql(
            table: str,
            column_name: str,
            ref_condition: str,
            test_condition: str,
            min: int = 0,
            max: int | None = None,
            bins: int = 10,
            input_alias: str | None = None
    ) -> str:
        """Returns SQL subquery string that returns PSI for given column.

        Args:
            table (string): The database.schema.table_name to test.
            column_name (str): The name of the column being checked. Used in reporting tables.
            ref_condition (str): SQL WHERE clause filter for reference value.
            test_condition(str): SQL WHERE clause filter for test value.
            min (int):
            max (int):
            bins (int):
            input_alias (string): The alias used to fetch this column from the input object.
                By default, this will be {colum_name + "_psi"}
        """
        max = f"(SELECT MAX({column_name}) FROM {table} WHERE {ref_condition})" if not max else max
        sql = f"""(WITH ref AS (
                    SELECT
                        COALESCE(WIDTH_BUCKET({column_name}, {min}, {max}, {bins}), {min} - 1) AS bin,
                        COUNT(1) AS n,
                        RATIO_TO_REPORT(n) OVER () AS n_pct
                    FROM {table}
                    WHERE {ref_condition}
                    GROUP BY 1
                ),
                test AS (
                    SELECT
                        COALESCE(WIDTH_BUCKET({column_name}, {min}, {max}, {bins}), {min} - 1) AS bin,
                        COUNT(1) AS n,
                        RATIO_TO_REPORT(n) OVER () AS n_pct
                    FROM {table}
                    WHERE {test_condition}
                    GROUP BY 1
                )
                --return population_stability_index
                SELECT SUM((test.n_pct - ref.n_pct) * ln(test.n_pct / ref.n_pct)) AS psi
                FROM ref
                FULL OUTER JOIN test USING (bin)
            ) AS {input_alias or (column_name + '_psi')}
        """
        return sql
