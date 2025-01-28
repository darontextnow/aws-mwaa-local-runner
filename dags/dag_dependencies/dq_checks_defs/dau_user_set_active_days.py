"""DQ Checks for the dau_user_set_active_days table in Snowflake.

The checks defined below are set up to run once a day to ensure dau_user_set_active_days data added to Snowflake during
the previous 24 hours meets our assumptions and minimum requirements for good data quality.

Notes:
    As of 1/20/2023 no column have NULL or missing values in this table.

"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.no_duplicate_rows_check import NoDuplicateRowsCheck
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck
from de_utils.dq_checks_framework.check_classes.column_statistics_check import ColumnStatisticsCheck

NO_MISSING_DATA_COLS = ["date_utc", "user_set_id", "client_type", "cohort_utc", "day_from_cohort", "dau"]

SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        ARRAY_UNIQUE_AGG(client_type) AS client_types,
        SUM(CASE WHEN DATEDIFF("DAYS", cohort_utc, date_utc) <> day_from_cohort THEN 1 ELSE 0 END)
            AS invalid_day_from_cohort,
        {ColumnStatisticsCheck.get_statistics_sql(column_name="day_from_cohort", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="dau", percentile=.95)}
    FROM prod.analytics_staging.dau_user_set_active_days
    WHERE (date_utc = CAST(:run_date AS DATE))
"""

DUPES_COUNT_SQL = """
    SELECT COUNT(*) AS cnt_dups FROM (
        SELECT COUNT(*) AS cnt_dups
        FROM prod.analytics_staging.dau_user_set_active_days
        WHERE (DATE(date_utc) = CAST(:run_date AS DATE))
        GROUP BY date_utc, user_set_id, client_type
        HAVING COUNT(*) > 1
    );
"""


class DAUUserSetActiveDaysDQChecks(BaseDQChecks):
    name = "dau_user_set_active_days DQ Checks"
    description = "Aggregate DQ Checks for dau_user_set_active_days table"
    table_name = "analytics_staging.dau_user_set_active_days"

    def get_inputs(self):
        return [
            DBOInput(
                name="dau_user_set_active_days Prod Main Input",
                alias="main_input",
                src_sql=SQL
            ),

            DBOInput(
                name="dau_user_set_active_days Dupes Count Input",
                alias="dupes_count",
                src_sql=DUPES_COUNT_SQL
            )
        ]

    def get_checks(self):

        return [
            TotalCountCheck(
                filter_date_column_name="date_utc",
                total_count=self.main_input[0]["total_count"],
                lower_threshold=2500000,  # Count in last 6 months is steadily over 2.5M
                upper_threshold=4000000  # Good enough to ensure there isn't total duplication
            ),

            NoDuplicateRowsCheck(dups_count=self.dupes_count[0]["cnt_dups"]),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(input_object=self.main_input, column_names=NO_MISSING_DATA_COLS),

            # Fail if a new value for column 'client_type' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="client_type",
                values=self.main_input[0]["client_types"],
                expected_values=["TN_IOS_FREE", "TN_ANDROID", "2L_ANDROID", "TN_IOS"]
            ),

            Check(
                name="Invalid day_from_cohort",
                description="Fail if calculated field day_from_cohort is not correct",
                column_name="day_from_cohort",
                value=self.main_input[0]["invalid_day_from_cohort"],
                red_expr=":value == 0",
                red_error=":value rows with invalid day_from_cohort calculations were found.",
                todo=["Need to research why suddenly this calculation is off and correct the data."]
            ),

            ColumnStatisticsCheck(
                input_object=self.main_input,
                column_name="day_from_cohort",
                avg_expr="600 <= :value <= 900",
                median_expr="290 <= :value <= 550",
                min_expr="-10 <= :value <= 0",
                max_expr="1900 <= :value <= 3100",
                stddev_expr="540 <= :value <= 1000",
                percentile_expr="1660 <= :value <= 2600",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.main_input,
                column_name="dau",
                avg_expr="0.93 <= :value <= 0.95",
                median_expr=":value == 1",
                min_expr="0.0001 <= :value <= 0.006",
                max_expr="2 <= :value <= 35",
                stddev_expr="0.15 <= :value <= 0.2",
                percentile_expr=":value == 1",
                raise_yellow_alert=True
            )
        ]
