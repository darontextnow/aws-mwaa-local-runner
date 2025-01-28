"""DQ Checks for the analytics.metrics_daily_dau_generated_revenue in Snowflake.

metrics_daily_dau_generated_revenue table data has direct dependencies to tables:
    growth_dau_by_segment, revenue_user_daily_ad, users, user_sets

Upstream Owners: DE team owns all immediate tables upstream from this one
Downstream Owners: BA Team(SLT)
For detailed info, checkout the Excel sheet for current owners/usage:
    DataScience and Engineering/Documents/DE TEAM/SLT MODELS/SLT_DE_TRACKER.xlsx

Checks:
    1. Ensures the total row count is not abnormal.
    2. Ensure columns that should have no null values don't suddenly have null values introduced.
    3. Ensure total_dau sum are within normal range.
    4. Ensure total_dau_generated_revenue sum are within normal range.
    5. Ensure there are no duplicates.
    6. Ensure clients,country code match as per the SLT requirements.
"""

from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.column_statistics_check import ColumnStatisticsCheck
from de_utils.dq_checks_framework.check_classes.no_duplicate_rows_check import NoDuplicateRowsCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck

NO_MISSING_DATA_COLS = ["date_utc", "client_type", "country_code", "total_dau"]

DATABASE = "prod"
TABLE = "analytics.metrics_daily_dau_generated_revenue"

EXTRA_VALUES_TODO = [
    "Check to see if a new value needs to be included/added to existing values.",
    "Notify downstream users of potential issues this may raise.",
    "If this rise is expected, adjust this check accordingly. "
]

TODO_LIST = [
    (f"Check source table {TABLE} and upstream source tables: "
     "growth_dau_by_segment, revenue_user_daily_ad, users, user_sets for possible missing data "
     "or other issues causing abnormal sums."),
    "If sums are to be expected and are regularly outside range, adjust the range.",
    ("Note for yellow alerts: When encountering yellow alerts, ensure values stay within expected bounds. "
     "Typically, if there's a seasonal trend but values are within 10% of the norm, no immediate action is needed. "
     "Keep monitoring over the next few days to confirm it's a one-time event.")
]

SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        ARRAY_UNIQUE_AGG(client_type) AS client_types,
        SUM(total_dau) AS sum_total_dau,
        ARRAY_UNIQUE_AGG(country_code) AS country_codes
    FROM {DATABASE}.{TABLE}
    WHERE (date_utc = CAST(:run_date AS DATE))
"""

# Separating Revenue out as it has a 2 day lag
REVENUE_LAG_SQL = f"""
    SELECT
        {ColumnStatisticsCheck.get_statistics_sql(column_name="total_dau_generated_revenue", percentile=.95)},
        SUM(total_dau_generated_revenue) AS sum_total_dau_generated_revenue
    FROM {DATABASE}.{TABLE}
    WHERE (date_utc = CAST(:run_date AS DATE) - interval '1 day')
"""

DUPES_COUNT_SQL = f"""
    SELECT COUNT(*) AS cnt_dups FROM (
        SELECT COUNT(*) AS cnt_dups
        FROM {DATABASE}.{TABLE}
        WHERE (DATE(date_utc) = CAST(:run_date AS DATE))
        GROUP BY date_utc, client_type, country_code
        HAVING COUNT(*) > 1
    );
"""


class MetricsDailyDauGeneratedRevenueDQChecks(BaseDQChecks):
    name = "metrics_daily_dau_generated_revenue DQ Checks"
    description = "Aggregate DQ Checks for metrics_daily_dau_generated_revenue table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="metrics_daily_dau_generated_revenue Main Input",
                alias="main_input",
                src_sql=SQL
            ),

            DBOInput(
                name="metrics_daily_dau_generated_revenue Revenue Lag Input",
                alias="revenue_lag",
                src_sql=REVENUE_LAG_SQL
            ),

            DBOInput(
                name="metrics_daily_dau_generated_revenue Dupes Count Input",
                alias="dupes_count",
                src_sql=DUPES_COUNT_SQL
            )
        ]

    def get_checks(self):
        return [
            Check(
                name="metrics_daily_dau_generated_revenue Total Count",
                description="Ensure metrics_daily_dau_generated_revenue is same",
                column_name="total_count",
                value=self.main_input[0]["total_count"],
                todo=EXTRA_VALUES_TODO,
                get_previous_run_value=True,
                red_expr=":value == 6",  # 3 clients * 2 country codes
                red_error="value = :value implies missing client_type,country code combo on that date"
            ),

            NoDuplicateRowsCheck(dups_count=self.dupes_count[0]["cnt_dups"]),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(
                input_object=self.main_input,
                column_names=NO_MISSING_DATA_COLS
            ),

            # Fail if a new value for column 'client_type' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="client_type",
                values=self.main_input[0]["client_types"],
                expected_values=["TN_IOS_FREE", "ANDROID (INCL 2L)", "TN_WEB"]
            ),

            # Fail if a new value for column 'country_code' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="country_codes",
                values=self.main_input[0]["country_codes"],
                expected_values=["CA", "US"]
            ),

            # These checks will alert spikes and dips in the chart
            Check(
                name="Total Count Check",
                description="Ensure total_dau is within the normal expected range",
                column_name="total_dau",
                value=self.main_input[0]["sum_total_dau"],
                use_dynamic_thresholds=True,
                red_expr=":lower_threshold*0.9 < :value < :upper_threshold*1.5",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                yellow_expr=":lower_threshold < :value < :upper_threshold*1.39 -0.2*:lower_threshold",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure total_dau_generated_revenue is within the normal expected range",
                column_name="total_dau_generated_revenue",
                value=self.revenue_lag[0]["sum_total_dau_generated_revenue"],
                use_dynamic_thresholds=True,
                red_expr=":lower_threshold*0.9 < :value < :upper_threshold*1.5",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                yellow_expr=":lower_threshold < :value < :upper_threshold*1.39 -0.2*:lower_threshold",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            ),

            ColumnStatisticsCheck(
                input_object=self.revenue_lag,  # Calculated based on last year 2022-23
                column_name="total_dau_generated_revenue",
                avg_expr="310000 <= :value <= 400000",
                max_expr="1000000 <= :value <= 1250000",
                min_expr="1600 <= :value <= 4000",
                stddev_expr="400000 <= :value <= 600000",
                raise_yellow_alert=True
            )
        ]
