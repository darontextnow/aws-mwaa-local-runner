"""DQ Checks for the analytics.metrics_daily_ad_total_revenue in Snowflake.
metrics_daily_ad_total_revenue table data has direct dependency to tables:
    revenue_user_daily_ad, metrics_daily_iap_revenue, dau.user_sets, users

Upstream Owners: DE team owns all immediate tables upstream from this one
Downstream Owners: BA Team(SLT Tableau Dashboards)

For detailed info, checkout the Excel sheet for current owners/usage:
    DataScience and Engineering/Documents/DE TEAM/SLT MODELS/SLT_DE_TRACKER.xlsx

SLT specific checks:
    1. Ensures the total row count is not abnormal.
    2. Ensure columns that should have no null values don't suddenly have null values introduced.
    3. Ensure daily_iap_revenue sum are within normal range.
    4. Ensure daily_ad_revenue sum are within normal range.
    5. Ensure there are no duplicates.
    6. Ensure clients match as per the SLT requirements.

Issues: check out adops_report.py (in dq_check_defs) docstring for detailed issues,
    since this table's source data is coming from adops.report
"""

from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.no_duplicate_rows_check import NoDuplicateRowsCheck
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck
from de_utils.dq_checks_framework.check_classes.column_statistics_check import ColumnStatisticsCheck

DATABASE = "prod"
TABLE = "analytics.metrics_daily_ad_total_revenue"

TODO_LIST = [
    (f"Check source table {TABLE} and upstream source tables: [adops.report, core.adtracker] "
     "for possible missing data or other issues causing abnormal sums."),
    "If sums are to be expected and are regularly outside range, adjust the range.",
    ("Note for yellow alerts: When encountering yellow alerts, ensure values stay within expected bounds. "
     "Typically, if there's a seasonal trend but values are within 10% of the norm, no immediate action is needed. "
     "Keep monitoring over the next few days to confirm it's a one-time event")
]

# these columns are checked for NULL or '' values
NO_MISSING_DATA_COLS = ["date_utc", "client_type", "daily_ad_revenue", "daily_iap_revenue"]

MAIN_SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        ARRAY_UNIQUE_AGG(client_type) AS client_types,
        ARRAY_UNIQUE_AGG(country_code) AS country_codes
    FROM {DATABASE}.{TABLE}
    WHERE (date_utc = CAST(:run_date AS DATE))
"""

# Separating Revenue out as it has a 2 day lag
REVENUE_LAG_SQL = f"""
    SELECT
        {ColumnStatisticsCheck.get_statistics_sql(column_name="daily_ad_revenue")},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="daily_iap_revenue")},
        SUM(DAILY_AD_REVENUE) AS total_daily_ad_revenue,
        SUM(DAILY_IAP_REVENUE) AS total_daily_iap_revenue
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


class MetricsDailyAdTotalRevenueDQChecks(BaseDQChecks):
    name = "metrics_daily_ad_total_revenue DQ Checks"
    description = "Aggregate DQ Checks for metrics_daily_ad_total_revenue table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="metrics_daily_ad_total_revenue Main Input",
                alias="main_input",
                src_sql=MAIN_SQL
            ),

            DBOInput(
                name="metrics_daily_ad_total_revenue Revenue Lag Input",
                alias="revenue_lag",
                src_sql=REVENUE_LAG_SQL
            ),

            DBOInput(
                name="metrics_daily_ad_total_revenue Dupes Count Input",
                alias="dupes_count",
                src_sql=DUPES_COUNT_SQL
            )
        ]

    def get_checks(self):
        return [

            ColumnStatisticsCheck(
                input_object=self.revenue_lag,
                column_name="daily_ad_revenue",
                stddev_expr="5000 <= :value <= 24000",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.revenue_lag,
                column_name="daily_iap_revenue",
                stddev_expr="900 <= :value <= 3500",
                raise_yellow_alert=True
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

            # These checks will alert spikes and dips in the SLT chart
            Check(
                name="Total Count Check",
                description="Ensure daily_ad_revenue is within the normal expected range",
                column_name="daily_ad_revenue",
                value=self.revenue_lag[0]["total_daily_ad_revenue"],
                use_dynamic_thresholds=True,
                red_expr=":lower_threshold*0.9 < :value < :upper_threshold*1.5",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                yellow_expr=":lower_threshold < :value < :upper_threshold*1.39 -0.2*:lower_threshold",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure daily_iap_revenue is within the normal expected range",
                column_name="daily_iap_revenue",
                value=self.revenue_lag[0]["total_daily_iap_revenue"],
                use_dynamic_thresholds=True,
                red_expr=":lower_threshold*0.9 < :value < :upper_threshold*1.5",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                yellow_expr=":lower_threshold < :value < :upper_threshold*1.39 -0.2*:lower_threshold",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            )
        ]
