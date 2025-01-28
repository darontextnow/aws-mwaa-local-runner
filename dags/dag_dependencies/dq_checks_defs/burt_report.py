"""DQ Checks for the core.burt_report in Snowflake with specific checks to cover SLT report needs.
This table pulls in all the raw data from the burt_extract.

Upstream Owners: DE Team(core.burt_report table) and Adops Team (Actual Data).
Downstream Owners: Data Team(BA + DS)

POC: Brad(Adops)

DQ checks:
    1. Ensures the total row count is not abnormal
    2. Ensure columns that should have no null values don't suddenly have null values introduced.
    3. Ensure Impressions counts are within normal range.
    4. Ensure no incomming duplicate records.
    5. Ensures there is no missing date in the last 30 days.
    6. Ensures the total row count in the last 30 days are not abnormal.

"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.column_statistics_check import ColumnStatisticsCheck
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck
from de_utils.dq_checks_framework.check_classes.no_duplicate_rows_check import NoDuplicateRowsCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.percent_value_to_total_check import PercentValueToTotalCheck

TABLE = "core.burt_report"
INVALID_VALUES_TODO = [
    "Keep monitoring over the next few days to confirm it's a one-time event.",
    ("If the alert fires 2 or more days in a row, check with Adops team via Slack channel "
     "#ad-ops-requests to see if these column values are expected, and accordingly update the new threshold limits."),
    ("Notify the downstream users in #team-data channel.")
]
IMPRESSIONS_COUNTS_TODO = [
    ("Check source table 'core.burt_report' and upstream source file "
     "'s3://textnow-uploads-prod/burt/' "
     "for possible missing data or other issues causing abnormal sums."),
    "If sums are to be expected and are regularly outside range, adjust the range."
]

# these columns are checked for NULL or '' values
NO_MISSING_DATA_COLS = ["date"]

MAIN_SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        -- Next line masks known issue (platform with value like 'NULL') for now while eng looks into it.
        ARRAY_UNIQUE_AGG(CASE WHEN platform LIKE '%(NULL)_(NULL)%' THEN 'TN_WEB' ELSE platform END) AS platforms,
        SUM(impressions) AS impressions,
        SUM(CASE WHEN platform IN ('TN_ANDROID','2L_ANDROID') THEN impressions ELSE 0 END) AS android_impressions,
        SUM(CASE WHEN platform = 'TN_IOS_FREE' THEN impressions ELSE 0 END) AS ios_impressions,
        SUM(CASE WHEN UPPER(platform) NOT IN (
            --next line are known valid values
            'TN_ANDROID', 'TN_IOS_FREE', 'TN_WEB', '2L_ANDROID', 'TN_UNATTRIBUTABLE',
            --next lines are known values that get reassigned by Brad's cleanup
            'TN_IPHONE', 'TEXTNOW 5_ANDROID', 'TEXTNOW 5_IOS', '2NDLINE - SECOND PHONE NUMBER_ANDROID',
            'COM.ENFLICK.ANDROID.TN2NDLINE_OTHER', 'COM.ENFLICK.ANDROID.TEXTNOW_PHONE',
            'COM.ENFLICK.ANDROID.TEXTNOW_TABLET', '2NDLINE - ANDROID_ANDROID') 
            THEN 1 ELSE 0 END) AS invalid_platforms,
        SUM(CASE WHEN country = '' THEN 1 ELSE 0 END) AS country_is_empty,
        {ColumnStatisticsCheck.get_statistics_sql(column_name="impressions", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="net_revenue_usd", percentile=.95)}
    FROM prod.{TABLE}
    WHERE (date = CAST(:run_date AS DATE))
"""

DUPES_COUNT_SQL = f"""
    SELECT COUNT(*) AS cnt_dups FROM (
        SELECT COUNT(*) AS cnt_dups
        FROM prod.{TABLE}
        WHERE (date = CAST(:run_date AS DATE))
        GROUP BY date, ad_format, ad_type, advertiser, buyer, campaign, country, creative_size, deal, device_type, 
                dsp, ad_tech_platform, inventory_type, partner, line_item, platform, app, revenue_channel
        HAVING COUNT(*) > 1
    );
"""

DUPES_COUNT_30DAYS_SQL = f"""
    SELECT COUNT(*) AS cnt_dups FROM (
        SELECT COUNT(*) AS cnt_dups
        FROM prod.{TABLE}
        WHERE (date BETWEEN CAST(:run_date AS DATE) - INTERVAL '29 DAYS' AND CAST(:run_date AS DATE))
        GROUP BY date, ad_format, ad_type, advertiser, buyer, campaign, country, creative_size, deal, device_type, 
                dsp, ad_tech_platform, inventory_type, partner, line_item, platform, app, revenue_channel
        HAVING COUNT(*) > 1
    );
"""

DATES_COUNT_SQL = f"""
    SELECT COUNT(DISTINCT new_loaded_dates) AS cnt_dates FROM (
        SELECT DISTINCT DATE AS new_loaded_dates
        FROM prod.{TABLE}
        WHERE (date BETWEEN CAST(:run_date AS DATE) - INTERVAL '29 DAYS' AND CAST(:run_date AS DATE))
    );
"""


class BurtReportDQChecks(BaseDQChecks):
    name = "core.burt_report DQ Checks"
    description = "Aggregate DQ Checks for core.burt_report table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="adops_report Main Input",
                alias="main_input",
                src_sql=MAIN_SQL
            ),

            DBOInput(
                name="adops_report Dupes Count Input",
                alias="dupes_count",
                src_sql=DUPES_COUNT_SQL
            ),

            DBOInput(
                name="adops_report Dupes Count Input for 30 days period",
                alias="dupes_count_30days",
                src_sql=DUPES_COUNT_30DAYS_SQL
            ),

            DBOInput(
                name="adops_report Dates Count Input for 30 days period",
                alias="dates_count",
                src_sql=DATES_COUNT_SQL
            )
        ]

    def get_checks(self):
        return [
            TotalCountCheck(
                filter_date_column_name="date",
                total_count=self.main_input[0]["total_count"],
                # use_dynamic_thresholds=True,
                # # Explanation for threshold calculations can be found in Jira ticket DS-2325
                # red_expr=":lower_threshold < :value < :upper_threshold*1.39 -0.39*:lower_threshold",
                lower_threshold=200000,
                upper_threshold=300000
            ),

            NoDuplicateRowsCheck(dups_count=self.dupes_count[0]["cnt_dups"]),

            NoDuplicateRowsCheck(dups_count=self.dupes_count_30days[0]["cnt_dups"]),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(input_object=self.main_input, column_names=NO_MISSING_DATA_COLS),

            # Fail when the delete and insert fails to load all the 30 days worth of data
            Check(
                name="Total Count Check",
                description="Ensure sum there are 30 distinct days",
                column_name="date",
                value=self.dates_count[0]["cnt_dates"],
                red_expr=":value == 30",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=[
                    "Investigate which date is missing in the source file."
                    "Notify Adops team via Slack channel #ad-ops-requests for the missing date",
                    "Notify downstream users of potential issues this may raise."
                ]
            ),

            # Fail or Warn if android impressions count is outside expected range. Affects SLT Dashboard
            Check(
                name="Total Count Check",
                description="Ensure sum total of android_impressions is within acceptable range",
                column_name="android_impressions",
                value=self.main_input[0]["android_impressions"],
                red_expr="40000000 < :value < 75000000",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=IMPRESSIONS_COUNTS_TODO
            ),

            # Fail or Warn if ios impressions count is outside expected range. Affects SLT Dashboard
            Check(
                name="Total Count Check",
                description="Ensure sum total of ios_impressions is within acceptable range",
                column_name="ios_impressions",
                value=self.main_input[0]["ios_impressions"],
                red_expr="20000000 < :value < 45000000",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=IMPRESSIONS_COUNTS_TODO
            ),

            PercentValueToTotalCheck(
                name="Invalid Values Rate",
                description="Ensure percentage of invalid platform values remains low",
                column_name="platform",
                value=self.main_input[0]["invalid_platforms"],
                total=self.main_input[0]["total_count"],
                yellow_expr=":value <= 1",
                yellow_warning="Percentage of invalid platform values (:value) has risen above threshold 1%.",
                todo=INVALID_VALUES_TODO
            ),

            PercentValueToTotalCheck(
                name="Country Is Empty Rate",
                description="Ensure percentage where country is empty remains low",
                column_name="country",
                value=self.main_input[0]["country_is_empty"],
                total=self.main_input[0]["total_count"],
                yellow_expr=":value <= 3",
                yellow_warning="Percentage of empty country values (:value) has risen above acceptable threshold 4%.",
                todo=[
                    "Notify Adops team via Slack channel #ad-ops-requests of increase in empty values in this column.",
                    "Notify downstream users of potential issues this may raise.",
                    "If this rise is expected, adjust this check accordingly."
                ]
            )

            # ColumnStatisticsCheck(
            #     input_object=self.main_input,
            #     column_name="impressions",
            #     avg_expr="10000 <= :value <= 25000",
            #     median_expr=":value == 0",
            #     max_expr="10000000 <= :value <= 20000000",
            #     stddev_expr="160000 <= :value <= 400000",
            #     raise_yellow_alert=True
            # ),
            #
            # ColumnStatisticsCheck(
            #     input_object=self.main_input,
            #     column_name="net_revenue_usd",
            #     avg_expr="30 <= :value <= 70",
            #     median_expr=":value == 0",
            #     max_expr="40000 <= :value <= 70000",
            #     stddev_expr="600 <= :value <= 1200",
            #     raise_yellow_alert=True
            # )
        ]
