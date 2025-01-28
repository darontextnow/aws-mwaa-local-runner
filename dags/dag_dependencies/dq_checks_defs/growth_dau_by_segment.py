"""DQ Checks for the growth_dau_by_segment table in Snowflake.

The checks defined below are set up to run once a day to ensure growth_dau_by_segment data added to Snowflake during
the previous 24 hours meets our assumptions and minimum requirements for good data quality.

Notes:
    consumable_type missing values rate as of 1/1/2023 = 0.999488
    ad_upgrade_type missing values rate as of 1/1/2023 = 0.999351
    phone_num_upgrade_type missing values rate as of 1/1/2023 = 0.999917
    # Per Eric Wong, these NULL rates are higher than he would have expected.
    # But don't spend time on this right now as Eric doesn't know of anyone using these fields.
"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck
from de_utils.dq_checks_framework.check_classes.no_duplicate_rows_check import NoDuplicateRowsCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck
from de_utils.dq_checks_framework.check_classes.column_statistics_check import ColumnStatisticsCheck

NO_MISSING_DATA_COLS = ["date_utc", "user_set_id", "client_type", "sub_type", "rfm_segment", "dau"]

SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        ARRAY_UNIQUE_AGG(client_type) AS client_types,
        ARRAY_UNIQUE_AGG(sub_type) AS sub_types,
        ARRAY_UNIQUE_AGG(rfm_segment) AS rfm_segments,
        ARRAY_UNIQUE_AGG(consumable_type) AS consumable_types,
        ARRAY_UNIQUE_AGG(ad_upgrade_type) AS ad_upgrade_types,
        ARRAY_UNIQUE_AGG(phone_num_upgrade_type) AS phone_num_upgrade_types,
        {ColumnStatisticsCheck.get_statistics_sql(column_name="dau", percentile=.95)}
    FROM prod.analytics.growth_dau_by_segment
    WHERE (date_utc = CAST(:run_date AS DATE))
"""

DUPES_COUNT_SQL = """
    SELECT COUNT(*) AS cnt_dups FROM (
        SELECT COUNT(*) AS cnt_dups
        FROM prod.analytics.growth_dau_by_segment
        WHERE (DATE(date_utc) = CAST(:run_date AS DATE))
        GROUP BY date_utc, user_set_id, client_type
        HAVING COUNT(*) > 1
    );
"""

TODO_LIST = [
    ("Check these upstream source tables [dau.bad_sets, core.users, analytics.user_set_daily_activities, "
     "analytics_staging.user_segment_user_set_daily_summary] "
     "for possible missing data or other issues causing abnormality."),
    "If sums are to be expected and are regularly outside range, adjust the range.",
    ("Note for yellow alerts: When encountering yellow alerts, ensure values stay within expected bounds. "
     "Typically, if there's a deviation within 10% of the norm, no immediate action is needed. "
     "However, keep monitoring over the next few days to confirm it's a one-time event, and if continues "
     "investigate the upstream source tables for any abnormalities")

]


class GrowthDAUBySegmentDQChecks(BaseDQChecks):
    name = "growth_dau_by_segment DQ Checks"
    description = "Aggregate DQ Checks for growth_dau_by_segment table"
    table_name = "analytics.growth_dau_by_segment"

    def get_inputs(self):
        return [
            DBOInput(
                name="growth_dau_by_segment Prod Main Input",
                alias="main_input",
                src_sql=SQL
            ),

            DBOInput(
                name="growth_dau_by_segment Dupes Count Input",
                alias="dupes_count",
                src_sql=DUPES_COUNT_SQL
            )
        ]

    def get_checks(self):
        return [
            TotalCountCheck(
                filter_date_column_name="date_utc",
                total_count=self.main_input[0]["total_count"],
                use_dynamic_thresholds=True,
                # Count in last 3 months is steadily over 2.5M
                red_expr="2400000 < :value < 3500000",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                yellow_expr=":lower_threshold < :value < :upper_threshold",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            ),

            NoDuplicateRowsCheck(dups_count=self.dupes_count[0]["cnt_dups"]),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(input_object=self.main_input, column_names=NO_MISSING_DATA_COLS),

            # Fail if a new value for column 'client_type' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="client_type",
                values=self.main_input[0]["client_types"],
                expected_values=["TN_IOS_FREE", "TN_ANDROID", "TN_WEB", "2L_ANDROID", "TN_IOS"]
            ),

            # Fail if a new value for column 'sub_types' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="sub_type",
                values=self.main_input[0]["sub_types"],
                expected_values=["Non-Sub", "free/no data", "paid/no data", "paid/with data",
                                 "TN Employee/with data", "free/with data"]
            ),

            # Fail if a new value for column 'rfm_segment' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="rfm_segment",
                values=self.main_input[0]["rfm_segments"],
                expected_values=["promising", "other_users", "at_risk", "loyal", "new_users", "infrequent", "leaving"]
            ),

            # Fail if a new value for column 'consumable_type' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="consumable_type",
                values=self.main_input[0]["consumable_types"],
                expected_values=["ILD Credits (IAP)", "ILD Credits (Internal)"]
            ),

            # Fail if a new value for column 'ad_upgrade_type' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="ad_upgrade_type",
                values=self.main_input[0]["ad_upgrade_types"],
                expected_values=["Ad Free+ (monthly)", "Ad Free Lite (monthly)", "Ad Free+ (annually)"]
            ),

            # Fail if a new value for column 'phone_num_upgrade_type' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="phone_num_upgrade_type",
                values=self.main_input[0]["phone_num_upgrade_types"],
                expected_values=["Lock In Number (annually)", "Premium Number (annually)", "Lock In Number (monthly)",
                                 "Premium Number (monthly)"]
            ),

            ColumnStatisticsCheck(
                input_object=self.main_input,
                column_name="dau",
                avg_expr="0.93 <= :value <= 0.95",
                median_expr=":value == 1",
                min_expr="0.0001 <= :value <= 0.08",
                max_expr="2.5 <= :value <= 5",
                stddev_expr="0.16 <= :value <= 0.18",
                percentile_expr=":value == 1",
                raise_yellow_alert=True
            )
        ]
