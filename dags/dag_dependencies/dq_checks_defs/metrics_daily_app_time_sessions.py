"""DQ Checks for the analytics.metrics_daily_app_time_sessions in Snowflake.
metrics_daily_app_time_sessions table data has direct dependencies to tables:
    core.leanplum_sessions, dau.bad_sets, dau.user_set

Upstream Owners: DE team owns all immediate tables upstream from this one
Downstream Owners: BA Team(SLT)

For detailed info, checkout the Excel sheet for current owners/usage:
    DataScience and Engineering/Documents/DE TEAM/SLT MODELS/SLT_DE_TRACKER.xlsx

Checks:
    1. Ensures the total row count for the day == 2.
    2. Ensure there are only to client_types as expected. One in each row.
    3. Based on the checks above, no need to check for dupes as it won't be possible.
    4. Ensure columns that should have no null values don't suddenly have null values introduced.
    5. Ensure Impressions counts are within normal range.
    6. Ensure time_in_app_hours_per_day counts are within normal range.
"""

from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck

DATABASE = "prod"
TABLE = "analytics.metrics_daily_app_time_sessions"

TODO_LIST = [
    (f"Check source table {TABLE} and upstream source tables: party_planner_realtime.applifecyclechanged, "
     "dau.bad_sets, dau.user_set for possible missing data or other issues causing abnormal sums."),
    "If sums are to be expected and are regularly outside range, adjust the range.",
    ("Temporarily disabling yellow alerts until historical data is available for the ML model. "
     "Will re-enable once the data is in place.")
]

NO_MISSING_DATA_COLS = ["date_utc", "client_type", "num_sessions", "time_in_app_mins_per_day",
                        "time_in_app_hours_per_day", "inserted_timestamp"]

SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        ARRAY_UNIQUE_AGG(client_type) AS client_types,
        SUM(CASE WHEN client_type = 'TN_IOS_FREE' THEN TIME_IN_APP_HOURS_PER_DAY ELSE 0 END) 
            AS ios_total_time_in_app_hours_per_day,
        SUM(CASE WHEN client_type = 'ANDROID (INCL 2L)' THEN TIME_IN_APP_HOURS_PER_DAY ELSE 0 END) 
            AS android_total_time_in_app_hours_per_day,
        SUM(CASE WHEN client_type = 'TN_IOS_FREE' THEN NUM_SESSIONS ELSE 0 END) AS ios_total_num_sessions,
        SUM(CASE WHEN client_type = 'ANDROID (INCL 2L)' THEN NUM_SESSIONS ELSE 0 END) AS android_total_num_sessions
    FROM {DATABASE}.{TABLE}
    WHERE (date_utc = CAST(:run_date AS DATE))
"""


class MetricsDailyAppTimeSessionsDQChecks(BaseDQChecks):
    name = "metrics_daily_app_time_sessions DQ Checks"
    description = "Aggregate DQ Checks for metrics_daily_app_time_sessions table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="metrics_daily_app_time_sessions Main Input",
                alias="main_input",
                src_sql=SQL
            )
        ]

    def get_checks(self):
        return [
            Check(
                name="metrics_daily_app_time_sessions Total Count",
                description="Ensure total count of rows for the run_date == 2 as expected",
                column_name="total_count",
                value=self.main_input[0]["total_count"],
                todo=[
                    "Investigate why there is more or less than two rows as there normally is.",
                    "Notify downstream users of potential issues this may raise.",
                    "If this change is expected, adjust this checks herein accordingly."
                ],
                get_previous_run_value=True,
                red_expr=":value == 2",  # 2 clients
                red_error="Only two rows per day are expected in this table. Currently the count is :value."
            ),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(
                input_object=self.main_input,
                column_names=NO_MISSING_DATA_COLS
            ),

            # Fail if a new value for column 'client_type' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="client_type",
                values=self.main_input[0]["client_types"],
                expected_values=["TN_IOS_FREE", "ANDROID (INCL 2L)"]
            ),

            # These checks will alert spikes and dips in the chart
            Check(
                name="Total Count Check",
                description="Ensure total counts for ANDROID (INCL 2L) stays within normal range.",
                column_name="android_total_num_sessions",
                value=self.main_input[0]["android_total_num_sessions"],
                use_dynamic_thresholds=False,
                red_expr="17000000 < :value < 23000000",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                # yellow_expr=":lower_threshold < :value < :upper_threshold*1.39 -0.2*:lower_threshold",
                # yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure total counts for ANDROID (INCL 2L) stays within normal range.",
                column_name="android_total_time_in_app_hours_per_day",
                value=self.main_input[0]["android_total_time_in_app_hours_per_day"],
                use_dynamic_thresholds=False,
                red_expr="300000 < :value < 460000",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                # yellow_expr=":lower_threshold < :value < :upper_threshold*1.39 -0.2*:lower_threshold",
                # yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure total counts for IOS stays within normal range.",
                column_name="ios_total_num_sessions",
                value=self.main_input[0]["ios_total_num_sessions"],
                use_dynamic_thresholds=False,
                red_expr="11000000 < :value < 14000000",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                # yellow_expr=":lower_threshold < :value < :upper_threshold*1.39 -0.2*:lower_threshold",
                # yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure total counts for IOS stays within normal range.",
                column_name="ios_total_time_in_app_hours_per_day",
                value=self.main_input[0]["ios_total_time_in_app_hours_per_day"],
                use_dynamic_thresholds=False,
                red_expr="130000 < :value < 190000",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                # yellow_expr=":lower_threshold < :value < :upper_threshold*1.39 -0.2*:lower_threshold",
                # yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            )
        ]
