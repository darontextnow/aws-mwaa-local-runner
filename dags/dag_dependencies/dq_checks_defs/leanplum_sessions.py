"""
DQ Checks for the core.leanplum_sessions table in Snowflake.

The checks defined below are set up to run once a day to ensure data added to Snowflake
core.leanplum_sessions table for run_date meets our requirements for data quality.

Notes:
    Currently only implementing minimal checks to ensure data isn't missing/duplicated. Expand checks as needed.
    Data is loaded by leanplum_export_date, but we report by time column. There is a significant chunk of data
        coming in for time up to two days ago. Thus, 1 day lag thresholds will not be the same as 2 day lag.
"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck
from de_utils.dq_checks_framework.check_classes.no_duplicate_rows_check import NoDuplicateRowsCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck

TABLE = "core.leanplum_sessions"
NO_MISSING_DATA_COLS = [
    "session_id", "user_id", "app", "user_bucket", "time", "first_run", "timezone_offset_seconds",
    "leanplum_export_date", "is_session", "sdk_version", "duration", "prior_time_spent_in_app", "prior_states",
    "prior_events", "prior_sessions"
]

SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        ARRAY_UNIQUE_AGG(app) AS apps,
        ARRAY_UNIQUE_AGG(client) AS clients
    FROM prod.{TABLE}
    WHERE (time BETWEEN CAST(:run_date AS TIMESTAMP_NTZ) 
        AND CAST(TIMESTAMP_FROM_PARTS(:run_date, '23:59:59') AS TIMESTAMP_NTZ))
"""

DUPES_COUNT_SQL = f"""
    SELECT COUNT(*) AS cnt_dups FROM (
        SELECT COUNT(*) AS cnt_dups
        FROM prod.{TABLE}
        WHERE (time BETWEEN CAST(:run_date AS TIMESTAMP_NTZ) 
            AND CAST(TIMESTAMP_FROM_PARTS(:run_date, '23:59:59') AS TIMESTAMP_NTZ))
        GROUP BY session_id, user_id
        HAVING COUNT(*) > 1
    );
"""


class LeanplumSessionsDQChecks(BaseDQChecks):
    name = "leanplum_sessions DQ Checks"
    description = "Aggregate DQ Checks for leanplum_sessions table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="leanplum_sessions Prod Main Input",
                alias="main_input",
                src_sql=SQL
            ),

            DBOInput(
                name="leanplum_sessions Dupes Count Input",
                alias="dupes_count",
                src_sql=DUPES_COUNT_SQL
            )
        ]

    def get_checks(self):

        return [
            TotalCountCheck(
                filter_date_column_name="date_utc",
                total_count=self.main_input[0]["total_count"],
                lower_threshold=20000000,  # Counts in Q1 2023 are steadily over 25M, but adjusting down for lag.
                upper_threshold=90000000,  # duplicate check already in place, 65M was high for Q1 2023
                todo=["Query prod.core.leanplum_sessions table to see which app(s) count(s) by DATE(time) are low.",
                      "Send a note to leanplum support team in #leanplum_support to inquire about delays in sessions.",
                      "Once issues is resolved, rerun LP DQ Checks and downstream tasks (including snowflake_etl_1d)."]
            ),

            NoDuplicateRowsCheck(dups_count=self.dupes_count[0]["cnt_dups"]),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(input_object=self.main_input, column_names=NO_MISSING_DATA_COLS),

            # Fail if a new value for column 'app' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="app",
                values=self.main_input[0]["apps"],
                expected_values=["TN_IOS_FREE", "TN_ANDROID", "2L_ANDROID"]
            ),

            # Fail if a new value for column 'app' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="client",
                values=self.main_input[0]["clients"],
                expected_values=["__unsubscribeNew__", "__push__", "null", "ios", "android"]
            )
        ]
