"""
DQ Checks for the core.embrace_metrics_log in Snowflake.
core.embrace_metrics_log table data has dependency on embrace metrics api

Upstream Owners: EMBRACE API TEAM
Downstream Owners: BA TEAM

EMBRACE METRICS POC: Peter(6/2023)

Checks:
    1. Ensures the total row count is not abnormal per day.
    2. Ensure columns that should have no null values don't suddenly have null values introduced.
    3. Ensure each metrics and clients are expected as per the requirement
    4. Ensure each metric counts for each client are not abnormal
"""

from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck
from de_utils.dq_checks_framework.check_classes.no_duplicate_rows_check import NoDuplicateRowsCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput

TABLE = "core.embrace_metrics_log"

TODO_LIST = [
    f"Check source table {TABLE} and upstream source is from the API"
    f" for possible missing data or other issues causing abnormality.",
    "If sums are to be expected and are regularly outside range, adjust the range."
]

NO_MISSING_DATA_COLS = ['date_utc', 'client_type', 'metric_name', 'app_version',
                        'api_timestamp', 'value', 'inserted_at']

MAIN_SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        ARRAY_UNIQUE_AGG(metric_name) AS metric_names,
        ARRAY_UNIQUE_AGG(client_type) AS client_types,
        SUM(CASE WHEN ((client_type = 'TN_ANDROID') AND (metric_name = 'daily_crashes_total')) THEN value ELSE 0 END) 
        AS tn_android_daily_crashes_total,
        SUM(CASE WHEN ((client_type = 'TN_ANDROID') AND (metric_name = 'daily_sessions_total')) THEN value ELSE 0 END) 
        AS tn_android_daily_sessions_total,
        SUM(CASE WHEN ((client_type = 'TN_ANDROID') AND (metric_name = 'daily_users')) THEN value ELSE 0 END) 
        AS tn_android_daily_users,
        SUM(CASE WHEN ((client_type = 'TN_IOS_FREE') AND (metric_name = 'daily_crashes_total')) THEN value ELSE 0 END) 
        AS tn_ios_free_daily_crashes_total,
        SUM(CASE WHEN ((client_type = 'TN_IOS_FREE') AND (metric_name = 'daily_sessions_total')) THEN value ELSE 0 END) 
        AS tn_ios_daily_sessions_total,
        SUM(CASE WHEN ((client_type = 'TN_IOS_FREE') AND (metric_name = 'daily_users')) THEN value ELSE 0 END) 
        AS tn_ios_free_daily_users,
        SUM(CASE WHEN ((client_type = '2L_ANDROID') AND (metric_name = 'daily_crashes_total')) THEN value ELSE 0 END) 
        AS secondline_android_daily_crashes_total,
        SUM(CASE WHEN ((client_type = '2L_ANDROID') AND (metric_name = 'daily_sessions_total')) THEN value ELSE 0 END) 
        AS secondline_android_daily_sessions_total,
        SUM(CASE WHEN ((client_type = '2L_ANDROID') AND (metric_name = 'daily_users')) THEN value ELSE 0 END) 
        AS secondline_android_daily_users
    FROM prod.{TABLE}
    WHERE ( date_utc = CAST(:run_date AS DATE))
"""

DUPES_COUNT_SQL = f"""
    SELECT COUNT(*) AS cnt_dups FROM (
        SELECT COUNT(*) AS cnt_dups
        FROM prod.{TABLE}
        WHERE
            (date_utc = CAST(:run_date AS DATE))
            AND (metric_name not in ('hourly_network_requests_external_failures', 'hourly_demo_us_and_ca_dau',
             'hourly_demo_authenticate_requests', 'hourly_anr_by_version', 'hourly_internal_network_requests_by_status',
             'five_minute_network_requests_external_failures', 'five_minute_internal_network_requests_by_status',
             'five_minute_demo_us_and_ca_dau', 'five_minute_demo_authenticate_requests' ,'five_minute_anr_by_version'))
        GROUP BY api_timestamp,client_type,metric_name,device_model,app_version,os_version
        HAVING COUNT(*) > 1
    );
"""


class EmbraceMetricsLogDQChecks(BaseDQChecks):
    name = "core_embrace_metrics_log DQ Checks"
    description = "Aggregate DQ Checks for core_embrace_metrics_log table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="core_embrace_metrics_log Main Input",
                alias="main_input",
                src_sql=MAIN_SQL
            ),

            DBOInput(
                name="core_embrace_metrics_log Dupes Count Input",
                alias="dupes_count",
                src_sql=DUPES_COUNT_SQL
            )
        ]

    def get_checks(self):
        return [

            TotalCountCheck(
                filter_date_column_name="date",
                total_count=self.main_input[0]["total_count"],
                lower_threshold=1000000,
                upper_threshold=1800000
            ),

            NoDuplicateRowsCheck(dups_count=self.dupes_count[0]["cnt_dups"]),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(
                input_object=self.main_input,
                column_names=NO_MISSING_DATA_COLS
            ),

            # Fail if a new value for column 'metric_name' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="metric_name",
                values=self.main_input[0]["metric_names"],
                expected_values=['crash_total', 'crashed_free_session_pct', 'crashed_session_pct', 'crashed_user_total',
                                 'daily_crash_free_session_by_device_rate', 'daily_crash_free_session_rate',
                                 'daily_crashed_users', 'daily_crashes_total', 'daily_sessions_by_device_total',
                                 'daily_sessions_total', 'daily_users', 'five_minute_anr_by_version',
                                 'five_minute_anr_free_sessions', 'five_minute_demo_authenticate_requests',
                                 'five_minute_demo_us_and_ca_dau', 'five_minute_internal_network_requests_by_status',
                                 'five_minute_network_requests_2xx', 'five_minute_network_requests_3xx',
                                 'five_minute_network_requests_4xx', 'five_minute_network_requests_5xx',
                                 'five_minute_network_requests_external_failures', 'hourly_anr_by_version',
                                 'hourly_anr_free_sessions', 'hourly_crash_free_session_by_device_rate',
                                 'hourly_crash_free_session_rate', 'hourly_crashed_users', 'hourly_crashes_total',
                                 'hourly_demo_authenticate_requests', 'hourly_demo_us_and_ca_dau',
                                 'hourly_internal_network_requests_by_status', 'hourly_network_requests_2xx',
                                 'hourly_network_requests_3xx', 'hourly_network_requests_4xx',
                                 'hourly_network_requests_5xx', 'hourly_network_requests_external_failures',
                                 'hourly_sessions_by_device_total', 'hourly_sessions_total', 'hourly_users',
                                 'sessions_by_device_model_total', 'sessions_total', 'user_total']
            ),

            # Fail if a new value for column 'client_type' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="client_type",
                values=self.main_input[0]["client_types"],
                expected_values=["TN_IOS_FREE", "TN_ANDROID", "2L_ANDROID"]
            ),

            Check(
                name="Total Count Check",
                description="Ensure sum total of tn_android_daily_crashes_total is within acceptable range",
                column_name="tn_android_daily_crashes_total",
                value=self.main_input[0]["tn_android_daily_crashes_total"],
                red_expr="18000 <= :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure sum total of tn_android_daily_sessions_total is within acceptable range",
                column_name="tn_android_daily_sessions_total",
                value=self.main_input[0]["tn_android_daily_sessions_total"],
                red_expr="30000000 <= :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure sum total of tn_android_daily_users is within acceptable range",
                column_name="tn_android_daily_users",
                value=self.main_input[0]["tn_android_daily_users"],
                red_expr="2000000 <= :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure sum total of tn_ios_free_daily_crashes_total is within acceptable range",
                column_name="tn_ios_free_daily_crashes_total",
                value=self.main_input[0]["tn_ios_free_daily_crashes_total"],
                red_expr="11000 <= :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure sum total of tn_ios_daily_sessions_total is within acceptable range",
                column_name="tn_ios_daily_sessions_total",
                value=self.main_input[0]["tn_ios_daily_sessions_total"],
                red_expr="4000000 <= :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure sum total of tn_ios_free_daily_users is within acceptable range",
                column_name="tn_ios_free_daily_users",
                value=self.main_input[0]["tn_ios_free_daily_users"],
                red_expr="500000 <= :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure sum total of secondline_android_daily_crashes_total is within acceptable range",
                column_name="secondline_android_daily_crashes_total",
                value=self.main_input[0]["secondline_android_daily_crashes_total"],
                red_expr="300 <= :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure sum total of secondline_android_daily_sessions_total is within acceptable range",
                column_name="secondline_android_daily_sessions_total",
                value=self.main_input[0]["secondline_android_daily_sessions_total"],
                red_expr="900000 <= :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure sum total of secondline_android_daily_users is within acceptable range",
                column_name="secondline_android_daily_users",
                value=self.main_input[0]["secondline_android_daily_users"],
                red_expr="95000 <= :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=TODO_LIST
            )
        ]
