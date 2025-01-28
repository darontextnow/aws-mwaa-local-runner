"""
DQ Checks for the core.messages table in Snowflake.

The checks defined below are set up to run once a day to ensure data added to Snowflake
core.messages table for run_date meets our requirements for data quality.

Notes:
    Currently only implementing minimal checks to ensure data isn't missing/duplicated. Expand checks as needed.
    There are a lot of duplicate rows (all columns duplicated) in this table. Not sure if that's expected or not.

"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck

TABLE = "core.messages"
NO_MISSING_DATA_COLS = ["created_at", "http_response_status", "client_type"]

SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        SUM(CASE WHEN (username IS NULL) AND (error_code IS NULL) THEN 1 ELSE 0 END) AS username_missing,
        SUM(CASE WHEN (contact_value IS NULL) AND (error_code IS NULL) THEN 1 ELSE 0 END) AS contact_value_missing,
        ARRAY_UNIQUE_AGG(client_type) AS client_types
    FROM prod.{TABLE}
    WHERE (created_at BETWEEN CAST(:run_date AS TIMESTAMP_NTZ) 
        AND CAST(TIMESTAMP_FROM_PARTS(:run_date, '23:59:59') AS TIMESTAMP_NTZ))
"""


class CoreMessagesDQChecks(BaseDQChecks):
    name = "Core.Messages DQ Checks"
    description = "Aggregate DQ Checks for core.messages table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="core_messages Prod Main Input",
                alias="main_input",
                src_sql=SQL
            )
        ]

    def get_checks(self):

        return [
            TotalCountCheck(
                filter_date_column_name="created_at",
                total_count=self.main_input[0]["total_count"],
                lower_threshold=20000000,  # Counts in Q1 2023 are steadily over 20M.
                upper_threshold=60000000
            ),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(input_object=self.main_input, column_names=NO_MISSING_DATA_COLS),

            Check(
                name="No Missing Values Check",
                description="username should not be missing when error_code is NULL",
                column_name="username",
                value=self.main_input[0]["username_missing"],
                todo=["username should not be missing when error_code is NULL",
                      "Check upstream and try to determine why username is missing."],
                yellow_expr=":value == 0",
                yellow_warning=":value records were found with username missing while error_code is NULL"
            ),

            Check(
                name="No Missing Values Check",
                description="contact_value should not be missing when error_code is NULL",
                column_name="contact_value",
                value=self.main_input[0]["contact_value_missing"],
                todo=["contact_value should not be missing when error_code is NULL",
                      "Check upstream and try to determine why contact_value is missing."],
                yellow_expr=":value == 0",
                yellow_warning=":value records were found with contact_value missing while error_code is NULL"
            ),

            # Fail if a new value for column 'client_type' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="client_type",
                values=self.main_input[0]["client_types"],
                expected_values=["TN_IOS_FREE", "TN_ANDROID", "TN_WEB", "2L_ANDROID"]
            )
        ]
