"""
DQ Checks for the core.registrations table in Snowflake.

The checks defined below are set up to run once a day to ensure data added to Snowflake
core.registrations table for run_date meets our requirements for data quality.

Notes:
    Some of these checks test assumptions made by prod.core.user_profiles_with_pi model to ensure upstream integrity.

"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck
from de_utils.dq_checks_framework.check_classes.no_duplicate_rows_check import NoDuplicateRowsCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck
from de_utils.constants import SF_WH_MEDIUM

TABLE = "core.registrations"
NO_MISSING_DATA_COLS = ["created_at", "http_response_status", "username", "client_type",
                        "client_ip", "is_organic"]

SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        SNOWFLAKE.CORE.DUPLICATE_COUNT(SELECT username FROM prod.{TABLE}) AS cnt_dups,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        ARRAY_UNIQUE_AGG(client_type) AS client_types
        --ARRAY_UNIQUE_AGG(gender) AS genders  --there have not been any non-null gender in this table since 2020-12-08
    FROM prod.{TABLE}
    WHERE (created_at BETWEEN CAST(:run_date AS TIMESTAMP_NTZ) 
        AND CAST(TIMESTAMP_FROM_PARTS(:run_date, '23:59:59') AS TIMESTAMP_NTZ))
"""


class CoreRegistrationsDQChecks(BaseDQChecks):
    name = "core.registrations DQ Checks"
    description = "Aggregate DQ Checks for core.registrations table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="core_registrations Prod Main Input",
                alias="main_input",
                src_sql=SQL,
                warehouse=SF_WH_MEDIUM
            )
        ]

    def get_checks(self):

        return [
            TotalCountCheck(
                filter_date_column_name="created_at",
                total_count=self.main_input[0]["total_count"],
                lower_threshold=100000,  # Counts in Q1 2023 low was 150K.
                upper_threshold=600000
            ),

            NoDuplicateRowsCheck(dups_count=self.main_input[0]["cnt_dups"]),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(input_object=self.main_input, column_names=NO_MISSING_DATA_COLS),

            # Fail if a new value for column 'client_type' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="client_type",
                values=self.main_input[0]["client_types"],
                expected_values=["TN_IOS_FREE", "TN_ANDROID", "TN_WEB", "2L_ANDROID"]
            )
        ]
