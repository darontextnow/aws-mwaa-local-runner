"""DQ Checks for core.user_profiles_with_pi table in Snowflake."""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.no_duplicate_rows_check import NoDuplicateRowsCheck
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck

TABLE = "core.user_profiles_with_pi"
NO_MISSING_DATA_COLS = ["user_id_hex", "username"]


SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        SNOWFLAKE.CORE.DUPLICATE_COUNT(SELECT username FROM prod.{TABLE}) AS cnt_dup_username,
        SNOWFLAKE.CORE.DUPLICATE_COUNT(SELECT user_id_hex FROM prod.{TABLE}) AS cnt_dup_user_id_hex,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)}
    FROM prod.{TABLE}
    WHERE (date_utc = CAST(:run_date AS DATE))
"""


class UserProfilesWithPIDQChecks(BaseDQChecks):
    name = "user_profiles_with_pi DQ Checks"
    description = "DQ Checks for user_profiles_with_pi table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="Main Input",
                alias="main_input",
                src_sql=SQL
            )
        ]

    def get_checks(self):

        return [
            TotalCountCheck(
                filter_date_column_name="date_utc",
                total_count=self.main_input[0]["total_count"],
                lower_threshold=30000,
                upper_threshold=60000
            ),

            NoDuplicateRowsCheck(
                dups_count=self.main_input[0]["cnt_dup_user_id_hex"],
                name="No Duplicate user_id_hex",
                description="Ensure :table_name table has no duplicate user_id_hex.",
                todo=[
                    "The user_profiles model assumes there are no dupes in core.users.",
                    "Thus, this dupes issue must be addressed immediately by deleting/merging dupes"
                    " and fixing the issue upstream."
                ]
            ),

            NoDuplicateRowsCheck(
                dups_count=self.main_input[0]["cnt_dup_username"],
                name="No Duplicate Usernames",
                description="Ensure :table_name table has no duplicate usernames.",
                todo=[
                    "The user_profiles model assumes there are no dupes in core.users.",
                    "Thus, this dupes issue must be addressed immediately by deleting/merging dupes"
                    " and fixing the issue upstream."
                ]
            ),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(input_object=self.main_input, column_names=NO_MISSING_DATA_COLS),
        ]
