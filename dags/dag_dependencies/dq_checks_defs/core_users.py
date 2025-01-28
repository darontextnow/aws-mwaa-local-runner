"""
DQ Checks for the core.users table in Snowflake.

The checks defined below are set up to run once a day to ensure data added to Snowflake
core.users table for run_date meets our requirements for data quality.

Notes:
    Some of these checks test assumptions made by prod.core.user_profiles_with_pi model to ensure upstream integrity.

"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.no_duplicate_rows_check import NoDuplicateRowsCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck
from de_utils.constants import SF_WH_MEDIUM

TABLE = "core.users"
NO_MISSING_DATA_COLS = ["user_id_hex", "username", "last_update", "timestamp"]

SQL = f"""
    SELECT
        COUNT(*) AS count_records_updated,
        SNOWFLAKE.CORE.DUPLICATE_COUNT(SELECT username FROM prod.{TABLE}) AS cnt_dup_username,
        SNOWFLAKE.CORE.DUPLICATE_COUNT(SELECT user_id_hex FROM prod.{TABLE}) AS cnt_dup_user_id_hex,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        ARRAY_UNIQUE_AGG(gender) AS genders
    FROM prod.{TABLE}
    WHERE 
        (timestamp >= CAST(:run_date AS TIMESTAMP_NTZ))
        AND (timestamp < CAST(:run_date AS TIMESTAMP_NTZ) + INTERVAL '1 DAY')
"""


class CoreUsersDQChecks(BaseDQChecks):
    name = "core.users DQ Checks"
    description = "Aggregate DQ Checks for core.users table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="core_users Prod Main Input",
                alias="main_input",
                src_sql=SQL,
                warehouse=SF_WH_MEDIUM
            )
        ]

    def get_checks(self):

        return [
            Check(
                name="Count Updated Records",
                description="Ensure count of records updated by mysql shards stays within expected norms.",
                column_name="COUNT(*)",
                value=self.main_input[0]["count_records_updated"],
                todo=["Range out of norm could mean there is an issue with MySQL shard data or our sync.",
                      "Check with backend team to confirm if number of records being updated is correct.",
                      "Ensure we are still receiving all updates from MySQL shards."],
                # adjusting the upper threshold up way above normal here while T&S does a large disable campaign
                # TODO: adjust upper threshold back down to a normal level once T&S disables are normalized
                yellow_expr="100000 <= :value <= 20000000",
                yellow_warning="Count of updated records is outside normal range (:yellow_expr)"
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

            # Fail if a new value for column 'client_type' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="gender",
                values=self.main_input[0]["genders"],
                expected_values=["M", "F"]
            )

        ]
