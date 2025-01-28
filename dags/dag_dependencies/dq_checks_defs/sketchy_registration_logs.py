"""DQ Checks for the core.sketchy_registration_logs in Snowflake.

These DQ Checks are currently designed to run hourly after the target table is updated.

core.sketchy_registration_logs table data has dependency on DS ML model features

Upstream Owners: DE Team(owns airflow dag) and DS Team (owns the ML spark script).
Downstream Owners: DS Team(TnS)
"""

from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck

TABLE = "core.sketchy_registration_logs"
NO_MISSING_DATA_COLS = ["event_timestamp", "date_utc", "client_type"]
NO_NULL_VAL_COLS = ["username"]

TODO_LIST = [
    (f"Check source table {TABLE} and upstream source files: s3a://tn-data-lake/incoming/sketchy_analysis/ "
     "for possible missing data or other issues causing abnormality."),
    "If hourly counts are to be expected and are regularly outside hourly thresholds, adjust the thresholds."
]

MAIN_SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        {NoMissingValuesCheck.get_null_counts_sql(NO_NULL_VAL_COLS)},
        {NoMissingValuesCheck.get_empty_counts_sql(NO_NULL_VAL_COLS)},
        ARRAY_UNIQUE_AGG(client_type) AS client_types
    FROM {TABLE}
    WHERE 
        (CAST(event_timestamp AS DATE) = CAST(:run_date AS DATE))
        AND (EXTRACT(HOUR FROM event_timestamp) = EXTRACT(HOUR FROM TO_TIMESTAMP(:run_date)))
"""


class SketchyRegistrationLogsDQChecks(BaseDQChecks):
    name = "sketchy_registration_logs hourly DQ Checks"
    description = "Aggregate DQ Checks for core_sketchy_registration_logs table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="sketchy_registration_logs Main Input",
                alias="main_input",
                src_sql=MAIN_SQL
            )
        ]

    def get_checks(self):
        return [
            Check(
                name="Total Count Check",
                description="Ensure the total counts are not going below the lower threshold",
                column_name="COUNT(*)",
                value=self.main_input[0]["total_count"],
                red_expr="2000 <= :value",
                red_error="The value (:value) is below the lower threshold: :red_expr",
                todo=TODO_LIST
            ),

            Check(
                name="No Null Usernames Check",
                description="Ensure username column has no NULL values",
                column_name="username",
                value=self.main_input[0]["username_null"],
                red_expr=":value == 0",
                red_error=":value records found with NULL username",
                todo=["There should be no NULL values for username.",
                      "Talk to sketchy team about fixing whatever issue they introduced to allow NULLs."]
            ),

            Check(
                name="Empty username values",
                description="Ensure propensity of empty username values remains low",
                column_name="username",
                value=self.main_input[0]["username_empty"],
                red_expr=":value < 20",
                red_error=":value records found with empty ('') username",
                todo=["This number needs to remain as low as possible.",
                      "Talk to sketchy team about fixing any issue introduced to allow for more empty values."]
            ),

            NoMissingValuesCheck(input_object=self.main_input, column_names=NO_MISSING_DATA_COLS),

            MatchExpectedValuesCheck(
                column_name="client_type",
                values=self.main_input[0]["client_types"],
                expected_values=["TN_IOS_FREE", "TN_ANDROID", "TN_WEB", "2L_ANDROID"]
            )
        ]
