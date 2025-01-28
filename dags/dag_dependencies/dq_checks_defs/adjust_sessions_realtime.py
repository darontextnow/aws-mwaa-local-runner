"""
DQ Checks for the adjust.sessions_with_pi table in Snowflake.
adjust.sessions_with_pi table holds sessions data collected through adjust relatime callbacks.
The DQ check will run every 4 hour, to make sure data is coming through the snowpipe (kind of heartbeat)
Upstream Owners: Adjust (3rd party) and Growth Team.

Downstream Owners: DS Team

Checks:
    1. Ensures the total row count is not abnormal per day.
    2. Ensure the mentioned columns do not have null values.
    3. Ensure sessions on ios and android platforms are within the range .
    4. Ensure sessions through organic and through the paid channels are within the range.
"""

from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.check import Check

TABLE = "adjust.sessions_with_pi"

TODO_LIST = [
    f"Check source table {TABLE}, snowpipe and upstream firehose delivery streams and adjust callback configurations"
    f" for possible missing data or other issues causing abnormality.",
    "If sums are to be expected and are regularly outside range, adjust the range."
]

NO_MISSING_DATA_COLS = ["installed_at", "app_id", "adid"]

MAIN_SQL = f"""
    SELECT 
      COUNT(*) AS total_count,
      SUM(CASE WHEN UPPER(APP_ID) = '314716233' THEN 1 ELSE 0 END) AS ios_sessions,
      {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
      SUM(CASE WHEN UPPER(APP_ID) IN ('COM.ENFLICK.ANDROID.TEXTNOW','COM.ENFLICK.ANDROID.TN2NDLINE') THEN 1 
          ELSE 0 END) AS android_sessions,
      SUM(CASE WHEN is_organic THEN 1 ELSE 0 END) AS organic_sessions,
      SUM(CASE WHEN NOT is_organic THEN 1 ELSE 0 END) AS paid_sessions
    FROM prod.{TABLE}
    WHERE 
        (UPPER(APP_ID) IN ('COM.ENFLICK.ANDROID.TEXTNOW','COM.ENFLICK.ANDROID.TN2NDLINE','314716233'))
        AND (UPPER(country) IN ('US', 'CA'))
        AND (created_at BETWEEN CAST(:run_date AS TIMESTAMP) AND DATEADD(hour, 4, CAST(:run_date AS TIMESTAMP)))
"""


class AdjustSessionsRealtimeDQChecks(BaseDQChecks):
    name = "adjust_sessions_with_pi DQ Checks"
    description = "Every 4 Hour DQ Check for adjust_sessions_with_pi table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="adjust_sessions_with_pi Main Input",
                alias="main_input",
                src_sql=MAIN_SQL
            )
        ]

    def get_checks(self):
        return [
            Check(
                name="Total Count Check",
                description="Ensure total sessions is above the lower threshold",
                column_name="count_total_sessions",
                value=self.main_input[0]["total_count"],
                red_expr="475000 <= :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=TODO_LIST
            ),


            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(
                input_object=self.main_input,
                column_names=NO_MISSING_DATA_COLS
            ),

            Check(
                name="Total Count Check",
                description="Ensure ios_sessions is within the normal expected range",
                column_name="count_ios_sessions",
                value=self.main_input[0]["ios_sessions"],
                red_expr="168000 <= :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure android_sessions is within the normal expected range",
                column_name="count_android_sessions",
                value=self.main_input[0]["android_sessions"],
                red_expr="305000 <= :value",
                red_error="The value (:value) has gone below the lower threshold: :red_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure organic_sessions is within the normal expected range",
                column_name="count_organic_sessions",
                value=self.main_input[0]["organic_sessions"],
                red_expr="220000 <= :value ",
                red_error="The value (:value) has gone below the lower threshold: :red_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure paid_sessions is within the normal expected range",
                column_name="count_paid_sessions",
                value=self.main_input[0]["paid_sessions"],
                red_expr="190000 <= :value",
                red_error="The value (:value) has gone below the lower threshold: :red_expr",
                todo=TODO_LIST
            )
        ]
