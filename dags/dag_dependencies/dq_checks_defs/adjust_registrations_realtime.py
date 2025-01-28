"""
DQ Checks for the adjust.registrations table in Snowflake.
adjust.registrations table holds registrations data collected through adjust relatime callbacks.
The DQ check will run every 4 hour, to make sure data is coming through the snowpipe (kind of heartbeat)
Upstream Owners: Adjust (3rd party) and Growth Team.

Downstream Owners: DS Team

Checks:
    1. Ensures the total row count is not abnormal per day.
    2. Ensure the mentioned columns do not have null values.
    3. Ensure registrations on ios and android platforms are within the range .
    4. Ensure registrations through organic and through the paid channels are within the range.
"""

from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.check import Check

TABLE = "adjust.registrations"

TODO_LIST = [
    f"Check source table {TABLE}, snowpipe and upstream firehose delivery streams and adjust callback configurations"
    f" for possible missing data or other issues causing abnormality.",
    "If sums are to be expected and are regularly outside range, adjust the range."
]

NO_MISSING_DATA_COLS = ["installed_at", "app_id", "adid"]

MAIN_SQL = f"""
    SELECT 
      COUNT(*) AS total_count,
      SUM(CASE WHEN UPPER(APP_ID) = '314716233' THEN 1 ELSE 0 END) AS ios_registrations,
      {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
      SUM(CASE WHEN UPPER(APP_ID) IN ('COM.ENFLICK.ANDROID.TEXTNOW','COM.ENFLICK.ANDROID.TN2NDLINE') THEN 1 ELSE 0 END)
          AS android_registrations,
      SUM(CASE WHEN is_organic THEN 1 ELSE 0 END) AS organic_registrations,
      SUM(CASE WHEN  not is_organic THEN 1 ELSE 0 END ) AS paid_registrations
    FROM prod.{TABLE}
    WHERE 
        (UPPER(APP_ID) IN ('COM.ENFLICK.ANDROID.TEXTNOW','COM.ENFLICK.ANDROID.TN2NDLINE','314716233'))
        AND (UPPER(country) IN ('US', 'CA'))
        AND (created_at BETWEEN CAST(:run_date AS TIMESTAMP) AND DATEADD(hour, 4, CAST(:run_date AS TIMESTAMP)))
"""


class AdjustRegistrationsRealtimeDQChecks(BaseDQChecks):
    name = "adjust_registrations DQ Checks"
    description = "Every 4 Hour DQ Check for adjust_registrations table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="adjust_registrations Main Input",
                alias="main_input",
                src_sql=MAIN_SQL
            )
        ]

    def get_checks(self):
        return [
            Check(
                name="Total Count Check",
                description="Ensure total registrations is above the lower threshold",
                column_name="count_total_registrations",
                value=self.main_input[0]["total_count"],
                red_expr="5400 <= :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=TODO_LIST
            ),


            NoMissingValuesCheck(
                input_object=self.main_input,
                column_names=NO_MISSING_DATA_COLS
            ),

            Check(
                name="Total Count Check",
                description="Ensure ios_registrations is within the normal expected range",
                column_name="count_ios_registrations",
                value=self.main_input[0]["ios_registrations"],
                red_expr="1100 <= :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure android_registrations is within the normal expected range",
                column_name="count_android_registrations",
                value=self.main_input[0]["android_registrations"],
                red_expr="2200 <= :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure organic_registrations is within the normal expected range",
                column_name="count_organic_registrations",
                value=self.main_input[0]["organic_registrations"],
                red_expr="2000 <= :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure paid_registrations is within the normal expected range",
                column_name="count_paid_registrations",
                value=self.main_input[0]["paid_registrations"],
                red_expr="2000 <= :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=TODO_LIST
            )
        ]
