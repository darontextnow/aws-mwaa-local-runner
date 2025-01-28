"""DQ Checks for the analytics.adjust_installs in Snowflake.
adjust_installs table data has direct dependency to tables:
    adjust.installs_with_pi, adjust.raw_events, adjust.apps

For detailed info, checkout the Excel sheet for current owners/usage:
    DataScience and Engineering/Documents/DE TEAM/SLT MODELS/SLT_DE_TRACKER.xlsx

SLT specific checks:
    1. Ensures the total row count is not abnormal.
    2. Ensure columns that should have no null values don't suddenly have null values introduced.
    3. Ensure client based installs are within normal range.
    4. Ensure orgain/paid installs are within normal range.
    5. Ensure there are no duplicates.
    6. Ensure clients match as per the SLT requirements.
"""

from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck

DATABASE = "prod"
TABLE = "analytics_staging.adjust_installs"

TODO_LIST = [
    f"Check source table {TABLE} and upstream source tables: [adjust.installs_with_pi, adjust.raw_events, adjust.apps]"
    f" for possible missing data or other issues causing abnormality.",
    "If sums are to be expected and are regularly outside range, adjust the range."
]

NO_MISSING_DATA_COLS = ["installed_at", "client_type", "adjust_id"]

MAIN_SQL = f"""
    SELECT COUNT(*) AS total_count,
        SUM(CASE WHEN UPPER(client_type) = 'TN_IOS_FREE' THEN 1 ELSE 0 END) AS ios_installs,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        SUM(CASE WHEN UPPER(client_type) IN ('TN_ANDROID', '2L_ANDROID') THEN 1 ELSE 0 END) AS android_installs,
        SUM(CASE WHEN IS_ORGANIC = 'TRUE' THEN 1 END) AS organic_installs,
        SUM(CASE WHEN IS_ORGANIC = 'FALSE' THEN 1 END) AS paid_installs
    FROM {DATABASE}.{TABLE}
    LEFT JOIN prod.fraud_alerts.installs USING (adjust_id)
    WHERE 
        COALESCE(score, 1.0) >= 0.8
        AND is_untrusted = FALSE
        AND UPPER(client_type) IN ('TN_ANDROID', 'TN_IOS_FREE', '2L_ANDROID')
        AND UPPER(country_code) IN ('US', 'CA')
        AND (CAST(INSTALLED_AT AS DATE) = CAST(:run_date AS DATE))
"""


class AdjustInstallsDQChecks(BaseDQChecks):
    name = "adjust_installs DQ Checks"
    description = "Aggregate DQ Checks for adjust_installs table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="adjust_installs Main Input",
                alias="main_input",
                src_sql=MAIN_SQL
            )
        ]

    def get_checks(self):

        return [
            Check(
                name="Total Count Check",
                description="Ensure total installs is within the normal expected range",
                column_name="count_total_installs",
                value=self.main_input[0]["total_count"],
                yellow_expr="100000 <= :value",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            ),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(
                input_object=self.main_input,
                column_names=NO_MISSING_DATA_COLS
            ),

            # These checks will alert spikes and dips in the SLT chart
            Check(
                name="Total Count Check",
                description="Ensure ios_installs is within the normal expected range",
                column_name="count_ios_installs",
                value=self.main_input[0]["ios_installs"],
                yellow_expr="18000 <= :value",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure android_installs is within the normal expected range",
                column_name="count_android_installs",
                value=self.main_input[0]["android_installs"],
                yellow_expr="39000 <= :value",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure organic_installs is within the normal expected range",
                column_name="count_organic_installs",
                value=self.main_input[0]["organic_installs"],
                yellow_expr="34000 <= :value",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure paid_installs is within the normal expected range",
                column_name="count_paid_installs",
                value=self.main_input[0]["paid_installs"],
                yellow_expr="21000 <= :value",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            )
        ]
