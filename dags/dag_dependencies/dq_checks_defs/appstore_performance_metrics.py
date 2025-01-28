"""
DQ Checks for the performance metrics pulled from appstore in Snowflake.
Currently core.appstore_performance_metrics table data is pulled from the appstore api through app_store_api_report DAG

Upstream Owners: DE Team and IOS team.
Downstream Owners: DS Team(CoreExp POD)

Checks:
    1. Ensures the total row count is not abnormal per day.
    2. Ensure columns that should have no null values don't suddenly have null values introduced.
    3. Ensure percentile, app_version, device, platform values match as per expectation.
    4. Ensure no incomming duplicate records.
"""

from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck
from de_utils.dq_checks_framework.check_classes.no_duplicate_rows_check import NoDuplicateRowsCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck

TABLE = "core.appstore_performance_metrics"

NO_MISSING_DATA_COLS = ["dag_run_date", "percentile", "app_version", "points_value", "platform", "device",
                        "metrics_identifier", "inserted_at", "inserted_by"]

MAIN_SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        ARRAY_UNIQUE_AGG(device) AS device,
        ARRAY_UNIQUE_AGG(percentile) AS percentile,
        ARRAY_UNIQUE_AGG(metrics_identifier) AS metrics_identifier
    FROM prod.{TABLE}
    WHERE (DAG_RUN_DATE = CAST(:run_date AS DATE))
"""

DUPES_COUNT_SQL = f"""
    SELECT COUNT(*) AS cnt_dups FROM (
        SELECT COUNT(*) AS cnt_dups
        FROM prod.{TABLE}
        WHERE (DAG_RUN_DATE = CAST(:run_date AS DATE))
        GROUP BY dag_run_date, device, app_version, percentile
        HAVING COUNT(*) > 1
    );
"""


class AppstorePerformanceMetricsDQChecks(BaseDQChecks):
    name = "appstore_performance_metrics DQ Checks"
    description = "Aggregate DQ Checks for appstore_performance_metrics table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="appstore_performance_metrics Main Input",
                alias="main_input",
                src_sql=MAIN_SQL
            ),

            DBOInput(
                name="appstore_performance_metrics Dupes Count Input",
                alias="dupes_count",
                src_sql=DUPES_COUNT_SQL
            )
        ]

    def get_checks(self):
        return [

            Check(
                name="appstore_performance_metrics Total Count",
                description="Ensure appstore_performance_metrics is same",
                column_name="total_count",
                value=self.main_input[0]["total_count"],
                todo=["Check with Harish to see if the threshold needs to be updated."],
                yellow_expr=":value > 30",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr"
            ),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(
                input_object=self.main_input,
                column_names=NO_MISSING_DATA_COLS
            ),

            NoDuplicateRowsCheck(dups_count=self.dupes_count[0]["cnt_dups"]),

            # Fail if a new value for column 'vendor' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="metrics_identifier",
                values=self.main_input[0]["metrics_identifier"],
                expected_values=["launchTime"]
            ),

            # Fail if a new value for column 'percentile' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="percentile",
                values=self.main_input[0]["percentile"],
                expected_values=["percentile.fifty", "percentile.ninety"]
            )
        ]
