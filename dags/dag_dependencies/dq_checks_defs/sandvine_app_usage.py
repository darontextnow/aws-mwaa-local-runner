"""DQ Checks for the core.sandvine_app_usage in Snowflake.

Upstream Owners: DE Team(owns snowpipe)
Downstream Owners: BA Team(Alston)

Checks:
    1. Ensures the total row count is not abnormal
    2. Ensures there is no missing columns values or null values
    3. Ensures the rate of null values is withing the allowed limit
"""

from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck
from de_utils.dq_checks_framework.check_classes.missing_values_rate_check import MissingValuesRateCheck

TABLE = "core.sandvine_app_usage"

NO_MISSING_DATA_COLS = ["date_utc", "rxbytes", "subscriberip",
                        "subscriberid", "txbytes", "totalbytes", "uuid"]

MAIN_SQL = f"""
    SELECT 
        COUNT(*) AS total_count,
        SUM(CASE WHEN NVL(protocol, '') = '' THEN 1 ELSE 0 END) AS protocol_missing_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)}
    FROM prod.{TABLE}
    WHERE (date_utc = CAST(:run_date AS DATE))
"""

TODO_NOTE = [(f"Check source table {TABLE} and upstream source files: "
              f"s3://textnow-uploads-prod/sandvine/snowflake/AppUsageCDRs/ "
              f"for possible null values in the column or other issues causing this abnormality."),
             "Then reach out to Harish on which DQ Check is throwing the alert."
             ]


class SandvineAppUsageDQChecks(BaseDQChecks):
    name = "core_sandvine_app_usage hourly DQ Checks"
    description = "Aggregate DQ Checks for core_sandvine_app_usage table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="core_sandvine_app_usage Main Input",
                alias="main_input",
                src_sql=MAIN_SQL
            )
        ]

    def get_checks(self):
        return [

            TotalCountCheck(
                filter_date_column_name="date_utc",
                total_count=self.main_input[0]["total_count"],
                lower_threshold=5000000,
                upper_threshold=10000000,
                todo=TODO_NOTE
            ),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(input_object=self.main_input, column_names=NO_MISSING_DATA_COLS),

            # protocol should not be null, but sandvine may have some null. Ensure rate stays low until bug is fixed.
            MissingValuesRateCheck(
                column_name="protocol",
                missing_count=self.main_input[0]["protocol_missing_count"],
                total_count=self.main_input[0]["total_count"],
                threshold=0.000009,
                todo=TODO_NOTE + ["protocol should not be null, but sandvine may have some "
                                  "null values. Ensure rate stays low until bug is fixed."]
            )
        ]
