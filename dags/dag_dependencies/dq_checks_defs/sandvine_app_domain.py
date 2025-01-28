"""DQ Checks for the core.sandvine_app_domain in Snowflake.

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

TABLE = "core.sandvine_app_domain"

NO_MISSING_DATA_COLS = ["appname", "date_utc", "starttime",
                        "stoptime", "subscriberip", "subscriberid", "uuid"]

MAIN_SQL = f"""
    SELECT 
        COUNT(*) AS total_count,
        SUM(CASE WHEN NVL(domainname, '') = '' THEN 1 ELSE 0 END) AS domainname_missing_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
    FROM prod.{TABLE}
    WHERE (date_utc = CAST(:run_date AS DATE))
"""

TODO_NOTE = [(f"Check source table {TABLE} and upstream source files: "
              f"s3://textnow-uploads-prod/sandvine/snowflake/appDomainCDRs/ "
              f"for possible null values in the column or other issues causing this abnormality."),
             "Then reach out to Harish on which DQ Check is throwing the alert."
             ]


class SandvineAppDomainDQChecks(BaseDQChecks):
    name = "core_sandvine_app_domain hourly DQ Checks"
    description = "Aggregate DQ Checks for core_sandvine_app_domain table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="core_sandvine_app_domain Main Input",
                alias="main_input",
                src_sql=MAIN_SQL
            )
        ]

    def get_checks(self):
        return [

            TotalCountCheck(
                filter_date_column_name="date_utc",
                total_count=self.main_input[0]["total_count"],
                lower_threshold=500000,
                upper_threshold=1700000,
                todo=TODO_NOTE
            ),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(input_object=self.main_input, column_names=NO_MISSING_DATA_COLS),

            # domainname should not be null, but sandvine may have some null. Ensure rate stays low until bug is fixed.
            MissingValuesRateCheck(
                column_name="domainname",
                missing_count=self.main_input[0]["domainname_missing_count"],
                total_count=self.main_input[0]["total_count"],
                threshold=0.000005,
                todo=TODO_NOTE + ["domainname should not be null, but sandvine may have some "
                                  "null values. Ensure rate stays low until bug is fixed."]
            )
        ]
