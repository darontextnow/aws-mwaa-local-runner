"""
DQ Checks for the core.iap in Snowflake.
core.iap table data has dependency on IAP dynamodb table and a direct dependency to tables:
    party_planner_realtime.streamrecord

Upstream Owners: DE Team(party_planner_realtime.streamrecord snowpipe) and IAP Team (Dyanmodb IAP table).
Downstream Owners: DS Team(TnS)

IAP POC: Chris Harrington(3/2023)

Checks:
    1. Ensures the total row count is not abnormal per day.
    2. Ensure columns that should have no null values don't suddenly have null values introduced.
    3. Ensure vendor, producttype, status, dynamodb events match as per expectation.
    4. Ensure no incomming duplicate records.
"""

from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck
from de_utils.dq_checks_framework.check_classes.no_duplicate_rows_check import NoDuplicateRowsCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck

TABLE = "core.iap"

NO_MISSING_DATA_COLS = ["pk", "sk", "userid", "producttype", "productid", "status",
                        "eventname", "inserted_timestamp", "updated_timestamp"]

MAIN_SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        ARRAY_UNIQUE_AGG(vendor) AS vendor,
        ARRAY_UNIQUE_AGG(producttype) AS producttype,
        ARRAY_UNIQUE_AGG(productid) AS productid,
        ARRAY_UNIQUE_AGG(status) AS status,
        ARRAY_UNIQUE_AGG(eventname) AS eventname
    FROM prod.{TABLE}
    WHERE (CAST(inserted_timestamp AS DATE) = CAST(:run_date AS DATE))
"""

DUPES_COUNT_SQL = f"""
    SELECT COUNT(*) AS cnt_dups FROM (
        SELECT COUNT(*) AS cnt_dups
        FROM prod.{TABLE}
        WHERE (CAST(inserted_timestamp AS DATE) = CAST(:run_date AS DATE))
        GROUP BY pk, sk
        HAVING COUNT(*) > 1
    );
"""


class CoreIAPDQChecks(BaseDQChecks):
    name = "core_iap DQ Checks"
    description = "Aggregate DQ Checks for core_iap table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="core_iap Main Input",
                alias="main_input",
                src_sql=MAIN_SQL
            ),

            DBOInput(
                name="core_iap Dupes Count Input",
                alias="dupes_count",
                src_sql=DUPES_COUNT_SQL
            )
        ]

    def get_checks(self):
        return [

            TotalCountCheck(
                filter_date_column_name="date",
                total_count=self.main_input[0]["total_count"],
                lower_threshold=40000,
                upper_threshold=140000
            ),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(
                input_object=self.main_input,
                column_names=NO_MISSING_DATA_COLS
            ),

            NoDuplicateRowsCheck(dups_count=self.dupes_count[0]["cnt_dups"]),

            # Fail if a new value for column 'eventname' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="eventname",
                values=self.main_input[0]["eventname"],
                expected_values=["INSERT", "MODIFY"]
            ),

            # Fail if a new value for column 'vendor' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="vendor",
                values=self.main_input[0]["vendor"],
                expected_values=["APP_STORE", "PLAY_STORE"]
            ),

            # Fail if a new value for column 'producttype' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="producttype",
                values=self.main_input[0]["producttype"],
                expected_values=["IAP_TYPE_CONSUMABLE", "IAP_TYPE_SUBSCRIPTION"]
            ),

            # Fail if a new value for column 'status' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="status",
                values=self.main_input[0]["status"],
                expected_values=["CANCEL", "DID_CHANGE_RENEWAL_STATUS", "DID_FAIL_TO_RENEW",
                                 "DID_RECOVER", "DID_RENEW", "INITIAL_BUY", "INTERACTIVE_RENEWAL",
                                 "ONE_TIME_PRODUCT_CANCELED", "ONE_TIME_PRODUCT_PURCHASED",
                                 "SUBSCRIPTION_CANCELED", "SUBSCRIPTION_EXPIRED", "SUBSCRIPTION_IN_GRACE_PERIOD",
                                 "SUBSCRIPTION_ON_HOLD", "SUBSCRIPTION_PAUSED", "SUBSCRIPTION_PAUSE_SCHEDULE_CHANGED",
                                 "SUBSCRIPTION_PURCHASED", "SUBSCRIPTION_RECOVERED", "SUBSCRIPTION_RENEWED",
                                 "SUBSCRIPTION_RESTARTED", "SUBSCRIPTION_REVOKED", "ONE_TIME_PRODUCT_PENDING"]
            )
        ]
