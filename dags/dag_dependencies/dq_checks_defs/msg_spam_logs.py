"""DQ Checks for the core.cm_msg_spam_logs in Snowflake.
core.cm_msg_spam_logs table data has dependency on DS ML model features

Upstream Owners: DE Team(owns airflow dag) and DS Team (owns the ML spark script).
Downstream Owners: DS Team(TnS)

POC: Dheeraj(5/2023)

Checks:
    1. Ensures the total row count is not abnormal
"""

from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput

TABLE = "core.cm_msg_spam_logs"

TODO_LIST = [
    (f"Check source table {TABLE} and upstream source files: "
     "s3a://tn-snowflake-prod/logs/kafka-connect-ds/k8s-container.trust-and-safety.message-spam/ "
     "for possible missing data or other issues causing abnormality."),
    "If sums are to be expected and are regularly outside range, adjust the range."
]

MAIN_SQL = f"""
    SELECT COUNT(*) AS total_count
    FROM prod.{TABLE}
    WHERE 
        (CAST(source_file_time AS DATE) = CAST(:run_date AS DATE))
        AND (EXTRACT(HOUR FROM source_file_time) = EXTRACT(HOUR FROM TO_TIMESTAMP(:run_date)))
"""


class MsgSpamLogsDQChecks(BaseDQChecks):
    name = "core_cm_msg_spam_logs hourly DQ Checks"
    description = "Aggregate DQ Checks for core_cm_msg_spam_logs table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="core_cm_msg_spam_logs Main Input",
                alias="main_input",
                src_sql=MAIN_SQL
            )
        ]

    def get_checks(self):
        return [

            TotalCountCheck(
                filter_date_column_name="source_file_time",
                total_count=self.main_input[0]["total_count"],
                lower_threshold=14000,
                upper_threshold=1500000,
                todo=TODO_LIST
            )
        ]
