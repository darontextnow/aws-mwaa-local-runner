"""DQ Checks for the dq_checks metadata tables in Snowflake.

NOTE: DEPRECATED THESE DQ CHECKS PER DS-2514 ON 3/11/2024.
KEEPING QUERIES AROUND IN CASE NEEDED IN THE FUTURE.
"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck

DQ_CHECKS_NO_MISSING_DATA_COLS = ["check_id", "executed_at", "checks_name", "checks_description",
                                  "check_name", "check_description", "status", "alert_status",
                                  "value", "inserted_at"]
DQ_CHECKS_INPUTS_NO_MISSING_DATA_COLS = ["input_id", "check_id", "source", "name", "alias", "index",
                                         "inserted_at", "code"]

DQ_CHECKS_SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(DQ_CHECKS_NO_MISSING_DATA_COLS)},
        SUM(CASE WHEN status IN ('RED', 'YELLOW') AND NVL(check_error, '') = '' THEN 1 ELSE 0 END) 
            AS check_error_missing,
        SUM(CASE WHEN status <> 'GREEN' AND alert_status = 'NO_ALERT_SENT' THEN 1 ELSE 0 END) 
            AS alert_not_sent,
        SUM(CASE WHEN status = 'ERROR' AND NVL(run_error, '') = '' THEN 1 ELSE 0 END) 
            AS run_error_missing,
        SUM(CASE WHEN status = 'RED' AND NVL(red_expr, '') = '' THEN 1 ELSE 0 END) 
            AS red_expr_missing,
        SUM(CASE WHEN status = 'YELLOW' AND NVL(yellow_expr, '') = '' THEN 1 ELSE 0 END) 
            AS yellow_expr_missing,
        SUM(CASE WHEN NVL(red_expr, '') = '' AND NVL(yellow_expr, '') = '' THEN 1 ELSE 0 END) 
            AS red_and_yellow_expr_missing,
        ARRAY_UNIQUE_AGG(status) AS status,
        ARRAY_UNIQUE_AGG(alert_status) AS alert_status
    FROM prod.analytics.dq_checks
    WHERE (DATE(inserted_at) = CAST(:run_date AS DATE))
"""

# the following query will return any DQ checks names which ran yesterday, but did not run today
MISSING_DQ_CHECKS_SQL = """
    SELECT ARRAY_UNIQUE_AGG(yesterday.name) AS missing_dq_checks
    FROM (
        SELECT name
        FROM prod.core.dq_checks
        WHERE (DATE(executed_at) = DATEADD(DAY, -1, CAST(:run_date AS DATE)))
        GROUP BY 1
    ) yesterday
    LEFT JOIN(
        SELECT name
        FROM prod.core.dq_checks
        WHERE (DATE(executed_at) = CAST(:run_date AS DATE))
        GROUP BY 1
    ) today ON (yesterday.name = today.name)
    WHERE (today.name IS NULL);
"""

NO_MISSING_INPUTS_SQL = """
    SELECT COUNT(DISTINCT c.check_id) AS count_missing_inputs
    FROM (
        SELECT DISTINCT check_id FROM prod.core.dq_checks WHERE (DATE(executed_at) = CAST(:run_date AS DATE))
    ) c
    LEFT JOIN (
        SELECT DISTINCT check_id FROM prod.core.dq_checks_inputs WHERE (DATE(inserted_at) = CAST(:run_date AS DATE))
    ) i ON (c.check_id = i.check_id)
    WHERE (i.check_id IS NULL)
"""

DQ_CHECKS_INPUTS_SQL = f"""
    SELECT
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(DQ_CHECKS_INPUTS_NO_MISSING_DATA_COLS)}
    FROM prod.analytics.dq_checks_inputs
    WHERE (DATE(inserted_at) = CAST(:run_date AS DATE))
"""

CODE_IS_DISTINCT_SQL = """
    SELECT COUNT(*) AS cnt_dups FROM (
        SELECT COUNT(*) AS cnt_dups
        FROM prod.core.dq_checks_inputs_code
        WHERE (DATE(inserted_at) >= DATEADD(DAY, -7, CAST(:run_date AS DATE)))
        GROUP BY code
        HAVING COUNT(*) > 1
    );
"""


class DQChecks(BaseDQChecks):
    name = "dq_checks metadata tables DQ Checks"
    description = "DQ Checks for dq_checks reporting tables"
    table_name = "core.dq_checks"

    def get_inputs(self):
        return [
            DBOInput(name="DQ Checks Input",
                     alias="dq_checks_input",
                     src_sql=DQ_CHECKS_SQL),

            DBOInput(name="Missing DQ Checks Input",
                     alias="missing_dq_checks",
                     src_sql=MISSING_DQ_CHECKS_SQL),

            DBOInput(name="Missing Inputs",
                     alias="missing_inputs",
                     src_sql=NO_MISSING_INPUTS_SQL),

            DBOInput(name="DQ Checks Inputs Input",
                     alias="dq_checks_inputs_input",
                     src_sql=DQ_CHECKS_INPUTS_SQL),

            DBOInput(name="DQ Checks Duplicate Code Input",
                     alias="duplicate_code_input",
                     src_sql=CODE_IS_DISTINCT_SQL)
        ]

    def get_checks(self):

        return [
            TotalCountCheck(
                filter_date_column_name="inserted_at",
                total_count=self.dq_checks_input[0]["total_count"],
                lower_threshold=200,  # current min, will be growing for a while. Adjust later.
                upper_threshold=10000  # It's going to be growing for a while. Adjust later
            ),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(
                input_object=self.dq_checks_input,
                column_names=DQ_CHECKS_NO_MISSING_DATA_COLS,
                todo=["Email Daron this alert details for follow as there appears to be a issue in dq_checks code."]
            ),

            Check(
                name="Missing check_error",
                description="Ensure when status is RED or YELLOW that there is a check_error to go with it.",
                column_name="check_error",
                value=self.dq_checks_input[0]["check_error_missing"],
                red_expr=":value == 0",
                red_error=":value rows have a missing check_error(s).",
                todo=["Email Daron this alert details for follow as there appears to be a issue in dq_checks code."]
            ),

            Check(
                name="Invalid Alert Status",
                description="Alert Status should be RED_ALERT_SENT or YELLOW_ALERT_SENT, but it is NO_ALERT_SENT",
                column_name="alert_status",
                value=self.dq_checks_input[0]["alert_not_sent"],
                yellow_expr=":value == 0",
                yellow_warning=":value rows have invalid alert_status = NO_ALERT_SENT.",
                todo=["Email Daron this alert details for follow as there appears to be a issue in dq_checks code."]
            ),

            Check(
                name="Missing run_error",
                description="Ensure when status = ERROR that there is a run_error to go with it.",
                column_name="run_error",
                value=self.dq_checks_input[0]["run_error_missing"],
                red_expr=":value == 0",
                red_error=":value rows have a missing run_error(s).",
                todo=["Email Daron this alert details for follow as there appears to be a issue in dq_checks code."]
            ),

            Check(
                name="Missing red_expr",
                description="Ensure when status = RED that there is a red_expr to go with it.",
                column_name="red_expr",
                value=self.dq_checks_input[0]["red_expr_missing"],
                red_expr=":value == 0",
                red_error=":value rows have a missing red_expr(s).",
                todo=["Email Daron this alert details for follow as there appears to be a issue in dq_checks code."]
            ),

            Check(
                name="Missing yellow_expr",
                description="Ensure when status = YELLOW that there is a yellow_expr to go with it.",
                column_name="yellow_expr",
                value=self.dq_checks_input[0]["yellow_expr_missing"],
                red_expr=":value == 0",
                red_error=":value rows have a missing yellow_expr(s).",
                todo=["Email Daron this alert details for follow as there appears to be a issue in dq_checks code."]
            ),

            Check(
                name="Missing red_expr and yellow_expr",
                description="Ensure every check has either a red_expr or a yellow_expr.",
                column_name="red_expr and yellow_expr",
                value=self.dq_checks_input[0]["red_and_yellow_expr_missing"],
                red_expr=":value == 0",
                red_error=":value rows have a missing red_expr and yellow_expr. At least one expr must be included",
                todo=["Email Daron this alert details for follow as there appears to be a issue in dq_checks code."]
            ),

            # Note that UNKNOWN is a valid value if early check in loops of checks raises error.
            MatchExpectedValuesCheck(
                column_name="status",
                values=self.dq_checks_input[0]["status"],
                expected_values=["GREEN", "YELLOW", "RED", "ERROR", "RUN_AGAIN", "UNKNOWN"],
                todo=["Email Daron this alert details for follow as there appears to be a issue in dq_checks code."]
            ),

            # Note that UNKNOWN is a valid value if early check in loops of checks raises error.
            MatchExpectedValuesCheck(
                column_name="alert_status",
                values=self.dq_checks_input[0]["alert_status"],
                # Note I intentionally left out the value = ALERT_FAILED_TO_SEND from list below so we will
                #    be warned of the event should that status appear. Need to know why ALERTS may be failing to send.
                expected_values=["NO_ALERT_SENT", "RED_ALERT_SENT", "YELLOW_WARNING_SENT", "UNKNOWN"],
                todo=["Email Daron this alert details for follow as there appears to be a issue in dq_checks code."]
            ),

            Check(
                name="Missing DQ Checks",
                description="Alerts if check ran yesterday is not ran again today.",
                column_name="missing_dq_checks",
                value=self.missing_dq_checks[0]["missing_dq_checks"],
                yellow_expr=":value == []",  # No rows should be returned
                yellow_warning="The following DQ checks were ran yesterday, but did not run today: :value",
                todo=["Email Daron this alert details for follow as there appears to be a issue in dq_checks code."]
            ),

            # Checks for dq_checks_inputs view
            Check(
                name="Missing inputs",
                description="Ensure every row in dq_checks has at least one related input",
                column_name="missing_inputs",
                value=self.missing_inputs[0]["count_missing_inputs"],
                red_expr=":value == 0",
                red_error=":value checks found that have no input in inputs table.",
                todo=["Email Daron this alert details for follow as there appears to be a issue in dq_checks code."]
            ),

            Check(
                name="DISTINCT code column check",
                description="Ensure code column is distinct across all rows",
                column_name="code",
                value=self.duplicate_code_input[0]["cnt_dups"],
                red_expr=":value == 0",
                red_error=":value rows found that contain duplicates for code column.",
                todo=["Email Daron this alert details for follow as there appears to be a issue in dq_checks code."]
            ),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(
                input_object=self.dq_checks_inputs_input,
                column_names=DQ_CHECKS_INPUTS_NO_MISSING_DATA_COLS,
                todo=["Email Daron this alert details for follow as there appears to be a issue in dq_checks code."]
            ),
        ]
