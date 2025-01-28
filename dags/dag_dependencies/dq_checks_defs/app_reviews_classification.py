"""
DQ Checks for the analytics.app_reviews in Snowflake.
analytics.app_reviews table data comes from the DAG: app_reviews_classification

Upstream Owners: DE Team (DAG mentioned above) & DS Team (App Review Model Image - Diego)
Downstream Owners: DS Team (Diego) 

Checks:
    1. Ensures the total row count is not abnormal per day.
    2. Ensure columns that should have no null values don't suddenly have null values introduced.
    3. Ensure no incoming duplicate records.
"""

from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.no_duplicate_rows_check import NoDuplicateRowsCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck

TABLE = "analytics.app_reviews"

NO_MISSING_DATA_COLS = ["id", "original_text", "last_modified", "client_type"]

MAIN_SQL = f"""
    SELECT
        COUNT(*) AS total_count, 
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)}
    FROM prod.{TABLE}
    WHERE (CAST(last_modified AS DATE) = CAST(:run_date AS DATE));
"""

DUPES_COUNT_SQL = f"""
    SELECT COUNT(*) AS cnt_dups FROM (
        SELECT COUNT(*) AS cnt_dups
        FROM prod.{TABLE}
        WHERE (CAST(last_modified AS DATE) = CAST(:run_date AS DATE))
        GROUP BY id, original_text
        HAVING COUNT(*) > 1
    );
"""

AGG_CHECKS_SQL = """
    SELECT
        SUM(CASE WHEN filter_none_meaning THEN 1 ELSE 0 END)/7 AS avg_filter_none_meaning,
        SUM(CASE WHEN calling_negative THEN 1 ELSE 0 END)/7    AS avg_calling_negative,
        SUM(CASE WHEN messaging_negative THEN 1 ELSE 0 END)/7  AS avg_messaging_negative,
        SUM(CASE WHEN app_negative THEN 1 ELSE 0 END)/7        AS avg_app_negative,
        SUM(CASE WHEN ads_negative THEN 1 ELSE 0 END)/7        AS avg_ads_negative,
        SUM(CASE WHEN cost_negative THEN 1 ELSE 0 END)/7       AS avg_cost_negative  
    FROM analytics.app_reviews
    WHERE last_modified::date >= CAST(:run_date AS DATE) - interval '6 days'
          AND last_modified::date <= CAST(:run_date AS DATE);
"""


class AppReviewsClassificationDQChecks(BaseDQChecks):
    name = "analytics_app_reviews DQ Checks"
    description = "Aggregate DQ Checks for analytics_app_reviews table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="analytics_app_reviews Main Input",
                alias="main_input",
                src_sql=MAIN_SQL
            ),

            DBOInput(
                name="analytics_app_reviews Dupes Count Input",
                alias="dupes_count",
                src_sql=DUPES_COUNT_SQL
            ),

            DBOInput(
                name="analytics_app_reviews Avg Neg Counts Input",
                alias="agg_counts",
                src_sql=AGG_CHECKS_SQL
            )
        ]

    def get_checks(self):

        return [

            TotalCountCheck(
                filter_date_column_name="date",
                total_count=self.main_input[0]["total_count"],
                lower_threshold=1,
                upper_threshold=150
            ),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(
                input_object=self.main_input,
                column_names=NO_MISSING_DATA_COLS
            ),

            NoDuplicateRowsCheck(dups_count=self.dupes_count[0]["cnt_dups"]),

            Check(
                name="Avg None Meaning Reviews",
                description="7 day average of number of reviews for tagged with no meaning",
                column_name="avg_filter_none_meaning",
                value=self.agg_counts[0]["avg_filter_none_meaning"],
                yellow_expr="7 <= :value <= 25",
                yellow_warning="7 day average for no meaning tagged reviews is outside normal range (:yellow_expr)",
                red_expr=":value <= 35",
                red_error="Average reviews with none meaning has exceeded the upper threshold (:red_expr)",
                todo=["Check with DS Team, why suddenly the no meaning tagged reviews is abnormal"]
            ),

            Check(
                name="Avg Calling Negative",
                description="7 day average of number of reviews for calling tagged reviews",
                column_name="avg_calling_negative",
                value=self.agg_counts[0]["avg_calling_negative"],
                yellow_expr="1 <= :value <= 12",
                yellow_warning="7 day avg for negative calling tagged reviews is outside normal range (:yellow_expr)",
                red_expr=":value <= 17",
                red_error="Average negative calling tagged reviews has exceeded the upper threshold (:red_expr)",
                todo=["Check with DS Team, why suddenly the negative tagged reviews is abnormal"]
            ),

            Check(
                name="Avg Messaging Negative",
                description="7 day average for negative tagged messaging reviews is above normal range.",
                column_name="avg_messaging_negative",
                value=self.agg_counts[0]["avg_messaging_negative"],
                yellow_expr="1 <= :value <= 8",
                yellow_warning="Avg negative messaging tagged reviews, :yellow_expr, is not within expected range",
                red_expr=":value <= 11",
                red_error="Average negative messaging tagged reviews has exceeded the upper threshold (:red_expr)",
                todo=["Check with DS Team, why suddenly the negative tagged messages is abnormal"]
            ),

            Check(
                name="Avg App Negative",
                description="7 day average of number of reviews for app tagged reviews",
                column_name="avg_app_negative",
                value=self.agg_counts[0]["avg_app_negative"],
                yellow_expr="1 <= :value <= 12",
                yellow_warning="7D average for negative app tagged reviews, :yellow_expr, is outside normal range.",
                red_expr=":value <= 14",
                red_error="Average negative app tagged reviews has exceeded the upper threshold (:red_expr)",
                todo=["Check with DS Team, why suddenly the negative tagged reviews is abnormal"]
            ),

            Check(
                name="Avg Ads Negative",
                description="7 day average of number of reviews for negative ads tagged reviews",
                column_name="avg_ads_negative",
                value=self.agg_counts[0]["avg_ads_negative"],
                yellow_expr="0.5 <= :value <= 5",
                yellow_warning="7D average for negative ads tagged reviews, :yellow_expr, is outside normal range.",
                red_expr=":value <= 6.5",
                red_error="Average negative ads tagged reviews has exceeded the upper threshold (:red_expr)",
                todo=["Check with DS Team, why suddenly the negative tagged reviews is abnormal"]
            ),

            Check(
                name="Avg Cost Negative",
                description="7 day average of number of reviews for cost tagged reviews",
                column_name="avg_messaging_negative",
                value=self.agg_counts[0]["avg_cost_negative"],
                yellow_expr="0.5 <= :value <= 5",
                yellow_warning="7 day average for negative cost tagged reviews, :yellow_expr, is outside normal range.",
                red_expr=":value <= 7.5",
                red_error="Average negative cost tagged reviews has exceeded the upper threshold (:red_expr)",
                todo=["Check with DS Team, why suddenly the negative tagged reviews is abnormal"]
            )

        ]
