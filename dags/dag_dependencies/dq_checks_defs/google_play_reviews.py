"""
DQ Checks for the core.google_play_reviews in Snowflake.
core.google_play_reviews table data comes from 2 DAGs:
    google_play_reviews_extract_v1
    android_competitor_reviews_extract_v1

Upstream Owners: DE Team (DAGs mentioned above)
Downstream Owners: DS Team (NLP App Reviews Classification - Diego)

Checks:
    1. Ensures the total row count is not abnormal per day. (ANDROID APPS)
    2. Ensure columns that should have no null values don't suddenly have null values introduced.
    3. Ensure id format matches as per expectation.
    4. Ensure client_type and app_name events match as per expectation.
    5. Ensure no incoming duplicate records.

The DQ Checks on this have been split into two parts because of the lag in acquiring all the reviews from each DAG.
Currently, we are running the textnow and 2ndline reviews with a 1 Day lag and for all competitors with a 2 Day lag.
The main reason for the difference in the lag for each DQ check is because:
    1. receive a complete dataset for textnow and 2ndline reviews from the Google Play API on each DAG run
        and we only run the DAG once a day without any overlap of times, so we will be able to do a complete DQ Check
        on those results after the run (1 day lag on the main dag)
        But we do not get all the reviews for the competitors for a single day after a single run since it is
        performing scraping of the reviews from Google Play, so we would end up having a complete dataset after
        2 days of runs and each DAG run has an overlap of 2 days, where it popluates data back to 2 days.

    2. We currently use textnow and 2ndline reviews for our app_reviews_classification project which runs daily, and
        hence we would need to perform our dq checks on these reviews before we pass them over to the classifier. Which
        would require us to perform the checks with a 1 Day lag for textnow and 2ndline and a 2 day lag for rest which
        currently don't have any downstream usages.
"""

from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck
from de_utils.dq_checks_framework.check_classes.no_duplicate_rows_check import NoDuplicateRowsCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck

TABLE = "core.google_play_reviews"

TODO_LIST = [
    f"Check source table {TABLE} and DAG run of google_play_reviews_extract_v1"
    f" for possible missing data or other issues causing abnormality.",
    "If sums are to be expected and are regularly outside range, adjust the thresholds."
]

NO_MISSING_DATA_COLS = ["id", "text", "last_modified", "app_name"]

COMPETITOR_SPECIFIC_SQL = f"""
    SELECT
        COUNT(*) AS total_competitor_count, 
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        SUM(CASE WHEN 
            (REGEXP_LIKE(id, '[a-z0-9]{{8}}-[a-z0-9]{{4}}-[a-z0-9]{{4}}-[a-z0-9]{{4}}-[a-z0-9]{{12}}') = FALSE)
            THEN 1 ELSE 0 END) AS review_id_invalid_fmt,
        SUM(CASE WHEN lower(TEXT) = 'n/a' THEN 1 ELSE 0 END) AS invalid_review_text
    FROM prod.{TABLE}
    WHERE (CAST(last_modified AS DATE) = (CAST(:run_date AS DATE) - interval '1 day')) 
          AND app_name IN ('WHATSAPP', 'TEXTFREE', 'TEXTME');
"""

NLP_APP_REVIEW_SPECIFIC_SQL = f"""
    SELECT
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        SUM(CASE WHEN 
            (REGEXP_LIKE(id, '[a-z0-9]{{8}}-[a-z0-9]{{4}}-[a-z0-9]{{4}}-[a-z0-9]{{4}}-[a-z0-9]{{12}}') = FALSE)
            THEN 1 ELSE 0 END) AS textnow_review_id_invalid_fmt,
        ARRAY_UNIQUE_AGG(client_type) AS client_type,
        COUNT(*) AS total_android_count
    FROM prod.{TABLE}
    WHERE (CAST(last_modified AS DATE) = CAST(:run_date AS DATE)) AND app_name IN ('TEXTNOW', '2ND_LINE');
"""

DUPES_COUNT_SQL = f"""
    SELECT COUNT(*) AS cnt_dups FROM (
        SELECT COUNT(*) AS cnt_dups
        FROM prod.{TABLE}
        WHERE (CAST(last_modified AS DATE) = CAST(:run_date AS DATE))
        GROUP BY id, text
        HAVING COUNT(*) > 1
    );
"""


class GooglePlayReviewsDQChecks(BaseDQChecks):
    name = "core_google_play_reviews DQ Checks"
    description = "Aggregate DQ Checks for core_google_play_reviews table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="core_google_play_reviews Main Input",
                alias="main_input",
                src_sql=COMPETITOR_SPECIFIC_SQL
            ),

            DBOInput(
                name="core_google_play_reviews Dupes Count Input",
                alias="dupes_count",
                src_sql=DUPES_COUNT_SQL
            ),

            DBOInput(
                name="core_google_play_reviews TextNow Reviews Input",
                alias="textnow_count",
                src_sql=NLP_APP_REVIEW_SPECIFIC_SQL
            )
        ]

    def get_checks(self):

        return [

            Check(
                name="Total TextNow Reviews Count Check",
                description="Ensure total reviews from TextNow & 2ndLine is within the normal expected range",
                column_name="ignore",
                value=self.textnow_count[0]["total_android_count"],
                red_expr="40 <= :value <= 150", 
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=TODO_LIST
            ),

            # Ensure columns we expect to always have a value actually have 100% values.
            # Delayed for Android Competitors Check
            NoMissingValuesCheck(
                input_object=self.main_input,
                column_names=NO_MISSING_DATA_COLS
            ),
            # TextNow and 2ndLine Check
            NoMissingValuesCheck(
                input_object=self.textnow_count,
                column_names=NO_MISSING_DATA_COLS
            ),

            NoDuplicateRowsCheck(dups_count=self.dupes_count[0]["cnt_dups"]),

            # Fail if a new value for column 'client_type' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="client_type",
                values=self.textnow_count[0]["client_type"],
                expected_values=["TN_ANDROID", "2L_ANDROID"]
            ),

            # Fail if the multiple google review texts are found to be N/A
            # (this breaks the model for app_review_classification)
            Check(
                name="Invalid Review Text",
                description="multiple review texts found to be N/A",
                column_name="text",
                value=self.main_input[0]["invalid_review_text"],
                red_expr=":value < 5",
                red_error=":value rows found with invalid text value of N/A.",
                todo=[("Need to research why suddenly invalid data format is detected from"
                       " the DAGs and correct the data.")]
            ),

            # Fail if the google review id format does not match the expected format
            # Delayed check for Android Competitor Reviews
            Check(
                name="Invalid Format",
                description="id column is not in expected format.",
                column_name="id",
                value=self.main_input[0]["review_id_invalid_fmt"],
                red_expr=":value == 0",
                red_error=":value rows found with invalid format for id column.",
                todo=[("Need to research why suddenly invalid data format is detected from"
                       " the android_competitor_reviews DAG and correct the data.")]
            ),

            # TextNow and 2ndLine Check
            Check(
                name="Invalid Format",
                description="id column is not in expected format.",
                column_name="id",
                value=self.textnow_count[0]["textnow_review_id_invalid_fmt"],
                red_expr=":value == 0",
                red_error=":value rows found with invalid format for id column.",
                todo=[("Research why suddenly invalid data format is detected from"
                       " the google_play_reviews_extract DAG and correct the data.")]
            )
        ]
