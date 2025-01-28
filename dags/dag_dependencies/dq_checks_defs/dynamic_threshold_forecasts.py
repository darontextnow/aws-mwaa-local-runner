"""DQ Checks for the core.dynamic_threshold_forecasts in Snowflake.

These DQ Checks are currently designed to run weekly after the target table is updated.
core.dynamic_threshold_forecasts table data has dependency on ML model which predicts the thresholds of the DQ Checks.

Upstream and Downstream Owners: DE+DS Team
    Upstream source: s3://tn-snowflake-dev/dynamic_thresholds/predicted_data/
    Downstream usage: All dynamic DQ checks

Checks:
    1. Ensure columns that should have no null values don't suddenly have null values introduced.
    2. Ensures the respective lower and upper threshold bounds are not negative.
    3. Ensure table_name, column_name match as per expectation.
    4. Ensure we have predicted data for the future until the next weekly DAG run.
"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck

TABLE = "core.dynamic_threshold_forecasts"

NO_MISSING_DATA_COLS = ["run_timestamp", "table_name", "column_name", "lower_threshold", "upper_threshold",
                        "predicted_value", "inserted_at", "inserted_by"]

MAIN_SQL = f"""
    SELECT
        MAX(run_timestamp::DATE)- MAX(CAST(:run_date AS DATE)) as count_of_future_days,
        MIN(lower_threshold) AS lower_threshold,
        MIN(upper_threshold) AS upper_threshold,
        ARRAY_UNIQUE_AGG(table_name) AS table_name,
        ARRAY_UNIQUE_AGG(column_name) AS column_name,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)}
    FROM prod.{TABLE}
    WHERE 
        check_name = 'Total Count Check'
        AND (run_timestamp::DATE >= CAST(:run_date AS DATE));
"""

VALID_ALERT_SQL = """
    SELECT
        DIV0NULL(SUM(CASE WHEN (is_valid_alert = TRUE) OR (is_valid_alert IS NULL) THEN 1 ELSE 0 END),
            SUM(CASE WHEN is_valid_alert = FALSE THEN 1 ELSE 0 END)) AS false_true_alert_ratio,
        DIV0NULL(SUM(CASE WHEN is_valid_alert = FALSE THEN 1 ELSE 0 END), SUM(CASE WHEN (is_valid_alert = TRUE) 
            OR (is_valid_alert IS NULL) THEN 1 ELSE 0 END)) AS true_false_alert_ratio
    FROM prod.core.dq_checks_details
    WHERE 
        (use_dynamic_thresholds = TRUE)
        AND (status IN ('RED', 'YELLOW'))
        AND (inserted_at::DATE >= CAST(:run_date AS DATE) - INTERVAL '7 days');
"""


class DynamicThresholdForecastsDQChecks(BaseDQChecks):
    name = "dynamic_threshold_forecasts weekly DQ Checks"
    description = "Aggregate DQ Checks for core_dynamic_threshold_forecasts table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="dynamic_threshold_prediction Main Input",
                alias="main_input",
                src_sql=MAIN_SQL
            ),

            DBOInput(
                name="dynamic_threshold_prediction dq_checks_details Input",
                alias="dq_checks_details_input",
                src_sql=VALID_ALERT_SQL
            )
        ]

    def get_checks(self):
        return [

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(
                input_object=self.main_input,
                column_names=NO_MISSING_DATA_COLS
            ),

            # These checks will alert for negative predictions
            Check(
                name="Total Count Check",
                description="Ensure count_of_future_days is as expected",
                column_name="lower_threshold",
                value=self.main_input[0]["count_of_future_days"],
                red_expr=":value >= 7",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=["Notify Harish about this alert failure as there appears to be a issue "
                      "in the ML model which is not predicting for future days"]

            ),

            # These checks will alert for negative predictions
            Check(
                name="Total Count Check",
                description="Ensure lower_threshold is non negative",
                column_name="lower_threshold",
                value=self.main_input[0]["lower_threshold"],
                red_expr="0 < :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=["Notify Harish about this alert failure as there appears to be a issue "
                      "in the ML model which is predicting negative values"]
            ),

            # These checks will alert for negative predictions
            Check(
                name="Total Count Check",
                description="Ensure upper_threshold is non negative",
                column_name="upper_threshold",
                value=self.main_input[0]["upper_threshold"],
                red_expr="0 < :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                todo=["Notify Harish about this alert failure as there appears to be a issue "
                      "in the ML model which is predicting negative values"]
            ),

            # Fail if a new value for column 'table_name' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="table_name",
                values=self.main_input[0]["table_name"],
                expected_values=["adops.report", "analytics.growth_dau_by_segment",
                                 "analytics.metrics_daily_ad_total_revenue", "analytics.metrics_daily_iap_revenue",
                                 "analytics.metrics_daily_app_time_sessions",
                                 "analytics.metrics_daily_dau_generated_revenue",
                                 "analytics.metrics_daily_sim_dau_generated_revenue",
                                 "analytics_staging.adjust_installs", "analytics_staging.base_user_daily_subscription"],
                todo=["Notify Harish about this alert failure as there appears to be a issue "
                      "in the ML model which is populating unexpected table_name values"]
            ),

            # Fail if a new value for column 'column_name' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="column_name",
                values=self.main_input[0]["column_name"],
                expected_values=["android_net_units_sold", "android_daily_iap_revenue", "android_impressions",
                                 "android_total_num_sessions", "android_total_time_in_app_hours_per_day",
                                 "COUNT(*)", "count_android_installs", "count_organic_installs", "ios_impressions",
                                 "ios_total_num_sessions", "ios_total_time_in_app_hours_per_day", "paid_sim_orders",
                                 "sim_dau", "total_dau", "total_dau_generated_revenue", "daily_iap_revenue",
                                 "daily_ad_revenue", "ios_net_units_sold", "ios_daily_iap_revenue"],
                todo=["Notify Harish about this alert failure as there appears to be a issue "
                      "in the ML model which is populating unexpected column_name values"]
            ),

            Check(
                name="Is Valid Alert Rate Check",
                description="Ensure false_true_alert_ratio is within the normal expected range",
                column_name="is_valid_alert_true_rate",
                value=self.dq_checks_details_input[0]["false_true_alert_ratio"],
                yellow_expr=":value < 20",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=["Ensure that the false alerts to true alerts ratio is accurately updated over the next few days "
                      "if other dynamically created Yellow and Red alerts exceed their thresholds. Notify Harish "
                      "about this alert failure if the issue persists in the subsequent runs."]
            ),

            Check(
                name="Is Valid Alert Rate Check",
                description="Ensure true_false_alert_ratio is within the normal expected range",
                column_name="is_valid_alert_false_rate",
                value=self.dq_checks_details_input[0]["true_false_alert_ratio"],
                yellow_expr=":value < 5",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=["Ensure that the true alerts to false alerts ratio is accurately updated over the next few days "
                      "if other dynamically created Yellow and Red alerts exceed their thresholds. Notify Harish "
                      "about this alert failure if the issue persists in the subsequent runs."]
            )
        ]
