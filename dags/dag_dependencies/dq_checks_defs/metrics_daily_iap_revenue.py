"""DQ Checks for the analytics.metrics_daily_iap_revenue in Snowflake.
metrics_daily_iap_revenue table data has direct dependency to tables:
    appstore.appstore_sales, google_play.google_play_sales

For detailed info, checkout the Excel sheet for current owners/usage:
    DataScience and Engineering/Documents/DE TEAM/SLT MODELS/SLT_DE_TRACKER.xlsx

SLT specific checks:
    1. Ensures the total row count is not abnormal.
    2. Ensure columns that should have no null values don't suddenly have null values introduced.
    3. Ensure daily_iap_net_revenue sum are within normal range.
    4. Ensure net_units_sold sum are within normal range.
    5. Ensure clients match as per the SLT requirements.
    6. Ensure product_category as per the SLT requirements.
"""

from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck

DATABASE = "prod"
TABLE = "analytics.metrics_daily_iap_revenue"

TODO_LIST = [
    (f"Check source table {TABLE} and upstream source tables: [appstore.appstore_sales, google_play.google_play_sales] "
     "for possible missing data or other issues causing abnormality."),
    "If sums are to be expected and are regularly outside range, adjust the range.",
    ("Note for yellow alerts: When encountering yellow alerts, ensure that the values do not deviate significantly. "
     "Typically, if you observe a seasonal trend but the value is within 10% of the expected range, "
     "no immediate action is required. However, continue monitoring over the next few days to confirm that everything "
     "remains within normal thresholds, and that this yellow alert is indeed a one-time occurrence.")
]

NO_MISSING_DATA_COLS = ["date_utc", "product_category", "net_units_sold", "net_revenue"]

MAIN_SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        ARRAY_UNIQUE_AGG(product_category) AS product_category,
        SUM(CASE WHEN UPPER(client_type) IN ('TN_ANDROID', '2L_ANDROID') THEN net_units_sold ELSE 0 END) 
            AS android_net_units_sold,
        SUM(CASE WHEN UPPER(client_type) = 'TN_IOS_FREE' THEN net_units_sold ELSE 0 END) 
            AS ios_net_units_sold,
        SUM(CASE WHEN UPPER(client_type) IN ('TN_ANDROID', '2L_ANDROID') THEN net_revenue ELSE 0 END) 
            AS android_daily_iap_revenue,
        SUM(CASE WHEN UPPER(client_type) = 'TN_IOS_FREE' THEN net_revenue ELSE 0 END) AS ios_daily_iap_revenue
    FROM {DATABASE}.{TABLE}
    WHERE
        (date_utc = CAST(:run_date AS DATE) - INTERVAL '1 day')
        AND UPPER(COUNTRY_CODE) IN ('US', 'CA')
        AND UPPER(CLIENT_TYPE) IN ('TN_IOS_FREE', 'TN_ANDROID', '2L_ANDROID')
"""


class MetricsDailyIapRevenueDQChecks(BaseDQChecks):
    name = "metrics_daily_iap_revenue DQ Checks"
    description = "Aggregate DQ Checks for metrics_daily_iap_revenue table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="metrics_daily_iap_revenue Main Input",
                alias="main_input",
                src_sql=MAIN_SQL
            )
        ]

    def get_checks(self):
        return [
            TotalCountCheck(
                filter_date_column_name="date",
                total_count=self.main_input[0]["total_count"],
                use_dynamic_thresholds=True,
                red_expr=":lower_threshold*0.9 < :value < :upper_threshold*1.5",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                yellow_expr=":lower_threshold < :value < :upper_threshold*1.39 -0.2*:lower_threshold",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            ),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(
                input_object=self.main_input,
                column_names=NO_MISSING_DATA_COLS
            ),

            # Fail if a new value for column 'product_category' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="product_category",
                values=self.main_input[0]["product_category"],
                expected_values=["Ad Free+", "Ad Free Lite", "International Credit - $5",
                                 "Lock In Number", "Other", "Premium Number", "TextNow Pro"]
            ),

            # These checks will alert spikes and dips in the SLT chart
            Check(
                name="Total Count Check",
                description="Ensure net_units_sold is within the normal expected range",
                column_name="android_net_units_sold",
                value=self.main_input[0]["android_net_units_sold"],
                use_dynamic_thresholds=True,
                red_expr=":lower_threshold*0.9 < :value < :upper_threshold*1.5",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                yellow_expr=":lower_threshold < :value < :upper_threshold*1.39 -0.2*:lower_threshold",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure net_units_sold is within the normal expected range",
                column_name="ios_net_units_sold",
                value=self.main_input[0]["ios_net_units_sold"],
                use_dynamic_thresholds=True,
                red_expr=":lower_threshold*0.9 < :value < :upper_threshold*1.5",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                yellow_expr=":lower_threshold < :value < :upper_threshold*1.39 -0.2*:lower_threshold",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure daily_iap_net_revenue is within the normal expected range",
                column_name="android_daily_iap_revenue",
                value=self.main_input[0]["android_daily_iap_revenue"],
                use_dynamic_thresholds=True,
                red_expr=":lower_threshold*0.9 < :value < :upper_threshold*1.5",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                yellow_expr=":lower_threshold < :value < :upper_threshold*1.39 -0.2*:lower_threshold",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure daily_iap_net_revenue is within the normal expected range",
                column_name="ios_daily_iap_revenue",
                value=self.main_input[0]["ios_daily_iap_revenue"],
                use_dynamic_thresholds=True,
                red_expr=":lower_threshold*0.9 < :value < :upper_threshold*1.5",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                yellow_expr=":lower_threshold < :value < :upper_threshold*1.39 -0.2*:lower_threshold",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            )
        ]
