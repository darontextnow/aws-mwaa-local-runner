"""DQ Checks for the base_user_daily_subscription table in Snowflake.

The checks defined below are set up to run once a day to ensure base_user_daily_subscription data added to Snowflake
during the previous 24 hours meets our assumptions and minimum requirements for good data quality.

Upstream Owners: Backend Team.
Notes:
    1 day lag should be fine for this table.
    No columns as of 1/16/2023 have any NULL values.
    date_utc is never NULL and as of 1/16/2023 there are no future dates.
    There is a period in Q4 2022 with plan_id = 74 which no longer exists in core.plans table.
        Thus, we get NULL for plan_name. The counts are so small we're ignoring and treating this as an anomaly.
    Not testing plan_name and product_name here as they come directly from current core.plans and core.products tables.

SLT specific checks:
    1. Ensures each subscription types sum are within normal range.
"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.no_duplicate_rows_check import NoDuplicateRowsCheck
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck

NO_MISSING_DATA_COLS = ["date_utc", "username", "mdn", "network", "carrier", "item_uuid",
                        "product_id", "product_name", "plan_id", "plan_name",
                        "status", "is_free", "plan_family"]

SLT_TODO_LIST = [
    ("Check source table base_user_daily_subscription and upstream dependencies "
     "for possible missing data or other issues causing abnormality."),
    "If sums are to be expected and are regularly outside range, adjust the range.",
    ("Note for yellow alerts: When encountering yellow alerts, ensure values stay within expected bounds. "
     "Typically, if there's a seasonal trend but values are within 10% of the norm, no immediate action is needed. "
     "Keep monitoring over the next few days to confirm it's a one-time event.")
]

SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        ARRAY_UNIQUE_AGG(network) AS networks,
        ARRAY_UNIQUE_AGG(carrier) AS carriers,
        ARRAY_UNIQUE_AGG(status) AS status,
        ARRAY_UNIQUE_AGG(plan_family) AS plan_family
        --SUM(CASE WHEN is_free = FALSE THEN 1 ELSE 0 END) AS is_free_false_rate
    FROM prod.analytics_staging.base_user_daily_subscription
    WHERE (date_utc = CAST(:run_date AS DATE))
"""

SLT_SUB_SQL = """
    SELECT
        SUM(CASE WHEN IS_FREE = TRUE THEN 1 ELSE 0 END) AS free_subscription,
        SUM(CASE WHEN IS_FREE = FALSE THEN 1 ELSE 0 END) AS paid_subscription,
        SUM(CASE WHEN plan_id IN (67, 59, 68, 69, 56, 76, 78, 79, 80, 81, 82, 83, 84) 
                THEN 1 ELSE 0 END) AS paid_data_plan_users
    FROM analytics_staging.base_user_daily_subscription
    WHERE (date_utc = CAST(:run_date AS DATE))
"""

SLT_ACTIVATION_SQL = """
    WITH activations AS (
        SELECT
            a.date_utc AS activation_date,
            CASE WHEN DATEDIFF('day', us.cohort_utc, a.date_utc) <= 7 THEN 'new users'
                ELSE 'existing users' END AS userset_segment
        FROM prod.analytics_staging.base_user_daily_subscription a
        JOIN prod.analytics.users u ON (u.username = a.username)
        JOIN prod.analytics.user_sets us ON (us.user_set_id = u.user_set_id)
        WHERE (a.product_id = 392)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY a.item_uuid ORDER BY a.date_utc) = 1
    )
    SELECT
        SUM(CASE WHEN LOWER(userset_segment) = 'existing users'
            THEN 1 ELSE 0 END) AS existing_users_user_set_activations,
        SUM(CASE WHEN LOWER(userset_segment) = 'new users'
            THEN 1 ELSE 0 END) AS new_users_user_set_activations
    FROM activations
    WHERE (activation_date = CAST(:run_date AS DATE))
"""

DUPES_COUNT_SQL = """
    SELECT COUNT(*) AS cnt_dups FROM (
        SELECT COUNT(*) AS cnt_dups
        FROM prod.analytics_staging.base_user_daily_subscription
        WHERE (DATE(date_utc) = CAST(:run_date AS DATE))
        GROUP BY date_utc, username
        HAVING COUNT(*) > 1
    );
"""


class BaseUserDailySubscriptionDQChecks(BaseDQChecks):
    name = "base_user_daily_subscription DQ Checks"
    description = "Aggregate DQ Checks for base_user_daily_subscription table"
    table_name = "analytics_staging.base_user_daily_subscription"

    def get_inputs(self):
        return [
            DBOInput(
                name="base_user_daily_subscription Prod Main Input",
                alias="main_input",
                src_sql=SQL
            ),

            DBOInput(
                name="base_user_daily_subscription SLT SIM Activation Input",
                alias="slt_act_sql_input",
                src_sql=SLT_ACTIVATION_SQL
            ),

            DBOInput(
                name="base_user_daily_subscription SLT SIM Subscription Input",
                alias="slt_sub_sql_input",
                src_sql=SLT_SUB_SQL
            ),

            DBOInput(
                name="base_user_daily_subscription Dupes Count Input",
                alias="dupes_count",
                src_sql=DUPES_COUNT_SQL
            )
        ]

    def get_checks(self):
        return [
            # Ensure total count of rows remains within expected range
            TotalCountCheck(
                filter_date_column_name="date_utc",
                total_count=self.main_input[0]["total_count"],
                lower_threshold=150000,
                upper_threshold=200000,
                todo=SLT_TODO_LIST
            ),

            NoDuplicateRowsCheck(dups_count=self.dupes_count[0]["cnt_dups"]),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(input_object=self.main_input, column_names=NO_MISSING_DATA_COLS),

            # Fail if a new value for column 'network' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="network",
                values=self.main_input[0]["networks"],
                expected_values=["T-Mobile", "Kore"]
            ),

            # Fail if a new value for column 'carrier' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="carrier",
                values=self.main_input[0]["carriers"],
                expected_values=["PWG", "Kore"]
            ),

            # Fail if a new value for column 'status' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="status",
                values=self.main_input[0]["status"],
                expected_values=["ACTIVE", "THROTTLED"]
            ),

            # Fail if a new value for column 'plan_family' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="plan_family",
                values=self.main_input[0]["plan_family"],
                expected_values=["NWTT", "2 GB", "Talk & Text", "1 GB", "250 MB", "5 GB", "3 GB", "7 GB",
                                 "500 MB", "Unlimited", "others", "350 MB", "50 MB", "80 MB", "10 GB", "300 MB"]
            ),

            # These checks will alert for variations in the SLT chart
            Check(
                name="Total Count Check",
                description="Ensure free_subscription is within the normal expected range",
                column_name="count_free_subscription",
                value=self.slt_sub_sql_input[0]["free_subscription"],
                red_expr="100000 <= :value <= 300000",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                yellow_expr="130000 <= :value <= 230000",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=SLT_TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure paid_subscription is within the normal expected range",
                column_name="count_paid_subscription",
                value=self.slt_sub_sql_input[0]["paid_subscription"],
                red_expr="3000 < :value < 20000",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                yellow_expr="5800 < :value < 12000",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=SLT_TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure paid_data_plan_users is within the normal expected range",
                column_name="count_paid_data_plan_subscription",
                value=self.slt_sub_sql_input[0]["paid_data_plan_users"],
                red_expr="3000 < :value < 20000",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                yellow_expr="5800 < :value < 12000",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=SLT_TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure existing_users_user_set_activations is within the normal expected range",
                column_name="existing_users_sim_activations",
                value=self.slt_act_sql_input[0]["existing_users_user_set_activations"],
                red_expr="50 <= :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                yellow_expr="100 <= :value <= 4000",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=SLT_TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure new_users_user_set_activations is within the normal expected range",
                column_name="new_users_sim_activations",
                value=self.slt_act_sql_input[0]["new_users_user_set_activations"],
                yellow_expr="10 <= :value <= 600",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=SLT_TODO_LIST
            )
        ]
