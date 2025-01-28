"""
DQ Checks for the user_daily_profit table.
Cost related columns should be checked with standard 1 day lag. Cost data is always missing on days 1 and 2 of month.
All revenue data currently has a 2 day lag due to lag in libring source. Extra lag is adjusted in query WHERE clauses.

Contains checks for every total column in table ensuring total sum of each column matches total sum
in upstream source tables. This helps guarantee no data is missing when compared to source.

Contains checks for every total column to warn us should any values move outside given ranges of their norm. Using
column stats such as avg, min, max, stddev, median, percentile to help ensure totals stay within established norms.

Contains checks to ensure columns that should have no null values don't suddenly have null values introduced.

Most of the current thresholds are based on querying user_daily_profit table for date range:
"(date_utc BETWEEN CAST('2022-01-01' AS DATE) AND CAST('2023-01-08' AS DATE))"
for daily min and max for each stat.

"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck
from de_utils.dq_checks_framework.check_classes.column_statistics_check import ColumnStatisticsCheck
from de_utils.dq_checks_framework.check_classes.no_duplicate_rows_check import NoDuplicateRowsCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.src_and_dst_values_match_check import SrcAndDstValuesMatchCheck
from de_utils.dq_checks_framework.check_classes.percent_value_to_total_check import PercentValueToTotalCheck

TABLE = "analytics.user_daily_profit"
PKEY_COLS = ["date_utc", "username"]
ONE_DAY_LAG_FILTER = "(date_utc = CAST(:run_date AS DATE))"
TWO_DAY_LAG_FILTER = "(date_utc = DATEADD(DAY, -1, CAST(:run_date AS DATE)))"

# these columns are checked for NULL or '' values
NO_MISSING_DATA_COLS = ["date_utc", "username"]

MAIN_SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        SUM(mms_cost) AS mms_cost,
        SUM(sms_cost) AS sms_cost,
        SUM(message_cost) AS message_cost,
        SUM(calling_cost) AS calling_cost,
        SUM(tmobile_mrc_cost) AS tmobile_mrc_cost,
        SUM(tmobile_overage_cost) AS tmobile_overage_cost,
        SUM(tmobile_cost) AS tmobile_cost
        --profit is just a simple add/subtract of above columns, thus not including sum check here
    FROM prod.{TABLE}
    WHERE {ONE_DAY_LAG_FILTER}
"""

# Separating out Revenue from Main SQL in order to add extra day lag
REVENUE_SQL = f"""
    SELECT
        COUNT(*) AS total_count,  --needed for getting percent of value for credit_revenue_gt_0 check
        SUM(ad_revenue) AS ad_revenue,
        SUM(iap_credit_revenue) AS iap_credit_revenue,
        SUM(iap_sub_revenue) AS iap_sub_revenue,
        SUM(iap_other_revenue) AS iap_other_revenue,
        SUM(wireless_subscription_revenue) AS wireless_subscription_revenue,
        SUM(credit_revenue) AS credit_revenue,
        SUM(device_and_plan_revenue) AS device_and_plan_revenue,
        SUM(other_purchase_revenue) AS other_purchase_revenue,
        SUM(CASE WHEN credit_revenue > 0 THEN 1 ELSE 0 END) AS credit_revenue_gt_0_cnt
    FROM prod.{TABLE}
    WHERE {TWO_DAY_LAG_FILTER}
"""

DUPES_SQL = f"""
    SELECT COUNT(*) AS cnt_dups FROM (
        SELECT 1
        FROM prod.{TABLE}
        WHERE {ONE_DAY_LAG_FILTER}
        GROUP BY {", ".join(PKEY_COLS)}
        HAVING COUNT(*) > 1
    );
"""

AD_REVENUE_SRC_SQL = f"""
    SELECT SUM(NVL(adjusted_revenue, 0)) AS ad_revenue
    FROM prod.analytics_staging.revenue_user_daily_ad
    WHERE {TWO_DAY_LAG_FILTER};
"""

IAP_REVENUE_SRC_SQL = f"""
    SELECT
        SUM(CASE WHEN product_category ILIKE 'International Credit%' OR product_category = 'Calling Forward'
              THEN iap_revenue ELSE 0 END) AS iap_credit_revenue,
        SUM(CASE WHEN product_category NOT ILIKE 'International Credit%' 
                AND product_category != 'Calling Forward' AND product_category != 'Other' 
              THEN iap_revenue ELSE 0 END) AS iap_sub_revenue,
        SUM(CASE WHEN product_category = 'Other' 
              THEN iap_revenue ELSE 0 END) AS iap_other_revenue
    FROM prod.analytics_staging.revenue_user_daily_iap
    WHERE {TWO_DAY_LAG_FILTER};
"""

DAILY_PURCHASE_REVENUE_SRC_SQL = f"""
    SELECT
        SUM(CASE WHEN item_category = 'subscription' THEN revenue ELSE 0 END) AS wireless_subscription_revenue,
        SUM(CASE WHEN item_category = 'credit' THEN revenue ELSE 0 END) AS credit_revenue,
        SUM(CASE WHEN item_category = 'device_and_plan' THEN revenue ELSE 0 END) AS device_and_plan_revenue,
        SUM(CASE WHEN item_category = 'other' THEN revenue ELSE 0 END) AS other_purchase_revenue
    FROM prod.analytics_staging.revenue_user_daily_purchase
    WHERE {TWO_DAY_LAG_FILTER};
"""

# This source table cost_user_daily_message_cost is empty for days 1 and 2 of every month
#    because it's source table cost_monthly_message_cost is not populated for month until day 3 of month
MESSAGES_COST_SRC_SQL = f"""
    SELECT 
        SUM(NVL(total_mms_cost, 0)) AS mms_cost,
        SUM(NVL(total_sms_cost, 0)) AS sms_cost,
        SUM(NVL(total_mms_cost, 0)) + SUM(NVL(total_sms_cost, 0)) AS message_cost
    FROM prod.analytics_staging.cost_user_daily_message_cost
    WHERE {ONE_DAY_LAG_FILTER}
"""

CALL_COST_SRC_SQL = f"""
    SELECT SUM(NVL(termination_cost, 0)) + SUM(NVL(layered_paas_cost, 0)) AS calling_cost
    FROM prod.analytics_staging.cost_user_daily_call_cost
    WHERE {ONE_DAY_LAG_FILTER}
"""

TMOBILE_COST_SQL = f"""
    SELECT 
        SUM(NVL(mrc_cost, 0)) AS tmobile_mrc_cost,
        SUM(NVL(mb_usage_overage_cost, 0)) AS tmobile_overage_cost,
        SUM(NVL(mrc_cost, 0)) + SUM(NVL(mb_usage_overage_cost, 0)) AS tmobile_cost
    FROM prod.analytics_staging.cost_user_daily_tmobile_cost 
    WHERE {ONE_DAY_LAG_FILTER}
"""

COL_STATS_COST_SQL = f"""
    SELECT
        {ColumnStatisticsCheck.get_statistics_sql(column_name="mms_cost", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="sms_cost", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="message_cost", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="calling_cost", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="did_cost", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="tmobile_mrc_cost", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="tmobile_overage_cost", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="tmobile_cost", percentile=.95)}
    FROM prod.{TABLE} AS main
    LEFT JOIN (
        SELECT username FROM prod.{TABLE}
        WHERE 
            (date_utc = CAST(:run_date AS DATE)) 
            AND (
                (ad_revenue > 35)
                OR (tmobile_cost > 35)
                OR (message_cost > 10)
                OR (calling_cost > 30)
            )
    ) AS anomalies ON (main.username = anomalies.username)
    WHERE
        {ONE_DAY_LAG_FILTER}
        AND (anomalies.username IS NULL)  --filters out anomaly records
"""

COL_STATS_REVENUE_SQL = f"""
    SELECT
        {ColumnStatisticsCheck.get_statistics_sql(column_name="ad_revenue", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="iap_credit_revenue", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="iap_sub_revenue", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="iap_other_revenue", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="wireless_subscription_revenue", percentile=.95)},
        --deprecated stats check for credit_revenue column in favor of rate check included below.
        {ColumnStatisticsCheck.get_statistics_sql(column_name="device_and_plan_revenue", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="other_purchase_revenue", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="profit", percentile=.95)}
    FROM prod.{TABLE} AS main
    LEFT JOIN (
        SELECT username FROM prod.{TABLE}
        WHERE 
            (date_utc = CAST(:run_date AS DATE)) 
            AND (
                (ad_revenue > 35)
                OR (tmobile_cost > 35)
                OR (message_cost > 10)
                OR (calling_cost > 30)
            )
    ) AS anomalies ON (main.username = anomalies.username)
    WHERE
        {TWO_DAY_LAG_FILTER}
        AND (anomalies.username IS NULL)  --filters out anomaly records
"""


class UserDailyProfitDQChecks(BaseDQChecks):
    name = "user_daily_profits DQ Checks"
    description = "Aggregate DQ Checks for user_daily_profits table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(name="user_daily_profit Main Input",
                     alias="main_input",
                     src_sql=MAIN_SQL),

            DBOInput(name="user_daily_profit Revenue Input",
                     alias="revenue_input",
                     src_sql=REVENUE_SQL),

            DBOInput(name="user_daily_profit_dupes Input",
                     alias="user_daily_profit_dupes",
                     src_sql=DUPES_SQL),

            DBOInput(name="Ad Revenue Source Input",
                     alias="ad_rev_input",
                     src_sql=AD_REVENUE_SRC_SQL),

            DBOInput(name="IAP Revenue Source Input",
                     alias="iap_rev_input",
                     src_sql=IAP_REVENUE_SRC_SQL),

            DBOInput(name="Daily Purchase Source Input",
                     alias="daily_purchase_input",
                     src_sql=DAILY_PURCHASE_REVENUE_SRC_SQL),

            DBOInput(name="Messages Cost Source Input",
                     alias="messages_cost_input",
                     src_sql=MESSAGES_COST_SRC_SQL),

            DBOInput(name="Call Cost Source Input",
                     alias="call_cost_input",
                     src_sql=CALL_COST_SRC_SQL),

            DBOInput(name="TMobile Cost Source Input",
                     alias="tmobile_cost_input",
                     src_sql=TMOBILE_COST_SQL),

            DBOInput(name="Column Stats Cost Input",
                     alias="col_stats_cost",
                     src_sql=COL_STATS_COST_SQL),

            DBOInput(name="Column Stats Revenue Input",
                     alias="col_stats_revenue",
                     src_sql=COL_STATS_REVENUE_SQL)
        ]

    def get_checks(self):
        return [
            TotalCountCheck(
                filter_date_column_name="date_utc",
                total_count=self.main_input[0]["total_count"],
                lower_threshold=14000000,  # min over 3rd and 4th QTR 2022 was 15.8M
                upper_threshold=25000000  # Good enough to ensure there isn't total duplication
            ),

            NoDuplicateRowsCheck(dups_count=self.user_daily_profit_dupes[0]["cnt_dups"]),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(input_object=self.main_input, column_names=NO_MISSING_DATA_COLS),

            SrcAndDstValuesMatchCheck(src_input=self.ad_rev_input, dst_input=self.revenue_input,
                                      column_names="ad_revenue"),

            SrcAndDstValuesMatchCheck(src_input=self.iap_rev_input, dst_input=self.revenue_input,
                                      column_names=["iap_credit_revenue", "iap_sub_revenue", "iap_other_revenue"]),

            SrcAndDstValuesMatchCheck(src_input=self.daily_purchase_input, dst_input=self.revenue_input,
                                      column_names=["wireless_subscription_revenue", "credit_revenue",
                                                    "device_and_plan_revenue", "other_purchase_revenue"]),

            SrcAndDstValuesMatchCheck(src_input=self.messages_cost_input, dst_input=self.main_input,
                                      column_names=["mms_cost", "sms_cost", "message_cost"]),

            SrcAndDstValuesMatchCheck(src_input=self.call_cost_input, dst_input=self.main_input,
                                      column_names="calling_cost"),

            SrcAndDstValuesMatchCheck(src_input=self.tmobile_cost_input, dst_input=self.main_input,
                                      column_names=["tmobile_mrc_cost", "tmobile_overage_cost", "tmobile_cost"]),

            ColumnStatisticsCheck(
                input_object=self.col_stats_revenue,
                column_name="ad_revenue",
                avg_expr="0.008 <= :value <= 0.025",
                median_expr=":value == 0",
                stddev_expr="0.06 <= :value <= 0.15",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats_revenue,
                column_name="iap_credit_revenue",
                avg_expr="0.000002 <= :value <= 0.00012",
                median_expr=":value == 0",
                max_expr="7 <= :value <= 235",
                stddev_expr="0.004 <= :value <= 0.02",
                percentile_expr=":value == 0",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats_revenue,
                column_name="iap_sub_revenue",
                avg_expr="0.00001 <= :value <= 0.01",
                median_expr=":value == 0",
                max_expr="19 <= :value <= 100",
                stddev_expr="0.01 <= :value <= 0.2",
                percentile_expr=":value == 0",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats_revenue,
                column_name="iap_other_revenue",
                avg_expr="0 <= :value <= 0.0005",
                median_expr=":value == 0",
                max_expr="0 <= :value <= 75",
                stddev_expr="0 <= :value <= 0.04",
                percentile_expr=":value == 0",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats_revenue,
                column_name="wireless_subscription_revenue",
                avg_expr="0.00009 <= :value <= 0.0013",
                median_expr=":value == 0",
                max_expr="25 <= :value <= 600",
                stddev_expr="0.04 <= :value <= 0.16",
                percentile_expr=":value == 0",
                raise_yellow_alert=True
            ),

            # Note this column has been 0 for many years as item_category='device_and_plan_revenue' does not exist.
            ColumnStatisticsCheck(
                input_object=self.col_stats_revenue,
                column_name="device_and_plan_revenue",
                avg_expr=":value == 0",  # all 0's as the category doesn't exist upstream.
                median_expr=":value == 0",
                max_expr=":value == 0",
                stddev_expr=":value == 0",
                percentile_expr=":value == 0",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats_revenue,
                column_name="other_purchase_revenue",
                avg_expr="0.000001 <= :value <= 0.0001",
                median_expr=":value == 0",
                max_expr="1 <= :value <= 300",
                stddev_expr="0.003 <= :value <= 0.03",
                percentile_expr=":value == 0",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats_cost,
                column_name="mms_cost",
                avg_expr="0.00015 <= :value <= 0.0003",
                median_expr=":value == 0",
                max_expr="0.27 <= :value <= 18",
                stddev_expr="0.0016 <= :value <= 0.015",
                percentile_expr="0 <= :value <= 0.0004",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats_cost,
                column_name="sms_cost",
                avg_expr="0.000038 <= :value <= 0.000085",
                median_expr=":value == 0",
                max_expr="0.05 <= :value <= 163",
                stddev_expr="0.0004 <= :value <= 0.04",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats_cost,
                column_name="message_cost",
                avg_expr="0.00013 <= :value <= 0.00035",
                median_expr=":value == 0",
                max_expr="0.25 <= :value <= 5",
                stddev_expr="0.0018 <= :value <= 0.01",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats_cost,
                column_name="calling_cost",
                avg_expr="0.0003 <= :value <= 0.003",
                median_expr=":value == 0",
                max_expr="4 <= :value <= 40",
                stddev_expr="0.006 <= :value <= 0.5",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats_cost,
                column_name="did_cost",
                avg_expr="0.0003 <= :value <= 0.00037",
                median_expr="0.0003 <= :value <= 0.0004",
                max_expr="0.12 <= :value <= 0.134",
                stddev_expr="0.00006 <= :value <= 0.00013",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats_cost,
                column_name="tmobile_mrc_cost",
                avg_expr="0.0005 <= :value <= 0.0015",
                median_expr=":value == 0",
                max_expr=":value == 2.1666666666666665",
                stddev_expr="0.007 <= :value <= 0.0185",
                percentile_expr=":value == 0",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats_cost,
                column_name="tmobile_overage_cost",
                avg_expr=":value <= 0.00046",
                median_expr=":value == 0",
                max_expr=":value <= 50",
                stddev_expr=":value <= 0.06",
                percentile_expr=":value == 0",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats_cost,
                column_name="tmobile_cost",
                avg_expr="0.0005 <= :value <= 0.002",
                median_expr=":value == 0",
                max_expr="2 <= :value <= 50",
                stddev_expr="0.01 <= :value <= 0.07",
                percentile_expr=":value == 0",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats_revenue,
                column_name="profit",
                avg_expr="0.003 <= :value <= 0.03",
                median_expr="-0.00040 <= :value <= 0.001",
                max_expr="35 <= :value <= 400",
                stddev_expr="0.09 <= :value <= 0.55",
                raise_yellow_alert=True
            ),

            # following check replaces column stats check for column credit_revenue
            # too few users have a credit_revenue value > 0 which makes stats checks too volatile to be useful.
            # Thus, ensure percent of users with credit_revenue > 0 remains very low
            PercentValueToTotalCheck(
                name="credit_revenue percent of total",
                description="Ensure percent of total of users with credit_revenue > 0 remains very low.",
                column_name="credit_revenue",
                value=self.revenue_input[0]["credit_revenue_gt_0_cnt"],
                total=self.revenue_input[0]["total_count"],
                yellow_expr=":value < 0.005",
                yellow_warning="The percent of users with credit_revenue > 0 has gone above expected max of 0.005%",
                todo=["If percent rises above 1%, need to follow up with upstream team as to why.",
                      "If it's a one time thing, ignore.",
                      "If this may happen regularly, adjust yellow_expr accordingly."]
            )
        ]
