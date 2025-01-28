"""DQ Checks for the Snowflake analytics.user_daily_activities table.

There is a lot of duplicate columns/overlapping columns in this table and user_set_daily_activities.
This checks module contains only checks for total columns in this table not already checked
in the user_set_daily_activities checks.

Contains checks for every total column to warn us should any values move outside given ranges of their norm. Using
column stats such as avg, min, max, stddev, median, percentile to help ensure totals stay within established norms.

Contains checks to ensure columns that should have no null values don't suddenly have null values introduced.

Notes:
    Per Jordan Shaw, we continue to send anomaly alerts to anomaly-usage-alerts channel for high call volumes only.
    All other anomaly alerts were deprecated as Jordan said they are not actionable for his team.
    3/2023 - replaced column stats check for total_mms_cost and total_sms_cost with cost_per_message checks.
       Making sure cost_per_message is the same for all users for given day and is within normal range.

"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.anomaly_records_check import AnomalyRecordsCheck
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck
from de_utils.dq_checks_framework.check_classes.column_statistics_check import ColumnStatisticsCheck
from de_utils.dq_checks_framework.check_classes.no_duplicate_rows_check import NoDuplicateRowsCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck
from de_utils.dq_checks_framework.check_classes.src_and_dst_values_match_check import SrcAndDstValuesMatchCheck

TABLE = "analytics.user_daily_activities"
PKEY_COLS = ["date_utc", "username", "client_type"]

# these columns are checked for NULL or '' values in the user_daily_activities_table
NO_MISSING_DATA_COLS = ["date_utc", "username", "client_type"]

DST_SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        ARRAY_UNIQUE_AGG(client_type) AS client_types,
        SUM(total_mms_cost) AS total_mms_cost,
        SUM(total_sms_cost) AS total_sms_cost,
        SUM(total_termination_cost) AS total_termination_cost,
        SUM(total_layered_paas_cost) AS total_layered_paas_cost,
        COUNT(DISTINCT CASE WHEN mms_messages > 0 
            THEN CAST(total_mms_cost/mms_messages AS DECIMAL(14,13)) 
            ELSE CAST(total_mms_cost/1 AS DECIMAL(14,13)) END)
            AS count_distinct_mms_cost_per_message,
        SUM(total_mms_cost/NULLIFZERO(mms_messages))/
            SUM(CASE WHEN mms_messages > 0 THEN 1 ELSE NULL END)
            AS mms_cost_per_message,
        COUNT(DISTINCT CASE WHEN sms_messages > 0 
            THEN CAST(total_sms_cost/sms_messages AS DECIMAL(14,13)) 
            ELSE CAST(total_sms_cost/1 AS DECIMAL(14,13)) END)
            AS count_distinct_sms_cost_per_message,
        SUM(total_sms_cost/NULLIFZERO(sms_messages))/
            SUM(CASE WHEN sms_messages > 0 THEN 1 ELSE NULL END)
            AS sms_cost_per_message
        --all other total columns already checked in USER_SET_DAILY_ACTIVITIES checks
        --total_adjusted_revenue is separated out in it's own query below
    FROM prod.{TABLE}
    WHERE (date_utc = CAST(:run_date AS DATE))
"""

DUPES_SQL = f"""
    SELECT COUNT(*) AS cnt_dups FROM (
        SELECT 1
        FROM prod.{TABLE}
        WHERE (date_utc = DATE(:run_date))
        GROUP BY {", ".join(PKEY_COLS)}
        HAVING COUNT(*) > 1
    );
"""

# Must separate total_adjusted_revenue out from all other columns as this column has a 2-day lag before it's populated
TOTAL_ADJ_REV_DST_SQL = f"""
    SELECT
        SUM(total_adjusted_revenue) AS total_adjusted_revenue,
        {ColumnStatisticsCheck.get_statistics_sql(column_name="total_adjusted_revenue", percentile=.95)}
    FROM prod.{TABLE}
    WHERE (date_utc = DATEADD(DAY, -1, CAST(:run_date AS DATE)))
"""

TOTAL_ADJ_REV_SRC_SQL = """
    SELECT SUM(NVL(adjusted_revenue, 0)) AS total_adjusted_revenue
    FROM prod.analytics_staging.revenue_user_daily_ad
    WHERE (date_utc = DATEADD(DAY, -1, CAST(:run_date AS DATE)));
"""

# This source table cost_user_daily_message_cost is empty for days 1 and 2 of every month
#    because it's source table cost_monthly_message_cost is not populated for month until day 3 of month
MESSAGES_SRC_SQL = """
    SELECT 
        SUM(NVL(total_mms_cost, 0)) AS total_mms_cost,
        SUM(NVL(total_sms_cost, 0)) AS total_sms_cost
    FROM prod.analytics_staging.cost_user_daily_message_cost
    WHERE (date_utc = CAST(:run_date AS DATE))
"""

OTHER_COST_SRC_SQL = """
    SELECT 
        SUM(NVL(termination_cost, 0)) AS total_termination_cost,
        SUM(NVL(layered_paas_cost, 0)) AS total_layered_paas_cost
    FROM prod.analytics_staging.cost_user_daily_call_cost
    WHERE (date_utc = CAST(:run_date AS DATE))
"""

COL_STATS_SQL = f"""
    SELECT
        {ColumnStatisticsCheck.get_statistics_sql(column_name="total_termination_cost", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="total_layered_paas_cost", percentile=.95)}
    FROM prod.{TABLE} AS main
    LEFT JOIN (
        SELECT username, client_type
        FROM prod.{TABLE}
        WHERE 
            (date_utc = CAST(:run_date AS DATE))
            AND ( --the following are the anomaly record thresholds we agreed to filter out from stats checks
                (ad_impressions > 9000)  
                OR (mms_messages > 600)
                OR (sms_messages > 600)
                OR (total_outgoing_calls > 800)
                OR (total_incoming_calls > 800)
            )
    ) AS anomaly ON (main.username = anomaly.username) AND (main.client_type = anomaly.client_type)
    WHERE 
        (date_utc = CAST(:run_date AS DATE))
        AND (anomaly.username IS NULL)  --filters out anomaly records
"""

ANOMALY_CALLS_REPORTING_SQL = f"""
    SELECT 
        date_utc, user_id_hex, client_type, 
        CONCAT(total_outgoing_calls, ',', total_outgoing_unique_calling_contacts, ',',
        total_incoming_calls, ',', total_incoming_unique_calling_contacts) AS calls 
    FROM prod.{TABLE} a
    JOIN prod.core.users b ON (a.username = b.username) 
    WHERE 
        (date_utc = CAST(:run_date AS DATE)) 
        AND ((total_outgoing_calls > 800) OR (total_incoming_calls > 800))
"""


class UserDailyActivitiesDQChecks(BaseDQChecks):
    name = "user_daily_activities DQ Checks"
    description = "Aggregate DQ Checks for user_daily_activities tables"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(name="user_daily_activities Input",
                     alias="dst_input",
                     src_sql=DST_SQL),

            DBOInput(name="Dupes Input",
                     alias="dupes",
                     src_sql=DUPES_SQL),

            DBOInput(name="total_adjusted_revenue Destination Input",
                     alias="total_adj_rev_dst",
                     src_sql=TOTAL_ADJ_REV_DST_SQL),

            DBOInput(name="total_adjusted_revenue Source Input",
                     alias="total_adj_rev_src",
                     src_sql=TOTAL_ADJ_REV_SRC_SQL),

            DBOInput(name="Messages Source Input",
                     alias="messages_input",
                     src_sql=MESSAGES_SRC_SQL),

            DBOInput(name="Other Costs Source Input",
                     alias="other_costs_input",
                     src_sql=OTHER_COST_SRC_SQL),

            DBOInput(name="Column Stats Input",
                     alias="col_stats",
                     src_sql=COL_STATS_SQL),

            DBOInput(name="Anomaly calls Input",
                     alias="anomaly_calls",
                     src_sql=ANOMALY_CALLS_REPORTING_SQL),

        ]

    def get_checks(self):

        return [
            TotalCountCheck(
                filter_date_column_name="date_utc",
                total_count=self.dst_input[0]["total_count"],
                lower_threshold=3500000,  # min over 3rd and 4th QTR 2022 was 3.5M
                upper_threshold=6000000  # Good enough to ensure there isn't total duplication
            ),

            NoDuplicateRowsCheck(dups_count=self.dupes[0]["cnt_dups"]),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(input_object=self.dst_input, column_names=NO_MISSING_DATA_COLS),

            # Fail if a new value for column 'client_type' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="client_type",
                values=self.dst_input[0]["client_types"],
                expected_values=["TN_IOS_FREE", "TN_IOS", "TN_ANDROID", "TN_WEB", "2L_ANDROID", "OTHER"]
            ),

            SrcAndDstValuesMatchCheck(src_input=self.total_adj_rev_src, dst_input=self.total_adj_rev_dst,
                                      column_names="total_adjusted_revenue"),

            SrcAndDstValuesMatchCheck(src_input=self.messages_input, dst_input=self.dst_input,
                                      column_names=["total_mms_cost", "total_sms_cost"]),

            SrcAndDstValuesMatchCheck(src_input=self.other_costs_input, dst_input=self.dst_input,
                                      column_names=["total_termination_cost", "total_layered_paas_cost"]),

            Check(
                name="Count Distinct Check",
                description="Ensure DISTINCT COUNT of cost/mms_message is 2",
                column_name="mms_cost_per_message",
                value=self.dst_input[0]["count_distinct_mms_cost_per_message"],
                red_expr=":value == 2",  # 1 cost for those who sent messages and cost == 0 for those who didn't
                red_error=":value different costs/mms_message were found. There should only be 2.",
                todo=["The cost for users that didn't send any mms messages should be 0.",
                      "The cost per message should be a constant number for all users that did send messages."
                      "Research how it is possible the cost/mms_message suddenly varies."]
            ),

            Check(
                name="Cost In Range",
                description="Ensure cost/mms_message is within expected range.",
                column_name="mms_cost_per_message",
                value=self.dst_input[0]["mms_cost_per_message"],
                yellow_expr="0.00034 <= :value <= 0.00035",
                yellow_warning="The value (:value) has gone outside the expected range: :yellow_expr",
                todo=["See if there is data missing from message volumes or from upstream costs.",
                      "Explore latest trends for cost/message to see if cost is trending outside these thresholds."]
            ),

            Check(
                name="Count Distinct Check",
                description="Ensure DISTINCT COUNT of cost/sms_message is 2",
                column_name="sms_cost_per_message",
                value=self.dst_input[0]["count_distinct_sms_cost_per_message"],
                red_expr=":value == 2",  # 1 cost for those who sent messages and cost == 0 for those who didn't
                red_error=":value different costs/sms_message were found. There should only be 2.",
                todo=["The cost for users that didn't send any sms messages should be 0.",
                      "The cost per message should be a constant number for all users that did send messages."
                      "Research how it is possible the cost/sms_message suddenly varies."]
            ),

            Check(
                name="Cost In Range",
                description="Ensure cost/sms_message is within expected range.",
                column_name="sms_cost_per_message",
                value=self.dst_input[0]["sms_cost_per_message"],
                yellow_expr="0.00004 <= :value <= 0.00008",
                yellow_warning="The value (:value) has gone outside the expected range: :yellow_expr",
                todo=["See if there is data missing from message volumes or from upstream costs.",
                      "Explore latest trends for cost/message to see if cost is trending outside these thresholds."]
            ),

            # Note this next check is running with a 2-day lag whereas all others are on 1-day lag
            ColumnStatisticsCheck(
                input_object=self.total_adj_rev_dst,
                column_name="total_adjusted_revenue",
                avg_expr="0.02 <= :value <= 0.09",
                stddev_expr="0.12 <= :value <= 0.27",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="total_termination_cost",
                avg_expr="0.001 <= :value <= 0.003",
                stddev_expr="0.009 <= :value <= 0.026",
                percentile_expr="0.004 <= :value <= 0.011",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="total_layered_paas_cost",
                avg_expr=":value == 0",
                median_expr=":value == 0",
                max_expr=":value == 0",
                stddev_expr=":value == 0",
                percentile_expr=":value == 0",
                raise_yellow_alert=True
            ),

            # Jordan Shaw stated they do not have any other call fraud detection in place. So, keep this alerts going.
            # C04PLEUJD50 - #anomaly_usage_alerts
            AnomalyRecordsCheck(
                slack_channel="C04PLEUJD50",
                name="calls above accepted threshold of 800",
                column_name="calls",
                where_clause_expr=("(date_utc = CAST(:run_date AS DATE)) "
                                   "AND ((total_outgoing_calls > 800) OR (total_incoming_calls > 800))"),
                pkey_columns=["date_utc", "user_id_hex", "client_type"],
                anomaly_input_object=self.anomaly_calls
            )
        ]
