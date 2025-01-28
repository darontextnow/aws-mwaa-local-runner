"""DQ Checks for Snowflake user_set_daily_activities table.

Note all mms/sms messaging data is empty on 1st and 2nd day of every month due to missing data upstream. Adjusting
for this schedule anomaly in DAG.

Contains checks for every total column in table ensuring total sum of each column
matches total sum in earlier source tables. This helps guarantee no data is missing when compared to source.

Contains checks for every total column to warn us should any values move outside given ranges of their norm. Using
column stats such as avg, min, max, stddev, median, percentile to help ensure totals stay within established norms.

Contains checks to ensure columns that should have no null values don't suddenly have null values introduced.

Many of the current thresholds are based on the following:
    - querying user_set_daily_activities table for date range: "(date_utc BETWEEN CAST('2022-01-01' AS DATE)
        AND CAST('2023-01-08' AS DATE))" for daily min and max for each stat.

"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck
from de_utils.dq_checks_framework.check_classes.column_statistics_check import ColumnStatisticsCheck
from de_utils.dq_checks_framework.check_classes.no_duplicate_rows_check import NoDuplicateRowsCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck
from de_utils.dq_checks_framework.check_classes.src_and_dst_values_match_check import SrcAndDstValuesMatchCheck

TABLE = "analytics.user_set_daily_activities"
PKEY_COLS = ["date_utc", "user_set_id", "client_type"]

# these columns are checked for NULL or '' values in the user_set_daily_activities_table
NO_MISSING_DATA_COLS = ["date_utc", "user_set_id", "client_type"]

DST_SQL = f"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        ARRAY_UNIQUE_AGG(client_type) AS client_types,
        SUM(dau) AS dau,
        SUM(ad_impressions) AS ad_impressions,
        SUM(mms_messages) AS mms_messages,
        SUM(sms_messages) AS sms_messages,
        SUM(total_incoming_calls) AS total_incoming_calls,
        SUM(CASE WHEN total_incoming_calls <> total_incoming_free_calls + total_incoming_paid_calls THEN 1 ELSE 0 END)
            AS count_incoming_free_plus_paid_not_equal_to_total,
        SUM(total_incoming_call_duration) AS total_incoming_call_duration,
        SUM(total_incoming_domestic_calls) AS total_incoming_domestic_calls,
        SUM(total_incoming_free_calls) AS total_incoming_free_calls,
        SUM(total_incoming_international_calls) AS total_incoming_international_calls,
        SUM(total_incoming_paid_calls) AS total_incoming_paid_calls,
        SUM(total_incoming_unique_calling_contacts) AS total_incoming_unique_calling_contacts,
        SUM(total_outgoing_calls) AS total_outgoing_calls,
        SUM(CASE WHEN total_outgoing_calls <> total_outgoing_free_calls + total_outgoing_paid_calls THEN 1 ELSE 0 END)
            AS count_outgoing_free_plus_paid_not_equal_to_total,
        SUM(total_outgoing_call_duration) AS total_outgoing_call_duration,
        SUM(total_outgoing_domestic_calls) AS total_outgoing_domestic_calls,
        SUM(total_outgoing_free_calls) AS total_outgoing_free_calls,
        SUM(total_outgoing_international_calls) AS total_outgoing_international_calls,
        SUM(total_outgoing_paid_calls) AS total_outgoing_paid_calls,
        SUM(total_outgoing_unique_calling_contacts) AS total_outgoing_unique_calling_contacts,
        SUM(video_call_initiations) AS video_call_initiations
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

DAU_SRC_SQL = """
    SELECT SUM(NVL(dau, CASE WHEN client_type = 'TN_WEB' THEN 1 ELSE 0 END)) AS dau
    FROM (
        SELECT
            date_utc,
            user_set_id,
            client_type,
            dau
        FROM prod.analytics_staging.dau_user_set_active_days
        WHERE (date_utc = CAST(:run_date AS DATE))
    ) a
    FULL OUTER JOIN (
        SELECT
            date_utc,
            COALESCE(set_uuid, username) AS user_set_id,
            client_type
        FROM prod.analytics.user_daily_activities
        LEFT JOIN prod.dau.user_set USING (username)
        WHERE (date_utc = CAST(:run_date AS DATE))
        GROUP BY 1, 2, 3
    ) b USING (date_utc, user_set_id, client_type)
"""

IMPRESSIONS_SRC_SQL = """
    SELECT SUM(NVL(adjusted_impressions, 0)) AS ad_impressions
    FROM prod.analytics_staging.revenue_user_daily_ad
    WHERE (date_utc = CAST(:run_date AS DATE));
"""

# This source table cost_user_daily_message_cost is empty for days 1 and 2 of every month
#    because it's source table cost_monthly_message_cost is not populated for month until day 3 of month
MESSAGES_SRC_SQL = """
    SELECT 
        SUM(NVL(mms_number_messages, 0)) AS mms_messages,
        SUM(NVL(sms_number_messages, 0)) AS sms_messages
    FROM prod.analytics_staging.cost_user_daily_message_cost
    WHERE (date_utc = CAST(:run_date AS DATE))
"""

INCOMING_SQL = """
    SELECT 
        SUM(total_incoming_calls) AS total_incoming_calls,
        SUM(total_incoming_call_duration) AS total_incoming_call_duration,
        SUM(total_incoming_domestic_calls) AS total_incoming_domestic_calls,
        SUM(total_incoming_free_calls) AS total_incoming_free_calls,
        SUM(total_incoming_international_calls) AS total_incoming_international_calls,
        SUM(total_incoming_paid_calls) AS total_incoming_paid_calls,
        SUM(total_incoming_unique_calling_contacts) AS total_incoming_unique_calling_contacts
    FROM (
        SELECT 
            COUNT(*) AS total_incoming_calls,
            SUM(call_secs) AS total_incoming_call_duration,
            SUM(CASE WHEN is_domestic = True THEN 1 ELSE 0 END) AS total_incoming_domestic_calls,
            SUM(CASE WHEN is_free = True THEN 1 ELSE 0 END) AS total_incoming_free_calls,
            SUM(CASE WHEN is_domestic = False THEN 1 ELSE 0 END) AS total_incoming_international_calls,
            SUM(CASE WHEN is_free = False THEN 1 ELSE 0 END) AS total_incoming_paid_calls,
            COUNT(DISTINCT incoming_call_contact) AS total_incoming_unique_calling_contacts
        FROM prod.analytics.incoming_calls 
        WHERE 
            (date_utc = CAST(:run_date AS DATE))
            AND (username <> '')    --this is needed on some days to filter out small number of empty users
            AND (client_type <> '') --not sure if this one ever hits, but this is from source query
        GROUP BY date_utc, username, client_type
    )
"""

OUTGOING_SQL = """
    SELECT 
        SUM(total_outgoing_calls) AS total_outgoing_calls,
        SUM(total_outgoing_call_duration) AS total_outgoing_call_duration,
        SUM(total_outgoing_domestic_calls) AS total_outgoing_domestic_calls,
        SUM(total_outgoing_free_calls) AS total_outgoing_free_calls,
        SUM(total_outgoing_international_calls) AS total_outgoing_international_calls,
        SUM(total_outgoing_paid_calls) AS total_outgoing_paid_calls,
        SUM(total_outgoing_unique_calling_contacts) AS total_outgoing_unique_calling_contacts
    FROM (
        SELECT 
            COUNT(*) AS total_outgoing_calls,
            SUM(call_secs) AS total_outgoing_call_duration,
            SUM(CASE WHEN is_domestic = True THEN 1 ELSE 0 END) AS total_outgoing_domestic_calls,
            SUM(CASE WHEN is_free = True THEN 1 ELSE 0 END) AS total_outgoing_free_calls,
            SUM(CASE WHEN is_domestic = False THEN 1 ELSE 0 END) AS total_outgoing_international_calls,
            SUM(CASE WHEN is_free = False THEN 1 ELSE 0 END) AS total_outgoing_paid_calls,
            COUNT(DISTINCT outgoing_call_contact) AS total_outgoing_unique_calling_contacts
        FROM prod.analytics.outgoing_calls 
        WHERE (date_utc = CAST(:run_date AS DATE))
        GROUP BY date_utc, username, client_type
    )
"""

# There is often no rows expected from this table. Using NVL here to return 0 on purpose
VIDEO_SQL = """
    SELECT NVL(SUM(NVL(video_call_initiations, 0)), 0) AS video_call_initiations
    FROM (
        SELECT
            created_at::DATE AS date_utc,
            "client_details.client_data.user_data.username" AS username,
            CASE
                WHEN "client_details.client_data.client_platform" = 'IOS' THEN 'TN_IOS_FREE'
                WHEN "client_details.client_data.client_platform" = 'CP_ANDROID' THEN 'TN_ANDROID'
                WHEN "client_details.client_data.client_platform" = 'WEB' THEN 'TN_WEB'
            END AS client_type,
            COUNT(1) AS video_call_initiations
        FROM prod.party_planner.videocallinitiate
        WHERE (date_utc = CAST(:run_date AS DATE)) AND "payload.success"
        GROUP BY created_at::DATE, username, client_type
    );
"""

COL_STATS_SQL = f"""
    SELECT
        {ColumnStatisticsCheck.get_statistics_sql(column_name="dau", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="ad_impressions", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="mms_messages", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="sms_messages", percentile=.95)},
        -- total_incoming_calls covered by total_incoming_free_calls + total_incoming_paid_calls
        {ColumnStatisticsCheck.get_statistics_sql(column_name="total_incoming_call_duration", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="total_incoming_domestic_calls", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="total_incoming_free_calls", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="total_incoming_international_calls", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="total_incoming_paid_calls", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="total_incoming_unique_calling_contacts", 
                                                  percentile=.95)},
        -- total_outgoing_calls covered by total_outgoing_free_calls + total_outgoing_paid_calls
        {ColumnStatisticsCheck.get_statistics_sql(column_name="total_outgoing_call_duration", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="total_outgoing_domestic_calls", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="total_outgoing_free_calls", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="total_outgoing_international_calls", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="total_outgoing_paid_calls", percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="total_outgoing_unique_calling_contacts", 
                                                  percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="video_call_initiations", percentile=.95)}
    FROM prod.{TABLE} AS main
    LEFT JOIN (
        SELECT user_set_id, client_type FROM prod.{TABLE}
        WHERE
            (date_utc = CAST(:run_date AS DATE))
            AND ( -- see notes in user_daily_activities.py as to why we're using these filters
                (ad_impressions > 9000)
                OR (mms_messages > 600)
                OR (sms_messages > 600)
                OR (total_incoming_calls > 800)
                OR (total_outgoing_calls > 800)
            )
    ) AS anomaly ON (main.user_set_id = anomaly.user_set_id) AND (main.client_type = anomaly.client_type)
    WHERE 
        (date_utc = CAST(:run_date AS DATE))
        AND (anomaly.user_set_id IS NULL)  --filters out anomaly records
"""


class UserSetDailyActivitiesDQChecks(BaseDQChecks):
    name = "user_set_daily_activities DQ Checks"
    description = "Aggregate DQ Checks for user_set_daily_activities table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(name="user_set_daily_activities Input",
                     alias="dst_input",
                     src_sql=DST_SQL),

            DBOInput(name="dupes Input",
                     alias="dupes",
                     src_sql=DUPES_SQL),

            DBOInput(name="DAU Source Input",
                     alias="dau_input",
                     src_sql=DAU_SRC_SQL),

            DBOInput(name="Ad Impressions Source Input",
                     alias="impressions_input",
                     src_sql=IMPRESSIONS_SRC_SQL),

            DBOInput(name="Messages Source Input",
                     alias="messages_input",
                     src_sql=MESSAGES_SRC_SQL),

            DBOInput(name="incoming_calls Source Input",
                     alias="incoming_input",
                     src_sql=INCOMING_SQL),

            DBOInput(name="outgoing_calls Source Input",
                     alias="outgoing_input",
                     src_sql=OUTGOING_SQL),

            DBOInput(name="Video Source Input",
                     alias="video_input",
                     src_sql=VIDEO_SQL),

            DBOInput(name="Column Stats Input",
                     alias="col_stats",
                     src_sql=COL_STATS_SQL)
        ]

    def get_checks(self):

        return [
            TotalCountCheck(
                filter_date_column_name="date_utc",
                total_count=self.dst_input[0]["total_count"],
                lower_threshold=4000000,  # min 2023-24 was 4M
                upper_threshold=6300000  # Good enough to ensure there isn't total duplication
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

            Check(
                name="Total Calls Calculation Check",
                description="Ensure total calls is equal to paid_calls plus free_calls",
                column_name="count_incoming_free_plus_paid_not_equal_to_total",
                value=self.dst_input[0]["count_incoming_free_plus_paid_not_equal_to_total"],
                red_expr=":value == 0",
                red_error=(":value records found where total_incoming_paid_calls + total_incoming_free_calls <>"
                           " total_incoming_calls"),
                todo=["This should not be possible. Research how these numbers/the calculation got off"]
            ),

            Check(
                name="Total Calls Calculation Check",
                description="Ensure total calls is equal to paid_calls plus free_calls",
                column_name="count_outgoing_free_plus_paid_not_equal_to_total",
                value=self.dst_input[0]["count_outgoing_free_plus_paid_not_equal_to_total"],
                red_expr=":value == 0",
                red_error=(":value records found where total_outgoing_paid_calls + total_outgoing_free_calls <>"
                           " total_outgoing_calls"),
                todo=["This should not be possible. Research how these numbers/the calculation got off"]
            ),

            SrcAndDstValuesMatchCheck(src_input=self.dau_input, dst_input=self.dst_input, 
                                      column_names="dau"),

            SrcAndDstValuesMatchCheck(src_input=self.impressions_input, dst_input=self.dst_input,
                                      column_names="ad_impressions"),

            SrcAndDstValuesMatchCheck(src_input=self.messages_input, dst_input=self.dst_input,
                                      column_names=["mms_messages", "sms_messages"]),

            SrcAndDstValuesMatchCheck(src_input=self.incoming_input, dst_input=self.dst_input,
                                      column_names=["total_incoming_calls", "total_incoming_call_duration",
                                                    "total_incoming_domestic_calls", "total_incoming_free_calls",
                                                    "total_incoming_international_calls", "total_incoming_paid_calls",
                                                    "total_incoming_unique_calling_contacts"]),

            SrcAndDstValuesMatchCheck(src_input=self.outgoing_input, dst_input=self.dst_input,
                                      column_names=["total_outgoing_calls", "total_outgoing_call_duration",
                                                    "total_outgoing_domestic_calls", "total_outgoing_free_calls",
                                                    "total_outgoing_international_calls", "total_outgoing_paid_calls",
                                                    "total_outgoing_unique_calling_contacts"]),

            SrcAndDstValuesMatchCheck(src_input=self.video_input, dst_input=self.dst_input,
                                      column_names="video_call_initiations"),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="dau",
                avg_expr="0.5 <= :value <= 0.68",
                median_expr=":value == 1",
                max_expr="3 <= :value <= 12",
                stddev_expr="0.44 <= :value <= 0.5",
                percentile_expr=":value == 1",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="ad_impressions",
                avg_expr="11 <= :value <= 28",
                median_expr="0 <= :value <= 2",
                # max_expr="2200 <= :value <= 9000",  # replaced by filter in query
                stddev_expr="40 <= :value <= 80",
                percentile_expr="59 <= :value <= 146",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="mms_messages",
                avg_expr="1.2 <= :value <= 3.2",
                median_expr=":value == 0",
                # max_expr="400 <= :value <= 600",  # replaced by filter in query
                stddev_expr="10 <= :value <= 15",
                percentile_expr="4 <= :value <= 14",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="sms_messages",
                avg_expr="2.2 <= :value <= 4.2",
                median_expr=":value == 0",
                # max_expr="0 <= :value <= 600",  # replaced by filter in query
                stddev_expr="11 <= :value <= 20",
                percentile_expr="10 <= :value <= 32",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="total_incoming_call_duration",
                avg_expr="70 <= :value <= 110",
                median_expr=":value == 0",
                max_expr="60000 <= :value <= 500000",
                stddev_expr="500 <= :value <= 900",
                percentile_expr="175 <= :value <= 350",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="total_incoming_domestic_calls",
                avg_expr="0.9 <= :value <= 1.67",
                median_expr=":value == 0",
                # max_expr="550 <= :value <= 800",  # replaced by filter in query
                stddev_expr="3 <= :value <= 12",
                percentile_expr="4 <= :value <= 8",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="total_incoming_free_calls",
                avg_expr="0.9 <= :value <= 1.67",
                median_expr=":value == 0",
                # max_expr="550 <= :value <= 800",  # replaced by filter in query
                stddev_expr="3 <= :value <= 12",
                percentile_expr="4 <= :value <= 8",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="total_incoming_international_calls",
                avg_expr="0.002 <= :value <= 0.013",
                median_expr=":value == 0",
                stddev_expr="0.10 <= :value <= 0.69",
                percentile_expr=":value == 0",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="total_incoming_paid_calls",
                avg_expr=":value == 0",
                max_expr="0 <= :value <= 23",  # typically this is 0, has gone up higher on some days
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="total_incoming_unique_calling_contacts",
                avg_expr="0.4 <= :value <= 1.02",
                median_expr=":value == 0",
                max_expr="120 <= :value <= 3500",
                stddev_expr="1.0 <= :value <= 4.32",
                percentile_expr="2 <= :value <= 4",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="total_outgoing_call_duration",
                avg_expr="100 <= :value <= 200",
                median_expr=":value == 0",
                max_expr="70000 <= :value <= 1000000",
                stddev_expr="500 <= :value <= 1500",
                percentile_expr="380 <= :value <= 700",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="total_outgoing_domestic_calls",
                avg_expr="0.9 <= :value <= 1.5",
                median_expr=":value == 0",
                # max_expr="500 <= :value <= 800",  # replaced by filter in query
                stddev_expr="4 <= :value <= 7",
                percentile_expr="6 <= :value <= 9",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="total_outgoing_free_calls",
                avg_expr="1.08 <= :value <= 1.86",
                median_expr=":value == 0",
                # max_expr="500 <= :value <= 800",  # replaced by filter in query
                stddev_expr="4 <= :value <= 15",
                percentile_expr="6 <= :value <= 9",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="total_outgoing_international_calls",
                avg_expr="0.009 <= :value <= 0.018",
                median_expr=":value == 0",
                # max_expr="220 <= :value <= 800",  # replaced by filter in query
                stddev_expr="0.4 <= :value <= 2.32",
                percentile_expr=":value == 0",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="total_outgoing_paid_calls",
                avg_expr="0.0004 <= :value <= 0.005",
                median_expr=":value == 0",
                max_expr="19 <= :value <= 900",
                stddev_expr="0.042 <= :value <= 0.75",
                percentile_expr=":value == 0",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="total_outgoing_unique_calling_contacts",
                avg_expr="0.48 <= :value <= 0.7",
                median_expr=":value == 0",
                max_expr="200 <= :value <= 6950",
                stddev_expr="1 <= :value <= 8",
                percentile_expr="3 <= :value <= 4",
                raise_yellow_alert=True
            ),

            ColumnStatisticsCheck(
                input_object=self.col_stats,
                column_name="video_call_initiations",
                avg_expr=":value == 0",
                max_expr="0 <= :value <= 1150",
                stddev_expr="0 <= :value <= 0.55",  # typically 0
                percentile_expr=":value == 0",
                raise_yellow_alert=True
            )
        ]
