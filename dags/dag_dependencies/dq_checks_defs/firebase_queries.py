"""DQ Checks for the Firebase queries - R3,Moments,sim_active and sim_purchase events.

Checks total count of app instance id's that are generated for each event,if the count is within
the limits then the events can be sent out to google analytics else we need to alert the
growth team and make sure the data is right in the underlying table.


Most of the current thresholds are based on looking at the charts in the Firebase console for every event for 
"(dates BETWEEN CAST('2022-05-01' AS DATE) AND CAST('2023-02-15' AS DATE))"
for daily min and max for each stat.

"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check

TABLE = "party_planner_realtime.applifecyclechanged"


MOMENTS_DAY1_SQL = """
    WITH pp_events AS (
        SELECT DISTINCT
            "client_details.android_bonus_data.adjust_id" adid,
            "client_details.android_bonus_data.google_analytics_unique_id" gaid,
            created_at
        FROM party_planner_realtime.applifecyclechanged
        WHERE
            (date_utc >= CAST(:run_date AS DATE) - INTERVAL '2 DAYS')
            AND (adid IS NOT NULL)
            AND (gaid IS NOT NULL)
            AND (gaid != '')
    ),
    early_mover AS (
        SELECT DISTINCT adjust_id AS adid
        FROM analytics_staging.ua_early_mover_event a
        WHERE
            (installed_at >= CAST(:run_date AS DATE) - INTERVAL '2 DAYS')
            AND (combined_event = 1) --- meets both outbound calls and session condition
            AND (client_type = 'TN_ANDROID')
    )    
    SELECT COUNT(DISTINCT new_gaids.gaid) AS moments
    FROM (
        SELECT DISTINCT LOWER(pp_events.gaid) AS gaid
        FROM early_mover
        JOIN pp_events ON (early_mover.adid = pp_events.adid)
    ) new_gaids
"""

EARLY_MOVER_DAY1_SQL = """
    SELECT count(distinct new_gaids.gaid) AS early_mover
    FROM
    (
      SELECT DISTINCT lower(gaid) AS gaid
      FROM
      (
         SELECT DISTINCT adjust_id  AS adid
         FROM analytics_staging.ua_early_mover_event a
         WHERE 
           installed_at >= CAST(:run_date AS DATE) - INTERVAL '2 days'
           AND (outbound_call = 1 or lp_sessions = 1) AND client_type = 'TN_ANDROID'
      )early_mover
      JOIN
      (
        SELECT DISTINCT "client_details.android_bonus_data.adjust_id" adid,
                        "client_details.android_bonus_data.google_analytics_unique_id" gaid,
                        created_at
        FROM party_planner_realtime.applifecyclechanged
        WHERE 
          date_utc >= CAST(:run_date AS DATE) - INTERVAL '2 days'
          AND adid IS NOT NULL
          AND gaid IS NOT NULL
          AND gaid != ''
      )pp_events
      USING(adid)
    )new_gaids
    LEFT JOIN analytics_staging.ua_events_sent_to_firebase b
    ON  new_gaids.gaid = b.gaid
    WHERE b.gaid IS NULL
"""


class FirebaseQueriesDQChecks(BaseDQChecks):
    name = "Firebase Queries DQ Checks"
    description = "Check the deviceid counts to be sent to GA4 is within the threshold limits"
    table_name = TABLE

    def get_inputs(self):
        return [

            DBOInput(name="Firebase Moments_Day1 Source Input",
                     alias="moments_day1_input_query",
                     src_sql=MOMENTS_DAY1_SQL),

            DBOInput(name="Firebase Early_Mover_Day1 Source Input",
                     alias="early_mover_day1_input_query",
                     src_sql=EARLY_MOVER_DAY1_SQL)
        ]

    def get_checks(self):
        return [

            Check(name="Total Number of Moments event",
                  description="Ensure that the distinct gaids of Moments event are within normal range",
                  column_name="moments",
                  value=self.moments_day1_input_query[0]["moments"],
                  red_expr="8000 <= :value <= 16000",
                  red_error="The value (:value) has gone outside the normal range: :red_expr",
                  yellow_expr="9000 <= :value",  # min max calculated based on dec month
                  yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                  todo=[
                      ("Check the counts for your source table 'adjust.installs_with_pi' and"
                       " app_lifecycle_changed and party_planner_realtime.applifecyclechanged "
                       "and core.registrations are in range."),
                      "Check if the snowpipe adjust.installs_with_pi snowpipe is running fine."
                       ]),

            Check(name="Total Number of early_mover events",
                  description="Ensure that the distinct gaids of early mover event are within normal range",
                  column_name="early_mover",
                  value=self.early_mover_day1_input_query[0]["early_mover"],
                  red_expr="9000 <= :value <= 16500",
                  red_error="The value (:value) has gone outside the normal range: :red_expr",
                  yellow_expr="10000 <= :value",
                  yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                  todo=[
                      ("Check the counts for your source table 'adjust.installs_with_pi' and"
                       " app_lifecycle_changed and party_planner_realtime.applifecyclechanged and "
                       "core.registrations are in range."),
                      "Check if the snowpipe adjust.installs_with_pi snowpipe is running fine."
                       ])
        ]
