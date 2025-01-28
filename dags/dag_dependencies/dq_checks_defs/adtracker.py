"""DQ Checks for the ADTRACKER table in Snowflake.

The checks defined below are set up to run once a day to ensure adtracker data added to Snowflake during
the previous 24 hours meets our assumptions and minimum requirements for good data quality.
Only running once a day currently to keep query load down.

TODO: Check with engineering on the following

adtracker.timestamp column is erroneous: lots of future and past dates. Contains NULLs. (Jira - ADS-1425)
We need a better (accurate) timestamp column we can use from source.
If eng can't provide one, implement an inserted_at column in adtracker.
Consider switching to new column for all downstream tables currently using timestamp.

NULL usernames:
Prior to 9/6/2023 the rate of null username is 100-200K in some days. This is not expected. (Jira - ADS-1426)
On 9/6/2023 the rate of nulls skyrocketed due to bug on client introduced in version 23.35.
As of 9/21 we still are getting 100% nulls for client version 23.35. Waiting for fix in 23.37. (Jira - DS-2309)
These nulls are coming from TN_ANDROID, 2L_ANDROID, and TN_IOS_FREE.
Eng code tries to obtain username and if can't goes with "". Eng to look at improving the code to improve accuracy here.

Jira - ADS-1427 - A small percentage of invalid usernames show up periodically. Per ADS Eng, there is no fix for this.
Could be from usernames created before username restrictions were put in place. Just allow for it.

Very low occurrence of type IS NULL. Type should not be NULL.
Per Chuan @ eng, it should not be NULL. They need to check into why it's NULL.
For now, switched type from NoNull check to a rate check to ensure it stays very low.

ad_id, ad_name, ad_placement, ad_platform, cpm, experiment_[id|name|variant], ip_address are 100% null in latest 7 days.
ignore at this time. Can check with Brad eventually if these columns are needed and remove them if not.

"""
from de_utils.dq_checks_framework.enums import CheckType
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.missing_values_rate_check import MissingValuesRateCheck
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck
from de_utils.dq_checks_framework.check_classes.no_missing_values_check import NoMissingValuesCheck

NO_MISSING_DATA_COLS = ["client_type"]
MISSING_VALUES_RATE_CHECK_TODO = [
    "Ideally, :column_name should not be NULL.",
    "Notify eng (Jira: ADS project) that the rate has gone above a normal level (as indicated by this failure)",
    "Adjust the threshold for this test if eng says this rate can be expected."
    "Notify downstream users if the rate is significant as it will affect reports/etc."
]

SQL = rf"""
    SELECT
        COUNT(*) AS total_count,
        {NoMissingValuesCheck.get_null_and_empty_counts_sql(NO_MISSING_DATA_COLS)},
        SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS timestamp_missing_count,
        --version 23.35 had a terrible bug in it where all rows had NULL usernames. Filtering this version out.
        SUM(CASE WHEN NVL(username, '') = '' AND (client_version NOT LIKE '23.35%') THEN 1 ELSE 0 END) 
            AS username_missing_count,
        SUM(CASE WHEN NVL(type, '') = '' THEN 1 ELSE 0 END) AS type_missing_count,
        SUM(CASE WHEN NVL(ad_type, '') = '' THEN 1 ELSE 0 END) AS ad_type_missing_count,
        SUM(CASE WHEN NVL(adid, '') = '' THEN 1 ELSE 0 END) AS adid_missing_count,
        SUM(CASE WHEN NVL(client_version, '') = '' THEN 1 ELSE 0 END) AS client_version_missing_count,
        -- if client_type is an Android type then it is an Android phone and should have android_id
        SUM(CASE WHEN client_type IN ('TN_ANDROID', '2L_ANDROID') AND NVL(android_id, '') = '' 
            THEN 1 ELSE 0 END) AS android_id_missing_count,
        SUM(CASE WHEN 
            (REGEXP_LIKE(client_version, '^[0-9][0-9]?\.[0-9][0-9]?\.[0-9](_RC)?[0-9]?(\.[0-9][0-9]?)?') = FALSE)
            THEN 1 ELSE 0 END) AS client_version_invalid_fmt,
        ARRAY_UNIQUE_AGG(type) AS types,
        ARRAY_UNIQUE_AGG(client_type) AS client_types,
        ARRAY_UNIQUE_AGG(ad_format) AS ad_formats,
        ARRAY_UNIQUE_AGG(ad_network) AS ad_networks
    FROM prod.core.adtracker
    WHERE (date_utc = CAST(:run_date AS DATE))
"""

INVALID_AD_TYPES = """
    SELECT OBJECT_AGG(ad_type, cnt) AS invalid_ad_types FROM (
        SELECT ad_type, COUNT(*) cnt
        FROM prod.core.adtracker
        WHERE 
            (date_utc = CAST(:run_date AS DATE))
            AND (ad_type NOT IN ('Banner', 'Banner Chathead', 'Banner Dialpad', 'KeyBoard Banner', 
                'Native In Message Stream Large', 'Native In Stream Video Static', 'Native Call Screen Large', 
                'Post Call Interstitial', 'Interstitial', 'Conversation Navigation Interstitial', 
                'Native Interstitial', 'Native Text Message', 'Main Screen Interstitial', 
                'Post Conversation Interstitial', 'Native Post Call',
                'Native Outgoing Call', 'Native In Stream', 'Native Call Screen', 
                'Sponsored Message Native', 'Sponsored Message Native Rich Media',
                'In Call Medium Rectangle', 'Medium Rectangle',
                'POne Custom Template', 'Pre-roll', 'Reward Video'))
        GROUP BY 1
    );
"""

FUTURE_DATES_SQL = """
    SELECT SUM(CASE WHEN date_utc > CAST(:run_date AS DATE) THEN 1 ELSE 0 END)/COUNT(*) * 100 AS percent_future_dates 
    FROM prod.core.adtracker 
    WHERE (inserted_at BETWEEN CAST(:run_date AS DATE) - INTERVAL '1 DAY' AND CAST(:run_date AS DATE));"""


class AdtrackerDQChecks(BaseDQChecks):
    name = "adtracker DQ Checks"
    description = "Aggregate DQ Checks for adtracker table"
    table_name = "core.adtracker"

    def get_inputs(self):
        return [
            DBOInput(
                name="AdTracker Prod Main Input",
                alias="dst_input",
                src_sql=SQL
            ),

            DBOInput(
                name="Invalid ad_type Input",
                alias="invalid_ad_types",
                src_sql=INVALID_AD_TYPES
            ),

            DBOInput(
                name="Future dates",
                alias="future_dates",
                src_sql=FUTURE_DATES_SQL
            )
        ]

    def get_checks(self):

        return [
            # This table is slowly being deprecated. Count is expected to drop over the following weeks.
            # Thus, only ensuring counts aren't suddenly unusually high.
            Check(
                name="Total Count Check",
                description="total count check for adtracker data being loaded",
                column_name="total_count",
                value=self.dst_input[0]["total_count"],
                yellow_expr=":value <= 300000000",
                yellow_warning="Total Count on adtracker is much higher than normal range (:yellow_expr)",
                todo=["Check for duplication of data in the current date range"]
            ),

            # Ensure columns we expect to always have a value actually have 100% values.
            NoMissingValuesCheck(input_object=self.dst_input, column_names=NO_MISSING_DATA_COLS),

            # timestamp should never be Null. Waiting on fix for this from eng, or we implement our own timestamp.
            MissingValuesRateCheck(
                column_name="timestamp",
                missing_count=self.dst_input[0]["timestamp_missing_count"],
                total_count=self.dst_input[0]["total_count"],
                threshold=0.00005,
                todo=MISSING_VALUES_RATE_CHECK_TODO
            ),

            # username should not be null or '', but eng has a bug. For now ensure rate stays low until bug is fixed.
            MissingValuesRateCheck(
                column_name="username",
                missing_count=self.dst_input[0]["username_missing_count"],
                total_count=self.dst_input[0]["total_count"],
                threshold=0.001,
                todo=MISSING_VALUES_RATE_CHECK_TODO
            ),

            # type should not be null, but eng may have bug. For now ensure rate stays low until bug is fixed.
            MissingValuesRateCheck(
                column_name="type",
                missing_count=self.dst_input[0]["type_missing_count"],
                total_count=self.dst_input[0]["total_count"],
                threshold=0.00005,
                todo=MISSING_VALUES_RATE_CHECK_TODO
            ),

            MissingValuesRateCheck(
                column_name="ad_type",
                missing_count=self.dst_input[0]["ad_type_missing_count"],
                total_count=self.dst_input[0]["total_count"],
                threshold=0.00005,
                todo=MISSING_VALUES_RATE_CHECK_TODO
            ),

            # adid should not be null for mobile, but it is in many cases. Tracking rate for now until there is a fix.
            MissingValuesRateCheck(
                column_name="adid",
                missing_count=self.dst_input[0]["adid_missing_count"],
                total_count=self.dst_input[0]["total_count"],
                threshold=0.0015,
                todo=MISSING_VALUES_RATE_CHECK_TODO
            ),

            # client_version should not be null, but it is in very minimal cases. Tracking rate for now.
            MissingValuesRateCheck(
                column_name="client_version",
                missing_count=self.dst_input[0]["client_version_missing_count"],
                total_count=self.dst_input[0]["total_count"],
                threshold=0.00005,
                todo=MISSING_VALUES_RATE_CHECK_TODO
            ),

            MissingValuesRateCheck(
                column_name="android_id",
                missing_count=self.dst_input[0]["android_id_missing_count"],
                total_count=self.dst_input[0]["total_count"],
                threshold=0.00005,
                todo=MISSING_VALUES_RATE_CHECK_TODO
            ),

            # Fail if a new value for column 'type' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="type",
                values=self.dst_input[0]["types"],
                expected_values=["originating_request", "ad_failed", "ad_show_effective", "request",
                                 "click", "ad_load", "impression", "ad_show", "paid_event"]
            ),

            # Fail if a new value for column 'client_type' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="client_type",
                values=self.dst_input[0]["client_types"],
                expected_values=["TN_IOS_FREE", "TN_IOS", "TN_ANDROID", "2L_ANDROID", "TN_WEB"]
            ),

            # Fail if a new value for column 'ad_format' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="ad_format",
                values=self.dst_input[0]["ad_formats"],
                expected_values=["320x50", "16:9", "320x480", "1x1", "300x250", "fullscreen"]
            ),

            # Fail if a new value for column 'ad_network' is found that is not in expected values list
            MatchExpectedValuesCheck(
                column_name="ad_network",
                values=self.dst_input[0]["ad_networks"],
                expected_values=["AdMob", "MoPub", "Smaato", "InHouse", "InMobi", "Verizon", "AppLovin",
                                 "GAM", "Facebook", "FYBER", "HyBid", "MobileFuse", "Vungle", "IronSource",
                                 "Banner", "Main Screen Interstitial", "Native Outgoing Call", "Native In Stream",
                                 "Pre-roll", "Native Call Screen", "Medium Rectangle", "Native Text Message",
                                 "OpenX", "HTML", "Direct Sales", "Yahoo (Gemini & Flurry)"],
                check_type=CheckType.YELLOW
            ),

            # Occasional anomalies for this column have come through. It can be expected.
            # Per DQ team, these can be ignored if the propensity remains low. Otherwise, must fix if large #s appear.
            Check(
                name="Invalid Values",
                description="Ensure number of invalid values for ad_type column remains low",
                column_name="ad_type",
                value=self.invalid_ad_types[0]["invalid_ad_types"],
                yellow_expr="all(val < 200 for val in :value.values())",
                yellow_warning="The following invalid values were found in the ad_type column: :value.",
                todo=["Rate of invalid values up to 200 a day for ad_type can be expected per eng.",
                      "Alert eng (via Jira: ADS project) to rise in rate of invalid ad_types.",
                      "Alert downstream users if invalid number is large enough to impact reporting."]
            ),

            # We are seeing and expecting some, occasional anomalies come through for client_version
            # The DQ team decided this warning can be ignored if propensity of anomalies is low.
            Check(
                name="Invalid Format",
                description="client_version column is not in expected format.",
                column_name="client_version",
                value=self.dst_input[0]["client_version_invalid_fmt"],
                yellow_expr=":value == 0",
                yellow_warning=":value rows found with invalid format for client_version column.",
                todo=["This warning can be ignored if number of rows is lower than 200.",
                      "Otherwise, alert eng (Jira: ADS project) of increase in rate of invalid client_version",
                      "Tracking via reporting tables how often this warning appears."]
            ),

            Check(
                name="Future Dates Check",
                description="Ensure percentage of total rows with future dates remains low",
                column_name="percent_future_dates",
                value=self.future_dates[0]["percent_future_dates"],
                red_expr=":value < 0.01",
                red_error="Percentage of rows with date_utc in the future (:value) has gone above acceptable limit.",
                todo=["There is a small % of future date_utc expected as this timestamp comes from user's device.",
                      "If this percentage goes above 0.01% this could indicate issues with source data or parsing "
                      "of the timestamp column."]
            )
        ]
