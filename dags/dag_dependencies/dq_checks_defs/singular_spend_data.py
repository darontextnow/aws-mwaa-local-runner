"""DQ Checks for the singular.marketing_data table in Snowflake.
The checks defined below are set up to run once a day to ensure data added to Snowflake is good before dbt load
Notes:
    DQ checks to make sure every partner's spend range is within the threshold limit
    DQ checks to validate platforms/client_type costs
    DQ checks to validate the platform/client_type
"""
from de_utils.constants import ENV_ENUM
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.check_classes.match_expected_values_check import MatchExpectedValuesCheck
from datetime import datetime, timedelta

TABLE = "singular.marketing_data"
run_date = "{{ ds }}"
PARTNER_NAME = ('Appier', 'SingleTap by Digital Turbine', 'Kayzen',  'TikTok Ads',
                'Apple Search Ads', 'Liftoff', 'AdWords', 'Smadex', 'Snapchat', 'Facebook',
                'MOLOCO', 'Dataseat', 'Bidease', 'Blisspoint')

# Added a mapping table in order to catch data issues when no rows have been sent for a network/partner
PARTNER_COST_SQL = f"""
    SELECT 
        OBJECT_AGG(CONCAT(CAST(date_utc AS string), ' ', partner_name), cost_per_partner::numeric(15,4))
        AS cost_per_partner 
    FROM (
        SELECT 
            mapping_data.singular_network_name AS partner_name,
            mapping_data.date AS date_utc,
            SUM(COALESCE(spend_data.adn_cost, 0))  AS cost_per_partner 
        FROM (
            SELECT DISTINCT singular_network_name, date
            FROM ua.ua_singular_to_adjust_network_mapping a
            JOIN (
                SELECT DISTINCT date
                FROM  prod.{TABLE} 
            ) b
            WHERE singular_network_name NOT IN ('Outbrain', 'Appreciate') 
            AND  date >=  DATEADD(dd, -6, CAST(:run_date AS DATE)) AND date <= CAST(:run_date AS DATE) - 1
            ORDER BY date, singular_network_name
        ) mapping_data
        LEFT OUTER JOIN (
            SELECT 
                data_connector_source_name,
                date AS date_utc,
                SUM(adn_cost)  AS adn_cost 
            FROM singular.marketing_data
            WHERE 
                data_connector_source_name IN {PARTNER_NAME}
                AND country_field IN ('USA', 'CAN')
                AND date >= DATEADD(dd, -6, CAST(:run_date AS DATE)) AND date <= CAST(:run_date AS DATE) - 1
            GROUP BY 1, 2
        ) spend_data 
         ON mapping_data.singular_network_name = spend_data.data_connector_source_name
         AND mapping_data.date = spend_data.date_utc
         GROUP BY 1, 2
         ORDER BY 2, 1
    )
"""

VALID_PLATFORM_SQL = f"""
    SELECT 
        OBJECT_AGG(CAST(date_utc AS string), platform) AS platforms_per_date
    FROM (
        SELECT date AS date_utc, ARRAY_UNIQUE_AGG(site_public_id) AS platform
        FROM prod.{TABLE} 
        WHERE 
            data_connector_source_name IN {PARTNER_NAME}
            AND date >= DATEADD(dd, -6, CAST(:run_date AS DATE)) AND date <= CAST(:run_date AS DATE) - 1
            AND adn_cost > 0
            AND country_field IN ('USA', 'CAN')
        GROUP BY 1
    ) 
    ORDER BY date_utc
"""


def alert_dq_check_ua_team_failure(
        name: str,
        detail_name: str,
        table_name: str,
        column_name: str,
        message: str,
        check_code: "Link",  # noqa: F821
        executed_ts: datetime = datetime.utcnow(),  # noqa: B008
        **kwargs
):
    """Alert specific to an individual DQ Check red failure."""
    from de_utils.slack import send_message
    from de_utils.slack.tokens import get_de_alerts_token

    msg = f"""
        :red_circle: DQ Check Failed. 
        *Name*: {name}
        *Detail Name*: {detail_name}
        *Table Name*: {table_name}
        *Column Name*: {column_name}
        *Error*: {message}
        *Execution Time*: {executed_ts}  
    """
    send_message(env=ENV_ENUM, channel="#tmp_manual_ua_adjustments", message=msg,
                 get_auth_token_func=get_de_alerts_token, username="MWAA DQ Checks", icon_emoji=":thinking_face:")


# noinspection PyUnresolvedReferences
def alert_dq_check_ua_team_warning(
        name: str,
        detail_name: str,
        table_name: str,
        column_name: str,
        message: str,
        check_code: "Link",  # noqa: F821
        executed_ts: datetime = datetime.utcnow(),  # noqa: B008
        **kwargs
):
    """Alert specific to an individual DQ Check yellow warning."""
    msg = f"""
        :large_yellow_circle: DQ Check Warning. 
        *Name*: {name}
        *Detail Name*: {detail_name}
        *Table Name*: {table_name}
        *Column Name*: {column_name}
        *Warning*: {message}
        *Execution Time*: {executed_ts}  
    """
    from de_utils.slack import send_message
    from de_utils.slack.tokens import get_de_alerts_token
    send_message(env=ENV_ENUM, channel="#tmp_manual_ua_adjustments", message=msg,
                 get_auth_token_func=get_de_alerts_token, username="MWAA DQ Checks", icon_emoji=":thinking_face:")


class SingularSpendDataDQChecks(BaseDQChecks):
    name = "Adspend Data DQ Checks"
    description = "Aggregate DQ Checks for singular.marketing_data table"
    table_name = TABLE

    def get_inputs(self):

        return [
            DBOInput(
                name="Valid Partner spend check input",
                alias="partner_spend_input",
                src_sql=PARTNER_COST_SQL
            ),

            DBOInput(
                name="Valid Platform check input",
                alias="platform_name_check_input",
                src_sql=VALID_PLATFORM_SQL
            )

        ]

    def get_checks(self, end_dt=None):
        lookback_window = 6
        end_dt = (datetime.strptime(self.params["run_date"], "%Y-%m-%d") - timedelta(1)).date()
        date_list = [(end_dt - timedelta(x)).isoformat() for x in range(1, lookback_window)]
        date_list = [x[:10] for x in date_list]

        # Jan 2024 threshold
        partner_spend_range = {'Appier': ["150", "16000"],
                               'SingleTap by Digital Turbine': ["800", "3100"],
                               'Kayzen': ["200", "5000"],
                               'TikTok Ads': ["900", "6800"],
                               'Apple Search Ads': ["300", "7600"],
                               'Liftoff': ["1000", "10000"],
                               'AdWords': ["22000", "100000"],
                               'Smadex': ["3000", "16000"],
                               'Snapchat': ["1450", "7000"],
                               'Facebook': ["300", "2700"],
                               'MOLOCO': ["2500", "10000"],
                               'Bidease': ["40", "25000"],
                               'Dataseat': ["700", "5000"],
                               'Blisspoint': ["2000", "25000"]
                               }

        checks = []
        # Dq check to validate each partner spend data
        for dt in date_list:
            dt_platform_network_check = datetime.strptime(dt, "%Y-%m-%d").date()  # convert str to date object
            for partner, cost in partner_spend_range.items():
                checks.append(
                    Check(
                        name=f"{partner}_cost_in_range",
                        description="For reach Partner, adspend value should fall within a specified range.",
                        column_name="cost_per_partner",
                        value=self.partner_spend_input[0]["cost_per_partner"][dt + ' ' + partner],
                        yellow_expr=f"{cost[0]} <= :value <= {cost[1]}",
                        yellow_warning=f"The value (:value) has gone outside the normal range: :yellow_expr for {dt}",
                        # Send customized alerts to the UA Team for partner cost discrepancies.
                        yellow_alert_func=alert_dq_check_ua_team_warning,
                        todo=["Check with UA Team if that's the expected value, or is it an anomaly"
                              "If it's not expected value as per UA Team, then wait for UA Team's feedback"]

                    )
                )

        checks.append(MatchExpectedValuesCheck(
            column_name="platforms_per_date",
            values=self.platform_name_check_input[0]["platforms_per_date"][dt_platform_network_check],
            expected_values=["com.enflick.android.TextNow", 314716233, "https://www.textnow.com/", "TextNow_OTHER",
                             "TextNow_MIXED"]
        )
        )

        return checks
