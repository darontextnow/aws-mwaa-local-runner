"""Module to hold constants used anywhere in project"""

DQ_CHECK_CHANGES_LOG_URL = ("https://enflick.sharepoint.com/:x:/s/DataScienceandEngineering/"
                            "EUAjKmtpc0dCv3njqq_3Q8QBi6orzAKsLLNpqgL0XUpeJQ?e=vWTLNG")

DQ_CHECKS_DEFS_REPO_URL = ("https://github.com/Enflick/de-airflow/tree/master/de_airflow/"
                           "dags/dag_dependencies/dq_checks_defs/")

SF_QUERY_ID_URL = "https://app.snowflake.com/gkqiykk/textnow/#/compute/history/queries?user=ALL&uuid="

# This is the url to the AWS Lambda Function that updates the is_valid_alert column in Snowflake dq_checks table.
DQ_CHECK_FALSE_ALERT_UPDATER_URL = "https://hrsjfg7m23y2wc2ln4vpif6xpi0jhghl.lambda-url.us-east-1.on.aws/"

REPORTING_TABLES_SCHEMA = "core"  # Name of Snowflake schema where dq_check reporting tables are housed
