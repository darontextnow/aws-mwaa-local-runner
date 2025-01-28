"""DQ Checks for tables populated by dau_processing_sf DAG and for
the source tables dbt uses for populating dau_user_set_active_days.
The intention of these checks is to ensure the dbt_daily dag does not run when there are clearly issues
with these upstream tables that will be propagated downstream via the very long-running dbt_daily run.

These checks should run after dau_processing_sf DAG completes successfully and before dbt_daily run.
These checks are very minimal only to ensure we are not missing or duplicating data before we run downstream ETL.

Upstream Process:  dau_processing_sf.py populates these source tables

Notes:
    It is not possible to have duplicates in play here due to GROUP BY on source query. Thus, not checking for dupes.

"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.total_count_check import TotalCountCheck
from de_utils.constants import ENV_VALUE

# The following is taken from source SQL of table dau_user_set_active_days
all_tables_sql = f"""
    WITH set_client_type AS (
        SELECT date_utc, set_uuid, client_type
        FROM {ENV_VALUE}.dau.user_set
        JOIN {ENV_VALUE}.dau.user_device_master USING(username)
        WHERE (date_utc = CAST(:run_date AS DATE))
        UNION SELECT date_utc, set_uuid, client_type
        FROM {ENV_VALUE}.dau.device_set
        JOIN {ENV_VALUE}.dau.user_device_master USING(adid)
        WHERE (date_utc < CURRENT_DATE)
    )
    SELECT COUNT(*) AS total_count, SUM(dau) AS dau_sum
    FROM {ENV_VALUE}.dau.daily_active_users
    JOIN set_client_type USING (date_utc, set_uuid)
    JOIN {ENV_VALUE}.dau.set_cohort USING (set_uuid, client_type)
    WHERE (date_utc = CAST(:run_date AS DATE));
"""

individual_tables_sql = """
SELECT
    (SELECT COUNT(*) FROM prod.dau.user_device_master WHERE (date_utc = '2023-08-10')) AS user_device_master_count,
    (SELECT COUNT(*) FROM prod.dau.user_set WHERE (DATE(processed_at) = '2023-08-10')) AS user_set_count,
    (SELECT COUNT(*) FROM prod.dau.device_set WHERE (DATE(processed_at) = '2023-08-10')) AS device_set_count,
    (SELECT COUNT(*) FROM prod.dau.daily_active_users WHERE (date_utc = '2023-08-10')) AS daily_active_users_count,
    (SELECT COUNT(*) FROM prod.dau.set_cohort WHERE (cohort_utc = '2023-08-10')) AS set_cohort_count
"""


class DAUSourceTablesDQChecks(BaseDQChecks):
    name = "dbt_dau_source_tables DQ Checks"
    description = "Aggregate DQ Checks for the tables dau_user_set_active_days is sourced from"
    table_name = "dau.multiple_source_tables"

    def get_inputs(self):
        return [
            DBOInput(
                name="All DAU Source Table Input",
                alias="all_tables_input",
                src_sql=all_tables_sql
            ),

            DBOInput(
                name="Individual Source Table Input",
                alias="individual_tables_input",
                src_sql=individual_tables_sql
            )
        ]

    def get_checks(self):

        return [
            TotalCountCheck(
                filter_date_column_name="date_utc",
                total_count=self.all_tables_input[0]["total_count"],
                lower_threshold=2500000,
                upper_threshold=4500000
            ),

            TotalCountCheck(
                filter_date_column_name="date_utc",
                total_count=self.all_tables_input[0]["dau_sum"],
                column_name="dau_sum",
                lower_threshold=2500000,
                upper_threshold=4500000
            ),

            TotalCountCheck(
                filter_date_column_name="date_utc",
                total_count=self.individual_tables_input[0]["user_device_master_count"],
                column_name="user_device_master_count",
                lower_threshold=4500000,
                upper_threshold=8500000
            ),

            TotalCountCheck(
                filter_date_column_name="DATE(processed_at)",
                total_count=self.individual_tables_input[0]["user_set_count"],
                column_name="user_set_count",
                lower_threshold=70000,
                upper_threshold=250000
            ),

            TotalCountCheck(
                filter_date_column_name="DATE(processed_at)",
                total_count=self.individual_tables_input[0]["device_set_count"],
                column_name="device_set_count",
                lower_threshold=60000,
                upper_threshold=250000
            ),

            TotalCountCheck(
                filter_date_column_name="date_utc",
                total_count=self.individual_tables_input[0]["daily_active_users_count"],
                column_name="daily_active_users_count",
                lower_threshold=2500000,
                upper_threshold=5500000
            ),

            TotalCountCheck(
                filter_date_column_name="date_utc",
                total_count=self.individual_tables_input[0]["set_cohort_count"],
                column_name="set_cohort_count",
                lower_threshold=35000,
                upper_threshold=300000
            )

        ]
