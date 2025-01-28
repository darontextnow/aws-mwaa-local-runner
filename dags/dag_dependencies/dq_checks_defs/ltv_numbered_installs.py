"""DQ Checks for ltv_numbered_installs table.
This replaces DBT model testing Eric had implemented. Thresholds have been taken directly from the DBT tests.
"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.distribution_shift_psi_check import DistributionShiftPSI

# Force these DQ checks to use prod DB as they will always fail in dev testing using dev DB
TABLE = "prod.analytics_staging.ltv_numbered_installs"
TODO = ["Check if there is a delay or missing data in the Adjust install pipeline and user set pipeline.",
        "Check with Eric Wong as to what the follow up should be."]

SQL = f"""
    SELECT
        {DistributionShiftPSI.get_psi_sql(
            table=TABLE,  column_name="install_number", 
            ref_condition="(date_utc = DATE(:run_date) - INTERVAL '1 DAY') AND (client_type = 'TN_ANDROID')",
            test_condition="(date_utc = DATE(:run_date)) AND (client_type = 'TN_ANDROID')",
            min=1, max=50, bins=25, input_alias="install_number_android_psi")},
        {DistributionShiftPSI.get_psi_sql(
            table=TABLE,  column_name="install_number", 
            ref_condition="(date_utc = DATE(:run_date) - INTERVAL '1 DAY') AND (client_type = 'TN_IOS_FREE')",
            test_condition="(date_utc = DATE(:run_date)) AND (client_type = 'TN_IOS_FREE')",
            min=1, max=50, bins=25, input_alias="install_number_ios_psi")}
"""


class LTVNumberedInstallsDQChecks(BaseDQChecks):
    name = "ltv_numbered_installs DQ Checks"
    description = "DQ Checks for ltv_numbered_installs table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="Main Input",
                alias="input",
                src_sql=SQL
            )
        ]

    def get_checks(self):
        return [
            DistributionShiftPSI(
                column_name="install_number_android",
                psi=self.input[0]["install_number_android_psi"],
                yellow_expr=":value < 0.3",
                todo=TODO
            ),

            DistributionShiftPSI(
                column_name="install_number_ios",
                psi=self.input[0]["install_number_ios_psi"],
                yellow_expr=":value < 0.6",
                todo=TODO
            )
        ]
