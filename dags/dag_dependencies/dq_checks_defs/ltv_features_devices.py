"""DQ Checks for ltv_features_devices table.
This replaces DBT model testing Eric had implemented. Thresholds have been taken directly from the DBT tests.
"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.distribution_shift_psi_check import DistributionShiftPSI
from de_utils.dq_checks_framework.check_classes.column_statistics_check import ColumnStatisticsCheck

# Force these DQ checks to use prod DB as they will always fail in dev testing using dev DB
TABLE = "prod.analytics_staging.ltv_features_devices"
TODO = ["Check if there is a delay or missing data in the Adjust install pipeline and user set pipeline.",
        "Check with Eric Wong as to what the follow up should be."]
REF = "DATE_UTC = DATE(:run_date) - interval '15 days'"
TEST = "DATE_UTC = DATE(:run_date) - interval '8 days'"

SQL = f"""
    SELECT
        {ColumnStatisticsCheck.get_statistics_sql(column_name="install_week_of_year")},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="install_dow")},
        {DistributionShiftPSI.get_psi_sql(
            table=TABLE,  column_name="click_to_install_hours", ref_condition=REF, test_condition=TEST, min=0, max=48)}
    FROM {TABLE}
    WHERE (date_utc = DATE(:run_date))
"""


class LTVFeaturesDevicesDQChecks(BaseDQChecks):
    name = "ltv_features_devices DQ Checks"
    description = "DQ Checks for ltv_features_devices table"
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
            ColumnStatisticsCheck(
                input_object=self.input,
                column_name="install_week_of_year",
                min_expr=":value >= 0",
                max_expr=":value <= 53",
                raise_red_alert=True,
                todo=TODO
            ),

            ColumnStatisticsCheck(
                input_object=self.input,
                column_name="install_dow",
                min_expr=":value >= 0",
                max_expr=":value <= 6",
                raise_red_alert=True,
                todo=TODO
            ),

            DistributionShiftPSI(
                column_name="click_to_install_hours",
                psi=self.input[0]["click_to_install_hours_psi"],
                yellow_expr=":value < 0.3",
                todo=TODO
            ),

        ]
