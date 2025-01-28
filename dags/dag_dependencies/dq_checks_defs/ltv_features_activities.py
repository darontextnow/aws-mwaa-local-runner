"""DQ Checks for ltv_features_activities table.
This replaces DBT model testing Eric had implemented. Thresholds have been taken directly from the DBT tests.
"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.distribution_shift_psi_check import DistributionShiftPSI

# Force these DQ checks to use prod DB as they will always fail in dev testing using dev DB
TABLE = "prod.analytics_staging.ltv_features_activities"
TODO = ["Check if there is a delay or missing data in the Adjust install pipeline and user set pipeline.",
        "If this Yellow Alert continues for more than a week, then check with Eric Wong as "
        "to what the follow up should be."]
REF = "DATE_UTC = DATE(:run_date) - interval '15 days'"
TEST = "DATE_UTC = DATE(:run_date) - interval '8 days'"
BINS = 20

SQL = f"""
    SELECT
        {DistributionShiftPSI.get_psi_sql(
            table=TABLE, column_name="ad_impressions_7d",  ref_condition=REF, test_condition=TEST, bins=BINS
        )},
        {DistributionShiftPSI.get_psi_sql(
            table=TABLE,  column_name="mms_messages_7d", ref_condition=REF, test_condition=TEST, bins=BINS
        )},
        {DistributionShiftPSI.get_psi_sql(
            table=TABLE,  column_name="sms_messages_7d", ref_condition=REF, test_condition=TEST, bins=BINS
        )},
        {DistributionShiftPSI.get_psi_sql(
            table=TABLE,  column_name="total_outgoing_calls_7d", ref_condition=REF, test_condition=TEST, bins=BINS
        )},
        {DistributionShiftPSI.get_psi_sql(
            table=TABLE,  column_name="total_outgoing_free_calls_7d", ref_condition=REF, test_condition=TEST, bins=BINS
        )},
        {DistributionShiftPSI.get_psi_sql(
            table=TABLE,  column_name="total_outgoing_unique_calling_contacts_7d", ref_condition=REF, 
            test_condition=TEST, bins=BINS
        )}
"""


class LTVFeaturesActivitiesDQChecks(BaseDQChecks):
    name = "ltv_features_activities DQ Checks"
    description = "DQ Checks for ltv_features_activities table"
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
                column_name="ad_impressions_7d",
                psi=self.input[0]["ad_impressions_7d_psi"],
                yellow_expr=":value < 0.4",
                todo=TODO
            ),

            DistributionShiftPSI(
                column_name="mms_messages_7d",
                psi=self.input[0]["mms_messages_7d_psi"],
                yellow_expr=":value < 0.4",
                todo=TODO
            ),

            DistributionShiftPSI(
                column_name="sms_messages_7d",
                psi=self.input[0]["sms_messages_7d_psi"],
                yellow_expr=":value < 0.4",
                todo=TODO
            ),

            DistributionShiftPSI(
                column_name="total_outgoing_calls_7d",
                psi=self.input[0]["total_outgoing_calls_7d_psi"],
                yellow_expr=":value < 0.4",
                todo=TODO
            ),

            DistributionShiftPSI(
                column_name="total_outgoing_free_calls_7d",
                psi=self.input[0]["total_outgoing_free_calls_7d_psi"],
                yellow_expr=":value < 0.25",
                todo=TODO
            ),

            DistributionShiftPSI(
                column_name="total_outgoing_unique_calling_contacts_7d",
                psi=self.input[0]["total_outgoing_unique_calling_contacts_7d_psi"],
                yellow_expr=":value < 0.4",
                todo=TODO
            )
        ]
