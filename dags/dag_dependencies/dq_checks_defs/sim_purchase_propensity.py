"""DQ Checks for sim_purchase_propensity table.
This replaces DBT model testing Eric had implemented. Thresholds have been taken directly from the DBT tests.
"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check

TABLE = "analytics.sim_purchase_propensity"
TODO = ["Check if there is a delay or missing data in the Adjust install pipeline and user set pipeline.",
        "Check with Eric Wong as to what the follow up should be."]

SQL = f"""
    WITH cdfs AS (
        SELECT
            report_date,
            ROUND(target_1_prediction / 0.05) * 0.05 AS bin,
            RATIO_TO_REPORT(COUNT(1)) OVER (PARTITION BY report_date) AS n_pct
        FROM {TABLE}
        JOIN user_features.user_features_summary USING (report_date, username)
        WHERE 
            ("LAST(COUNTRY_CODE)" = 'US')
            AND (report_date IN ('2022-08-18', CAST(:run_date AS DATE)))
        GROUP BY 1, 2
    )
    SELECT SUM((test.n_pct - base.n_pct) * LN(test.n_pct / base.n_pct)) AS population_stability_index
    FROM (SELECT bin, n_pct FROM cdfs WHERE (report_date = '2022-08-18')) AS base
    JOIN (SELECT bin, n_pct FROM cdfs WHERE (report_date != '2022-08-18')) AS test USING (bin)
"""


class SimPurchasePropensityDQChecks(BaseDQChecks):
    name = "sim_purchase_propensity DQ Checks"
    description = "DQ Checks for sim_purchase_propensity table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="Sim Propensity Drift Test",
                alias="psi",
                src_sql=SQL
            )
        ]

    def get_checks(self):
        return [
            Check(
                name="psi shift test",
                description="test_for_predicted_scores_drift",
                column_name="target_1_prediction",
                value=self.psi[0][0],
                red_expr=":value < 0.1",
                red_error="PSI for column :column_name is out of given range :red_expr",
                todo=TODO
            )

        ]
