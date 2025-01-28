"""DQ Checks for user_features_summary table.
This replaces DBT model testing Eric had implemented. Thresholds have been taken directly from the DBT tests.
"""
from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.column_statistics_check import ColumnStatisticsCheck

TABLE = "user_features.user_features_summary"
TODO = ["Check if there is a delay or missing data in the Adjust install pipeline and user set pipeline.",
        "Check with Eric Wong as to what the follow up should be."]

SQL = f"""
    SELECT
        {ColumnStatisticsCheck.get_statistics_sql(column_name='"COUNT(SMS_MESSAGES)_28d"', percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name='"COUNT(TOTAL_OUTGOING_CALLS)_28d"', percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name='"COUNT(NPS_MAX_SCORE)_28d"', percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name='"SUM(UI_MENU_OPEN)_28d"', percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name='"DAYS_FROM_LAST(SUB_TYPE)"', percentile=.95)},
        {ColumnStatisticsCheck.get_statistics_sql(column_name="user_set_lifetime_days", percentile=.95)}
    FROM {TABLE}
    WHERE (report_date = DATE(:run_date)) AND ("LAST(COUNTRY_CODE)" = 'US')
"""


class UserFeaturesSummaryDQChecks(BaseDQChecks):
    name = "user_features_summary DQ Checks"
    description = "DQ Checks for user_features_summary table"
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
                column_name='"COUNT(SMS_MESSAGES)_28d"',
                percentile_expr="14 <= :value <= 22",
                avg_expr="2.1 <= :value <= 3.5",
                min_expr=":value >= 0",
                max_expr=":value <= 5000",
                todo=TODO
            ),

            ColumnStatisticsCheck(
                input_object=self.input,
                column_name='"COUNT(TOTAL_OUTGOING_CALLS)_28d"',
                percentile_expr="16 <= :value <= 21",
                avg_expr="3 <= :value <= 5",
                min_expr=":value >= 0",
                max_expr=":value <= 5000",
                todo=TODO
            ),

            ColumnStatisticsCheck(
                input_object=self.input,
                column_name='"COUNT(NPS_MAX_SCORE)_28d"',
                percentile_expr=":value == 1",
                avg_expr="0.8 <= :value <= 1.01",
                min_expr=":value >= 0",
                max_expr=":value <= 20",
                todo=TODO
            ),

            ColumnStatisticsCheck(
                input_object=self.input,
                column_name='"SUM(UI_MENU_OPEN)_28d"',
                percentile_expr="24 <= :value <= 38",
                avg_expr="6 <= :value <= 11",
                min_expr=":value == 1",
                todo=TODO
            ),

            ColumnStatisticsCheck(
                input_object=self.input,
                column_name='"DAYS_FROM_LAST(SUB_TYPE)"',
                percentile_expr="22 <= :value <= 28",
                avg_expr="6 <= :value <= 10",
                min_expr=":value >= 0",
                max_expr=":value <= 28",
                todo=TODO
            ),

            ColumnStatisticsCheck(
                input_object=self.input,
                column_name="user_set_lifetime_days",
                percentile_expr="1700 <= :value <= 3000",
                min_expr=":value >= 0",
                max_expr=":value <= 3100",
                todo=TODO
            )

        ]
