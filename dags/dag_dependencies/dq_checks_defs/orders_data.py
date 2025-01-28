"""DQ Checks for the analytics.orders_data in Snowflake.
orders_data table data is the source table: [inventory_tables_extract_v1]

SLT reports has dependency on other dbt models: [CORE.user_sets, CORE.USERS]

For detailed info, checkout the Excel sheet for current owners/usage:
    DataScience and Engineering/Documents/DE TEAM/SLT MODELS/SLT_DE_TRACKER.xlsx

Here is the url:
    https://enflick.sharepoint.com/:x:/r/sites/DataScienceandEngineering/_layouts/
    15/Doc.aspx?sourcedoc=%7B7D1551B2-2911-448E-8FA4-75A8D2CB4BD4%7D&file=SLT_DE_TRACKER.xlsx&action=default&
    mobileredirect=true

SLT specific checks:
    1. Ensures the SIM orders count for each client is not abnormal.
"""

from de_utils.dq_checks_framework.inputs.dbo_input import DBOInput
from de_utils.dq_checks_framework import BaseDQChecks
from de_utils.dq_checks_framework.check_classes.check import Check

TABLE = "inventory.orders_data"
TODO_LIST = [
    ("Check source table [inventory.orders_data, inventory.order_products, core.user_sets, core.users] "
     "and upstream source tables (from inventory_tables_extract_v1, dbt_snowflake_daily DAG) "
     "for possible missing data or other issues causing abnormality."),
    "If sums are to be expected and are regularly outside range, adjust the range.",
    ("Note for yellow alerts: When encountering yellow alerts, ensure that the values do not deviate significantly. "
     "Typically, if you observe a seasonal trend but the value is within 10% of the expected range or "
     "if the business is running some experiments for certain period (Respective BA Leads should be aware of this), "
     "no immediate action is required. However, continue monitoring over the next few days to confirm that everything "
     "remains within normal thresholds, and that this yellow alert is indeed a one-time occurrence.")

]

SLT_SQL = f"""
    SELECT
        SUM(CASE WHEN UPPER(charge_id) IS NULL THEN 1 ELSE 0 END) AS free_sim_orders,
        SUM(CASE WHEN UPPER(charge_id) IS NOT NULL THEN 1 ELSE 0 END) AS paid_sim_orders
    FROM prod.{TABLE} a
    JOIN prod.inventory.order_products b ON (a.id = b.order_id)
    JOIN prod.analytics.users u ON (a.username=u.username)
    JOIN prod.analytics.user_sets e ON (e.user_set_id = u.user_set_id)
    JOIN prod.core.users f ON (f.username = a.username)
    WHERE
        (a.type='NormalOrder')
        AND (b.product_id = 392)
        AND (a.created_at::DATE = CAST(:run_date AS DATE))
        AND (UPPER(account_status)='ENABLED')
"""


class OrdersDataDQChecks(BaseDQChecks):
    name = "orders_data DQ Checks"
    description = "Aggregate DQ Checks for orders_data table"
    table_name = TABLE

    def get_inputs(self):
        return [
            DBOInput(
                name="orders_data Main Input",
                alias="main_input",
                src_sql=SLT_SQL
            )
        ]

    def get_checks(self):
        return [

            # These checks will alert spikes and dips in the SLT chart
            Check(
                name="Total Count Check",
                description="Ensure free_sim_orders is within the normal expected range",
                column_name="free_sim_orders",
                value=self.main_input[0]["free_sim_orders"],
                yellow_expr="1 < :value",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            ),

            Check(
                name="Total Count Check",
                description="Ensure paid_sim_orders is within the normal expected range",
                column_name="paid_sim_orders",
                value=self.main_input[0]["paid_sim_orders"],
                red_expr="200 < :value",
                red_error="The value (:value) has gone outside the normal range: :red_expr",
                yellow_expr="200 < :value < 1500",
                yellow_warning="The value (:value) has gone outside the normal range: :yellow_expr",
                todo=TODO_LIST
            )
        ]
