"""DAG for running DQ Checks on tables dependent on completion of dbt_snowflake_daily DAG.
Designed to run once each day as soon as the dag dbt_snowflake_daily has completed successfully (~1pm-2pm UTC).
"""
from airflow_utils import dag
from dag_dependencies.run_now_functions import (
    run_on_days_gt_two_of_month,
    run_on_first_and_second_days_of_month,
    run_on_days_31_1_2_of_month,
    run_on_days_other_than_31_1_2_of_month
)
from dag_dependencies.sensors.external_task_deferred_sensor import ExternalTaskDeferredSensor
from dag_dependencies.operators.dq_checks_operator import DQChecksOperator
from dag_dependencies.constants import DAG_RUN_DATE_1_DAY_LAG, DAG_DEFAULT_CATCHUP
from de_utils.slack.alerts import alert_on_failure, alert_sla_miss_high_priority
from datetime import datetime, timedelta

from dag_dependencies.dq_checks_defs.adjust_installs import AdjustInstallsDQChecks
from dag_dependencies.dq_checks_defs.base_user_daily_subscription import BaseUserDailySubscriptionDQChecks
from dag_dependencies.dq_checks_defs.dau_user_set_active_days import DAUUserSetActiveDaysDQChecks
from dag_dependencies.dq_checks_defs.firebase_queries import FirebaseQueriesDQChecks
from dag_dependencies.dq_checks_defs.growth_dau_by_segment import GrowthDAUBySegmentDQChecks
from dag_dependencies.dq_checks_defs.metrics_daily_app_time_sessions import MetricsDailyAppTimeSessionsDQChecks
from dag_dependencies.dq_checks_defs.metrics_daily_ad_total_revenue import MetricsDailyAdTotalRevenueDQChecks
from dag_dependencies.dq_checks_defs.metrics_daily_dau_generated_revenue import MetricsDailyDauGeneratedRevenueDQChecks
from dag_dependencies.dq_checks_defs.metrics_daily_iap_revenue import MetricsDailyIapRevenueDQChecks
from dag_dependencies.dq_checks_defs.metrics_daily_sim_dau_generated_revenue import (
    MetricsDailySimDauGeneratedRevenueDQChecks)
from dag_dependencies.dq_checks_defs.orders_data import OrdersDataDQChecks
from dag_dependencies.dq_checks_defs.user_daily_activities import UserDailyActivitiesDQChecks
from dag_dependencies.dq_checks_defs.user_daily_profit import UserDailyProfitDQChecks
from dag_dependencies.dq_checks_defs.user_set_daily_activities import UserSetDailyActivitiesDQChecks


@dag(
    start_date=datetime(2024, 7, 11),
    schedule="0 11 * * *",
    default_args={
        "owner": "Daron",
        "sla": timedelta(hours=2),
        "on_failure_callback": alert_on_failure
    },
    sla_miss_callback=alert_sla_miss_high_priority,
    max_active_runs=1,
    # Many queries are ran by the DQ checks below and at the same time as dbt_downstream_dags_trigerrer.
    # Thus, limiting to run only 1 check class at a time.
    max_active_tasks=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def dq_checks_dbt_daily_post_run():

    wait_dbt = ExternalTaskDeferredSensor(
        task_id="wait_for_dbt",
        external_dag_id="dbt_snowflake_daily",
        execution_date_fn=lambda dt: dt.replace(hour=10, minute=0),
        timeout=60 * 60 * 2  # 2 hours
    )

    dq_checks = (
        DQChecksOperator(
            task_id="run_adjust_installs_dq_checks",
            dq_checks_instance=AdjustInstallsDQChecks(),
            run_date=DAG_RUN_DATE_1_DAY_LAG,
        ),

        DQChecksOperator(
            task_id="run_base_user_daily_subscription_dq_checks",
            dq_checks_instance=BaseUserDailySubscriptionDQChecks(),
            run_date=DAG_RUN_DATE_1_DAY_LAG,
        ),

        DQChecksOperator(
            task_id="run_dau_user_set_active_days_dq_checks",
            dq_checks_instance=DAUUserSetActiveDaysDQChecks(),
            run_date=DAG_RUN_DATE_1_DAY_LAG,
        ),

        DQChecksOperator(
            task_id="run_growth_dau_by_segment_dq_checks",
            dq_checks_instance=GrowthDAUBySegmentDQChecks(),
            run_date=DAG_RUN_DATE_1_DAY_LAG,
        ),

        DQChecksOperator(
            task_id="run_metrics_daily_app_time_sessions",
            dq_checks_instance=MetricsDailyAppTimeSessionsDQChecks(),
            run_date=DAG_RUN_DATE_1_DAY_LAG,
        ),

        DQChecksOperator(
            task_id="run_metrics_daily_ad_total_revenue",
            dq_checks_instance=MetricsDailyAdTotalRevenueDQChecks(),
            run_date=DAG_RUN_DATE_1_DAY_LAG,
        ),

        DQChecksOperator(
            task_id="run_metrics_daily_dau_generated_revenue",
            dq_checks_instance=MetricsDailyDauGeneratedRevenueDQChecks(),
            run_date=DAG_RUN_DATE_1_DAY_LAG,
        ),

        DQChecksOperator(
            task_id="run_metrics_daily_iap_revenue",
            dq_checks_instance=MetricsDailyIapRevenueDQChecks(),
            run_date=DAG_RUN_DATE_1_DAY_LAG,
        ),

        DQChecksOperator(
            task_id="run_metrics_daily_sim_dau_generated_revenue",
            dq_checks_instance=MetricsDailySimDauGeneratedRevenueDQChecks(),
            run_date=DAG_RUN_DATE_1_DAY_LAG,
        ),

        DQChecksOperator(
            task_id="run_orders_data_dq_checks",
            dq_checks_instance=OrdersDataDQChecks(),
            run_date=DAG_RUN_DATE_1_DAY_LAG,
        ),

        DQChecksOperator(
            task_id="run_all_user_daily_activities_dq_checks",
            dq_checks_instance=UserDailyActivitiesDQChecks(),
            run_date=DAG_RUN_DATE_1_DAY_LAG,
            run_now_func=run_on_days_gt_two_of_month,
        ),

        # run only those checks for user_daily_activities on days 1 and 2 of month that won't fail due to missing data.
        DQChecksOperator(
            task_id="run_only_days_1_2_of_month_user_daily_activities_dq_checks",
            dq_checks_instance=UserDailyActivitiesDQChecks(),
            run_date=DAG_RUN_DATE_1_DAY_LAG,
            exclude_checks=["Src And Dst Values Match-total_mms_cost",
                            "Src And Dst Values Match-total_sms_cost",
                            "Count Distinct Check-mms_cost_per_message",
                            "Count Distinct Check-sms_cost_per_message",
                            "Cost In Range-mms_cost_per_message",
                            "Cost In Range-sms_cost_per_message"],
            run_now_func=run_on_first_and_second_days_of_month,
        ),

        # run all checks for user_daily_profit only on days of month > 2
        DQChecksOperator(
            task_id="run_all_user_daily_profit_dq_checks",
            dq_checks_instance=UserDailyProfitDQChecks(),
            run_date=DAG_RUN_DATE_1_DAY_LAG,
            run_now_func=run_on_days_other_than_31_1_2_of_month,
        ),

        # run only those checks for user_daily_profit on days 1 and 2 of month that will not fail due to missing data.
        DQChecksOperator(
            task_id="run_only_days_31_1_2_of_month_user_daily_profit_dq_checks",
            dq_checks_instance=UserDailyProfitDQChecks(),
            run_date=DAG_RUN_DATE_1_DAY_LAG,
            exclude_checks=[
                "Src And Dst Values Match-mms_cost",
                "Src And Dst Values Match-sms_cost",
                "Src And Dst Values Match-message_cost",
                "Column Statistics Check-mms_cost avg",
                "Column Statistics Check-mms_cost max",
                "Column Statistics Check-mms_cost stddev",
                "Column Statistics Check-mms_cost percentile",
                "Column Statistics Check-sms_cost avg",
                "Column Statistics Check-sms_cost max",
                "Column Statistics Check-sms_cost stddev",
                "Column Statistics Check-message_cost avg",
                "Column Statistics Check-message_cost max",
                "Column Statistics Check-message_cost stddev",
                "Column Statistics Check-tmobile_cost avg",
                "Column Statistics Check-tmobile_cost max",
                "Column Statistics Check-tmobile_cost stddev",
                "Column Statistics Check-tmobile_mrc_cost avg",
                "Column Statistics Check-tmobile_mrc_cost max",
                "Column Statistics Check-tmobile_mrc_cost stddev"
            ],
            run_now_func=run_on_days_31_1_2_of_month,
        ),

        # run all checks for user_set_daily_activities only on days of month > 2
        DQChecksOperator(
            task_id="run_all_user_set_daily_activities_dq_checks",
            dq_checks_instance=UserSetDailyActivitiesDQChecks(),
            run_date=DAG_RUN_DATE_1_DAY_LAG,
            run_now_func=run_on_days_gt_two_of_month,
        ),

        # run the checks for user_set_daily_activities on days 1, 2 of month that won't fail due to missing data.
        DQChecksOperator(
            task_id="run_only_days_1_2_of_month_user_set_daily_activities_dq_checks",
            dq_checks_instance=UserSetDailyActivitiesDQChecks(),
            run_date=DAG_RUN_DATE_1_DAY_LAG,
            exclude_checks=[
                "Src And Dst Values Match-mms_messages",
                "Src And Dst Values Match-sms_messages",
                "Column Statistics Check-mms_messages avg",
                "Column Statistics Check-mms_messages stddev",
                "Column Statistics Check-mms_messages percentile",
                "Column Statistics Check-sms_messages avg",
                "Column Statistics Check-sms_messages stddev",
                "Column Statistics Check-sms_messages percentile"
            ],
            run_now_func=run_on_first_and_second_days_of_month,
        ),

        # Run DQ checks only after a dbt snowflake dag is complete, as early_mover event is dependent on dbt table
        DQChecksOperator(
            task_id="run_firebase_queries_dq_checks",
            dq_checks_instance=FirebaseQueriesDQChecks(),
            run_date=DAG_RUN_DATE_1_DAY_LAG,
        )
    )

    wait_dbt >> dq_checks


dq_checks_dbt_daily_post_run()

if __name__ == "__main__":
    # dq_checks_dbt_daily_post_run().test(execution_date="2024-10-22")
    dq_checks_dbt_daily_post_run().test_dq_checks(execution_date="2024-10-24")
