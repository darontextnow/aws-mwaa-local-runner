"""Module contains a variety of functions that can be used for run_now_func argument on DQChecksOperator class."""
from datetime import datetime


def run_on_monday(context, run_date, run_params):
    if isinstance(run_date, str):
        run_date = datetime.strptime(run_date, "%Y-%m-%d")
    if run_date.weekday() == 0:
        return True
    return False


def run_on_non_mondays(context, run_date, run_params):
    if isinstance(run_date, str):
        run_date = datetime.strptime(run_date, "%Y-%m-%d")
    if run_date.weekday() != 0:
        return True
    return False


def run_on_first_and_second_days_of_month(context, run_date, run_params):
    if isinstance(run_date, str):
        run_date = datetime.strptime(run_date, "%Y-%m-%d")
    if run_date.day in [1, 2]:
        return True
    return False


def run_on_days_gt_two_of_month(context, run_date, run_params):
    if isinstance(run_date, str):
        run_date = datetime.strptime(run_date, "%Y-%m-%d")
    if run_date.day > 2:
        return True
    return False


def run_on_days_31_1_2_of_month(context, run_date, run_params):
    """user_daily_profit sets all costs to 0 on 31st of month and costs aren't available yet on days 1 and 2"""
    if isinstance(run_date, str):
        run_date = datetime.strptime(run_date, "%Y-%m-%d")
    if run_date.day in [31, 1, 2]:
        return True
    return False


def run_on_days_other_than_31_1_2_of_month(context, run_date, run_params):
    """user_daily_profit sets all costs to 0 on 31st of month and costs aren't available yet on days 1 and 2"""
    if isinstance(run_date, str):
        run_date = datetime.strptime(run_date, "%Y-%m-%d")
    if run_date.day > 2 and run_date.day != 31:
        return True
    return False
