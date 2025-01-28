"""Module containing all enums used for DQ Checks to help keep functionality honest/in sync."""

from enum import Enum


class Status(Enum):
    UNKNOWN = "UNKNOWN"
    GREEN = "GREEN"
    RED = "RED"
    YELLOW = "YELLOW"
    ERROR = "ERROR"
    RUN_AGAIN = "RUN_AGAIN"


class AlertStatus(Enum):
    UNKNOWN = "UNKNOWN"
    NO_ALERT_SENT = "NO_ALERT_SENT"
    RED_ALERT_SENT = "RED_ALERT_SENT"
    YELLOW_WARNING_SENT = "YELLOW_WARNING_SENT"
    ALERT_FAILED_TO_SEND = "ALERT_FAILED_TO_SEND"


class CheckType(Enum):
    RED = "RED"
    YELLOW = "YELLOW"
