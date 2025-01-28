"""Utility to print out current max and min values for a table for each stat for each given column for defined period.
Helpful in initially setting threshold ranges for ColumnStatsCheck (avg, min, max, stddev, etc.)
"""

STATS = ("avg", "median", "min", "max", "stddev", "percentile")
PERCENTILE = 0.95
TABLE = "prod.adops.report"
COLUMNS = ["dau", "ad_impressions", "mms_messages", "sms_messages", "total_incoming_calls",
           "total_incoming_call_duration","total_incoming_domestic_calls", "total_incoming_free_calls",
           "total_incoming_international_calls", "total_incoming_paid_calls", "total_incoming_unique_calling_contacts",
           "total_outgoing_calls", "total_outgoing_call_duration", "total_outgoing_domestic_calls",
           "total_outgoing_free_calls", "total_outgoing_international_calls", "total_outgoing_paid_calls",
           "total_outgoing_unique_calling_contacts", "video_call_initiations"]
COLUMNS = ["total_mms_cost", "total_sms_cost", "total_termination_cost", "total_layered_paas_cost"]
COLUMNS = ["day_from_cohort", "dau"]
COLUMNS = ["impressions", "clicks", "revenue", "requests", "iap_revenue"]
WHERE = "(date BETWEEN CAST('2022-12-01' AS DATE) AND CAST('2023-02-05' AS DATE))"
GROUP_BY = "date"


def get_thresholds(
        table: str,
        columns: list[str],
        where: str = None,
        group_by: str = None,
        stats = STATS,
        percentile: float = None
):
    """Prints out the min and max values found in given table for each given stat on each given column.
    This enables to see what the current range of thresholds are per stat.
    Note: this is currently only supporting Snowflake Prod as source. Can expand this as needed.
    See example constants above for all Args.

    Args:
        table (str): the full db.schema.table_name
        columns (List(str)): A list of column names to run stats on and return results for.
        where (str): Optional. A WHERE CLAUSE (filter) to add to query to limit time range (for big tables) or some
            other dimension(s) being queried.
        group_by (str): Optional. Add a GROUP BY expression to the query.
        stats (List(str): List of stats to calculate over each given column.
        percentile(float): If percentile is included in list of stats, must include the percentile of the value to find.
    """
    ocols = []
    icols = []
    for col in columns:
        for stat in stats:
            ocols.append(f"\nMIN({stat}_{col}) AS min_{stat}_{col},\nMAX({stat}_{col}) AS max_{stat}_{col}")
            if stat == "percentile":
                icols.append(f"\nPERCENTILE_CONT({percentile}) WITHIN GROUP(ORDER BY {col}) AS {stat}_{col}")
            else:
                icols.append(f"\n{stat.upper()}({col}) AS {stat}_{col}")

    sql = f"""
        SELECT {",".join(ocols)}
        FROM (
            SELECT {",".join(icols)}
            FROM {table}
            WHERE {where or ""}
            {"" if not group_by else "GROUP BY " + group_by}
        );
    """
    from de_utils.dq_checks_framework.utils.dbo_utils import execute
    from de_utils.constants import SF_CONN_ID
    cursor = execute(sql, airflow_conn_id=SF_CONN_ID)
    row = execute(sql, airflow_conn_id=SF_CONN_ID).fetchone()
    for col in cursor.keys():
        print(col, row[col])


if __name__ == "__main__":
    get_thresholds(
        table=TABLE,
        columns=COLUMNS,
        where=WHERE,
        group_by=GROUP_BY,
        stats=STATS,
        percentile=PERCENTILE
    )
