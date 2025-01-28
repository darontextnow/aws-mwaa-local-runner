"""Module for exposing a shared dbo object and shared utility functions related to snowflake."""


def execute(
        sql: str,
        airflow_conn_id: str,
        params: dict = None,
        autocommit: bool = True,
        warehouse: str = None  # None means the default Warehouse defined by airflow_conn params
):
    """Executes given sql statement (with optional parameters) and returns cursor object."""
    from de_utils.tndbo import get_dbo  # delay import to keep top level DAG parsing efficient
    dbo = get_dbo(airflow_conn_id)
    cursor = dbo.execute(sql=sql, params=params, autocommit=autocommit, warehouse=warehouse)
    return cursor
