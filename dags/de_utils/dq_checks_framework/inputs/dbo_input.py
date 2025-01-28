from de_utils.dq_checks_framework.inputs._base_input import BaseInput
from de_utils.constants import SF_WH_SMALL, SF_CONN_ID


class DBOInput(BaseInput):
    """Shared Input class implementation for all dbo (tndbo) implementations.
     Runs sql statement which returns input data and converts the results to an InputObject to be used in DQ Checks.

     Note: this class simply wraps the tndbo library methods that retrieve data. Thus, the DBOInput class can
        run a SELECT query in any tndbo supported dbo.

    Args:
        src_sql (str): The sql statement to execute for retrieving results to be used as input to DQ checks.
            Note: this will be used as the "code" arg in base class BaseInput
        dbo (str): The name of the database object (as required by tndbo) to use to execute src_query.
        warehouse (str): The Snowflake Warehouse to use when executing the query.
        See parent class for definition of additional args/kwargs.
    """
    def __init__(
            self,
            src_sql: str,
            dbo: str = SF_CONN_ID,
            warehouse: str = SF_WH_SMALL,
            *args,
            **kwargs
    ):
        super().__init__(source=dbo, code=src_sql, *args, **kwargs)
        self._dbo = dbo
        self.warehouse = warehouse
        self.query_id = None  # set below after execution

    # Can override as needed if you have DBO object that works differently than others
    def get_input(self, params: dict = None):
        """Returns an InputObject with results of query.
        If no rows are returned from query, will return empty lists for cols and vals."""
        from de_utils.dq_checks_framework.inputs._input_object import InputObject
        from de_utils.dq_checks_framework.utils.dbo_utils import execute
        cursor = execute(sql=self.code, params=params, airflow_conn_id=self._dbo, warehouse=self.warehouse)
        self.query_id = cursor.cursor.sfqid
        rows = cursor.fetchall()
        if not rows:
            return InputObject(cols=[], vals=[[]])
        return InputObject(cols=list(rows[0].keys()), vals=rows)
