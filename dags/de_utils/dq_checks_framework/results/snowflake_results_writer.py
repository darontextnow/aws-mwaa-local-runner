"""Module contains all the Objects and the Writer for processing DQ Check Results and writing them Snowflake.

Note: when using truncate below it's to ensure values passed by users don't go over the column size specified in
results tables. May need to adjust as needed. Using a size one less than set in tables to ensure any escaping of
strings will still fit in column size limits.
"""
from de_utils.dq_checks_framework.utils.dbo_utils import execute
from de_utils.enums import Env
from de_utils.constants import SF_CONN_ID
from de_utils.dq_checks_framework.constants import REPORTING_TABLES_SCHEMA
from datetime import datetime
from uuid import uuid4


class SnowflakeResultsWriter:
    """Class responsible for collecting DQCheck results and writing them out to Snowflake reporting tables.

    Args:
        env (Env): environment enum - Env.DEV, Env.PROD
    """
    def __init__(self, env: Env):
        self._env = env

    def write_dq_checks(
            self,
            check_id: str,
            name: str,
            executed_ts: datetime,
            description: str = None,
            table_name: str = None,
            params: dict = None,
            error: str = None
    ) -> uuid4:
        sql = f"""INSERT INTO {self._env.value}.{REPORTING_TABLES_SCHEMA}.dq_checks 
            (check_id, executed_at, name, description, params, run_error, table_name, inserted_at)
            VALUES (
                '{check_id}',
                '{str(executed_ts)}',
                {self._escape(name)},
                {self._escape(description, truncate_len=2999)},
                {self._escape(None if params == {} else str(params))},
                {self._escape(error, truncate_len=9999)},
                {self._escape(table_name, truncate_len=500)},
                CURRENT_TIMESTAMP
        )"""
        execute(sql=sql, airflow_conn_id=SF_CONN_ID)

    def write_inputs(self, check_id: str, inputs: list):
        # filter out Anomaly Inputs from list as they don't get written to this inputs table.
        sql = f"""INSERT INTO {self._env.value}.{REPORTING_TABLES_SCHEMA}.dq_checks_inputs 
            (input_id, check_id, code_id, source, name, alias, index, inserted_at, query_id)
            VALUES """
        code_ids = self._get_code_ids(inputs)
        values = []
        for idx, inpt in enumerate(inputs):
            input_id = uuid4().hex
            values.append(f"""(
                    '{input_id}',
                    '{check_id}',
                    '{code_ids[idx]}',
                    {self._escape(inpt.source)},
                    {self._escape(inpt.name)},
                    {self._escape(inpt.alias)},
                    {idx},
                    CURRENT_TIMESTAMP,
                    {self._escape(inpt.query_id)}
            )""")
        execute(sql=sql + ",".join(values), airflow_conn_id=SF_CONN_ID)

    def write_details(self, checks: list, check_id: str):
        """Inserting in batch here (1 execution) for much greater performance."""
        sql = f"""INSERT INTO {self._env.value}.{REPORTING_TABLES_SCHEMA}.dq_checks_details 
            (detail_id, check_id, name, description, status, alert_status, value, red_expr, yellow_expr,
            check_error, inserted_at, column_name, use_dynamic_thresholds)
            VALUES """
        values = []
        for check in checks:
            detail_id = uuid4().hex
            value = str(check.value)
            if type(check.value) == list:
                value = value.replace("\n", "")  # Remove newlines from list string representation

            values.append(f"""(
                    '{detail_id}',
                    '{check_id}',
                    {self._escape(check.name)},
                    {self._escape(check.description, truncate_len=2999)},
                    '{check.status.value}',
                    '{check.alert_status.value}',
                    {self._escape(value)},
                    {self._escape(check.red_expr)},
                    {self._escape(check.yellow_expr)},
                    {self._escape(check.error, truncate_len=9999)},
                    CURRENT_TIMESTAMP,
                    {self._escape(check.column_name, truncate_len=500)},
                    {check.use_dynamic_thresholds}
            )""")
        execute(sql=sql + ",".join(values), airflow_conn_id=SF_CONN_ID)

    def write_anomaly_records(self, checks: list, check_id: str):
        """Inserting in batch here (1 execution) for much better performance."""
        sql = f"""INSERT INTO {self._env.value}.{REPORTING_TABLES_SCHEMA}.dq_checks_anomaly_records
            (record_id, check_id, name, description, table_name, column_name, value, row_filter_expr, inserted_at) 
            VALUES """
        values = []
        for check in checks:
            for row in check.get_output_rows():
                record_id = uuid4().hex
                values.append(f"""(
                        '{record_id}',
                        '{check_id}',
                        {self._escape(check.name)},
                        {self._escape(check.description, truncate_len=2999)},
                        {self._escape(check.table_name)},
                        {self._escape(check.column_name)},
                        {self._escape(str(row[0]), truncate_len=1999)},
                        {self._escape(row[1])},
                        CURRENT_TIMESTAMP
                )""")
        if len(values) > 0:
            execute(sql=sql + ",".join(values), airflow_conn_id=SF_CONN_ID)

    # noinspection PyMethodMayBeStatic
    def _escape(self, val: any, truncate_len: int = None):
        if not val or val in ["None", "NONE", "none", "Null", "NULL", "null"]:
            val = "NULL"
        elif val and type(val) == str:
            if truncate_len:
                val = val[:truncate_len]
            val = "'" + val.replace("'", "''") + "'"
        return val

    def _get_code_ids(self, inputs: list) -> list:
        """Returns ordered list of code_id from INPUTS_CODE table for each input in inputs_list.
        If the code doesn't already exist in INPUTS_CODE table, a row will be inserted with a new code_id.
        Code should be unique, but Snowflake doesn't enforce unique constraints.
        This returns unique ids to use instead of inserting a duplicate if code already exists in table.
        """
        existing_ids = self._get_existing_code_ids(inputs)

        # Now create new code_ids and rows in table for any that don't already exist
        code_ids = []
        for idx, inpt in enumerate(inputs):
            code_id = None
            for row in existing_ids:
                if row["idx"] == idx:
                    code_id = row["code_id"]
            if not code_id:
                # code doesn't already exist in table, so add it.
                # Separate queries here is okay since it's only a one time insert.
                code_id = uuid4().hex
                sql = f"""INSERT INTO {self._env.value}.{REPORTING_TABLES_SCHEMA}.dq_checks_inputs_code 
                    (code_id, code, inserted_at)
                    VALUES (
                        '{code_id}',
                        {self._escape(inpt.code)},
                        CURRENT_TIMESTAMP
                )"""
                execute(sql=sql, airflow_conn_id=SF_CONN_ID)
            code_ids.append(code_id)

        return code_ids

    def _get_existing_code_ids(self, inputs: list):
        """Runs one query to return any existing code_ids already in inputs_code table.
        Returns a row for each existing code in table with idx column and code_id column."""
        sql = []
        for idx, inpt in enumerate(inputs):
            sql.append(f"""SELECT {idx} AS idx, CODE_ID AS code_id 
                        FROM {self._env.value}.{REPORTING_TABLES_SCHEMA}.dq_checks_inputs_code 
                        WHERE code = {self._escape(inpt.code)}""")
        return execute(sql=" UNION ALL ".join(sql), airflow_conn_id=SF_CONN_ID).fetchall()
