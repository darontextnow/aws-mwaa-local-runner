from functools import lru_cache

AIRFLOW_KEYS = {"host": "/ds/airflow/db_host",
                "port": "/ds/airflow/db_port",
                "user": "/ds/airflow/db_user",
                "password": "/ds/airflow/db_password"}


@lru_cache(maxsize=1)
def get_fernet_key() -> str:
    """ Returns airflow fernet key from AWS SSM """
    from de_utils.aws import get_secret  # delay import to keep top level DAG loads efficient
    return get_secret("airflow/variables/airflow_fernet_key")


def get_fernet():
    """ Returns a fernet encrypt/decrypter """
    from cryptography.fernet import Fernet  # delay import to keep top level DAG loads efficient
    return Fernet(get_fernet_key())


def snowflake_convert_row_dtypes(row) -> list:
    """Helper method to convert values in rows to correct data type.
    Needed to convert ARRAY and MAP dtypes from Snowflake pretty printed string formats
        into an actual list or dict Python dtypes.
    """
    vals = []
    for val in row:
        if type(val) == str and val.startswith("[") and val.endswith("]"):
            val = snowflake_str_list_to_list(val)
        if type(val) == str and val.startswith("{") and val.endswith("}"):
            val = snowflake_str_map_to_dict(val)
        vals.append(val)
    return vals


def snowflake_str_list_to_list(str_list: str) -> list:
    """Converts Snowflake's pretty print str representation of a list to an actual Python list and returns it.
    Will try to convert each element of the list to the appropriate Python type."""
    from de_utils.utils import convert_str_to_dtype
    if str_list == "[]":
        return []

    val = str_list.replace("[\n  ", "").replace("\n]", "")
    val = [convert_str_to_dtype(v.strip('"')) for v in val.split(",\n  ")]
    return val


def snowflake_str_map_to_dict(str_dict: str) -> dict:
    """Converts Snowflake's pretty print map representation of a dict to an actual Python dict and returns it.
    Will try to convert each element of the list to the appropriate Python type."""
    import re  # delay import to keep top level DAG loads efficient
    from de_utils.utils import convert_str_to_dtype

    if str_dict == "{}":
        return {}

    val = str_dict.replace("{\n  ", "").replace("\n}", "")
    d = {}
    for kv in re.split(r",\n\s\s\S", val):
        keyval = kv.split(": ")  # map columns in SF always return a dict with space after :
        if len(keyval) == 1:  # this is not a dict returned by a typical snowflake map column
            import json  # this may be a standard json column being returned (i.e. SHOW COLUMNS results)
            return json.loads(str_dict)  # try to return a standard json parsed dict
        key = convert_str_to_dtype(keyval[0].strip('"'))
        value = keyval[1]
        if value.startswith("["):  # if value is an array, convert it to a list
            value = snowflake_str_list_to_list(value.replace("\n    ", "\n  ").replace("\n  ]", "\n]"))
        else:
            value = convert_str_to_dtype(keyval[1].strip('"'))
        d[key] = value
    return d


def snowflake_union_queries(queries: list[str], union_all: bool = True) -> str:
    """Utility function to union together given list of SQL statements.
    Designed to try to handle complex cases like queries using CTE and queries with comments in them.

    args:
        queries (list[str]): list of sql statements to be unioned together.
        union_all (bool): By default, queries will be unioned using UNION ALL SELECT.
            Set to False to use UNION SELECT.
    """
    sql = ""
    union = f"UNION{' ALL' if union_all else ''}"
    for idx, q in enumerate(queries):
        if idx > 0:
            if "WITH" in q:  # WITH may be after comments
                sql += q.replace("WITH", f"{union} SELECT * FROM (\nWITH") + "\n)\n"
            else:  # Simply add UNION ALL before SELECT
                sql += f"{union} {q}\n"
        else:  # First query doesn't need the UNION ALL
            sql += q + "\n"
    return sql
