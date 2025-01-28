from de_utils.tndbo.utils import snowflake_str_list_to_list, snowflake_str_map_to_dict
from uuid import uuid4
from functools import lru_cache
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.connector.constants import QueryStatus
    from pandas import DataFrame

try:
    import snowflake
    from snowflake.connector import DictCursor
    from sqlalchemy.exc import ProgrammingError as AlchemyProgrammingError
    from snowflake.connector import ProgrammingError as SFProgrammingError
    from sqlalchemy import create_engine
    from sqlalchemy.engine.base import Engine
    from sqlalchemy.engine import CursorResult
    from sqlalchemy.engine.url import URL
    from sqlalchemy.sql import text
    from snowflake.sqlalchemy import URL as SF_URL
except ImportError:
    msg = ("Not all python dependencies for using tndbo have been installed."
           " To install tndbo related dependencies, run: pip install de_utils[tndbo]")
    raise ImportError(msg)


__all__ = ["DatabaseObject",
           "MySQLDatabaseObject",
           "SnowflakeDatabaseObject",
           "ProgrammingError"]


# Create our own ProgrammingError Exception so we don't have to deal with various engine flavours of ProgrammingError
class ProgrammingError(Exception):
    pass


@lru_cache(maxsize=10)
def get_engine_from_url(url: URL, autocommit: bool = True):
    """Per SQLAlchemy docs, engine should be held globally per URL for the lifetime of a single application process.
    This method caches engines per URL.
    """
    connect_args = {}
    if autocommit:
        # Tested and works for raw Snowflake connection that uses execute_async and for SQLAlchemy Connection.
        # Snowflake docs say autocommit is True by default, but that is not the case here using SQLAlchemy.
        connect_args["autocommit"] = True
    return create_engine(url, connect_args=connect_args)


class DatabaseObject(ABC):
    fmt = "jdbc"
    driver = None

    def __init__(
            self,
            credential,
            spark_session=None
    ):
        """
        Database object with access to read from and write to the underlying relational database

        :param credential: a credential object defined in credential.py
        :param spark_session: a live spark session
        """
        self.cred = credential
        self.spark = spark_session
        self.instance_id = str(uuid4())

    @property
    @abstractmethod
    def conn_type(self) -> str:
        return self._conn_type

    @property
    def engine_url(self) -> URL:
        """ SQLAlchemy url for creating engine """
        url = URL(
            self.cred.protocols.get(self.conn_type, "unknown+protocol"),
            host=self.cred.host,
            port=self.cred.port,
            database=self.cred.schema,
            username=self.cred.login,
            password=self.cred.password,
        )
        return url

    @property
    def jdbc_url(self) -> str:
        """ Returns JDBC url for accessing database through Spark """
        if self.conn_type == "mysql":
            return f"jdbc:{self.conn_type}://{self.cred.host}:{self.cred.port}/{self.cred.schema}"
        # snowflake doesn't use any jdbc urls
        if self.conn_type == "snowflake":
            return ""

    def spark_opts(self) -> dict:
        return self._get_spark_opts()

    def _get_spark_opts(self) -> dict:
        """ Returns basic spark options for reading/writing """
        opts = dict(
            url=self.jdbc_url,
            user=self.cred.login,
            password=self.cred.password,
        )
        if self.driver is not None:
            opts["driver"] = self.driver
        return opts

    def get_engine(self, autocommit: bool = True) -> Engine:
        """ Returns an SQLAlchemy engine. """
        return get_engine_from_url(url=self.engine_url, autocommit=autocommit)

    def get_conn(self, autocommit: bool = True):
        """ Returns an SQLAlchemy engine based connection """
        return self.get_engine(autocommit=autocommit).connect(autocommit)

    def get_column_defs(self, table: str) -> dict[str, dict[str, str]]:
        """Returns a dict of columns names: column_attrs for all columns in given table (schema.table_name)"""
        sql = f"""SELECT LOWER(column_name), data_type FROM information_schema.columns
                WHERE
                    (table_schema = '{table.split(".")[0]}') 
                    AND (table_name = '{table.split(".")[1]}')
                ORDER BY ordinal_position"""
        column_defs = {}
        for row in self.execute(sql).fetchall():
            col = f'"{row[0]}"' if row[0].startswith("@") else row[0]  # some columns start with @ (i.e. @shard)
            column_defs[col] = row[1]
        if not column_defs:
            raise RuntimeError(f"Table: {table} not found.")
        return column_defs

    def execute(
            self,
            sql: str,
            params: dict = None,
            async_execute: bool = False,
            autocommit: bool = True
    ) -> CursorResult:
        """Executes given sql statement (with optional parameters) and returns DictCursor object."""
        if async_execute:
            raise NotImplementedError("Support for async execution for this engine has not been implemented yet."
                                      " Create DE Jira if you need this added to de-utils.")
        conn = self.get_conn(autocommit=autocommit)
        if params:
            sql = text(sql)  # convert sql to sqlalchemy text type so params can be used
            cursor = conn.execute(sql, params)
        else:
            cursor = conn.execute(sql)
        return cursor

    def read(
            self,
            sql: str,
            params: list | dict | tuple | None = None,
            **kwargs
    ) -> "DataFrame":
        """
        Reads a sql query and returns the output as a pandas dataframe if no spark session is provided
        or a spark dataframe otherwise

        :param sql: sql query to be run against the underlying database
        :param params: query template parameters
        :param kwargs: any other configs/arguments accepted by either `spark.read.options` or `pd.read_sql`
        :return: pandas dataframe or spark dataframe
        """
        if self.spark:
            extra_opts = self.spark_opts()
            extra_opts.update(kwargs)
            return self.spark.read \
                .format(self.fmt) \
                .option("query", sql) \
                .options(**extra_opts) \
                .load()
        else:
            import pandas as pd
            return pd.read_sql(
                sql,
                self.get_engine(),
                params=params,
                **kwargs
            )

    def write(
            self,
            df,
            table: str,
            schema: str = "public",
            save_mode: str = "append",
            chunksize: int = 1000,
            **kwargs
    ):
        """
        Write a pandas or spark dataframe to the underlying relational database

        :param df: pandas dataframe for spark dataframe
        :param table: target table where data will be inserted into
        :param schema: schema of the target table
        :param save_mode: spark save modes ("overwrite", "append", "ignore", "error")
        :param chunksize: maximum batch size for each insert operation
        :param kwargs: any other arguments accepted by `sparksql.Dataframe.write` or `pd.Dataframe.to_sql`
        """
        if self.spark:
            extra_opts = self.spark_opts()
            extra_opts.update(kwargs)
            df.write.format(self.fmt) \
                .mode(save_mode) \
                .option("dbtable", f"{schema}.{table}") \
                .option("batchsize", chunksize) \
                .options(**extra_opts) \
                .save()
        else:
            df.to_sql(table,
                      self.get_engine(),
                      schema=schema,
                      if_exists='replace' if save_mode == 'overwrite' else save_mode,
                      index=False,
                      chunksize=chunksize,
                      **kwargs)


class MySQLDatabaseObject(DatabaseObject):
    conn_type = "mysql"
    driver = "com.mysql.jdbc.Driver"


class SnowflakeDatabaseObject(DatabaseObject):
    conn_type = "snowflake"
    fmt = "net.snowflake.spark.snowflake"
    warehouse = None

    @property
    def engine_url(self) -> SF_URL:
        """ Returns Snowflake's specific database URL for SQLAlchemy """
        conn_params = dict(
            account=self.cred.extra["account"],
            user=self.cred.login,
            password=self.cred.password,
            database=self.cred.extra["database"],
            warehouse=self.warehouse or self.cred.extra["warehouse"],
            role=self.cred.extra.get("role", ""),
        )
        if self.cred.schema:
            conn_params["schema"] = self.cred.schema
        url = SF_URL(**conn_params)
        return url

    def spark_opts(self) -> dict:
        """ Returns a dictionary of spark options specific to Snowflake """
        opts = self._get_spark_opts()
        opts.pop("url")
        opts["sfUrl"] = self.cred.host
        opts["sfUser"] = opts.pop("user")
        opts["sfPassword"] = opts.pop("password")
        opts["sfDatabase"] = self.cred.extra["database"]
        opts["sfWarehouse"] = self.warehouse or self.cred.extra["warehouse"]
        opts["sfRole"] = self.cred.extra["role"]
        if self.cred.schema:
            opts["sfSchema"] = self.cred.schema
        return opts

    def get_engine(self, autocommit: bool = True, warehouse: str = None) -> Engine:
        """ Returns an SQLAlchemy engine. """
        self.warehouse = warehouse
        return get_engine_from_url(url=self.engine_url, autocommit=autocommit)

    def get_conn(self, autocommit: bool = True, warehouse: str = None):
        """ Returns an sqlalchemy connection to SnowFlake """
        return self.get_engine(autocommit=autocommit, warehouse=warehouse).connect()

    def get_column_defs(self, table: str) -> dict[str, dict[str, str]]:
        """Returns a dict of columns names: column_attrs for all columns in given table (schema.table_name)

        Raises: snowflake.connector.errors.ProgrammingError if table does not exist.
        """
        pkey_defs = {}
        for row in self.execute(f"SHOW PRIMARY KEYS IN {table};").fetchall():
            col = f'"{row[4].lower()}"' if row[4].startswith("@") else row[4].lower()
            pkey_defs[col] = row[5]  # column index #5 should be key_sequence

        column_defs = {}
        for row in self.execute(f"SHOW COLUMNS IN {table};").fetchall():
            # some columns start with @ (i.e. @shard)
            col = f'"{row[2].lower()}"' if row[2].startswith("@") else row[2].lower()
            column_defs[col] = {"data_type": row[3],
                                "nullable": row[4] == "true",
                                "is_pkey": True if pkey_defs.get(col) else False}
            if pkey_defs.get(col):
                column_defs[col]["pkey_sequence"] = pkey_defs[col]
        if not column_defs:
            raise RuntimeError(f"Table: {table} not found.")
        return column_defs

    def execute(
            self,
            sql: str,
            params: dict = None,
            async_execute: bool = False,
            autocommit: bool = True,
            warehouse: str = None
    ) -> CursorResult:
        """Executes given sql statement (with optional parameters) and returns cursor object.

        Args:
            sql (str): The sql statement to be executed.
            params (dict): A dict of optional parameters to supply to sql statement.
            async_execute (bool): True means cursor returns immediately and does not wait for query to complete.
            autocommit (bool): Set to False if you need to manually commit transactions.
            warehouse (str): Optionally set name of Snowflake warehouse to use for execution.
        """
        if async_execute:
            cursor = self.get_engine(warehouse=warehouse).raw_connection().cursor().execute_async(sql, params)
            cursor["warehouse"] = warehouse  # setting for use later
            return cursor
        else:
            try:
                if params:
                    sql = text(sql)  # convert sql to sqlalchemy text type so params can be used
                    cursor = self.get_conn(autocommit=autocommit, warehouse=warehouse).execute(sql, params)
                else:
                    cursor = self.get_conn(autocommit=autocommit, warehouse=warehouse).execute(sql)
            except AlchemyProgrammingError as e:
                raise ProgrammingError(str(e))

        # Using next method to override SnowflakeCursor fetchone() method
        def _fetchone_(self):
            """Takes result row from fetchone and converts array/map to list/dict with proper dtypes.
            Note that fetchall(), fetchmany(), and iterating over results all use fetchone()."""
            result = self._fetchoneold()
            if result is None:
                return
            row = []
            for val in result:
                if isinstance(val, str) and val.startswith("[") and val.endswith("]"):
                    row.append(snowflake_str_list_to_list(val))
                elif isinstance(val, str) and val.startswith("{") and val.endswith("}"):
                    row.append(snowflake_str_map_to_dict(val))
                else:
                    row.append(val)
            return row

        # rename current fetchone() method so we can still use it to retrieve rows.
        cursor.cursor._fetchoneold = cursor.cursor.fetchone
        # override fetchone() method with customized method
        from types import MethodType
        cursor.cursor.fetchone = MethodType(_fetchone_, cursor.cursor)
        return cursor

    def get_async_execution_status(self, cursor) -> "QueryStatus":
        """Returns QueryStatus for async query execution.
        The supplied cursor must be cursor returned from execute method with async_execute arg = True.

        Raises:
            de_utils.tndbo.ProgrammingError if query execution fails.
        """
        raw_conn = self.get_engine(warehouse=cursor["warehouse"]).raw_connection()
        try:
            return raw_conn.get_query_status_throw_if_error(cursor["queryId"])
        except SFProgrammingError as e:
            raise ProgrammingError(str(e))

    def get_async_execution_results_cursor(self, cursor):
        class _DictRow:
            """Row object that allows col value retrieval by index and case-insensitive column name."""

            def __init__(self, row: dict):
                """row is a list of row values."""
                new_row = {}
                for k, v in row.items():
                    # Convert array and map string dtypes to list and dict
                    if isinstance(v, str) and v.startswith("[") and v.endswith("]"):
                        v = snowflake_str_list_to_list(v)
                    elif isinstance(v, str) and v.startswith("{") and v.endswith("}"):
                        v = snowflake_str_map_to_dict(v)
                    new_row[k.lower()] = v  # using lower case here to provide case-insensitive column naming
                self._row = new_row

            def __getitem__(self, x):
                if isinstance(x, str):
                    return self._row[x.lower()]  # provide case-insensitive column name retrieval
                else:
                    return list(self._row.values())[x]

            def get(self, x):
                if isinstance(x, str):
                    return self._row[x.lower()]  # provide case-insensitive column name retrieval
                else:
                    return self._row.get(x)

        class _DictCursor(DictCursor):
            def fetchone(self):
                row = super().fetchone()
                return None if row is None else _DictRow(row=row)

        cur = self.get_engine(warehouse=cursor["warehouse"]).raw_connection().cursor(_DictCursor)
        try:
            cur.get_results_from_sfqid(cursor["queryId"])
            return cur
        except SFProgrammingError as e:
            raise ProgrammingError(str(e))

    def execute_in_parallel(self, sqls: list[str], warehouse: str = None) -> list[str]:
        """Sends given list of sql statements to Snowflake at the same time.

        Method waits for all queries to finish executing before returning list of results.
        Meant for DDL type statements. Do not use with SELECT statements that return large results.
        Thus, it will be up to the Snowflake queue as to when the queries run.
        If the Snowflake instance has enough resources, queries will run at the same time.

        Args:
            sqls (List(str)): List of sql statements to be executed.
            warehouse (str): Optionally set name of Snowflake warehouse to use for execution.
        """
        async_cursors = []
        # Execute all queries in async mode so they can start running around the sound time.
        for query in sqls:
            async_cursors.append(self.execute(query, async_execute=True, warehouse=warehouse))
        # Loop through all async cursors and wait for all the queries to return results
        results = []
        for cursor in async_cursors:
            results_cursor = self.get_async_execution_results_cursor(cursor)
            results.append(results_cursor.fetchone()[0])
        return results

    def read(
            self,
            sql: str,
            params: list | dict | tuple | None = None,
            schema: str = "",
            warehouse: str | None = None,
            chunksize: int = 100000,
            **kwargs
    ):
        """Reads data from Snowflake into a pandas dataframe if no spark session was provided and
        a spark dataframe otherwise. Supply conn configs if you wish to override current configs
        defined in the airflow connection.

        Specifying chunksize greatly improves performance and memory usage for large result sets.
        Current chunksize default was tested for one large query result set.
        You can modify the default chunksize if needed to try to gain better perf/memory consumption.

        Args:
            sql (str): query to be run.
            params (dict): query parameter values.
            schema (str): schema to use.
            warehouse (str): warehouse to use.
            chunksize (int): Number of rows to read into pandas df chunks. Only for perf and memory usage optimizing.
            kwargs (dict): spark extra options.

        Returns:
            Pandas dataframe or spark dataframe containing results of executed query.
        """
        if self.spark:
            extra_opts = self.spark_opts()
            extra_opts.update(kwargs)
            extra_opts["sfWarehouse"] = warehouse or extra_opts["sfWarehouse"]
            if schema or extra_opts.get("sfSchema"):
                extra_opts["sfSchema"] = schema or extra_opts.get("sfSchema")
            return self.spark.read \
                .format(self.fmt) \
                .option("query", sql) \
                .options(**extra_opts) \
                .load()
        else:
            import pandas as pd
            chunks = pd.read_sql(
                sql,
                self.get_engine(warehouse=warehouse).connect().execution_options(stream_results=True),
                params=params,
                chunksize=chunksize,
                **kwargs
            )
            df = pd.concat(chunks, ignore_index="index_col" not in kwargs)
            return df

    def write(
            self,
            df,
            table: str,
            schema: str = "PUBLIC",
            warehouse: str | None = None,
            save_mode: str = "append",
            chunksize: int = 1000,
            **kwargs
    ):
        """
        Writes pandas or spark dataframe to Snowflake, schema/database in parameters
        will override schema/database settings in the underlying airflow connection.
        Supply conn configs if you wish to override current configs defined in the
        airflow connection.

        :param df: pandas dataframe for spark dataframe
        :param table: target table where data will be inserted into
        :param schema: schema of the target table
        :param warehouse: warehouse to use
        :param save_mode: spark save modes ("overwrite", "append", "ignore", "error")
        :param chunksize: maximum batch size for each insert operation
        :param kwargs: any other arguments accepted by `sparksql.Dataframe.write` or
                       `snowflake.connector.pandas_tools.write_pandas`
        """
        if self.spark:
            extra_opts = self.spark_opts()
            extra_opts.update(kwargs)
            extra_opts["sfWarehouse"] = warehouse or extra_opts["sfWarehouse"]
            extra_opts["sfSchema"] = schema or extra_opts.get("sfSchema")
            extra_opts["s3MaxFileSize"] = extra_opts.get("s3MaxFileSize") or 16000000
            df.write.format(self.fmt) \
                .mode(save_mode) \
                .option("dbtable", table) \
                .options(**extra_opts) \
                .save()
        else:
            df.to_sql(
                table,
                self.get_engine(warehouse=warehouse),
                schema=schema,
                if_exists='replace' if save_mode == 'overwrite' else save_mode,
                index=False,
                chunksize=chunksize,
                **kwargs
            )
