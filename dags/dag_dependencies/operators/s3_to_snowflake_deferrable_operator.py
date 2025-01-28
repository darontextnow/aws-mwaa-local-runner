from dag_dependencies.operators.snowflake_async_deferred_operator import SnowflakeAsyncDeferredOperator
from de_utils.constants import STAGE_SF, SF_CONN_ID
from enum import Enum


class S3ToSnowflakeDeferrableOperator(SnowflakeAsyncDeferredOperator):
    """
    Executes COPY command to load files from s3 to Snowflake.

    NOTE: from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
        is too generic for our use cases. Thus, using custom operator for our loads to Snowflake.

    Args:
        table (str): reference to a specific table in snowflake database
        file_format (str): reference to a specific file format
        schema (str): name of schema (will overwrite schema defined in connection)
        load_type (LoadType) Default=LoadType.APPEND. See LoadType Enum for list of valid options.
        snowflake_conn_id (str): reference to a specific snowflake connection
        stage (str): reference to a specific snowflake stage. If the stage's schema is not the same as the
            table one, it must be specified
        s3_loc (str): cloud storage location specified to limit the set of files to load
        s3_keys (list): reference to a list of S3 keys
        pattern (str): regex pattern to match on a list of S3 keys, will be overridden by s3_keys if that's provided
        in_copy_transform_sql (str): snowflake's in copy transform sql, stage and s3_loc not needed if this is used
        copy_columns (list[str]): List of column names to include in copy statement. Leave blank to use all columns.
        merge_update_columns (list[str]): List of column names to be updated when LoadType == MERGE. Leave blank to
            update all columns.
        merge_insert_columns (list[str]): List of column names to be inserted into when LoadType == MERGE. Leave blank
            to update all columns.
        merge_match_where_expr (str): An optional equality expression to include in MERGE statement
            on WHEN MATCHED {expr} THEN UPDATE. A common expr may be (staging.timestamp > tgt.timestamp)
            to ensure older runs with earlier update timestamps will not overwrite newer updated data.
        use_variant_staging (bool): whether to use a staging table with a single variant typed column
        alt_staging_table_schema (dict): Dict where column names are the keys and datatypes are the values.
            Useful when target table's schema in SF is different from the schema of data being copied into SF stage.
        warehouse (str): name of warehouse (will overwrite any warehouse defined in the connection's extra JSON)
        session_parameters (dict): You can set session-level parameters at the time you connect to Snowflake
        transform_sql (str): Optional[str] Default="SELECT * FROM staging".
        pre_exec_sql (list) Optional: A list of sql statements to run before executing.
        copy_options (str) Optional[str]: Additional options to include in the COPY statement.
        deferrable (bool): Default True. Specifies whether operator operates in deferrable mode.
        poll_interval (int): Time in seconds to wait between polling Snowflake for query status.
    """

    template_fields = ("s3_loc", "pre_exec_sql", "transform_sql", "in_copy_transform_sql")
    template_ext = (".sql",)
    ui_color = "#2d75b6"

    class LoadType(Enum):
        TRUNCATE = "TRUNCATE"
        KEEP_EXISTING = "KEEP_EXISTING"
        APPEND = "APPEND"
        MERGE = "MERGE"

    def __init__(
            self,
            *,
            table: str,
            file_format: str,
            schema: str,
            load_type: LoadType,
            snowflake_conn_id: str = SF_CONN_ID,
            stage: str = STAGE_SF,
            s3_loc: str | None = None,
            s3_keys: list[str] | None = None,
            pattern: str | None = None,
            in_copy_transform_sql: str | None = None,
            copy_columns: list[str] | None = None,
            merge_update_columns: list[str] | None = None,
            merge_insert_columns: list[str] | None = None,
            merge_match_where_expr: str | None = None,
            use_variant_staging: bool = False,
            alt_staging_table_schema: dict[str] | None = None,
            warehouse: str | None = None,
            session_parameters: dict | None = None,
            transform_sql: str = "SELECT * FROM staging",
            pre_exec_sql: list[str] | None = None,
            copy_options: str | None = None,
            deferrable: bool = True,
            poll_interval: int = 30,
            **kwargs
    ) -> None:
        if isinstance(load_type, S3ToSnowflakeDeferrableOperator.LoadType) is False:
            raise ValueError("load_type arg must be a enum from S3ToSnowflakeDeferrableOperator.LoadType")

        task_id = kwargs['task_id'].replace(".", "_").replace("-", "_")
        self.staging_table = f"{schema}.{table}_{task_id}_staging_data_interval_start"

        # run this cleanup function to drop staging table on_success, on_retry or on_failure of this task.
        def cleanup_fn(context):
            dis = context["data_interval_start"].strftime('%Y%m%d%H%M%S')
            tbl = self.staging_table.replace("data_interval_start", dis)
            self.get_snowflake_hook().run(f"DROP TABLE IF EXISTS {tbl}", autocommit=True)

        super().__init__(sql="",
                         snowflake_conn_id=snowflake_conn_id,
                         warehouse=warehouse,
                         poll_interval=poll_interval,
                         on_failure_callback=self.get_callback("on_failure_callback", cleanup_fn, kwargs),
                         on_success_callback=self.get_callback("on_success_callback", cleanup_fn, kwargs),
                         on_retry_callback=self.get_callback("on_retry_callback", cleanup_fn, kwargs),
                         **kwargs)
        self.s3_keys = s3_keys
        self.table = table
        self.warehouse = warehouse
        self.stage = stage
        self.s3_loc = s3_loc
        self.pattern = pattern
        self.file_format = file_format
        self.schema = schema
        self.target_table = f"{schema}.{table}"
        self.copy_columns = copy_columns
        self.merge_update_columns = merge_update_columns
        self.merge_insert_columns = merge_insert_columns
        self.merge_match_where_expr = merge_match_where_expr
        self.use_variant_staging = use_variant_staging
        self.alt_staging_table_schema = alt_staging_table_schema
        self.snowflake_conn_id = snowflake_conn_id
        self.session_parameters = session_parameters
        self.in_copy_transform_sql = in_copy_transform_sql
        self.transform_sql = transform_sql
        self.load_type = load_type
        self.pre_exec_sql = pre_exec_sql or []
        self.copy_options = copy_options
        self.deferrable = deferrable
        self.columns = None  # populated below if needed
        self.primary_keys = None  # will be populated if needed

    def execute(self, context: any) -> None:
        """This is the main method to implement when creating an operator."""
        snowflake_hook = self.get_snowflake_hook()
        self.log.info("Retrieving sql queries to execute for this run...")
        initial_sql, deferrable_sql = self.get_queries(context["data_interval_start"])

        self.log.info("Running initial_sql queries...")
        snowflake_hook.run(initial_sql, autocommit=True)
        if deferrable_sql:
            self.log.info("Running deferred query...")
            self.split_statements = False
            self.sql = deferrable_sql
            super().execute(context)  # Use execute method of SnowflakeAsyncDeferredOperator to run deferred

    def get_snowflake_hook(self):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        return SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            schema=self.schema,
            session_parameters=self.session_parameters,
        )

    def get_callback(self, callback_name: str, cleanup_fn, kwargs):
        """Returns list of combined callback functions for given callback_name."""
        callback = kwargs.pop(callback_name, kwargs["default_args"].get(callback_name, []))
        return [cleanup_fn] + (callback if isinstance(callback, list) else [callback])

    def get_queries(self, data_interval_start) -> (list[str], str):
        """Returns list of sql to be executed normally and final sql to be executed in deferred mode."""
        initial_sql = []
        deferrable_sql = None
        staging_table = self.staging_table.replace("data_interval_start", data_interval_start.strftime('%Y%m%d%H%M%S'))
        if staging_table not in self.transform_sql:
            if "analytics_staging" in self.transform_sql:
                self.transform_sql = (self.transform_sql.replace("analytics_staging.", "as_schema.")
                                      .replace("staging", f"{staging_table}")
                                      .replace("as_schema.", "analytics_staging."))
            else:
                self.transform_sql = self.transform_sql.replace("staging", f"{staging_table}")
        # This next sql must be done after __init__ otherwise the Jinja templates will not get resolved
        insert_sql = f"INSERT INTO {self.target_table} {self.transform_sql};"
        initial_sql.extend(self.pre_exec_sql)  # sql that needs to be executed before the load process
        initial_sql.append(self.get_staging_cts(staging_table))  # create staging table to load data
        initial_sql.append(self.get_copy_stmt(staging_table))  # copy command loads the data into staging table

        if self.load_type == self.LoadType.TRUNCATE:
            initial_sql.append(f"TRUNCATE TABLE {self.target_table};")  # TRUNCATE target table
            main_sql = insert_sql  # insert all records from staging table
        elif self.load_type == self.LoadType.KEEP_EXISTING:
            main_sql = self.get_keep_existing_stmt()  # insert just the non overlapped records from the staging table
        elif self.load_type == self.LoadType.APPEND:
            main_sql = insert_sql  # insert all the records from the staging table
        elif self.load_type == self.LoadType.MERGE:
            main_sql = self.get_merge_stmt()  # use MERGE stmt to update existing records and insert new records

        deferrable_sql = main_sql if self.deferrable else None
        initial_sql.extend([] if self.deferrable else [main_sql])
        return initial_sql, deferrable_sql

    def get_staging_cts(self, table_name: str):
        """Returns CTS to create staging table to temporarily load data into before inserting it into target table."""
        sql = f"CREATE TRANSIENT TABLE {table_name}"
        if self.use_variant_staging:
            sql += " (data VARIANT);"
        elif self.alt_staging_table_schema:
            schema = [f"{col} {dtype}" for col, dtype in self.alt_staging_table_schema.items()]
            sql += f" ({', '.join(schema)});"
        else:
            sql += f" LIKE {self.target_table};"
        return sql

    def get_copy_stmt(self, table_name: str):
        """Returns a Snowflake copy statement to load the data into given stage table_name."""
        files_identifier = ""
        if self.pattern:
            files_identifier = f"pattern = '{self.pattern}'"
        if self.s3_keys:
            files_identifier = f"""files=({", ".join(f"'{key}'" for key in self.s3_keys)})"""

        src = f"@{self.stage}/{self.s3_loc or ''}" if self.in_copy_transform_sql is None \
            else f"({self.in_copy_transform_sql})"

        sql = f"COPY INTO {table_name}\n"
        if self.copy_columns:
            sql += f"({','.join(self.copy_columns)})\n"
        sql += f"FROM {src}\n"
        sql += f"{files_identifier}\nfile_format={self.file_format} {self.copy_options or ''}"
        return sql

    def get_join_clause(self, target_table_name: str):
        """Returns a string with the join conditions based on primary keys"""
        filters = []
        for key, dtype in self.get_primary_keys().items():
            if dtype == "TEXT":
                null_val = "'l;kjsf2oiru'"
            elif "TIMESTAMP" in dtype:
                null_val = "'1900-01-02'::TIMESTAMP"
            elif dtype == "DATE":
                null_val = "'1900-01-02'::DATE"
            else:
                null_val = 99999999
            filters.append(f'NVL({target_table_name}.{key}, {null_val}) = NVL(staging.{key}, {null_val})')
        return " AND ".join(filters)

    def get_columns(self):
        """Captures and returns list of columns for target table"""
        from de_utils.tndbo import get_dbo
        if not self.columns:
            cols = get_dbo(SF_CONN_ID).get_column_defs(self.target_table)
            self.columns = list(cols.keys())
            self.primary_keys = {col: d["data_type"]["type"] for col, d in cols.items() if d["is_pkey"]}
        return self.columns

    def get_primary_keys(self):
        if not self.primary_keys:
            self.get_columns()
        if not self.primary_keys:
            msg = (f"Primary keys are required for load_type {self.load_type.value}. "
                   f"You must add primary keys to the table DLL.")
            raise RuntimeError(msg)
        return self.primary_keys

    def get_keep_existing_stmt(self):
        """Returns sql to insert just the non overlapped records from the staging table"""
        return f"""
            INSERT INTO {self.target_table}
            SELECT staging.*
            FROM ({self.transform_sql}) staging
            LEFT JOIN {self.target_table} ON {self.get_join_clause(self.target_table)}
            WHERE {self.target_table}.{list(self.get_primary_keys().keys())[0]} IS NULL;
        """

    def get_merge_stmt(self):
        """Returns a merge statement to merge updates in stage table to existing table"""
        merge_cols = self.merge_update_columns if self.merge_update_columns else self.get_columns()
        merge_cols = [f"tgt.{col} = staging.{col}" for col in merge_cols]
        insert_columns = self.merge_insert_columns if self.merge_insert_columns else self.get_columns()
        when_matched_expr = "" if not self.merge_match_where_expr else f" AND {self.merge_match_where_expr}"
        insert_cols = [f"staging.{col}" for col in insert_columns]

        return f"""
            MERGE INTO {self.target_table} tgt
            USING ({self.transform_sql}) staging
                ON {self.get_join_clause("tgt")}
            WHEN MATCHED{when_matched_expr} THEN UPDATE SET {', '.join(merge_cols)}
            WHEN NOT MATCHED THEN INSERT ({', '.join(insert_columns)})
                VALUES ({', '.join(insert_cols)})
        """
