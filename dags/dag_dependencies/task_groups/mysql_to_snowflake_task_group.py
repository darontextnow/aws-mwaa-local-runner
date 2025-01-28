from airflow.decorators import task_group
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from dag_dependencies.sensors.sql_deferrable_sensor import SqlDeferrableSensor
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from de_utils.constants import SF_CONN_ID, STAGE_SF, SF_BUCKET, SF_WH_SMALL
from dag_dependencies.constants import AIRFLOW_STAGING_KEY_PFX


@task_group(group_id="mysql_to_snowflake_task_group")
def mysql_to_snowflake_task_group(
        mysql_conn_id: str | dict,
        sql: str,
        target_table: str,
        load_type: S3ToSnowflakeDeferrableOperator.LoadType,
        deferrable_load: bool = True,
        load_poll_interval: int = 30,
        snowflake_conn_id: str = SF_CONN_ID,
        aws_conn_id: str | None = None,
        stage: str = STAGE_SF,
        target_schema: str = "public",
        merge_update_columns: list[str] | None = None,
        merge_insert_columns: list[str] | None = None,
        merge_match_where_expr: str | None = None,
        alt_staging_table_schema: dict[str, str] | None = None,
        sensor_sql: str | None = None,
        sensor_retries: int = 3,
        sensor_timeout: int = 3600,
        sensor_poll_interval: float = 120,
        params: dict | None = None,
        collect_pd_kwargs: dict | None = None,
        transform_sql: str = "SELECT * FROM staging",
        pre_exec_sql: list | None = None,
        sf_warehouse: str | None = SF_WH_SMALL
):
    """
    A Group of tasks used in many dags to retrieve data from MySQL and load it into s3 datalake and then into Snowflake.
    Note: No longer supporting rarely used clean functionality here. Do transforms/cleaning via SQL, not via Python.
    Note: No longer supporting rarely used bookmark_function in this new version. Can do it manually if needed.

    Args:
        mysql_conn_id (str | dict): Airflow connection id or dict of ids for MySQL server to source data from.
        sql (str): the sql query to be executed. If you want to execute a file, place the absolute path of it,
            ending with .sql extension. (templated)
        target_table (str): The Snowflake table_name to load data into.
        load_type (S3ToSnowflakeDeferrableOperator.LoadType)
        deferrable_load (bool): Set to True to make the load from s3 to Snowflake deferred.
        load_poll_interval (int): How many seconds to wait between polling snowflake for load query status.
        snowflake_conn_id (str): reference to a specific snowflake connection.
        aws_conn_id (str) Optional: AWS connection id in Airflow. Not required currently.
        stage (str): reference to a specific snowflake stage. If the stage's schema is not the same as the
            table one, it must be specified
        target_schema (str): Name of schema (will overwrite schema defined in connection).
        merge_update_columns (list[str]): List of column names to be updated when LoadType == MERGE.
        merge_insert_columns (list[str]): List of column names to be inserted into when LoadType == MERGE.
        merge_match_where_expr (str): An optional equality expression to include in MERGE statement
            on WHEN MATCHED {expr} THEN UPDATE. A common expr may be (staging.timestamp > tgt.timestamp)
            to ensure older runs with earlier update timestamps will not overwrite newer updated data.
        alt_staging_table_schema (dict): Dict where column names are the keys and datatypes are the values.
            Useful when target table's schema in SF is different from the schema of data being copied into SF stage.
        sensor_sql (str) Optional: Query to use for sensor task to determine whether to run collect task or not.
        sensor_retries (int): the number of retries that should be performed before failing the task.
        sensor_timeout (int):  Time, in seconds before the task times out and fails.
        sensor_poll_interval (float): Polling period in seconds to run sql and check for status.
        params (dict) Optional: Dictionary of parameters to pass to query execution for all queries in task group.
        collect_pd_kwargs (dict) Optional: Dictionary of kwargs to be passed during collect task to pandas
            when creating the csv of data pulled from MySQL. Default is {"index": False}
        transform_sql (str) Optional: SQL statement to use to transform results before writing to Snowflake target.
        pre_exec_sql (list) Optional: A list of sql statements to run before executing.
        sf_warehouse (str): name of warehouse (will overwrite any warehouse defined in the connection's extra JSON)

    Returns: (collect, load) as a tuple.
        collect is the collect data from mysql task and load is the load task to Snowflake.
    """
    params = params or {}
    params["target_table"] = target_table
    s3_dir = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ ts }}}}/{target_table}/"

    load = S3ToSnowflakeDeferrableOperator(
        task_id=f"load_{target_table.replace('.', '_')}",
        snowflake_conn_id=snowflake_conn_id,
        table=target_table,
        schema=target_schema,
        s3_loc=s3_dir,
        stage=stage,
        load_type=load_type,
        merge_update_columns=merge_update_columns,
        merge_insert_columns=merge_insert_columns,
        merge_match_where_expr=merge_match_where_expr,
        alt_staging_table_schema=alt_staging_table_schema,
        pre_exec_sql=pre_exec_sql,
        transform_sql=transform_sql,
        file_format=("(TYPE = CSV TIME_FORMAT = AUTO DATE_FORMAT = AUTO "
                     "FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)"),
        warehouse=sf_warehouse,
        params=params,
        deferrable=deferrable_load,
        poll_interval=load_poll_interval
    )

    if not isinstance(mysql_conn_id, dict):
        mysql_conn_id = {"data": mysql_conn_id}

    collect = None
    all_collects = []
    for shard_id, shard_conn_id in mysql_conn_id.items():
        shard_params = params.copy()
        # DO NOT delete this, as `shard_id` can be used in incoming queries
        shard_params["shard_id"] = shard_id

        if shard_id == "data":
            task_id = f"unload_{target_table.replace('.', '_')}"
            sensor_task_id = f"wait_{target_table.replace('.', '_')}"
        else:
            task_id = f"unload_{target_table.replace('.', '_')}_{shard_id}"
            sensor_task_id = f"wait_for_{target_table.replace('.', '_')}_{shard_id}"

        s3_key = f"{s3_dir}shard_{shard_id}.csv"

        # collect query results from MySQL query run
        collect = SqlToS3Operator(
            task_id=task_id,
            sql_conn_id=shard_conn_id,
            query=sql,
            aws_conn_id=aws_conn_id,
            s3_bucket=SF_BUCKET,
            s3_key=s3_key,
            file_format="csv",
            replace=True,
            pd_kwargs=collect_pd_kwargs or {"header": False, "index": False},
            params=shard_params  # will get passed down to BaseOperator as templated params
        )

        all_collects.append(collect)
        load.set_upstream(collect)

        if sensor_sql:
            sql_sensor = SqlDeferrableSensor(
                task_id=sensor_task_id,
                conn_id=shard_conn_id,
                sql=sensor_sql,
                retries=sensor_retries,
                timeout=sensor_timeout,
                poll_interval=sensor_poll_interval,
                params=shard_params
            )
            collect.set_upstream(sql_sensor)
    return all_collects, load  # for setting up downstream operators
