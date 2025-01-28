"""Module contains core functions for tndbo library."""


def get_dbo(
        airflow_conn_id: str,
        spark_session=None
):
    """Returns a database object ("SnowflakeDatabaseObject" | "MySQLDatabaseObject") based on given airflow_conn_id.

    Args:
        airflow_conn_id (str): airflow connection id
        spark_session (SparkSession): spark session

    Raises:
        NotImplementedError - if there is no matching database object for given airflow_conn_id
    """
    from de_utils.tndbo.credential import Credential  # delay import to keep DAG top level parsing efficient
    cred = Credential.get_from_sm(conn_id=airflow_conn_id)
    conn_type = cred.conn_type.lower()

    if conn_type == "mysql":
        from de_utils.tndbo.dbo import MySQLDatabaseObject
        return MySQLDatabaseObject(credential=cred, spark_session=spark_session)
    elif conn_type == "snowflake":
        from de_utils.tndbo.dbo import SnowflakeDatabaseObject
        return SnowflakeDatabaseObject(credential=cred, spark_session=spark_session)
    else:
        raise NotImplementedError(f"No database object implemented for {conn_type}")


def list_airflow_conn_ids() -> list:
    """ Returns a lists all relational database connection ids available from AWS Secrets Manager."""
    # This function is slow currently as it retrieves secrets one by one
    # If need this to be faster, can run for loop in parallel
    from de_utils.aws import list_secrets, get_secret  # delay import to keep DAG top level parsing efficient
    from botocore.exceptions import ClientError  # delay import to keep DAG top level parsing efficient
    secrets = list_secrets(filters=[{"Key": "name", "Values": ['airflow/connections/']}])
    conn_ids = []
    for secret in secrets:
        try:
            sec = get_secret(secret["Name"])
            if sec.startswith(("snowflake", "mysql")):
                conn_ids.append(secret["Name"].replace("airflow/connections/", ""))
        except ClientError as e:
            if 'ResourceNotFoundException' not in str(e):
                raise  # else pass as this means we have not added the secret value yet.

    return conn_ids
