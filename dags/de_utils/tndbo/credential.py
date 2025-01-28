from airflow.models.connection import Connection


class Credential:
    """Object containing credentials used to access a relational database.
    
    Args:
        conn_id (str): airflow connection id
        conn_type (str): type of connection, i.e. mysql, snowflake, etc.
        host (str): host of the relational database
        schema (str): schema or logical database in the relational database
        login (str): username for accessing the relational database
        password (str): password for accessing the relational database
        port (int): port of the relational database
        extra (str): extra connection info for the relational database
        is_encrypted (bool int): whether password is encrypted
        is_extra_encrypted (bool int): whether extra info is encrypted
        uri (str): The connection string as a uri
    """
    protocols = {
        "mysql": "mysql+pymysql",
    }

    def __init__(
            self,
            conn_id: str,
            conn_type: str,
            host: str | None = None,
            schema: str | None = None,
            login: str | None = None,
            password: str | None = None,
            port: int | None = None,
            extra: str | None = None,
            is_encrypted: int | None = 0,
            is_extra_encrypted: int | None = 0,
            uri: str | None = None
    ):
        
        self.conn_id = conn_id
        self.host = host
        self.schema = schema
        self.login = login
        self.port = port
        self._conn_type = conn_type
        self._password = password
        self._is_encrypted = is_encrypted
        self._extra = extra
        self._is_extra_encrypted = is_extra_encrypted
        self.uri = uri

    @property
    def conn_type(self) -> str:
        """ Connection type used to identify the driver for the underlying database """
        return self._conn_type.lower()

    @property
    def password(self) -> str:
        """ Password for accessing underlying database, decrypted as needed """
        if self._is_encrypted:
            return self.decrypt(self._password)
        else:
            return self._password

    @property
    def extra(self) -> dict:
        """ Extra connection info for accessing underlying database, decrypted as needed """
        import json  # delay import to keep top level DAG parsing efficient
        extra = self._extra
        if self._is_extra_encrypted:
            extra = self.decrypt(self._extra)
        if extra:
            return json.loads(extra)

    @staticmethod
    def decrypt(value: str) -> str:
        """ Decrypts fernet encrypted info """
        from .utils import get_fernet  # delay this import so Airflow has less to load at top level DAG
        fernet = get_fernet()
        return fernet.decrypt(value.encode("utf-8")).decode("utf-8")

    @staticmethod
    def get_airflow_connection(conn_id: str) -> Connection:
        """Returns airflow Connection object instance."""
        from de_utils.aws import get_secret  # delay this import so Airflow has less to load at top level DAG
        uri = get_secret(f"airflow/connections/{conn_id}")
        conn = Connection(uri=uri)
        conn.conn_id = conn_id
        conn.uri = uri
        return conn

    @classmethod
    def get_from_sm(cls, conn_id: str):
        """Returns a Credential object based on the provided airflow conn_id"""
        conn = Credential.get_airflow_connection(conn_id)
        return  cls(
            conn_id=conn.conn_id,
            conn_type=conn.conn_type,
            host=None if not conn.host else conn.host,  # convert '' to None
            schema=conn.schema,
            login=conn.login,
            password=conn.password,
            port=conn.port,
            extra=conn.extra,
            is_encrypted=False,
            is_extra_encrypted=False,
            uri=conn.uri
        )

    def get_uri(self) -> str:
        """Returns connection uri as string."""
        if self.conn_type == "snowflake":
            return (f"snowflake://{self.login}:{self.password}@/{self.schema}?account={self.extra['account']}"
                    f"&database={self.extra['database']}&region=us-east-1&warehouse={self.extra['warehouse']}")
        elif self.uri:
            return self.uri
        else:
            return Credential.get_airflow_connection(self.conn_id).get_uri()
