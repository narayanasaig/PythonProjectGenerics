# # DBConnections/postgre_auth_pool.py

import time
import boto3
import psycopg2
from psycopg2 import pool


class PostgreAuthPool(pool.SimpleConnectionPool):
    """
    A SimpleConnectionPool subclass that supports AWS IAM token-based authentication
    for PostgreSQL/Aurora PostgreSQL.
    """
    def __init__(
        self,
        minconn,
        maxconn,
        host,
        port,
        user,
        database,
        region,
        sslrootcert=None,
        sslmode="require",
        token_ttl=900,  # default 15 minutes
        **kwargs
    ):
        """
        :param minconn: minimum connections in pool
        :param maxconn: maximum connections in pool
        :param host: DB endpoint
        :param port: DB port
        :param user: DB user (the same user for generating the IAM token)
        :param database: DB name
        :param region: AWS region for token generation
        :param sslrootcert: optional SSL root cert file
        :param sslmode: SSL mode
        :param token_ttl: token time-to-live in seconds
        :param kwargs: additional psycopg2 connection arguments
        """
        self._host = host
        self._port = port
        self._user = user
        self._database = database
        self._region = region
        self._sslrootcert = sslrootcert
        self._sslmode = sslmode
        self._token_ttl = token_ttl

        self._client = boto3.client("rds")
        self._connection_created_at = {}

        super().__init__(minconn, maxconn, **kwargs)

    def _connect(self, key=None):
        """
        Create a brand-new physical connection to the database with a fresh IAM auth token.
        """
        # Generate IAM token
        token = self._client.generate_db_auth_token(
            DBHostname=self._host,
            Port=self._port,
            DBUsername=self._user,
            Region=self._region,
        )

        # Build new connection
        conn = psycopg2.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            password=token,
            dbname=self._database,
            sslrootcert=self._sslrootcert,
            sslmode=self._sslmode,
            **self.conninfo
        )
        self._connection_created_at[conn] = time.time()
        return conn

    def getconn(self, key=None):
        """
        Retrieve a connection from the pool. If it's older than token_ttl,
        close & recreate it with a fresh token.
        """
        conn = super().getconn(key=key)
        now = time.time()

        created_at = self._connection_created_at.get(conn, now)
        if (now - created_at) > self._token_ttl:
            # Token likely expired. Close & recreate
            try:
                conn.close()
            except Exception:
                pass
            if conn in self._connection_created_at:
                del self._connection_created_at[conn]

            # Create a new connection with a fresh token
            conn = self._connect()

        return conn

    def putconn(self, conn, key=None, close=False):
        """
        Return connection to the pool.
        """
        if close:
            try:
                conn.close()
            except Exception:
                pass
            if conn in self._connection_created_at:
                del self._connection_created_at[conn]

        super().putconn(conn, key=key, close=close)
