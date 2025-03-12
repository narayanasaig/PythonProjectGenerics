# DBConnections/oracle_kerberos_pool.py

import cx_Oracle
import time
from logging_config import get_logger

logger = get_logger(__name__)

class OracleKerberosPool(cx_Oracle.SessionPool):
    """
    A Kerberos-aware SessionPool class for Oracle that mirrors
    the method structure of postgre_auth_pool.py.

    Key differences from Postgres + AWS IAM:
      - We do NOT fetch any token in _connect(), as Kerberos is managed
        by the OS and sqlnet.ora.
      - 'external_auth=True' triggers OS-based Kerberos authentication.
      - We optionally track creation times for each session if needed.

    Usage:
      1) Ensure your environment has a valid Kerberos ticket (kinit, etc.).
      2) Make sure sqlnet.ora has 'SQLNET.AUTHENTICATION_SERVICES=(KERBEROS5)'.
      3) Instantiate this class with the desired pool size, DSN, etc.:
           pool = OracleKerberosPool(
               minconn=1, maxconn=5, dsn="host:1521/orclpdb",
               external_auth=True
           )
      4) Acquire sessions with pool.getconn(), release with pool.putconn().
    """

    def __init__(
        self,
        minconn: int,
        maxconn: int,
        dsn: str,
        user: str = "",
        password: str = "",
        external_auth: bool = True,
        increment: int = 1,
        getmode: int = cx_Oracle.SPOOL_ATTRVAL_NOWAIT,
        threaded: bool = True,
        **kwargs
    ):
        """
        :param minconn: Minimum number of sessions in the pool
        :param maxconn: Maximum number of sessions in the pool
        :param dsn: Oracle DSN (e.g. "hostname:1521/service_name")
        :param user: Typically empty if using Kerberos external auth
        :param password: Typically empty if using Kerberos external auth
        :param external_auth: Must be True to rely on OS-based Kerberos
        :param increment: Pool increment
        :param getmode: Non-blocking or blocking session acquire
        :param threaded: Whether cx_Oracle pool is thread-safe
        :param kwargs: Additional cx_Oracle.SessionPool arguments
        """
        # Track creation times for each session (if needed)
        self._connection_created_at = {}

        # Initialize parent SessionPool
        super().__init__(
            user=user or "",
            password=password or "",
            dsn=dsn,
            min=minconn,
            max=maxconn,
            increment=increment,
            external_auth=external_auth,
            getmode=getmode,
            threaded=threaded,
            **kwargs
        )

        logger.info(
            f"[OracleKerberosPool] Created SessionPool with DSN='{dsn}', "
            f"min={minconn}, max={maxconn}, increment={increment}, external_auth={external_auth}"
        )

    def _connect(self, key=None):
        """
        In cx_Oracle, physical sessions are created by the SessionPool itself.
        We keep this method for structural parity with postgre_auth_pool.py,
        but there's no token or password to fetch for Kerberos.

        If you wanted to forcibly create a new session outside the pool,
        you could do so, but typically that's handled by super().__init__().
        """
        pass

    def getconn(self, key=None):
        """
        Acquire a session from the pool, storing creation time if needed.
        Mirrors postgre_auth_pool.getconn().
        """
        session = self.acquire()  # built-in SessionPool method
        self._connection_created_at[session] = time.time()
        logger.debug("[OracleKerberosPool] Acquired a new session.")
        return session

    def putconn(self, session, key=None, close=False):
        """
        Return a session to the pool. If close=True, physically close it.
        Otherwise, just release it for reuse.

        :param session: The cx_Oracle.Connection to release
        :param close: If True, permanently close it.
        """
        if close:
            logger.debug("[OracleKerberosPool] Closing session explicitly.")
            try:
                session.close()
            except Exception as e:
                logger.warning(f"[OracleKerberosPool] Error closing session: {e}")
            self._connection_created_at.pop(session, None)
        else:
            logger.debug("[OracleKerberosPool] Releasing session to pool.")
            self.release(session)

    def closeall(self):
        """
        Close the entire pool and all associated sessions.
        """
        logger.info("[OracleKerberosPool] Closing entire pool.")
        super().close()
