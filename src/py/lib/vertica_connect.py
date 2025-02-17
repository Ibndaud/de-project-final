import vertica_python
import logging

from contextlib import contextmanager
from typing import Generator
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)


class VerticaConnect:
    def __init__(self, host: str, port: str, database: str, user: str, password: str, autocommit: bool = False) -> None:
        self.host = host
        self.port = int(port)
        self.database = database
        self.user = user
        self.password = password
        self.autocommit = autocommit

    def conn_info(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "autocommit": self.autocommit
        }

    def client(self):
        return vertica_python.connect(**self.conn_info())

    @contextmanager
    def connection(self) -> Generator[vertica_python.Connection, None, None]:
        conn = self.client()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.exception("Vertica connection error: %s", e)
            raise e
        finally:
            conn.close()


class VerticaConnectionBuilder:

    @staticmethod
    def vertica_conn(conn_id: str) -> VerticaConnect:
        conn = BaseHook.get_connection(conn_id)
        extra_params = conn.extra_dejson
        autocommit = extra_params.get('autocommit', 'false').lower() == 'true'

        vertica = VerticaConnect(str(conn.host),
                                 str(conn.port),
                                 str(conn.schema),
                                 str(conn.login),
                                 str(conn.password),
                                 autocommit)
        return vertica
