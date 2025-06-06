import pytest

pytest.importorskip("sqlalchemy.engine")
pytest.importorskip("pymysql")

import contextlib
from pathlib import Path

import pymysql
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from tests import backends
from tests.test_revisiting_backend import RevisitingBackendTest


# ----------------------------------------------------
# SQAlchemy base classes
# ----------------------------------------------------
class SQLAlchemyFIFO(backends.FIFOBackendTest):
    backend_class = "frontera.contrib.backends.sqlalchemy.FIFO"


class SQLAlchemyLIFO(backends.LIFOBackendTest):
    backend_class = "frontera.contrib.backends.sqlalchemy.LIFO"


class SQLAlchemyDFS(backends.DFSBackendTest):
    backend_class = "frontera.contrib.backends.sqlalchemy.DFS"


class SQLAlchemyBFS(backends.BFSBackendTest):
    backend_class = "frontera.contrib.backends.sqlalchemy.BFS"


class SQLAlchemyRevisiting(RevisitingBackendTest):
    backend_class = "frontera.contrib.backends.sqlalchemy.revisiting.Backend"


# ----------------------------------------------------
# SQLite Memory
# ----------------------------------------------------
class SQLiteMemory(backends.BackendTest):
    def get_settings(self):
        settings = super().get_settings()
        settings.SQLALCHEMYBACKEND_ENGINE = "sqlite:///:memory:"
        return settings


class TestSQLiteMemoryFIFO(SQLAlchemyFIFO, SQLiteMemory):
    pass


class TestSQLiteMemoryLIFO(SQLAlchemyLIFO, SQLiteMemory):
    pass


class TestSQLiteMemoryDFS(SQLAlchemyDFS, SQLiteMemory):
    pass


class TestSQLiteMemoryBFS(SQLAlchemyBFS, SQLiteMemory):
    pass


class TestSQLiteMemoryRevisiting(SQLAlchemyRevisiting):
    pass


# ----------------------------------------------------
# SQLite File
# ----------------------------------------------------
class SQLiteFile(backends.BackendTest):
    SQLITE_DB_NAME = "backend_test.db"

    def get_settings(self):
        settings = super().get_settings()
        settings.SQLALCHEMYBACKEND_ENGINE = "sqlite:///" + self.SQLITE_DB_NAME
        return settings

    def setup_backend(self, method):
        self._delete_test_db()

    def teardown_backend(self, method):
        self._delete_test_db()

    def _delete_test_db(self):
        with contextlib.suppress(OSError):
            Path(self.SQLITE_DB_NAME).unlink()


class TestSQLiteFileFIFO(SQLAlchemyFIFO, SQLiteFile):
    pass


class TestSQLiteFileLIFO(SQLAlchemyLIFO, SQLiteFile):
    pass


class TestSQLiteFileDFS(SQLAlchemyDFS, SQLiteFile):
    pass


class TestSQLiteFileBFS(SQLAlchemyBFS, SQLiteFile):
    pass


# ----------------------------------------------------
# DB Backend test base
# ----------------------------------------------------
class DBBackendTest:
    DB_DATABASE = "backend_test"
    DB_ENGINE = None
    DB_HOST = None
    DB_USER = None
    DB_PASSWORD = None

    def get_settings(self):
        settings = super().get_settings()
        settings.SQLALCHEMYBACKEND_ENGINE = self.DB_ENGINE
        return settings

    def setup_backend(self, method):
        self._delete_database()
        self._create_database()

    def teardown_backend(self, method):
        self._delete_database()

    def _delete_database(self):
        self._execute_sql(f"DROP DATABASE IF EXISTS {self.DB_DATABASE};")

    def _create_database(self):
        self._execute_sql(f"CREATE DATABASE {self.DB_DATABASE};")

    def _execute_sql(self, sql):
        raise NotImplementedError


# ----------------------------------------------------
# Mysql
# ----------------------------------------------------
class Mysql(DBBackendTest):
    DB_ENGINE = "mysql+pymysql://root:@localhost/backend_test"
    DB_HOST = "localhost"
    DB_USER = "root"
    DB_PASSWORD = ""

    def _execute_sql(self, sql):
        conn = pymysql.connect(
            host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PASSWORD
        )
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        conn.close()


class TestMysqlFIFO(Mysql, SQLAlchemyFIFO):
    pass


class TestMysqlLIFO(Mysql, SQLAlchemyLIFO):
    pass


class TestMysqlDFS(Mysql, SQLAlchemyDFS):
    pass


class TestMysqlBFS(Mysql, SQLAlchemyBFS):
    pass


# ----------------------------------------------------
# Postgres
# ----------------------------------------------------
class Postgres(DBBackendTest):
    DB_ENGINE = "postgres://postgres@localhost/backend_test"
    DB_HOST = "localhost"
    DB_USER = "postgres"
    DB_PASSWORD = ""

    def _execute_sql(self, sql):
        conn = connect(host=self.DB_HOST, user=self.DB_USER, password=self.DB_PASSWORD)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        conn.close()


class TestPostgresFIFO(Postgres, SQLAlchemyFIFO):
    pass


class TestPostgresLIFO(Postgres, SQLAlchemyLIFO):
    pass


class TestPostgresDFS(Postgres, SQLAlchemyDFS):
    pass


class TestPostgresBFS(Postgres, SQLAlchemyBFS):
    pass
