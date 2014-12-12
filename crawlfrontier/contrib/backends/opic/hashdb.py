"""
Interface definition for web page hash database
"""
from abc import ABCMeta, abstractmethod
import sqlite


class HashDBInterface(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def clear(self):
        """Delete all contents"""
        pass

    @abstractmethod
    def add(self, page_id, page_hash):
        pass

    @abstractmethod
    def set(self, page_id, page_hash):
        pass

    @abstractmethod
    def get(self, page_id):
        pass

    @abstractmethod
    def delete(self, page_id):
        pass


class SQLite(sqlite.Connection, HashDBInterface):
    def __init__(self, db=None):
        super(SQLite, self).__init__(db)

        self._cursor.executescript(
            """
            CREATE TABLE IF NOT EXISTS hashes (
               page_id   TEXT NOT NULL UNIQUE,
               page_hash TEXT NOT NULL
            );
            """
        )

    def clear(self):
        self._cursor.executescript(
            """
            DELETE FROM hashes;
            """
        )

    def add(self, page_id, page_hash):
        self._cursor.execute(
            """
            INSERT OR IGNORE INTO hashes VALUES (?,?)
            """,
            (page_id, page_hash)
        )

    def set(self, page_id, page_hash):
        self._cursor.execute(
            """
            UPDATE OR IGNORE hashes
            SET page_hash=?
            WHERE page_id=?
            """,
            (page_hash, page_id)
        )

    def get(self, page_id):
        self._cursor.execute(
            """
            SELECT page_hash FROM hashes WHERE page_id=?
            """,
            (page_id,)
        )
        result = self._cursor.fetchone()
        if result:
            result = result[0]

        return result

    def delete(self, page_id):
        self._cursor.execute(
            """
            DELETE FROM hashes WHERE page_id=?
            """,
            (page_id,)
        )
