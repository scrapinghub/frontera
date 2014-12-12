"""
Store for each link a pair of hub/authority weights
"""
from abc import ABCMeta, abstractmethod

import sqlite


class LinksDBInterface(object):
    """Interface definition for every LinksDB database"""
    __metaclass__ = ABCMeta

    @abstractmethod
    def clear(self):
        """Delete all contents"""
        pass

    @abstractmethod
    def close(self):
        """Close connection and commit changes """
        pass

    @abstractmethod
    def add(self, start, end, h_weight, a_weight):
        """Associate start->end edge with (h_weight, a_weight) """
        pass

    @abstractmethod
    def set(self, start, end, h_weight, a_weight):
        """Changes the weights associated with start->end"""
        pass

    @abstractmethod
    def delete(self, start, end):
        """Delete association"""
        pass

    @abstractmethod
    def get(self, start, end):
        """ Return a tuple with the weights """
        pass


class SQLite(sqlite.Connection, LinksDBInterface):
    """SQLite implementation of LinksDBInterface"""
    def __init__(self, db=None):
        super(SQLite, self).__init__(db)

        self._cursor.executescript(
            """
            CREATE TABLE IF NOT EXISTS links (
               start    TEXT NOT NULL,
               end      TEXT NOT NULL,
               h_weight REAL,
               a_weight REAL,

               PRIMARY KEY (start, end)
            );
            """
        )

    def clear(self):
        self._cursor.executescript(
            """
            DELETE FROM links
            """
        )

    def add(self, start, end, h_weight, a_weight):
        self._cursor.execute(
            """
            INSERT OR IGNORE INTO links VALUES (?, ?, ?, ?)
            """,
            (start, end, h_weight, a_weight)
        )

    def delete(self, start, end):
        self._cursor.execute(
            """
            DELETE FROM links WHERE start=? AND end=?
            """,
            (start, end)
        )

    def set(self, start, end, h_weight, a_weight):
        self._cursor.execute(
            """
            UPDATE OR IGNORE links
            SET h_weight=?, a_weight=?
            WHERE start=? and end=?
            """,
            (h_weight, a_weight, start, end)
        )

    def get(self, start, end):
        return self._cursor.execute(
            """
            SELECT h_weight, a_weight FROM links WHERE start=? and end=?
            """,
            (start, end)
        ).fetchone()
